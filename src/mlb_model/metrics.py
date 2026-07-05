"""Build docs/data/metrics.json — the numbers behind the site's Model
Performance section.

Everything is computed from the graded pick_history.json. Every block carries
its own sample size and a `reliable` flag (n >= SMALL_SAMPLE_THRESHOLD) so the
front-end can label small-sample noise as exactly that instead of implying
skill. Run standalone with:

    python -m mlb_model.metrics
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

DOCS_DATA = Path(__file__).resolve().parents[2] / "docs" / "data"
PICK_HISTORY_PATH = DOCS_DATA / "pick_history.json"
METRICS_PATH = DOCS_DATA / "metrics.json"

GRADED = {"win", "loss", "push"}
DECIDED = {"win", "loss"}

# Below this many graded bets a metric is statistically noise. Surfaced in the
# payload so the UI and the JSON agree on one number.
SMALL_SAMPLE_THRESHOLD = 100

# The all-games experiment (every slate game scored, not just picks) uses a
# stricter floor: below this many graded games the model-vs-market Brier race
# is labelled noise. Matches docs/protocol.md.
ALL_GAMES_SAMPLE_THRESHOLD = 200

CALIBRATION_BUCKETS = [(0.0, 0.45), (0.45, 0.50), (0.50, 0.55), (0.55, 0.60), (0.60, 1.01)]

# Home-win probabilities cluster in 0.35-0.65; tails get pooled buckets.
ALL_GAMES_CAL_BUCKETS = [
    (0.0, 0.35),
    (0.35, 0.40),
    (0.40, 0.45),
    (0.45, 0.50),
    (0.50, 0.55),
    (0.55, 0.60),
    (0.60, 0.65),
    (0.65, 1.01),
]

ROLLING_WINDOW_DAYS = 30


def _record(entries: Iterable[dict]) -> dict[str, Any]:
    graded = [e for e in entries if e.get("result") in GRADED]
    wins = sum(1 for e in graded if e["result"] == "win")
    losses = sum(1 for e in graded if e["result"] == "loss")
    pushes = sum(1 for e in graded if e["result"] == "push")
    # pnl is recorded at $100 flat stakes; report in units of 1u = $100.
    profit_units = round(sum(float(e.get("pnl", 0.0)) for e in graded) / 100.0, 4)
    staked_units = float(len(graded))
    return {
        "n": len(graded),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "hit_rate": round(wins / (wins + losses), 4) if (wins + losses) else None,
        "profit_units": profit_units,
        "roi": round(profit_units / staked_units, 4) if staked_units else None,
        "reliable": len(graded) >= SMALL_SAMPLE_THRESHOLD,
    }


def _group_records(entries: list[dict], key: str) -> dict[str, dict]:
    groups: dict[str, list[dict]] = {}
    for entry in entries:
        groups.setdefault(str(entry.get(key) or "unknown"), []).append(entry)
    return {name: _record(members) for name, members in sorted(groups.items())}


def _daily_series(entries: list[dict]) -> list[dict]:
    by_date: dict[str, list[dict]] = {}
    for entry in entries:
        if entry.get("result") in GRADED and entry.get("date"):
            by_date.setdefault(entry["date"], []).append(entry)
    series = []
    cum_units = 0.0
    for day in sorted(by_date):
        day_units = sum(float(e.get("pnl", 0.0)) for e in by_date[day]) / 100.0
        cum_units += day_units
        series.append(
            {
                "date": day,
                "n": len(by_date[day]),
                "profit_units": round(day_units, 4),
                "cum_units": round(cum_units, 4),
            }
        )
    return series


def _clv_block(entries: list[dict]) -> dict[str, Any]:
    with_clv = [
        e for e in entries if e.get("clv") is not None and e.get("date")
    ]
    with_clv.sort(key=lambda e: e["date"])
    cumulative = []
    running_sum = 0.0
    for i, entry in enumerate(with_clv, start=1):
        running_sum += float(entry["clv"])
        cumulative.append(
            {
                "date": entry["date"],
                "clv": round(float(entry["clv"]), 4),
                "cum_avg_clv": round(running_sum / i, 5),
                "n": i,
            }
        )
    values = [float(e["clv"]) for e in with_clv]
    return {
        "n": len(values),
        "mean_clv": round(sum(values) / len(values), 5) if values else None,
        "positive": sum(1 for v in values if v > 0),
        "negative": sum(1 for v in values if v < 0),
        "zero": sum(1 for v in values if v == 0),
        "cumulative": cumulative,
        "reliable": len(values) >= SMALL_SAMPLE_THRESHOLD,
        "proxy_note": (
            "CLV uses the last pre-first-pitch DraftKings snapshot as a closing proxy, "
            "not the true market close. Days with a single snapshot measure 0.0 by construction."
        ),
    }


def _calibration(entries: list[dict]) -> dict[str, Any]:
    decided = [
        e
        for e in entries
        if e.get("result") in DECIDED and e.get("model_probability") is not None
    ]
    rows = []
    for low, high in CALIBRATION_BUCKETS:
        members = [e for e in decided if low <= float(e["model_probability"]) < high]
        if members:
            wins = sum(1 for e in members if e["result"] == "win")
            avg_pred = sum(float(e["model_probability"]) for e in members) / len(members)
            rows.append(
                {
                    "bucket": f"{low:.2f}-{min(high, 1.0):.2f}",
                    "n": len(members),
                    "avg_predicted": round(avg_pred, 4),
                    "win_rate": round(wins / len(members), 4),
                    "gap": round(wins / len(members) - avg_pred, 4),
                }
            )
        else:
            rows.append(
                {"bucket": f"{low:.2f}-{min(high, 1.0):.2f}", "n": 0, "avg_predicted": None, "win_rate": None, "gap": None}
            )
    return {
        "n": len(decided),
        "rows": rows,
        "reliable": len(decided) >= SMALL_SAMPLE_THRESHOLD,
    }


def _brier(pairs: list[tuple[float, int]]) -> float | None:
    """Mean squared error of (probability, outcome) pairs."""
    if not pairs:
        return None
    return round(sum((p - o) ** 2 for p, o in pairs) / len(pairs), 6)


def _all_games_calibration(final_rows: list[dict]) -> dict[str, Any]:
    scored = [r for r in final_rows if r.get("model_home_prob") is not None]
    rows = []
    for low, high in ALL_GAMES_CAL_BUCKETS:
        members = [r for r in scored if low <= float(r["model_home_prob"]) < high]
        label = f"{low:.2f}-{min(high, 1.0):.2f}"
        if members:
            wins = sum(int(r["home_win"]) for r in members)
            avg_pred = sum(float(r["model_home_prob"]) for r in members) / len(members)
            rows.append(
                {
                    "bucket": label,
                    "n": len(members),
                    "avg_predicted": round(avg_pred, 4),
                    "realized": round(wins / len(members), 4),
                    "gap": round(wins / len(members) - avg_pred, 4),
                }
            )
        else:
            rows.append({"bucket": label, "n": 0, "avg_predicted": None, "realized": None, "gap": None})
    return {"n": len(scored), "rows": rows}


def _rolling_brier(final_rows: list[dict]) -> list[dict]:
    """Trailing ROLLING_WINDOW_DAYS-day Brier (model vs market), one point per
    date that has graded games, computed over paired rows only so the two
    curves always cover the same games."""
    from datetime import date as _date, timedelta as _timedelta

    paired = [
        r
        for r in final_rows
        if r.get("model_home_prob") is not None and r.get("market_home_prob") is not None
    ]
    by_date: dict[str, list[dict]] = {}
    for r in paired:
        by_date.setdefault(str(r["date"]), []).append(r)
    series = []
    for day in sorted(by_date):
        try:
            cutoff = (_date.fromisoformat(day) - _timedelta(days=ROLLING_WINDOW_DAYS - 1)).isoformat()
        except ValueError:
            continue
        window = [r for d, rows in by_date.items() if cutoff <= d <= day for r in rows]
        series.append(
            {
                "date": day,
                "n": len(window),
                "model_brier": _brier([(float(r["model_home_prob"]), int(r["home_win"])) for r in window]),
                "market_brier": _brier([(float(r["market_home_prob"]), int(r["home_win"])) for r in window]),
            }
        )
    return series


def _all_games_block(game_rows: list[dict]) -> dict[str, Any]:
    """The all-games experiment: model vs no-vig market Brier on every game
    the model priced, graded from final scores. See docs/protocol.md."""
    final_rows = [r for r in game_rows if r.get("status") == "final" and r.get("home_win") is not None]
    paired = [
        r
        for r in final_rows
        if r.get("model_home_prob") is not None and r.get("market_home_prob") is not None
    ]
    model_pairs = [(float(r["model_home_prob"]), int(r["home_win"])) for r in paired]
    market_pairs = [(float(r["market_home_prob"]), int(r["home_win"])) for r in paired]
    model_brier = _brier(model_pairs)
    market_brier = _brier(market_pairs)

    by_version: dict[str, dict[str, Any]] = {}
    for version in sorted({str(r.get("model_version") or "unknown") for r in paired}):
        members = [r for r in paired if str(r.get("model_version") or "unknown") == version]
        by_version[version] = {
            "n": len(members),
            "model_brier": _brier([(float(r["model_home_prob"]), int(r["home_win"])) for r in members]),
            "market_brier": _brier([(float(r["market_home_prob"]), int(r["home_win"])) for r in members]),
        }

    return {
        "n_logged": len(game_rows),
        "n_final": len(final_rows),
        "n_scored": len(paired),
        "n_pending": sum(1 for r in game_rows if r.get("status") == "pending"),
        "n_no_result": sum(1 for r in game_rows if r.get("status") == "no_result"),
        "model_brier": model_brier,
        "market_brier": market_brier,
        "brier_delta": round(model_brier - market_brier, 6)
        if model_brier is not None and market_brier is not None
        else None,
        "model_ahead": (model_brier < market_brier)
        if model_brier is not None and market_brier is not None
        else None,
        "calibration": _all_games_calibration(final_rows),
        "rolling": _rolling_brier(final_rows),
        "by_version": by_version,
        "threshold": ALL_GAMES_SAMPLE_THRESHOLD,
        "reliable": len(paired) >= ALL_GAMES_SAMPLE_THRESHOLD,
        "note": (
            "Model home-win probability vs the no-vig market probability from the "
            "same odds download, scored by Brier on every game the model priced. "
            "Lower is better. Below the threshold this comparison is noise."
        ),
    }


def build_metrics(
    history: list[dict],
    meta_model_status: dict | None = None,
    generated_at: str | None = None,
    game_log: list[dict] | None = None,
) -> dict[str, Any]:
    graded = [e for e in history if e.get("result") in GRADED]
    picks = [e for e in graded if not e.get("is_lean")]
    leans = [e for e in graded if e.get("is_lean")]
    return {
        "generated_at": generated_at
        or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "small_sample_threshold": SMALL_SAMPLE_THRESHOLD,
        "overall": _record(graded),
        "official_picks": _record(picks),
        "leans": _record(leans),
        "by_tier": _group_records(graded, "tier"),
        "by_market": _group_records(graded, "market_type"),
        "daily": _daily_series(graded),
        "clv": _clv_block(graded),
        "calibration": _calibration(graded),
        "all_games": _all_games_block(game_log or []),
        "meta_model": meta_model_status
        or {"state": "unknown", "message": "meta-model status not computed"},
    }


def load_history(path: Path = PICK_HISTORY_PATH) -> list[dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def write_metrics(metrics: dict, path: Path = METRICS_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return path


def main() -> None:
    from mlb_model.game_log import load_game_log
    from mlb_model.services.meta_model import MetaModel

    history = load_history()
    meta = MetaModel()
    status = meta.train_from_history(history)
    game_rows = load_game_log()
    path = write_metrics(build_metrics(history, status, game_log=game_rows))
    graded = sum(1 for e in history if e.get("result") in GRADED)
    finals = sum(1 for r in game_rows if r.get("status") == "final")
    print(
        f"Wrote {path} ({graded} graded entries; {finals}/{len(game_rows)} "
        f"all-games rows final; meta-model: {status['message']})"
    )


if __name__ == "__main__":
    main()
