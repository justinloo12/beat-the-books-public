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

CALIBRATION_BUCKETS = [(0.0, 0.45), (0.45, 0.50), (0.50, 0.55), (0.55, 0.60), (0.60, 1.01)]


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


def build_metrics(
    history: list[dict],
    meta_model_status: dict | None = None,
    generated_at: str | None = None,
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
    from mlb_model.services.meta_model import MetaModel

    history = load_history()
    meta = MetaModel()
    status = meta.train_from_history(history)
    path = write_metrics(build_metrics(history, status))
    graded = sum(1 for e in history if e.get("result") in GRADED)
    print(f"Wrote {path} ({graded} graded entries; meta-model: {status['message']})")


if __name__ == "__main__":
    main()
