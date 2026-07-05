"""Every-game prediction log: the model is scored on ALL games, not just picks.

The pick sample grows ~5/day; the model prices every game on the slate
(~15/day). Logging and grading every game grows the evidence base ~3x
faster, which is the whole experiment: can the model's home-win probability
beat the market's no-vig probability on Brier score?

Storage: docs/data/game_predictions.csv — append-only, committed by the
existing refresh workflow. One row per (date, matchup). The FIRST logged
prediction for a game is frozen; later intraday re-exports never revise it
(no-peeking: the logged number is the one the model is judged on).

Zero new odds-API calls: every number here is read from board payloads that
were already built from the day's single odds download.

Row lifecycle:
    pending  -> logged, waiting for a final score
    final    -> graded with home_win / Brier contributions
    no_result -> no final score found after GRADE_GIVE_UP_DAYS (postponed /
                 cancelled); excluded from all metrics
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

DOCS_DATA = Path(__file__).resolve().parents[2] / "docs" / "data"
GAME_LOG_PATH = DOCS_DATA / "game_predictions.csv"

# Model version tags. v1: raw Statcast aggregate rates (all boards exported
# before 2026-07-04). v2: empirical-Bayes-shrunk, park-adjusted event rates —
# unified with the weight-room-hero-sim methodology on 2026-07-04.
MODEL_VERSION_V1 = "v1-raw-rates"
MODEL_VERSION = "v2-eb-2026-07-04"

# After this many days without a final score, a pending row is closed as
# no_result (postponed/cancelled games).
GRADE_GIVE_UP_DAYS = 3

FIELDNAMES = [
    "date",
    "game_id",
    "away_team",
    "home_team",
    "model_version",
    "prob_source",
    "model_home_prob",
    "market_home_prob",
    "sim_home_prob",
    "model_total",
    "market_total_line",
    "status",
    "home_score",
    "away_score",
    "home_win",
    "model_brier",
    "market_brier",
]

_FLOAT_FIELDS = {
    "model_home_prob",
    "market_home_prob",
    "sim_home_prob",
    "model_total",
    "market_total_line",
    "model_brier",
    "market_brier",
}
_INT_FIELDS = {"home_score", "away_score", "home_win"}


def _slug(name: str) -> str:
    return "".join(c for c in name.lower().replace(" ", "-") if c.isalnum() or c == "-")


def game_id_for(day: str, away: str, home: str) -> str:
    return f"{day}_{_slug(away)}_at_{_slug(home)}"


# ---------------------------------------------------------------------------
# CSV I/O
# ---------------------------------------------------------------------------

def load_game_log(path: Path = GAME_LOG_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        for raw in csv.DictReader(fh):
            row: dict[str, Any] = {}
            for key in FIELDNAMES:
                value: Any = raw.get(key, "")
                if value == "" or value is None:
                    row[key] = None
                elif key in _FLOAT_FIELDS:
                    try:
                        row[key] = float(value)
                    except ValueError:
                        row[key] = None
                elif key in _INT_FIELDS:
                    try:
                        row[key] = int(float(value))
                    except ValueError:
                        row[key] = None
                else:
                    row[key] = str(value)
            rows.append(row)
    return rows


def save_game_log(
    rows: list[dict[str, Any]],
    path: Path = GAME_LOG_PATH,
    prior_count: int | None = None,
) -> None:
    """Write the log. Refuses to shrink the file (append-only safety, same
    contract as pick history's _save_history)."""
    if prior_count is not None and len(rows) < prior_count:
        print(
            f"SAFETY ABORT: refusing to save {len(rows)} game rows when "
            f"{prior_count} were loaded — this would delete history."
        )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in sorted(rows, key=lambda r: (str(r.get("date") or ""), str(r.get("game_id") or ""))):
            out = {}
            for key in FIELDNAMES:
                value = row.get(key)
                out[key] = "" if value is None else value
            writer.writerow(out)


# ---------------------------------------------------------------------------
# Extraction from a board payload
# ---------------------------------------------------------------------------

def rows_from_board(board: dict, model_version: str = MODEL_VERSION) -> list[dict[str, Any]]:
    """One prediction row per lineup card in an exported board payload.

    model_home_prob prefers the calibrated moneyline quote (the price the
    model actually puts on the market); when a board has no moneyline quote
    (some April-era boards), the raw simulation home-win probability is used
    and prob_source says so. market_home_prob is the no-vig home probability
    from the same already-downloaded odds; None when the board had no odds.
    """
    day = str(board.get("date") or "")
    rows: list[dict[str, Any]] = []
    for card in (board.get("daily", {}) or {}).get("lineup_cards", []) or []:
        matchup = str(card.get("matchup") or "")
        if " @ " not in matchup or not day:
            continue
        away, home = (s.strip() for s in matchup.split(" @ ", 1))

        model_home = market_home = None
        prob_source = "simulation"
        market_total_line = None
        for pk in card.get("top_game_picks", []) or []:
            market_type = pk.get("market_type")
            if market_type in ("moneyline", "h2h"):
                # Prefer the home-side quote; fall back to the complement of
                # the away side (exact for two-sided no-vig probabilities).
                # Some archived boards only ranked one moneyline side into
                # the card's top quotes.
                if pk.get("pick") == home:
                    if pk.get("model_probability") is not None:
                        model_home = float(pk["model_probability"])
                        prob_source = "moneyline_quote"
                    if pk.get("no_vig_probability") is not None:
                        market_home = float(pk["no_vig_probability"])
                elif pk.get("pick") == away and model_home is None:
                    if pk.get("model_probability") is not None:
                        model_home = round(1.0 - float(pk["model_probability"]), 4)
                        prob_source = "moneyline_quote"
                    if pk.get("no_vig_probability") is not None and market_home is None:
                        market_home = round(1.0 - float(pk["no_vig_probability"]), 4)
            elif market_type == "game_total" and market_total_line is None:
                if pk.get("line") is not None:
                    market_total_line = float(pk["line"])

        sim_home = card.get("home_win_prob")
        sim_home = float(sim_home) if sim_home is not None else None
        if model_home is None:
            model_home = sim_home
        if model_home is None:
            continue  # nothing to score

        model_total = card.get("simulated_total", card.get("projected_total"))
        rows.append(
            {
                "date": day,
                "game_id": game_id_for(day, away, home),
                "away_team": away,
                "home_team": home,
                "model_version": model_version,
                "prob_source": prob_source,
                "model_home_prob": round(model_home, 4),
                "market_home_prob": round(market_home, 4) if market_home is not None else None,
                "sim_home_prob": round(sim_home, 4) if sim_home is not None else None,
                "model_total": round(float(model_total), 2) if model_total is not None else None,
                "market_total_line": market_total_line,
                "status": "pending",
                "home_score": None,
                "away_score": None,
                "home_win": None,
                "model_brier": None,
                "market_brier": None,
            }
        )
    return rows


def append_rows(
    existing: list[dict[str, Any]], new_rows: Iterable[dict[str, Any]]
) -> tuple[list[dict[str, Any]], int]:
    """Merge new prediction rows into the log. Append-only: a game_id already
    present is NEVER revised — the first logged prediction is frozen."""
    seen = {row.get("game_id") for row in existing}
    merged = list(existing)
    added = 0
    for row in new_rows:
        if row.get("game_id") in seen:
            continue
        merged.append(row)
        seen.add(row.get("game_id"))
        added += 1
    return merged, added


def append_board(board: dict, path: Path = GAME_LOG_PATH, model_version: str = MODEL_VERSION) -> int:
    """Convenience for export_site: extract rows from a board payload and
    persist any new ones. Returns the number of rows appended."""
    existing = load_game_log(path)
    merged, added = append_rows(existing, rows_from_board(board, model_version))
    if added:
        save_game_log(merged, path, prior_count=len(existing))
    return added


# ---------------------------------------------------------------------------
# Grading
# ---------------------------------------------------------------------------

def _match_result(row: dict[str, Any], results: list[dict]) -> dict | None:
    from mlb_model.grade_picks import _teams_match

    for game in results:
        if _teams_match(str(row.get("away_team") or ""), game["away_team"]) and _teams_match(
            str(row.get("home_team") or ""), game["home_team"]
        ):
            return game
    return None


def grade_rows_for_date(
    rows: list[dict[str, Any]], day: str, results: list[dict], give_up: bool = False
) -> int:
    """Grade pending rows dated `day` against final-score dicts (same shape
    grade_picks._fetch_results returns). Idempotent: rows already final are
    untouched. Returns the number of rows newly graded (incl. no_result)."""
    graded = 0
    for row in rows:
        if row.get("date") != day or row.get("status") != "pending":
            continue
        game = _match_result(row, results)
        if game is None:
            if give_up:
                row["status"] = "no_result"
                graded += 1
            continue
        home_score = int(game["home_score"])
        away_score = int(game["away_score"])
        home_win = 1 if home_score > away_score else 0
        row["home_score"] = home_score
        row["away_score"] = away_score
        row["home_win"] = home_win
        model_p = row.get("model_home_prob")
        market_p = row.get("market_home_prob")
        row["model_brier"] = round((float(model_p) - home_win) ** 2, 6) if model_p is not None else None
        row["market_brier"] = round((float(market_p) - home_win) ** 2, 6) if market_p is not None else None
        row["status"] = "final"
        graded += 1
    return graded


def grade_pending(
    fetch_results: Callable[[date], list[dict]] | None = None,
    today: date | None = None,
    path: Path = GAME_LOG_PATH,
) -> int:
    """Grade every pending row older than today. One free MLB Stats API call
    per pending date. Safe to re-run: already-graded rows are skipped, and
    dates with no finals stay pending until GRADE_GIVE_UP_DAYS."""
    if fetch_results is None:
        from mlb_model.grade_picks import _fetch_results

        fetch_results = _fetch_results
    today = today or date.today()

    rows = load_game_log(path)
    prior_count = len(rows)
    pending_dates = sorted(
        {
            str(row["date"])
            for row in rows
            if row.get("status") == "pending" and row.get("date") and str(row["date"]) < today.isoformat()
        }
    )
    total = 0
    for day in pending_dates:
        try:
            results = fetch_results(date.fromisoformat(day))
        except Exception as exc:  # network hiccup — try again next run
            print(f"game_log: could not fetch results for {day}: {exc}")
            continue
        give_up = (today - date.fromisoformat(day)).days > GRADE_GIVE_UP_DAYS
        graded = grade_rows_for_date(rows, day, results, give_up=give_up)
        total += graded
        if graded:
            print(f"game_log: graded {graded} game(s) for {day}")
    if total:
        save_game_log(rows, path, prior_count=prior_count)
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Grade the every-game prediction log.")
    parser.add_argument(
        "--as-of",
        default=date.today().isoformat(),
        help="Treat this date as 'today' when deciding which rows are gradeable.",
    )
    args = parser.parse_args()
    graded = grade_pending(today=date.fromisoformat(args.as_of))
    rows = load_game_log()
    final = sum(1 for r in rows if r.get("status") == "final")
    print(
        f"game_log: {graded} newly graded — {len(rows)} logged, {final} final "
        f"({datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})"
    )


if __name__ == "__main__":
    main()
