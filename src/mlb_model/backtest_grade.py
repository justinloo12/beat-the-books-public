"""Grade backtest boards against actual MLB results into a flat results file.

REQUIRES network (MLB Stats API) for final scores. Reuses the proven grading
logic from grade_picks so backtest grading matches live grading exactly.

Usage:
  python -m mlb_model.backtest_grade --start 2025-06-01 --end 2025-09-28

Output: backtest/results.json — one row per graded pick/lean with the model's
probability, edge, tier, AND the legacy (pre-discount) edge/tier, plus the
result and P&L. This is the dataset backtest_report.py analyses.
"""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from mlb_model.grade_picks import _fetch_results, _find_game, _grade

BACKTEST_DIR = Path(__file__).resolve().parents[2] / "backtest" / "data"
RESULTS_PATH = Path(__file__).resolve().parents[2] / "backtest" / "results.json"


def _daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _entry(slate_date: date, pk: dict, is_lean: bool, game: dict | None) -> dict:
    if game is None:
        result, pnl_val = "no_result", 0.0
    else:
        result, pnl_val = _grade(pk, game)
    row = {
        "date": slate_date.isoformat(),
        "matchup": pk.get("matchup"),
        "market_type": pk.get("market_type"),
        "pick": pk.get("pick"),
        "line": pk.get("line"),
        "american_odds": pk.get("american_odds"),
        "model_probability": pk.get("model_probability"),
        "no_vig_probability": pk.get("no_vig_probability"),
        "edge": pk.get("edge"),
        "tier": pk.get("tier"),
        "legacy_model_probability": pk.get("legacy_model_probability"),
        "legacy_edge": pk.get("legacy_edge"),
        "legacy_tier": pk.get("legacy_tier"),
        "lineup_status": pk.get("lineup_status"),
        "is_lean": is_lean,
        "result": result,
        "pnl": pnl_val,
    }
    return row


def grade_date(slate_date: date) -> list[dict]:
    board_path = BACKTEST_DIR / f"{slate_date.isoformat()}.json"
    if not board_path.exists():
        return []
    board = json.loads(board_path.read_text(encoding="utf-8"))
    picks = board.get("picks", [])
    leans = board.get("leans", [])
    entries = [(p, False) for p in picks] + [(l, True) for l in leans]
    if not entries:
        return []
    games = _fetch_results(slate_date)
    if not games:
        return []
    rows: list[dict] = []
    for pk, is_lean in entries:
        game = _find_game(pk.get("matchup", ""), games)
        rows.append(_entry(slate_date, pk, is_lean, game))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Grade backtest boards against actual results.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    all_rows: list[dict] = []
    for slate_date in _daterange(start, end):
        rows = grade_date(slate_date)
        graded = sum(1 for r in rows if r["result"] in {"win", "loss", "push"})
        if rows:
            print(f"{slate_date}: {graded}/{len(rows)} graded")
        all_rows.extend(rows)

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
    print(f"\nWrote {len(all_rows)} rows to {RESULTS_PATH}")
    print("Next: python -m mlb_model.backtest_report")


if __name__ == "__main__":
    main()
