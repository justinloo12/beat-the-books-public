"""Run the model over a historical date range and save daily boards for grading.

REQUIRES network + full deps (Odds API historical access, MLB Stats API, pandas,
pydantic, httpx). Run on a provisioned machine or in CI — not a minimal sandbox.

Prerequisites:
  1. Statcast for the whole season must already be downloaded, e.g.:
       python -m mlb_model.download_data --statcast-start 2025-03-20 --statcast-end 2025-09-30
     (the per-date profiles filter this to <= each game date automatically).
  2. MLB_MODEL_ODDS_API_KEY must be set and have historical-odds access.

Usage:
  python -m mlb_model.backtest_run --start 2025-06-01 --end 2025-09-28

Each day costs ~30 Odds API credits (1 region x 3 markets x 10 for historical).
A June->end-of-season run (~120 days) is ~3,600 credits.

Output: backtest/data/<date>.json — the same board structure the live site
produces (picks, leans, lineup_cards), one file per day.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import traceback
from datetime import date, timedelta
from pathlib import Path

from mlb_model.download_data import download_odds
from mlb_model.services.daily_model import DailyPredictionService

BACKTEST_DIR = Path(__file__).resolve().parents[2] / "backtest" / "data"
MARKETS = ["h2h", "totals", "spreads"]


def _daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


async def run_day(service: DailyPredictionService, slate_date: date, skip_existing: bool) -> dict:
    out_path = BACKTEST_DIR / f"{slate_date.isoformat()}.json"
    if skip_existing and out_path.exists():
        return {"skipped_existing": True}

    # Pull the pre-game historical odds snapshot for this date (no-op if cached).
    await download_odds(slate_date, MARKETS)

    # skip_started_games=False: every historical game is in the past but must
    # still be projected from pre-game inputs.
    board = await service.daily_board(slate_date, skip_started_games=False)
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(board, indent=2, default=str), encoding="utf-8")
    return board


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest the model over a historical date range.")
    parser.add_argument("--start", required=True, help="First slate date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Last slate date (YYYY-MM-DD)")
    parser.add_argument("--skip-existing", action="store_true", help="Skip dates whose board file already exists")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    service = DailyPredictionService()

    total_picks = total_leans = total_games = 0
    for slate_date in _daterange(start, end):
        try:
            board = await run_day(service, slate_date, args.skip_existing)
            if board.get("skipped_existing"):
                print(f"{slate_date}: already present, skipped")
                continue
            picks = len(board.get("picks", []))
            leans = len(board.get("leans", []))
            games = len(board.get("lineup_cards", []))
            total_picks += picks
            total_leans += leans
            total_games += games
            print(f"{slate_date}: {games:2d} games | {picks} picks | {leans} leans")
        except Exception as exc:  # keep going; one bad day shouldn't end the run
            print(f"{slate_date}: ERROR {exc}")
            traceback.print_exc()

    print(f"\nDone. {total_games} games, {total_picks} picks, {total_leans} leans across the range.")
    print(f"Boards written to {BACKTEST_DIR}")
    print("Next: python -m mlb_model.backtest_grade --start", args.start, "--end", args.end)


if __name__ == "__main__":
    asyncio.run(main())
