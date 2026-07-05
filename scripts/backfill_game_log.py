"""Backfill docs/data/game_predictions.csv from the archived daily boards.

Every archived board in docs/data/20??-??-??.json contains the model's
per-game outputs (calibrated moneyline quotes, raw sim win prob, simulated
total) and the odds it was priced against. This script extracts one frozen
prediction row per game, tags it with the pre-shrinkage model version
(these boards were exported by the raw-rates model), and grades it against
final scores — using backtest/results_cache.json first (offline,
deterministic) and the free MLB Stats API only for uncached dates.

Zero odds-API calls. Idempotent: rows already in the log are never touched.

Usage:
    .venv/bin/python scripts/backfill_game_log.py
"""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mlb_model.game_log import (  # noqa: E402
    GAME_LOG_PATH,
    MODEL_VERSION_V1,
    append_rows,
    grade_rows_for_date,
    load_game_log,
    rows_from_board,
    save_game_log,
)
from mlb_model.grade_picks import _fetch_results  # noqa: E402

DOCS_DATA = ROOT / "docs" / "data"
RESULTS_CACHE = ROOT / "backtest" / "results_cache.json"
DATE_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")


def _load_results_cache() -> dict[str, list[dict]]:
    if RESULTS_CACHE.exists():
        try:
            return json.loads(RESULTS_CACHE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def results_for(day: str, cache: dict[str, list[dict]]) -> list[dict]:
    if day in cache:
        return cache[day]
    try:
        games = _fetch_results(date.fromisoformat(day))
    except Exception as exc:
        print(f"WARNING: could not fetch results for {day}: {exc}")
        return []
    cache[day] = games
    RESULTS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_CACHE.write_text(json.dumps(cache, indent=1), encoding="utf-8")
    return games


def main() -> None:
    boards: dict[str, dict] = {}
    for path in sorted(DOCS_DATA.glob("20??-??-??.json")):
        if not DATE_RE.match(path.stem):
            continue
        try:
            boards[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"skipping {path.name}: {exc}")

    existing = load_game_log()
    prior_count = len(existing)

    new_rows: list[dict] = []
    for day, board in boards.items():
        extracted = rows_from_board(board, model_version=MODEL_VERSION_V1)
        new_rows.extend(extracted)
        with_market = sum(1 for r in extracted if r["market_home_prob"] is not None)
        print(f"{day}: {len(extracted)} game(s) extracted ({with_market} with market odds)")

    merged, added = append_rows(existing, new_rows)
    print(f"\n{added} new row(s) appended ({prior_count} already logged)")

    cache = _load_results_cache()
    today = date.today()
    graded = 0
    for day in sorted({str(r["date"]) for r in merged if r.get("status") == "pending"}):
        if day >= today.isoformat():
            continue
        results = results_for(day, cache)
        graded += grade_rows_for_date(merged, day, results, give_up=True)

    save_game_log(merged, GAME_LOG_PATH, prior_count=prior_count)

    finals = [r for r in merged if r.get("status") == "final"]
    paired = [
        r for r in finals
        if r.get("model_home_prob") is not None and r.get("market_home_prob") is not None
    ]
    print(f"{graded} row(s) graded this run; log now holds {len(merged)} rows, {len(finals)} final, {len(paired)} scored vs market")
    if paired:
        mb = sum((float(r["model_home_prob"]) - int(r["home_win"])) ** 2 for r in paired) / len(paired)
        kb = sum((float(r["market_home_prob"]) - int(r["home_win"])) ** 2 for r in paired) / len(paired)
        print(f"model Brier {mb:.4f} vs market Brier {kb:.4f} over {len(paired)} games")
    print(f"wrote {GAME_LOG_PATH}")


if __name__ == "__main__":
    main()
