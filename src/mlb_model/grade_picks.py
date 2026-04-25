"""Grade prior-day picks against actual MLB game results.

Usage:
    python -m mlb_model.grade_picks                 # grade yesterday
    python -m mlb_model.grade_picks --date 2026-04-21
"""
from __future__ import annotations

import argparse
import json
import urllib.request
from datetime import date, timedelta
from pathlib import Path

DOCS_DATA = Path(__file__).resolve().parents[2] / "docs" / "data"
PICK_HISTORY_PATH = DOCS_DATA / "pick_history.json"

# Partial-name aliases so "Athletics" matches "Athletics" from the API
_ALIASES: dict[str, str] = {
    "D-backs": "Arizona Diamondbacks",
    "Diamondbacks": "Arizona Diamondbacks",
}


def _american_to_decimal(odds: int) -> float:
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def _pnl(result: str, odds: int, stake: float = 100.0) -> float:
    if result == "win":
        return round(stake * (_american_to_decimal(odds) - 1.0), 2)
    if result == "loss":
        return -stake
    return 0.0  # push or no_result


def _fetch_results(game_date: date) -> list[dict]:
    """Return list of final-score dicts for game_date from MLB Stats API."""
    url = (
        "https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&date={game_date.isoformat()}&hydrate=linescore"
    )
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (beat-the-books/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    games: list[dict] = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            state = game.get("status", {}).get("detailedState", "")
            if state not in ("Final", "Game Over", "Completed Early"):
                continue
            games.append(
                {
                    "away_team": game["teams"]["away"]["team"]["name"],
                    "home_team": game["teams"]["home"]["team"]["name"],
                    "away_score": game["teams"]["away"].get("score", 0) or 0,
                    "home_score": game["teams"]["home"].get("score", 0) or 0,
                }
            )
    return games


def _norm(name: str) -> str:
    return _ALIASES.get(name, name).lower().strip()


def _teams_match(pick_name: str, api_name: str) -> bool:
    p, a = _norm(pick_name), _norm(api_name)
    return p == a or p in a or a in p


def _find_game(matchup: str, games: list[dict]) -> dict | None:
    """Match 'Away @ Home' string to a finished game."""
    if " @ " not in matchup:
        return None
    away_pick, home_pick = (s.strip() for s in matchup.split(" @ ", 1))
    for g in games:
        if _teams_match(away_pick, g["away_team"]) and _teams_match(home_pick, g["home_team"]):
            return g
    return None


def _grade(pick: dict, game: dict) -> tuple[str, float]:
    """Return (result, pnl) given a pick dict and a finished game dict."""
    market = pick.get("market_type", "")
    side = pick.get("pick", "")
    line = float(pick.get("line") or 0.0)
    odds = int(pick.get("american_odds") or -110)
    away, home = game["away_score"], game["home_score"]
    total = away + home

    if market == "game_total":
        if total == line:
            result = "push"
        elif side == "Over":
            result = "win" if total > line else "loss"
        else:
            result = "win" if total < line else "loss"

    elif market in ("moneyline", "h2h"):
        if _teams_match(side, game["away_team"]):
            result = "win" if away > home else "loss"
        elif _teams_match(side, game["home_team"]):
            result = "win" if home > away else "loss"
        else:
            return "no_result", 0.0

    elif market in ("spreads", "runline"):
        # line is from picked team's perspective (e.g. -1.5 means must win by 2+)
        if _teams_match(side, game["home_team"]):
            adj = home + line - away
        elif _teams_match(side, game["away_team"]):
            adj = away + line - home
        else:
            return "no_result", 0.0
        result = "win" if adj > 0 else ("push" if adj == 0 else "loss")

    else:
        return "no_result", 0.0

    return result, _pnl(result, odds)


def _load_history() -> list[dict]:
    if PICK_HISTORY_PATH.exists():
        try:
            return json.loads(PICK_HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_history(entries: list[dict]) -> None:
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    PICK_HISTORY_PATH.write_text(json.dumps(entries, indent=2), encoding="utf-8")


def grade_date(game_date: date) -> int:
    """Grade picks for game_date. Returns number of picks graded."""
    picks_path = DOCS_DATA / f"{game_date.isoformat()}.json"
    if not picks_path.exists():
        print(f"No picks file for {game_date}: {picks_path}")
        return 0

    day_data = json.loads(picks_path.read_text(encoding="utf-8"))
    picks = day_data.get("daily", {}).get("picks", [])
    if not picks:
        print(f"No picks found in {picks_path}")
        return 0

    print(f"Fetching results for {game_date}…")
    games = _fetch_results(game_date)
    if not games:
        print(f"No final scores found for {game_date}")
        return 0
    print(f"  {len(games)} finished game(s) found")

    history = _load_history()
    # Build a set of already-graded (date, matchup, market, pick) tuples to avoid duplicates
    existing_keys: set[tuple] = {
        (e["date"], e["matchup"], e["market_type"], e["pick"])
        for e in history
    }

    new_count = 0
    for pk in picks:
        key = (game_date.isoformat(), pk["matchup"], pk["market_type"], pk["pick"])
        if key in existing_keys:
            continue

        game = _find_game(pk["matchup"], games)
        if game is None:
            result, pnl_val = "no_result", 0.0
        else:
            result, pnl_val = _grade(pk, game)

        entry: dict = {
            "date": game_date.isoformat(),
            "matchup": pk["matchup"],
            "market_type": pk["market_type"],
            "pick": pk["pick"],
            "line": pk.get("line"),
            "american_odds": pk.get("american_odds"),
            "edge": pk.get("edge"),
            "tier": pk.get("tier"),
            "result": result,
            "pnl": pnl_val,
        }
        if game:
            entry["away_score"] = game["away_score"]
            entry["home_score"] = game["home_score"]
            entry["final_total"] = game["away_score"] + game["home_score"]

        history.append(entry)
        existing_keys.add(key)
        new_count += 1
        label = "✓" if result == "win" else ("✗" if result == "loss" else "~")
        print(f"  {label} {pk['matchup']} | {pk['market_type']} {pk['pick']} → {result} ({pnl_val:+.2f})")

    _save_history(history)
    print(f"Saved {new_count} new grade(s) to {PICK_HISTORY_PATH}")
    return new_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Grade picks against actual MLB results.")
    parser.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Date to grade (YYYY-MM-DD). Defaults to yesterday.",
    )
    args = parser.parse_args()
    grade_date(date.fromisoformat(args.date))


if __name__ == "__main__":
    main()
