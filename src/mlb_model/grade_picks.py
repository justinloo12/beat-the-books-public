"""Grade prior-day picks against actual MLB game results.

Usage:
    python -m mlb_model.grade_picks                 # grade yesterday
    python -m mlb_model.grade_picks --date 2026-04-21
"""
from __future__ import annotations

import argparse
import json
import ssl
import urllib.request
from datetime import date, timedelta
from pathlib import Path

from mlb_model.services.odds_engine import (
    implied_probability_from_american,
    no_vig_two_sided,
)

DOCS_DATA = Path(__file__).resolve().parents[2] / "docs" / "data"
PICK_HISTORY_PATH = DOCS_DATA / "pick_history.json"
LIVE_ODDS_PATH = DOCS_DATA / "live_odds.json"

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
    context = ssl._create_unverified_context()
    with urllib.request.urlopen(req, context=context, timeout=30) as resp:
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


# ---------------------------------------------------------------------------
# Closing-line value (CLV)
#
# The refresh workflow rewrites docs/data/live_odds.json several times a day,
# the last pass at ~6pm ET. grade_picks runs the NEXT morning (11am ET) BEFORE
# the day's new odds download, so at grading time live_odds.json still holds
# the final odds snapshot captured for the slate being graded. That snapshot
# is used as the closing-line proxy. It is a proxy, not the true close: for
# early games it may have been captured after first pitch, for late games it
# is ~1-2 hours before it.
# ---------------------------------------------------------------------------


def _load_closing_games(game_date: date) -> list[dict]:
    """Return live-odds games for game_date, or [] when no snapshot matches."""
    if not LIVE_ODDS_PATH.exists():
        return []
    try:
        data = json.loads(LIVE_ODDS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    if data.get("date") != game_date.isoformat():
        return []
    return data.get("games", [])


def _closing_info(pick: dict, closing_games: list[dict]) -> tuple[int | None, float | None, float | None]:
    """Return (closing_odds, closing_line, clv) for a pick.

    clv is the no-vig implied probability of the pick's side at the closing
    snapshot minus the no-vig probability the pick was placed at. Positive =
    the pick beat the close. For totals, clv is only computed when the closing
    snapshot quotes the same total line the pick was made at — comparing
    implied probabilities across different points is not like-for-like.
    """
    matchup = pick.get("matchup", "")
    if " @ " not in matchup:
        return None, None, None
    away_pick, home_pick = (s.strip() for s in matchup.split(" @ ", 1))
    game = next(
        (
            g
            for g in closing_games
            if _teams_match(away_pick, g.get("away_team", "")) and _teams_match(home_pick, g.get("home_team", ""))
        ),
        None,
    )
    if game is None:
        return None, None, None

    pick_no_vig = pick.get("no_vig_probability")
    if pick_no_vig is None and pick.get("model_probability") is not None and pick.get("edge") is not None:
        pick_no_vig = float(pick["model_probability"]) - float(pick["edge"])

    market = pick.get("market_type", "")
    side = pick.get("pick", "")

    if market in ("moneyline", "h2h"):
        ml = game.get("moneyline") or {}
        if "away_odds" not in ml or "home_odds" not in ml:
            return None, None, None
        if _teams_match(side, game.get("away_team", "")):
            close_odds, close_nv = int(ml["away_odds"]), ml.get("away_no_vig")
        elif _teams_match(side, game.get("home_team", "")):
            close_odds, close_nv = int(ml["home_odds"]), ml.get("home_no_vig")
        else:
            return None, None, None
        if close_nv is None:
            away_p = implied_probability_from_american(int(ml["away_odds"]))
            home_p = implied_probability_from_american(int(ml["home_odds"]))
            away_nv, home_nv = no_vig_two_sided(away_p, home_p)
            close_nv = away_nv if _teams_match(side, game.get("away_team", "")) else home_nv
        clv = round(float(close_nv) - float(pick_no_vig), 4) if pick_no_vig is not None else None
        return close_odds, 0.0, clv

    if market == "game_total":
        totals = game.get("totals") or []
        over = next((o for o in totals if o.get("name") == "Over"), None)
        under = next((o for o in totals if o.get("name") == "Under"), None)
        if not over or not under:
            return None, None, None
        side_out = over if side.lower() == "over" else under
        other_out = under if side.lower() == "over" else over
        close_odds = int(side_out["price"])
        close_line = side_out.get("point")
        pick_line = pick.get("line")
        same_line = (
            close_line is not None and pick_line is not None and abs(float(close_line) - float(pick_line)) < 1e-9
        )
        clv = None
        if same_line and pick_no_vig is not None:
            side_p = implied_probability_from_american(close_odds)
            other_p = implied_probability_from_american(int(other_out["price"]))
            close_nv, _ = no_vig_two_sided(side_p, other_p)
            clv = round(close_nv - float(pick_no_vig), 4)
        return close_odds, close_line, clv

    # Spreads/runlines are not part of the live-odds snapshot — no CLV proxy.
    return None, None, None


def _load_history() -> list[dict]:
    if PICK_HISTORY_PATH.exists():
        try:
            return json.loads(PICK_HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_history(entries: list[dict], prior_count: int) -> None:
    if len(entries) < prior_count:
        print(
            f"SAFETY ABORT: refusing to save {len(entries)} entries when "
            f"{prior_count} were loaded — this would delete history. "
            "Run git pull and retry."
        )
        return
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
    leans = day_data.get("daily", {}).get("leans", [])
    all_entries = [(pk, False) for pk in picks] + [(ln, True) for ln in leans]
    if not all_entries:
        print(f"No picks or leans found in {picks_path}")
        return 0

    print(f"Fetching results for {game_date}…")
    games = _fetch_results(game_date)
    if not games:
        print(f"No final scores found for {game_date}")
        return 0
    print(f"  {len(games)} finished game(s) found")

    closing_games = _load_closing_games(game_date)
    if closing_games:
        print(f"  closing-line proxy available for {len(closing_games)} game(s)")
    else:
        print("  no closing-line snapshot available for this date (CLV will be null)")

    history = _load_history()
    prior_count = len(history)
    # Build a set of already-graded (date, matchup, market, pick) tuples to avoid duplicates
    existing_keys: set[tuple] = {
        (e["date"], e["matchup"], e["market_type"], e["pick"])
        for e in history
    }

    new_count = 0
    for pk, is_lean in all_entries:
        key = (game_date.isoformat(), pk["matchup"], pk["market_type"], pk["pick"])
        if key in existing_keys:
            continue

        game = _find_game(pk["matchup"], games)
        if game is None:
            result, pnl_val = "no_result", 0.0
        else:
            result, pnl_val = _grade(pk, game)

        try:
            closing_odds, closing_line, clv = _closing_info(pk, closing_games)
        except Exception:
            closing_odds, closing_line, clv = None, None, None

        entry: dict = {
            "date": game_date.isoformat(),
            "matchup": pk["matchup"],
            "market_type": pk["market_type"],
            "pick": pk["pick"],
            "line": pk.get("line"),
            "american_odds": pk.get("american_odds"),
            "edge": pk.get("edge"),
            "tier": pk.get("tier"),
            "model_probability": pk.get("model_probability"),
            "no_vig_probability": pk.get("no_vig_probability"),
            "closing_odds": closing_odds,
            "closing_line": closing_line,
            "clv": clv,
            "legacy_edge": pk.get("legacy_edge"),
            "legacy_tier": pk.get("legacy_tier"),
            "legacy_model_probability": pk.get("legacy_model_probability"),
            "is_lean": is_lean,
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
        kind = "lean" if is_lean else "pick"
        clv_note = f" clv {clv:+.4f}" if clv is not None else ""
        print(f"  {label} [{kind}] {pk['matchup']} | {pk['market_type']} {pk['pick']} → {result} ({pnl_val:+.2f}){clv_note}")

    _save_history(history, prior_count)
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
