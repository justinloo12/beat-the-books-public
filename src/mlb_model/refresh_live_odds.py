"""Convert already-downloaded odds files into docs/data/live_odds.json.

Reads the local h2h + totals files written by download_data.py — no extra
API calls. Run after download_data in the main CI workflow so the browser
always has a fresh live_odds.json to recalculate edge against.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from mlb_model.config import get_settings

DOCS_DATA = Path(__file__).resolve().parents[2] / "docs" / "data"
OUT_PATH = DOCS_DATA / "live_odds.json"


def _implied(american: int) -> float:
    if american < 0:
        return (-american) / (-american + 100)
    return 100 / (american + 100)


def _no_vig(a: int, b: int) -> tuple[float, float]:
    ra, rb = _implied(a), _implied(b)
    total = ra + rb
    return ra / total, rb / total


def build_live_odds(target_date: date) -> dict:
    settings = get_settings()
    odds_dir = settings.data_dir / "odds_api"

    h2h_path = odds_dir / f"{target_date.isoformat()}_h2h.json"
    totals_path = odds_dir / f"{target_date.isoformat()}_totals.json"

    h2h_games: list[dict] = []
    if h2h_path.exists():
        raw = json.loads(h2h_path.read_text(encoding="utf-8"))
        h2h_games = raw.get("data", [])

    totals_games: list[dict] = []
    if totals_path.exists():
        raw = json.loads(totals_path.read_text(encoding="utf-8"))
        totals_games = raw.get("data", [])

    totals_by_id: dict[str, list[dict]] = {}
    for game in totals_games:
        dk = next((b for b in game.get("bookmakers", []) if b["key"] == "draftkings"), None)
        if dk:
            for mkt in dk.get("markets", []):
                if mkt["key"] == "totals":
                    totals_by_id[game["id"]] = mkt.get("outcomes", [])

    games: list[dict] = []
    for game in h2h_games:
        away = game.get("away_team", "")
        home = game.get("home_team", "")
        dk = next((b for b in game.get("bookmakers", []) if b["key"] == "draftkings"), None)
        if not dk:
            continue
        h2h_outcomes = next((m for m in dk.get("markets", []) if m["key"] == "h2h"), {}).get("outcomes", [])
        away_odds = next((int(o["price"]) for o in h2h_outcomes if o["name"] == away), None)
        home_odds = next((int(o["price"]) for o in h2h_outcomes if o["name"] == home), None)
        moneyline: dict = {}
        if away_odds is not None and home_odds is not None:
            away_nv, home_nv = _no_vig(away_odds, home_odds)
            moneyline = {
                "away_odds": away_odds, "home_odds": home_odds,
                "away_no_vig": round(away_nv, 4), "home_no_vig": round(home_nv, 4),
            }
        totals = [
            {"name": o["name"], "point": o.get("point"), "price": int(o["price"])}
            for o in totals_by_id.get(game["id"], [])
        ]
        games.append({
            "away_team": away, "home_team": home,
            "commence_time": game.get("commence_time"),
            "moneyline": moneyline, "totals": totals,
        })

    return {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "date": target_date.isoformat(),
        "games": games,
    }


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    target = date.fromisoformat(args.date)
    data = build_live_odds(target)
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {len(data['games'])} games to {OUT_PATH}")


if __name__ == "__main__":
    main()

