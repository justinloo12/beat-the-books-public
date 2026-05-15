"""Fetch current odds and write docs/data/live_odds.json for browser-side edge refresh.

Runs independently of the full model — no statcast download, no simulation.
Just grabs the latest h2h + totals lines from The Odds API and normalises
them so the frontend can recalculate edge against the stored model_probability.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

import httpx

DOCS_DATA = Path(__file__).resolve().parents[2] / "docs" / "data"
OUT_PATH = DOCS_DATA / "live_odds.json"

_API_KEY = os.environ.get("MLB_MODEL_ODDS_API_KEY", "")
_BASE_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"


def _implied(american: int) -> float:
    if american < 0:
        return (-american) / (-american + 100)
    return 100 / (american + 100)


def _no_vig(a: int, b: int) -> tuple[float, float]:
    ra, rb = _implied(a), _implied(b)
    total = ra + rb
    return ra / total, rb / total


def fetch_live_odds() -> dict:
    if not _API_KEY:
        raise RuntimeError("MLB_MODEL_ODDS_API_KEY not set")

    games: list[dict] = []
    params_base = {
        "apiKey": _API_KEY,
        "regions": "us",
        "oddsFormat": "american",
        "dateFormat": "iso",
        "bookmakers": "draftkings",
    }

    with httpx.Client(timeout=30.0) as client:
        h2h_data = client.get(_BASE_URL, params={**params_base, "markets": "h2h"}).json()
        totals_data = client.get(_BASE_URL, params={**params_base, "markets": "totals"}).json()

    # Index totals by game id
    totals_by_id: dict[str, list[dict]] = {}
    for game in totals_data:
        dk = next((b for b in game.get("bookmakers", []) if b["key"] == "draftkings"), None)
        if dk:
            for mkt in dk.get("markets", []):
                if mkt["key"] == "totals":
                    totals_by_id[game["id"]] = mkt.get("outcomes", [])

    for game in h2h_data:
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
                "away_odds": away_odds,
                "home_odds": home_odds,
                "away_no_vig": round(away_nv, 4),
                "home_no_vig": round(home_nv, 4),
            }
        totals: list[dict] = []
        for outcome in totals_by_id.get(game["id"], []):
            totals.append({"name": outcome["name"], "point": outcome.get("point"), "price": int(outcome["price"])})

        games.append({
            "away_team": away,
            "home_team": home,
            "commence_time": game.get("commence_time"),
            "moneyline": moneyline,
            "totals": totals,
        })

    return {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "date": date.today().isoformat(),
        "games": games,
    }


def main() -> None:
    data = fetch_live_odds()
    DOCS_DATA.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {len(data['games'])} games to {OUT_PATH}")


if __name__ == "__main__":
    main()
