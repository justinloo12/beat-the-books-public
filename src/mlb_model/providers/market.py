from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from mlb_model.config import get_settings


class MarketProvider:
    def __init__(self) -> None:
        self.data_dir = get_settings().data_dir / "odds_api"

    async def healthcheck(self) -> dict[str, Any]:
        return {"provider": "market", "status": "stub"}

    async def fetch_market_snapshots(self) -> list[dict[str, Any]]:
        return []

    async def load_local_odds(self, slate_date: date) -> list[dict[str, Any]]:
        totals_path = self.data_dir / f"{slate_date.isoformat()}_totals.json"
        spreads_path = self.data_dir / f"{slate_date.isoformat()}_spreads.json"
        h2h_path = self.data_dir / f"{slate_date.isoformat()}_h2h.json"
        bundles: list[dict[str, Any]] = []
        by_matchup: dict[tuple[str, str], dict[str, Any]] = {}
        for path in [totals_path, spreads_path, h2h_path]:
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            for game in payload.get("data", []):
                key = (game["away_team"], game["home_team"])
                bundle = by_matchup.setdefault(
                    key,
                    {
                        "away_team": game["away_team"],
                        "home_team": game["home_team"],
                        "commence_time": game["commence_time"],
                        "markets": [],
                    },
                )
                bookmaker = next((book for book in game.get("bookmakers", []) if book.get("key") == "draftkings"), None)
                if not bookmaker:
                    continue
                for market in bookmaker.get("markets", []):
                    bundle["markets"].append(
                        {
                            "market_key": market["key"],
                            "last_update": market.get("last_update"),
                            "outcomes": market.get("outcomes", []),
                        }
                    )
        bundles.extend(by_matchup.values())
        return bundles
