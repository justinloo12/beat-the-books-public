from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx

_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "schedule_cache"
_ROSTER_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "roster_cache"


class MLBStatsProvider:
    base_url = "https://statsapi.mlb.com/api/v1"

    async def healthcheck(self) -> dict[str, Any]:
        return {"provider": "mlb_stats", "status": "ok"}

    async def fetch_slate(self, slate_date: date) -> list[dict[str, Any]]:
        cache_path = _CACHE_DIR / f"schedule_{slate_date.isoformat()}.json"
        url = f"{self.base_url}/schedule"
        params = {
            "sportId": 1,
            "date": slate_date.isoformat(),
            "hydrate": "lineups,probablePitcher,team",
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
            dates = payload.get("dates", [])
            games = dates[0].get("games", []) if dates else []
            if games:
                _CACHE_DIR.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(games), encoding="utf-8")
            return games
        except Exception:
            if cache_path.exists():
                return json.loads(cache_path.read_text(encoding="utf-8"))
            raise

    async def fetch_reliever_appearance_logs(self, team_id: int, season: int) -> list[dict[str, Any]]:
        return []

    async def fetch_lineups(self, game_pk: int) -> dict[str, Any]:
        return {}

    async def fetch_team_hitters(self, team_id: int) -> list[dict[str, Any]]:
        cache_path = _ROSTER_CACHE_DIR / f"roster_{team_id}.json"
        url = f"{self.base_url}/teams/{team_id}/roster"
        params = {"rosterType": "active"}
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
            roster = payload.get("roster", [])
            hitters = []
            for player in roster:
                position = (player.get("position") or {}).get("abbreviation")
                if position == "P":
                    continue
                person = player.get("person") or {}
                hitters.append(
                    {
                        "id": person.get("id"),
                        "fullName": person.get("fullName"),
                        "position": position,
                        "status": (player.get("status") or {}).get("description"),
                    }
                )
            if hitters:
                _ROSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(hitters), encoding="utf-8")
            return hitters
        except Exception:
            if cache_path.exists():
                return json.loads(cache_path.read_text(encoding="utf-8"))
            return []
