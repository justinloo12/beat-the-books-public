from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "schedule_cache"
_ROSTER_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "roster_cache"
_PLAYER_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "player_cache"


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

    async def fetch_pitcher_hand(self, player_id: int) -> str | None:
        cache_path = _PLAYER_CACHE_DIR / f"player_{player_id}.json"
        url = f"{self.base_url}/people/{player_id}"
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                payload = response.json()
            people = payload.get("people", [])
            if not people:
                return None
            person = people[0]
            _PLAYER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(person), encoding="utf-8")
            return ((person.get("pitchHand") or {}).get("code"))
        except Exception:
            if cache_path.exists():
                try:
                    person = json.loads(cache_path.read_text(encoding="utf-8"))
                    return ((person.get("pitchHand") or {}).get("code"))
                except Exception:
                    return None
            return None

    async def fetch_recent_lineup_by_opposing_hand(
        self,
        team_id: int,
        opposing_hand: str | None,
        before_date: date,
        lookback_days: int = 21,
    ) -> list[dict[str, Any]]:
        if not opposing_hand:
            return []
        roster = await self.fetch_team_hitters(team_id)
        roster_by_id = {int(player["id"]): player for player in roster if player.get("id")}
        url = f"{self.base_url}/schedule"
        params = {
            "sportId": 1,
            "teamId": team_id,
            "startDate": (before_date - timedelta(days=lookback_days)).isoformat(),
            "endDate": (before_date - timedelta(days=1)).isoformat(),
            "hydrate": "probablePitcher,lineups,team",
        }
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            return []

        dates = sorted(payload.get("dates", []), key=lambda item: item.get("date", ""), reverse=True)
        for slate in dates:
            for game in slate.get("games", []):
                home_team = (game.get("teams", {}).get("home", {}).get("team") or {})
                away_team = (game.get("teams", {}).get("away", {}).get("team") or {})
                if home_team.get("id") == team_id:
                    side = "home"
                    opp_side = "away"
                    lineup_players = ((game.get("lineups") or {}).get("homePlayers") or [])
                elif away_team.get("id") == team_id:
                    side = "away"
                    opp_side = "home"
                    lineup_players = ((game.get("lineups") or {}).get("awayPlayers") or [])
                else:
                    continue
                if len(lineup_players) < 7:
                    continue
                opp_probable = (((game.get("teams") or {}).get(opp_side) or {}).get("probablePitcher") or {})
                opp_pitcher_id = opp_probable.get("id")
                opp_pitch_hand = await self.fetch_pitcher_hand(int(opp_pitcher_id)) if opp_pitcher_id else None
                if opp_pitch_hand != opposing_hand:
                    continue
                projected = []
                for player in lineup_players[:9]:
                    player_id = player.get("id")
                    roster_item = roster_by_id.get(int(player_id)) if player_id is not None and int(player_id) in roster_by_id else {}
                    projected.append(
                        {
                            "id": player_id,
                            "fullName": player.get("fullName"),
                            "position": ((player.get("primaryPosition") or {}).get("abbreviation")),
                            "slot": len(projected) + 1,
                            "handedness": roster_item.get("handedness"),
                            "plate_appearances": roster_item.get("plate_appearances"),
                            "k_pct": roster_item.get("k_pct"),
                            "bb_pct": roster_item.get("bb_pct"),
                        }
                    )
                if projected:
                    return projected
        return []

    async def fetch_reliever_appearance_logs(self, team_id: int, season: int) -> list[dict[str, Any]]:
        return []

    async def fetch_lineups(self, game_pk: int) -> dict[str, Any]:
        return {}

    async def fetch_team_hitters(self, team_id: int) -> list[dict[str, Any]]:
        cache_path = _ROSTER_CACHE_DIR / f"roster_{team_id}.json"
        url = f"{self.base_url}/teams/{team_id}/roster"
        params = {
            "rosterType": "active",
            "hydrate": f"person(stats(type=[season],group=[hitting],season={date.today().year}),batSide)",
        }
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
                stats = (((person.get("stats") or [{}])[0].get("splits") or [{}])[0].get("stat") or {})
                plate_appearances = float(stats.get("plateAppearances") or 0.0)
                strikeouts = float(stats.get("strikeOuts") or 0.0)
                walks = float(stats.get("baseOnBalls") or 0.0)
                hitters.append(
                    {
                        "id": person.get("id"),
                        "fullName": person.get("fullName"),
                        "position": position,
                        "status": (player.get("status") or {}).get("description"),
                        "handedness": ((person.get("batSide") or {}).get("code")),
                        "plate_appearances": plate_appearances,
                        "k_pct": (strikeouts / plate_appearances) if plate_appearances else None,
                        "bb_pct": (walks / plate_appearances) if plate_appearances else None,
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
