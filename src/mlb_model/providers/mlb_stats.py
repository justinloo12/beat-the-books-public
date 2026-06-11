from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx

_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "schedule_cache"
_ROSTER_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "roster_cache"
_PLAYER_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "player_cache"
_BOXSCORE_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "boxscore_cache"


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
            "hydrate": "lineups,probablePitcher,team,seriesStatus",
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

    async def fetch_game_boxscore(self, game_pk: int) -> dict[str, Any]:
        cache_path = _BOXSCORE_CACHE_DIR / f"boxscore_{game_pk}.json"
        if cache_path.exists():
            try:
                return json.loads(cache_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        url = f"{self.base_url}/game/{game_pk}/boxscore"
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
            _BOXSCORE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data), encoding="utf-8")
            return data
        except Exception:
            return {}

    async def fetch_team_pitchers(self, team_id: int) -> list[dict[str, Any]]:
        cache_path = _ROSTER_CACHE_DIR / f"pitchers_{team_id}.json"
        url = f"{self.base_url}/teams/{team_id}/roster"
        params = {
            "rosterType": "active",
            "hydrate": f"person(stats(type=[season],group=[pitching],season={date.today().year}),pitchHand)",
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
            pitchers = []
            for player in payload.get("roster", []):
                if (player.get("position") or {}).get("abbreviation") != "P":
                    continue
                person = player.get("person") or {}
                stat = (((person.get("stats") or [{}])[0].get("splits") or [{}])[0].get("stat") or {})
                gs = int(stat.get("gamesStarted") or 0)
                gp = int(stat.get("gamesPlayed") or 0)
                sv = int(stat.get("saves") or 0)
                holds = int(stat.get("holds") or 0)
                era_str = str(stat.get("era") or "4.50")
                try:
                    era = float(era_str)
                except ValueError:
                    era = 4.50
                # Role classification: starter if majority of appearances are starts
                if gs > 0 and gs >= gp * 0.5:
                    role = "starter"
                elif sv >= 5 or (sv >= 2 and holds == 0):
                    role = "closer"
                elif holds >= 5:
                    role = "setup"
                else:
                    role = "reliever"
                pitchers.append({
                    "id": person.get("id"),
                    "fullName": person.get("fullName"),
                    "throws": ((person.get("pitchHand") or {}).get("code")),
                    "role": role,
                    "era": era,
                    "games_started": gs,
                    "games_played": gp,
                    "saves": sv,
                    "holds": holds,
                })
            if pitchers:
                _ROSTER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(pitchers), encoding="utf-8")
            return pitchers
        except Exception:
            if cache_path.exists():
                try:
                    return json.loads(cache_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            return []

    async def fetch_reliever_appearance_logs(self, team_id: int, before_date: date) -> list[dict[str, Any]]:
        """Return recent appearance logs for each reliever — used to score bullpen fatigue."""
        start_date = before_date - timedelta(days=4)
        end_date = before_date - timedelta(days=1)
        url = f"{self.base_url}/schedule"
        params = {
            "sportId": 1,
            "teamId": team_id,
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                sched = response.json()
        except Exception:
            return []

        roster = await self.fetch_team_pitchers(team_id)
        roster_by_id = {int(p["id"]): p for p in roster if p.get("id")}

        # appearances[pitcher_id] = [(days_ago, pitches_thrown)]
        appearances: dict[int, list[tuple[int, int]]] = {}

        dates_sorted = sorted(
            sched.get("dates", []),
            key=lambda d: d.get("date", ""),
            reverse=True,
        )

        for days_ago, date_entry in enumerate(dates_sorted, start=1):
            if days_ago > 3:
                break
            for game in date_entry.get("games", []):
                game_pk = game.get("gamePk")
                if not game_pk:
                    continue
                if (game.get("status") or {}).get("abstractGameState", "") != "Final":
                    continue
                boxscore = await self.fetch_game_boxscore(int(game_pk))
                if not boxscore:
                    continue
                home_id = ((game.get("teams") or {}).get("home", {}).get("team") or {}).get("id")
                side = "home" if home_id == team_id else "away"
                team_data = (boxscore.get("teams") or {}).get(side) or {}
                pitchers_list = team_data.get("pitchers") or []
                players = team_data.get("players") or {}

                for pitcher_id in pitchers_list:
                    roster_item = roster_by_id.get(int(pitcher_id), {})
                    if roster_item.get("role") == "starter":
                        continue
                    stats = (players.get(f"ID{pitcher_id}") or {}).get("stats", {}).get("pitching") or {}
                    pitches = int(stats.get("pitchesThrown") or 0)
                    pid = int(pitcher_id)
                    if pid not in appearances:
                        appearances[pid] = []
                    appearances[pid].append((days_ago, pitches))

        result = []
        for pitcher_id, app_list in appearances.items():
            roster_item = roster_by_id.get(pitcher_id, {})
            app_sorted = sorted(app_list, key=lambda x: x[0])
            pitches_yesterday = next((p for d, p in app_sorted if d == 1), 0)
            pitches_two_days_ago = next((p for d, p in app_sorted if d == 2), 0)
            days_since = min(d for d, _ in app_sorted)
            # Count unbroken run of days pitched starting from yesterday
            consecutive = 0
            for d in range(1, 5):
                if any(day == d for day, _ in app_sorted):
                    consecutive += 1
                else:
                    break
            result.append({
                "id": pitcher_id,
                "name": roster_item.get("fullName", str(pitcher_id)),
                "throws": roster_item.get("throws"),
                "role": roster_item.get("role", "reliever"),
                "pitches_yesterday": pitches_yesterday,
                "pitches_two_days_ago": pitches_two_days_ago,
                "days_since_last": days_since,
                "consecutive_days": consecutive,
                "era": float(roster_item.get("era") or 4.00),
                "xfip": float(roster_item.get("era") or 4.00),
            })
        return result

    async def fetch_bulk_reliever_id(self, team_id: int, before_date: date, lookback_days: int = 10) -> dict[str, Any] | None:
        """Find the pitcher most likely to be the bulk arm after an opener.

        Scans recent games, finds ones where the first pitcher threw < 2 IP (opener),
        and returns {"id": int, "name": str} for the non-opener who threw the most innings.
        """
        url = f"{self.base_url}/schedule"
        params = {
            "sportId": 1,
            "teamId": team_id,
            "startDate": (before_date - timedelta(days=lookback_days)).isoformat(),
            "endDate": (before_date - timedelta(days=1)).isoformat(),
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                sched = response.json()
        except Exception:
            return None

        dates_sorted = sorted(
            sched.get("dates", []),
            key=lambda d: d.get("date", ""),
            reverse=True,
        )

        for date_entry in dates_sorted:
            for game in date_entry.get("games", []):
                game_pk = game.get("gamePk")
                if not game_pk:
                    continue
                if (game.get("status") or {}).get("abstractGameState", "") != "Final":
                    continue
                boxscore = await self.fetch_game_boxscore(int(game_pk))
                if not boxscore:
                    continue
                home_id = ((game.get("teams") or {}).get("home", {}).get("team") or {}).get("id")
                side = "home" if home_id == team_id else "away"
                team_data = (boxscore.get("teams") or {}).get(side) or {}
                pitchers_list = team_data.get("pitchers") or []
                players = team_data.get("players") or {}

                if len(pitchers_list) < 2:
                    continue

                # Check opener criterion: first pitcher threw < 2 IP
                first_stats = (players.get(f"ID{pitchers_list[0]}") or {}).get("stats", {}).get("pitching") or {}
                try:
                    first_ip = float(str(first_stats.get("inningsPitched") or "0.0"))
                except ValueError:
                    first_ip = 0.0
                if first_ip >= 2.0:
                    continue  # traditional starter, not an opener game

                # Bulk arm = the non-opener pitcher with the most innings
                bulk_id, bulk_ip, bulk_name = None, 0.0, None
                for pid in pitchers_list[1:]:
                    p_entry = players.get(f"ID{pid}") or {}
                    p_stats = p_entry.get("stats", {}).get("pitching") or {}
                    try:
                        p_ip = float(str(p_stats.get("inningsPitched") or "0.0"))
                    except ValueError:
                        p_ip = 0.0
                    if p_ip > bulk_ip:
                        bulk_ip = p_ip
                        bulk_id = int(pid)
                        bulk_name = (p_entry.get("person") or {}).get("fullName") or str(pid)

                # Must have thrown at least 3 innings to be considered the bulk arm
                if bulk_id and bulk_ip >= 3.0:
                    return {"id": bulk_id, "name": bulk_name or str(bulk_id)}

        return None

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

    async def fetch_pitcher_season_role(self, pitcher_id: int, season: int) -> dict:
        """Return role metadata for a pitcher: is_opener, ip_per_start, games_started."""
        cache_path = _PLAYER_CACHE_DIR / f"role_{pitcher_id}_{season}.json"
        url = f"{self.base_url}/people/{pitcher_id}/stats"
        params = {"stats": "season", "group": "pitching", "season": season}
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
            splits = (payload.get("stats") or [{}])[0].get("splits") or []
            stat = splits[0].get("stat") if splits else {}
            result = self._parse_pitcher_role(stat)
            _PLAYER_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(result), encoding="utf-8")
            return result
        except Exception:
            if cache_path.exists():
                try:
                    return json.loads(cache_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            return {"is_opener": False, "ip_per_start": None, "games_started": 0}

    @staticmethod
    def _parse_pitcher_role(stat: dict) -> dict:
        gs = int(stat.get("gamesStarted") or 0)
        gp = int(stat.get("gamesPlayed") or 0)
        # inningsPitched from the API is a string like "45.2"
        ip_str = str(stat.get("inningsPitched") or "0.0")
        try:
            ip = float(ip_str)
        except ValueError:
            ip = 0.0
        ip_per_start = round(ip / gs, 2) if gs > 0 else None
        ip_per_app = round(ip / gp, 2) if gp > 0 else None
        # Opener criterion: averages < 2 IP per appearance across all games,
        # regardless of whether those appearances were starts or relief.
        is_opener = gp > 0 and ip_per_app is not None and ip_per_app < 2.0
        return {
            "is_opener": is_opener,
            "ip_per_start": ip_per_start,
            "ip_per_app": ip_per_app,
            "games_started": gs,
            "games_played": gp,
        }
