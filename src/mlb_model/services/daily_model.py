from __future__ import annotations

import sys
import traceback
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

# Expected PA per game by batting-order slot (1–9); normalized to sum=1.
# Leadoff bats ~38% more than the 9-hole over a full game.
_SLOT_PA_WEIGHTS = [0.129, 0.124, 0.120, 0.115, 0.111, 0.107, 0.102, 0.098, 0.093]

from mlb_model.config import get_settings
from mlb_model.utils import safe_mean
from mlb_model.providers.baseball import BaseballSavantProvider
from mlb_model.providers.market import MarketProvider
from mlb_model.providers.mlb_stats import MLBStatsProvider
from mlb_model.providers.weather import TomorrowWeatherProvider
from mlb_model.services.bullpen_model import BullpenModelService
from mlb_model.services.market_model import MarketSignalService
from mlb_model.services.matchup_model import MatchupModelService
from mlb_model.services.odds_engine import (
    american_to_decimal,
    classify_edge,
    implied_probability_from_american,
    no_vig_two_sided,
)
from mlb_model.services.pitcher_model import PitcherModelService
from mlb_model.services.run_expectation import RunExpectationService
from mlb_model.services.simulation_model import PARK_FACTORS, SimulationModelService
from mlb_model.services.weather_model import WeatherModelService


class DailyPredictionService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.baseball = BaseballSavantProvider()
        self.stats = MLBStatsProvider()
        self.market = MarketProvider()
        self.pitchers = PitcherModelService()
        self.matchups = MatchupModelService()
        self.runs = RunExpectationService()
        self.weather_provider = TomorrowWeatherProvider()
        self.weather = WeatherModelService()
        self.bullpens = BullpenModelService()
        self.market_model = MarketSignalService()
        self.simulation = SimulationModelService(trials=self.settings.load_model_settings().simulation["default_trials"])

    async def daily_picks(self, slate_date: date) -> dict[str, Any]:
        board = await self.daily_board(slate_date)
        return {"date": board["date"], "picks": board["picks"], "skipped": board["skipped"]}

    async def daily_board(self, slate_date: date) -> dict[str, Any]:
        try:
            games = await self.stats.fetch_slate(slate_date)
        except Exception as exc:
            return {
                "date": slate_date.isoformat(),
                "picks": [],
                "lineup_cards": [],
                "skipped": [{"matchup": "all games", "reason": f"slate fetch failed: {exc}"}],
            }
        odds_bundle = await self.market.load_local_odds(slate_date)
        game_map = {
            (game["away_team"], game["home_team"]): game
            for game in odds_bundle
        }
        picks: list[dict[str, Any]] = []
        lineup_cards: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for game in games:
            away_team = game["teams"]["away"]["team"]["name"]
            home_team = game["teams"]["home"]["team"]["name"]
            matchup_label = f"{away_team} @ {home_team}"
            try:
                away_team_id = game["teams"]["away"]["team"]["id"]
                home_team_id = game["teams"]["home"]["team"]["id"]
                probable_home = game["teams"]["home"].get("probablePitcher") or {}
                probable_away = game["teams"]["away"].get("probablePitcher") or {}
                home_confirmed = bool(game["teams"]["home"].get("lineup"))
                away_confirmed = bool(game["teams"]["away"].get("lineup"))
                if not probable_home.get("id") or not probable_away.get("id"):
                    skipped.append({"matchup": matchup_label, "reason": "missing probable starters"})
                    continue
                home_pitcher_hand = await self.stats.fetch_pitcher_hand(int(probable_home["id"]))
                away_pitcher_hand = await self.stats.fetch_pitcher_hand(int(probable_away["id"]))
                home_lineup = game["teams"]["home"].get("lineup") or await self.stats.fetch_recent_lineup_by_opposing_hand(
                    home_team_id,
                    away_pitcher_hand,
                    slate_date,
                ) or await self.stats.fetch_team_hitters(home_team_id)
                away_lineup = game["teams"]["away"].get("lineup") or await self.stats.fetch_recent_lineup_by_opposing_hand(
                    away_team_id,
                    home_pitcher_hand,
                    slate_date,
                ) or await self.stats.fetch_team_hitters(away_team_id)
                odds_game = game_map.get((away_team, home_team))

                context = await self._build_game_projection(
                    slate_date=slate_date,
                    home_team=home_team,
                    away_team=away_team,
                    home_pitcher=probable_home,
                    away_pitcher=probable_away,
                    home_lineup=home_lineup,
                    away_lineup=away_lineup,
                    home_lineup_confirmed=home_confirmed,
                    away_lineup_confirmed=away_confirmed,
                    venue=game.get("venue", {}).get("name", "Unknown"),
                    start_time=game.get("gameDate"),
                )
                game_picks, top_game_picks = self._score_markets(context, odds_game)
                context["top_game_picks"] = top_game_picks
                for pick in game_picks:
                    pick["lineup_status"] = "confirmed" if home_confirmed and away_confirmed else "projected"
                lineup_cards.append(self._build_lineup_card(context))
                picks.extend(game_picks)
                if not home_confirmed or not away_confirmed:
                    skipped.append({"matchup": matchup_label, "reason": "lineups not confirmed"})
                    continue
                if not odds_game:
                    skipped.append({"matchup": matchup_label, "reason": "no local odds file entry"})
                    continue
            except Exception as exc:
                traceback.print_exc(file=sys.stderr)
                skipped.append({"matchup": matchup_label, "reason": f"processing error: {exc}"})

        picks = sorted(
            picks,
            key=lambda item: (
                item.get("tier") != "strong",
                item.get("tier") != "moderate",
                -item.get("edge", 0.0),
                -item.get("model_probability", 0.0),
            ),
        )[:10]
        lineup_cards = sorted(lineup_cards, key=lambda item: item["matchup"])
        return {"date": slate_date.isoformat(), "picks": picks, "lineup_cards": lineup_cards, "skipped": skipped}

    async def _build_game_projection(
        self,
        slate_date: date,
        home_team: str,
        away_team: str,
        home_pitcher: dict[str, Any],
        away_pitcher: dict[str, Any],
        home_lineup: list[dict[str, Any]],
        away_lineup: list[dict[str, Any]],
        home_lineup_confirmed: bool,
        away_lineup_confirmed: bool,
        venue: str,
        start_time: str | None = None,
    ) -> dict[str, Any]:
        sample_start = slate_date - timedelta(days=365)
        home_pitcher_id = int(home_pitcher["id"])
        away_pitcher_id = int(away_pitcher["id"])
        # overall profiles for scoring and display
        home_pitcher_profile = self.baseball.build_pitcher_arsenal_profile(home_pitcher_id, start_date=sample_start, end_date=slate_date)
        away_pitcher_profile = self.baseball.build_pitcher_arsenal_profile(away_pitcher_id, start_date=sample_start, end_date=slate_date)
        # handedness-split profiles used when matching each batter
        home_pitcher_vs_l = self.baseball.build_pitcher_arsenal_profile(home_pitcher_id, batter_hand="L", start_date=sample_start, end_date=slate_date)
        home_pitcher_vs_r = self.baseball.build_pitcher_arsenal_profile(home_pitcher_id, batter_hand="R", start_date=sample_start, end_date=slate_date)
        away_pitcher_vs_l = self.baseball.build_pitcher_arsenal_profile(away_pitcher_id, batter_hand="L", start_date=sample_start, end_date=slate_date)
        away_pitcher_vs_r = self.baseball.build_pitcher_arsenal_profile(away_pitcher_id, batter_hand="R", start_date=sample_start, end_date=slate_date)
        home_pitcher_score = self.pitchers.score_pitcher(self._pitcher_stats_from_arsenal(home_pitcher_profile))
        away_pitcher_score = self.pitchers.score_pitcher(self._pitcher_stats_from_arsenal(away_pitcher_profile))
        home_pitcher_matchup = self.matchups.score_pitcher_profile(home_pitcher_profile)
        away_pitcher_matchup = self.matchups.score_pitcher_profile(away_pitcher_profile)

        home_matchups = self._lineup_matchups(
            home_lineup,
            away_pitcher_profile,
            away_pitcher_vs_l,
            away_pitcher_vs_r,
            slate_date,
            confirmed=home_lineup_confirmed,
        )
        away_matchups = self._lineup_matchups(
            away_lineup,
            home_pitcher_profile,
            home_pitcher_vs_l,
            home_pitcher_vs_r,
            slate_date,
            confirmed=away_lineup_confirmed,
        )
        home_offense = self.matchups.lineup_offense_score(home_matchups)
        away_offense = self.matchups.lineup_offense_score(away_matchups)
        home_lineup_avgs = self._lineup_averages(home_matchups)
        away_lineup_avgs = self._lineup_averages(away_matchups)

        weather_raw = await self.weather_provider.fetch_forecast(venue, start_time=start_time)
        weather_projection = self._weather_projection(weather_raw, venue, start_time)
        weather = self.weather.score_weather({"ballpark": venue, "is_indoor": False}, weather_projection)
        park_factor = PARK_FACTORS.get(venue or "", 1.0)

        home_bullpen = self.bullpens.score_bullpen([])
        away_bullpen = self.bullpens.score_bullpen([])

        home_runs = self.runs.expected_runs(
            team=home_team,
            pitcher_xba=float(away_pitcher_profile.get("xba") or 0.255),
            pitcher_k_pct=float(away_pitcher_profile.get("weighted_k_pct") or 0.228),
            pitcher_bb_pct=float(away_pitcher_profile.get("weighted_bb_pct") or 0.076),
            pitcher_hard_hit_pct=float(away_pitcher_profile.get("hard_hit_pct") or 0.375),
            pitcher_barrel_pct=float(away_pitcher_profile.get("barrel_pct") or 0.080),
            lineup_xwoba=home_lineup_avgs["xwoba"],
            lineup_k_pct=home_lineup_avgs["k_pct"],
            lineup_bb_pct=home_lineup_avgs["bb_pct"],
            lineup_hard_hit_pct=home_lineup_avgs["hard_hit_pct"],
            weather_multiplier=float(weather.get("weather_multiplier", 1.0)),
            park_factor=park_factor,
            bullpen_score=float(away_bullpen.get("bullpen_score", 65.0)),
            starter_ip_projection=away_pitcher_score["starter_ip_projection"],
            top_features=self._top_run_features_direct(
                pitcher_xba=float(away_pitcher_profile.get("xba") or 0.255),
                pitcher_k_pct=float(away_pitcher_profile.get("weighted_k_pct") or 0.228),
                lineup_xwoba=home_lineup_avgs["xwoba"],
                weather_multiplier=float(weather.get("weather_multiplier", 1.0)),
                bullpen_score=float(away_bullpen.get("bullpen_score", 65.0)),
            ),
        )
        away_runs = self.runs.expected_runs(
            team=away_team,
            pitcher_xba=float(home_pitcher_profile.get("xba") or 0.255),
            pitcher_k_pct=float(home_pitcher_profile.get("weighted_k_pct") or 0.228),
            pitcher_bb_pct=float(home_pitcher_profile.get("weighted_bb_pct") or 0.076),
            pitcher_hard_hit_pct=float(home_pitcher_profile.get("hard_hit_pct") or 0.375),
            pitcher_barrel_pct=float(home_pitcher_profile.get("barrel_pct") or 0.080),
            lineup_xwoba=away_lineup_avgs["xwoba"],
            lineup_k_pct=away_lineup_avgs["k_pct"],
            lineup_bb_pct=away_lineup_avgs["bb_pct"],
            lineup_hard_hit_pct=away_lineup_avgs["hard_hit_pct"],
            weather_multiplier=float(weather.get("weather_multiplier", 1.0)),
            park_factor=park_factor,
            bullpen_score=float(home_bullpen.get("bullpen_score", 65.0)),
            starter_ip_projection=home_pitcher_score["starter_ip_projection"],
            top_features=self._top_run_features_direct(
                pitcher_xba=float(home_pitcher_profile.get("xba") or 0.255),
                pitcher_k_pct=float(home_pitcher_profile.get("weighted_k_pct") or 0.228),
                lineup_xwoba=away_lineup_avgs["xwoba"],
                weather_multiplier=float(weather.get("weather_multiplier", 1.0)),
                bullpen_score=float(home_bullpen.get("bullpen_score", 65.0)),
            ),
        )
        return {
            "matchup": f"{away_team} @ {home_team}",
            "home_team": home_team,
            "away_team": away_team,
            "venue": venue,
            "start_time": self._format_start_time(start_time),
            "home_pitcher_name": home_pitcher.get("fullName") or home_pitcher.get("name", ""),
            "away_pitcher_name": away_pitcher.get("fullName") or away_pitcher.get("name", ""),
            "home_pitcher_profile": home_pitcher_profile,
            "away_pitcher_profile": away_pitcher_profile,
            "home_pitcher_vs_l": home_pitcher_vs_l,
            "home_pitcher_vs_r": home_pitcher_vs_r,
            "away_pitcher_vs_l": away_pitcher_vs_l,
            "away_pitcher_vs_r": away_pitcher_vs_r,
            "home_pitcher_score": home_pitcher_score,
            "away_pitcher_score": away_pitcher_score,
            "home_pitcher_matchup": home_pitcher_matchup,
            "away_pitcher_matchup": away_pitcher_matchup,
            "home_lineup_matchups": home_matchups,
            "away_lineup_matchups": away_matchups,
            "home_lineup_raw": home_lineup,
            "away_lineup_raw": away_lineup,
            "home_lineup_confirmed": home_lineup_confirmed,
            "away_lineup_confirmed": away_lineup_confirmed,
            "home_offense": home_offense,
            "away_offense": away_offense,
            "home_runs": home_runs.expected_runs,
            "away_runs": away_runs.expected_runs,
            "home_run_context": home_runs.model_dump(),
            "away_run_context": away_runs.model_dump(),
            "simulation": {
                "home_runs_mean": home_runs.expected_runs,
                "away_runs_mean": away_runs.expected_runs,
                "total_mean": round(home_runs.expected_runs + away_runs.expected_runs, 3),
                "home_win_prob": 0.5,
                "away_win_prob": 0.5,
                "runline_home_cover_prob": 0.5,
                "runline_away_cover_prob": 0.5,
            },
            "weather": {**weather_projection, **weather},
            "home_bullpen": home_bullpen,
            "away_bullpen": away_bullpen,
        }

    def _top_run_features_direct(
        self,
        pitcher_xba: float,
        pitcher_k_pct: float,
        lineup_xwoba: float,
        weather_multiplier: float,
        bullpen_score: float,
    ) -> list[dict[str, float | str]]:
        return [
            {"feature": "Pitcher xBA", "value": round(pitcher_xba, 3)},
            {"feature": "Pitcher K%", "value": round(pitcher_k_pct, 3)},
            {"feature": "Lineup xwOBA", "value": round(lineup_xwoba, 3)},
        ]

    def _lineup_averages(self, matchup_reports: list[dict[str, Any]]) -> dict[str, float]:
        if not matchup_reports:
            return {"xwoba": 0.318, "k_pct": 0.228, "bb_pct": 0.076, "hard_hit_pct": 0.375}
        w_xwoba = w_k = w_bb = w_hh = total_w = 0.0
        for report in matchup_reports:
            slot = report.get("slot")
            try:
                idx = int(slot) - 1 if slot != "-" else None
            except (TypeError, ValueError):
                idx = None
            weight = _SLOT_PA_WEIGHTS[idx] if idx is not None and 0 <= idx < 9 else (1.0 / 9.0)
            pm = report.get("pitch_matchup") or {}
            p = report.get("profile") or {}
            # Prefer usage-weighted pitch matchup stats; fall back to season profile
            xwoba = float(pm.get("matchup_xwoba") or p.get("xwoba") or 0.318)
            k     = float(pm.get("matchup_k_risk") or p.get("k_pct") or 0.228)
            bb    = float(pm.get("matchup_bb_upside") or p.get("bb_pct") or 0.076)
            hh    = float(pm.get("matchup_hard_hit_pct") or p.get("hard_hit_pct") or 0.375)
            w_xwoba += weight * xwoba
            w_k     += weight * k
            w_bb    += weight * bb
            w_hh    += weight * hh
            total_w += weight
        if total_w <= 0:
            return {"xwoba": 0.318, "k_pct": 0.228, "bb_pct": 0.076, "hard_hit_pct": 0.375}
        return {
            "xwoba":       round(w_xwoba / total_w, 4),
            "k_pct":       round(w_k     / total_w, 4),
            "bb_pct":      round(w_bb    / total_w, 4),
            "hard_hit_pct": round(w_hh   / total_w, 4),
        }

    def _lineup_matchups(
        self,
        lineup: list[dict[str, Any]],
        opposing_pitcher: dict[str, Any],
        opposing_pitcher_vs_l: dict[str, Any],
        opposing_pitcher_vs_r: dict[str, Any],
        slate_date: date,
        confirmed: bool,
    ) -> list[dict[str, Any]]:
        if not confirmed:
            return self._hitter_pool_matchups(lineup, slate_date)
        pitch_types = {pitch["pitch_type"] for pitch in opposing_pitcher.get("pitch_arsenal", [])}
        reports = []
        sample_start = slate_date - timedelta(days=365)
        for slot, batter in enumerate(lineup, start=1):
            display_slot = batter.get("battingOrder")
            if display_slot:
                try:
                    display_slot = int(str(display_slot)[0])
                except ValueError:
                    display_slot = slot
            else:
                display_slot = batter.get("slot", slot if slot <= 9 else None)
            batter_id = batter.get("id") or batter.get("person", {}).get("id")
            if not batter_id:
                continue
            profile = self.baseball.build_batter_matchup_profile(
                int(batter_id),
                pitcher_hand=opposing_pitcher.get("handedness"),
                pitch_types=pitch_types,
                start_date=sample_start,
                end_date=slate_date,
            )
            # use handedness-specific pitcher profile for more accurate matchup scoring
            batter_hand = profile.get("handedness")
            if batter_hand == "L" and opposing_pitcher_vs_l.get("pitch_arsenal"):
                pitcher_for_matchup = opposing_pitcher_vs_l
            elif batter_hand == "R" and opposing_pitcher_vs_r.get("pitch_arsenal"):
                pitcher_for_matchup = opposing_pitcher_vs_r
            else:
                pitcher_for_matchup = opposing_pitcher
            matchup = self.matchups.score_batter_vs_pitcher(profile, pitcher_for_matchup, lineup_slot=slot)
            pitch_matchup = self.baseball.compute_pitcher_matchup(
                profile.get("pitch_profiles", []),
                pitcher_for_matchup.get("pitch_arsenal", []),
            )
            reports.append(
                {
                    "slot": display_slot if display_slot is not None else "-",
                    "batter_id": int(batter_id),
                    "name": self._player_name(batter),
                    "profile": profile,
                    "matchup": matchup,
                    "pitch_matchup": pitch_matchup,
                    "pitcher_hand": opposing_pitcher.get("handedness"),
                }
            )
        return reports

    def _hitter_pool_matchups(
        self,
        hitters: list[dict[str, Any]],
        slate_date: date,
    ) -> list[dict[str, Any]]:
        reports = []
        for index, batter in enumerate(hitters, start=1):
            batter_id = batter.get("id") or batter.get("person", {}).get("id")
            if not batter_id:
                continue
            profile = self.baseball.build_batter_summary_profile(int(batter_id), slate_date.year)
            if profile.get("handedness") is None:
                profile["handedness"] = batter.get("handedness")
            if profile.get("k_pct") is None:
                profile["k_pct"] = batter.get("k_pct")
                profile["recent_k_pct"] = batter.get("k_pct")
            if profile.get("bb_pct") is None:
                profile["bb_pct"] = batter.get("bb_pct")
                profile["recent_bb_pct"] = batter.get("bb_pct")
            xwoba = float(profile.get("xwoba") or 0.315)
            matchup_score = max(20.0, min(80.0, 45 + (xwoba - 0.315) * 120))
            reports.append(
                {
                    "slot": batter.get("position") or "-",
                    "batter_id": int(batter_id),
                    "name": self._player_name(batter),
                    "profile": profile,
                    "matchup": {"matchup_score": round(matchup_score, 2), "pitch_scores": []},
                }
            )
        return reports

    def _pitcher_stats_from_arsenal(self, pitcher_profile: dict[str, Any]) -> dict[str, Any]:
        pitch_arsenal = pitcher_profile.get("pitch_arsenal", [])
        if not pitch_arsenal:
            return {}
        def _num(value: Any) -> float:
            try:
                return float(value) if value is not None else 0.0
            except (TypeError, ValueError):
                return 0.0

        weighted = lambda key: sum(_num(p.get(key)) * _num(p.get("usage_pct")) for p in pitch_arsenal)

        xba = float(pitcher_profile.get("xba") or 0.245)
        hard_hit = float(pitcher_profile.get("hard_hit_pct") or 0.36) * 100
        barrel = float(pitcher_profile.get("barrel_pct") or 0.08) * 100
        extension = float(pitcher_profile.get("extension") or 6.1)
        # map extension (typical range 5.4–7.0) to a 0–100 percentile
        extension_pct = max(0.0, min(100.0, (extension - 5.4) / 1.6 * 100))
        # estimate xERA from xBA (rough linear mapping: xBA .220=3.0, .270=5.0)
        xera = max(2.5, min(6.5, 3.0 + (xba - 0.220) / 0.050 * 1.0))
        # estimate recent form from recent_xba if available
        recent_xba = float(pitcher_profile.get("recent_xba") or xba)
        recent_xera = max(2.5, min(6.5, 3.0 + (recent_xba - 0.220) / 0.050 * 1.0))
        # weighted movement profile: more vertical movement correlates with higher GB%
        vert_movement = sum(abs(_num(p.get("vertical_movement"))) * _num(p.get("usage_pct")) for p in pitch_arsenal)
        gb_pct = max(30.0, min(58.0, 42.0 + vert_movement * 4.5))
        k_pct = weighted("k_pct") * 100
        bb_pct = weighted("bb_pct") * 100

        return {
            "xBA": xba,
            "HardHit%": hard_hit,
            "Barrel%": barrel,
            "ExtensionPercentile": round(extension_pct, 1),
            "Chase%": k_pct,
            "Whiff%": k_pct,
            "GB%": round(gb_pct, 1),
            "PitchMix": {p["pitch_type"]: p["usage_pct"] for p in pitch_arsenal},
            "ERA": round(xera, 2),
            "xERA": round(xera, 2),
            "Last3xERA": round(recent_xera, 2),
            "IP": 120.0,
            "DaysRest": 4,
            "FBv": 93.0,
            "Last5FBv": [93.0] * 5,
        }

    def _score_markets(self, context: dict[str, Any], odds_game: dict[str, Any] | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        home_lineup = [self._with_weather(entry, context["weather"]) for entry in context["home_lineup_matchups"]]
        away_lineup = [self._with_weather(entry, context["weather"]) for entry in context["away_lineup_matchups"]]
        simulation_summary = self.simulation.simulate_game(
            home_lineup=home_lineup,
            away_lineup=away_lineup,
            home_pitcher=context["home_pitcher_profile"],
            away_pitcher=context["away_pitcher_profile"],
            total_lines=self._market_total_lines(odds_game),
            home_target_runs=context["home_runs"],
            away_target_runs=context["away_runs"],
            home_bullpen_score=context["home_bullpen"]["bullpen_score"],
            away_bullpen_score=context["away_bullpen"]["bullpen_score"],
            home_starter_ip=context["home_pitcher_score"]["starter_ip_projection"],
            away_starter_ip=context["away_pitcher_score"]["starter_ip_projection"],
            venue=context["venue"],
        )
        context["simulation"] = {
            "home_runs_mean": simulation_summary.home_runs_mean,
            "away_runs_mean": simulation_summary.away_runs_mean,
            "total_mean": simulation_summary.total_mean,
            "home_win_prob": simulation_summary.home_win_prob,
            "away_win_prob": simulation_summary.away_win_prob,
            "runline_home_cover_prob": simulation_summary.runline_home_cover_prob,
            "runline_away_cover_prob": simulation_summary.runline_away_cover_prob,
        }
        context["simulation_players"] = simulation_summary.player_projections
        return self.simulation.build_market_picks(context, odds_game, simulation_summary)

    def _with_weather(self, matchup_entry: dict[str, Any], weather: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(matchup_entry)
        enriched_matchup = dict(matchup_entry.get("matchup", {}))
        enriched_matchup["weather"] = weather
        enriched["matchup"] = enriched_matchup
        return enriched

    def _weather_projection(self, payload: dict[str, Any], venue: str, start_time: str | None) -> dict[str, Any]:
        hourly = (((payload or {}).get("timelines") or {}).get("hourly") or [])
        if not hourly:
            return {
                "temperature_f": 72.0,
                "wind_speed_mph": 0.0,
                "wind_direction": "neutral",
                "humidity": 50.0,
                "wind_blowing_out": False,
                "wind_blowing_in": False,
                "weather_missing": True,
            }

        target_dt = None
        if start_time:
            try:
                target_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
            except ValueError:
                target_dt = None

        selected = hourly[0]
        if target_dt is not None:
            def _distance(entry: dict[str, Any]) -> float:
                raw_time = entry.get("time")
                if not raw_time:
                    return float("inf")
                try:
                    if raw_time.endswith("Z"):
                        candidate = datetime.fromisoformat(raw_time.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
                    else:
                        candidate = datetime.fromisoformat(raw_time)
                        if candidate.tzinfo is None:
                            candidate = candidate.replace(tzinfo=ZoneInfo("America/New_York"))
                    return abs((candidate - target_dt).total_seconds())
                except ValueError:
                    return float("inf")
            selected = min(hourly, key=_distance)

        values = selected.get("values", {})
        return {
            "temperature_f": values.get("temperature", 72.0),
            "wind_speed_mph": values.get("windSpeed", 0.0),
            "wind_direction": values.get("windDirection", "neutral"),
            "humidity": values.get("humidity", 50.0),
            "wind_blowing_out": False,
            "wind_blowing_in": False,
            "weather_missing": False,
        }

    def _build_lineup_card(self, context: dict[str, Any]) -> dict[str, Any]:
        return {
            "matchup": context["matchup"],
            "venue": context["venue"],
            "start_time": context["start_time"],
            "lineup_status": "confirmed" if context["home_lineup_confirmed"] and context["away_lineup_confirmed"] else "projected",
            "weather_stack_score": context["weather"]["weather_stack_score"],
            "weather": {
                "temperature_f": context["weather"]["temperature_f"],
                "wind_speed_mph": context["weather"]["wind_speed_mph"],
                "wind_direction": context["weather"]["wind_direction"],
                "humidity": context["weather"]["humidity"],
                "weather_missing": context["weather"].get("weather_missing", False),
            },
            "projected_total": round(context["home_runs"] + context["away_runs"], 2),
            "simulated_total": round(context["simulation"]["total_mean"], 2),
            "simulated_home_runs": round(context["simulation"]["home_runs_mean"], 2),
            "simulated_away_runs": round(context["simulation"]["away_runs_mean"], 2),
            "home_win_prob": round(context["simulation"]["home_win_prob"], 3),
            "away_win_prob": round(context["simulation"]["away_win_prob"], 3),
            "top_game_picks": context.get("top_game_picks", []),
            "home_pitcher": self._pitcher_card(context["home_team"], context.get("home_pitcher_name", ""), context["home_pitcher_profile"], context["home_pitcher_score"], context.get("home_pitcher_vs_l"), context.get("home_pitcher_vs_r")),
            "away_pitcher": self._pitcher_card(context["away_team"], context.get("away_pitcher_name", ""), context["away_pitcher_profile"], context["away_pitcher_score"], context.get("away_pitcher_vs_l"), context.get("away_pitcher_vs_r")),
            "home_lineup": self._lineup_card(
                context["home_team"],
                context["home_lineup_matchups"],
                context["home_lineup_confirmed"],
                context.get("simulation_players", {}).get("home", {}),
                context.get("away_pitcher_profile"),
            ),
            "away_lineup": self._lineup_card(
                context["away_team"],
                context["away_lineup_matchups"],
                context["away_lineup_confirmed"],
                context.get("simulation_players", {}).get("away", {}),
                context.get("home_pitcher_profile"),
            ),
        }

    def _pitcher_card(
        self,
        team: str,
        name: str,
        profile: dict[str, Any],
        score: dict[str, Any],
        vs_l: dict[str, Any] | None = None,
        vs_r: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        arsenal = profile.get("pitch_arsenal", [])[:6]
        return {
            "team": team,
            "name": name,
            "handedness": profile.get("handedness"),
            "arm_angle": profile.get("arm_angle"),
            "extension": profile.get("extension"),
            "xba": profile.get("xba"),
            "hard_hit_pct": profile.get("hard_hit_pct"),
            "barrel_pct": profile.get("barrel_pct"),
            "ev50": profile.get("ev50"),
            "weighted_run_value": profile.get("weighted_run_value"),
            "weighted_k_pct": profile.get("weighted_k_pct"),
            "weighted_bb_pct": profile.get("weighted_bb_pct"),
            "k_pct": profile.get("k_pct"),
            "bb_pct": profile.get("bb_pct"),
            "recent_k_pct": profile.get("recent_k_pct"),
            "recent_bb_pct": profile.get("recent_bb_pct"),
            "stuff_plus": profile.get("stuff_plus"),
            "movement_score": profile.get("movement_score"),
            "recent_xba": profile.get("recent_xba"),
            "recent_hard_hit_pct": profile.get("recent_hard_hit_pct"),
            "quality_score": score.get("quality_score"),
            "vulnerability_flag": score.get("vulnerability_flag"),
            "arsenal": arsenal,
            "arsenal_vs_l": (vs_l or {}).get("pitch_arsenal", [])[:6],
            "arsenal_vs_r": (vs_r or {}).get("pitch_arsenal", [])[:6],
        }

    @staticmethod
    def _pitch_vs_starter(
        batter_profiles: list[dict[str, Any]],
        pitcher_arsenal: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        batter_by_pitch = {p["pitch_type"]: p for p in batter_profiles}
        top_pitches = sorted(pitcher_arsenal, key=lambda p: p.get("usage_pct", 0), reverse=True)[:3]
        result = []
        for pitch in top_pitches:
            pt = pitch.get("pitch_type", "")
            bp = batter_by_pitch.get(pt, {})
            result.append({
                "pitch_type": pt,
                "pitch_name": pitch.get("pitch_name", pt),
                "usage_pct": pitch.get("usage_pct"),
                "pitcher_xba": pitch.get("xba"),
                "pitcher_k_pct": pitch.get("k_pct"),
                "batter_xwoba": bp.get("xwoba") if bp else None,
                "batter_k_pct": bp.get("k_pct") if bp else None,
                "batter_bb_pct": bp.get("bb_pct") if bp else None,
                "has_batter_data": bool(bp),
            })
        return result

    def _lineup_card(
        self,
        team: str,
        matchup_reports: list[dict[str, Any]],
        confirmed: bool,
        player_simulations: dict[int, dict[str, float]],
        opp_pitcher_profile: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        sorted_reports = sorted(
            matchup_reports,
            key=lambda r: r["matchup"].get("matchup_score") or 0.0,
            reverse=True,
        )
        opp_arsenal = (opp_pitcher_profile or {}).get("pitch_arsenal", [])
        fallback_k = (opp_pitcher_profile or {}).get("weighted_k_pct")
        fallback_bb = (opp_pitcher_profile or {}).get("weighted_bb_pct")
        return {
            "team": team,
            "confirmed": confirmed,
            "label": "Starting lineup" if confirmed else "Projected hitter pool",
            "players": [
                {
                    "slot": report["slot"],
                    "name": report["name"],
                    "handedness": report["profile"].get("handedness"),
                    # prefer pitch-matchup derived stats; fall back to season profile, then pitcher's rate
                    "xwoba": (report.get("pitch_matchup") or {}).get("matchup_xwoba") or report["profile"].get("xwoba"),
                    "hard_hit_pct": (report.get("pitch_matchup") or {}).get("matchup_hard_hit_pct") or report["profile"].get("hard_hit_pct") or None,
                    "bb_pct": (report.get("pitch_matchup") or {}).get("matchup_bb_upside") or report["profile"].get("bb_pct") or fallback_bb,
                    "k_pct": (report.get("pitch_matchup") or {}).get("matchup_k_risk") or report["profile"].get("k_pct") or fallback_k,
                    "has_pitch_matchup": bool(report.get("pitch_matchup")),
                    "pitcher_hand": report.get("pitcher_hand"),
                    "matchup_score": report["matchup"].get("matchup_score"),
                    "handedness_bonus": report["matchup"].get("handedness_bonus"),
                    "pitch_scores": report["matchup"].get("pitch_scores", [])[:4],
                    "best_pitch_matches": report["matchup"].get("pitch_scores", [])[:2],
                    "pitch_profiles": report["profile"].get("pitch_profiles", []),
                    "pitch_vs_starter": self._pitch_vs_starter(
                        report["profile"].get("pitch_profiles", []),
                        opp_arsenal,
                    ),
                    "simulation": player_simulations.get(report["batter_id"], {}),
                }
                for report in sorted_reports
            ],
        }

    def _player_name(self, batter: dict[str, Any]) -> str:
        return (
            batter.get("fullName")
            or batter.get("name")
            or (batter.get("person") or {}).get("fullName")
            or str(batter.get("id") or (batter.get("person") or {}).get("id") or "Unknown")
        )

    def _format_start_time(self, raw: str | None) -> str | None:
        if not raw:
            return None
        try:
            value = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(ZoneInfo("America/New_York"))
            return value.strftime("%-I:%M %p ET")
        except ValueError:
            return raw

    def _market_total_lines(self, odds_game: dict[str, Any] | None) -> set[float]:
        if not odds_game:
            return set()
        lines = set()
        for market in odds_game.get("markets", []):
            if market.get("market_key") != "totals":
                continue
            outcomes = market.get("outcomes", [])
            if outcomes:
                try:
                    lines.add(float(outcomes[0]["point"]))
                except (KeyError, TypeError, ValueError):
                    continue
        return lines

    def _extract_total_lines(self, odds_bundle: list[dict[str, Any]], away_team: str, home_team: str) -> set[float]:
        for game in odds_bundle:
            if game["away_team"] == away_team and game["home_team"] == home_team:
                return self._market_total_lines(game)
        return set()
