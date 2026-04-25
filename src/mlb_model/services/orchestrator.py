from __future__ import annotations

from datetime import date, datetime
from typing import Any

from mlb_model.db import session_scope
from mlb_model.models import PickTier
from mlb_model.providers.baseball import BaseballSavantProvider
from mlb_model.providers.market import MarketProvider
from mlb_model.providers.mlb_stats import MLBStatsProvider
from mlb_model.providers.umpire import UmpireProvider
from mlb_model.providers.weather import TomorrowWeatherProvider
from mlb_model.schemas import DashboardResponse
from mlb_model.services.bullpen_model import BullpenModelService
from mlb_model.services.calibration import CalibrationService
from mlb_model.services.history_model import HistoryModelService
from mlb_model.services.lineup_model import LineupModelService
from mlb_model.services.market_model import MarketSignalService
from mlb_model.services.odds_engine import (
    american_to_decimal,
    classify_edge,
    implied_probability_from_american,
    no_vig_one_sided,
    no_vig_two_sided,
)
from mlb_model.services.offense_model import OffenseModelService
from mlb_model.services.pitcher_model import PitcherModelService
from mlb_model.services.repository import ModelRepository
from mlb_model.services.run_expectation import RunExpectationService
from mlb_model.services.umpire_model import UmpireModelService
from mlb_model.services.weather_model import WeatherModelService


class ModelOrchestrator:
    def __init__(self) -> None:
        self.stats = MLBStatsProvider()
        self.baseball = BaseballSavantProvider()
        self.weather = TomorrowWeatherProvider()
        self.umpire = UmpireProvider()
        self.market = MarketProvider()
        self.pitchers = PitcherModelService()
        self.bullpens = BullpenModelService()
        self.offense = OffenseModelService()
        self.lineups = LineupModelService()
        self.history = HistoryModelService()
        self.weather_model = WeatherModelService()
        self.umpire_model = UmpireModelService()
        self.market_model = MarketSignalService()
        self.runs = RunExpectationService()
        self.calibration = CalibrationService()
        self.repo = ModelRepository()

    async def rebuild_slate(self, slate_date: date) -> dict[str, Any]:
        games = await self.stats.fetch_slate(slate_date)
        with session_scope() as session:
            for game in games:
                home = game["teams"]["home"]["team"]["name"]
                away = game["teams"]["away"]["team"]["name"]
                payload = {
                    "game_id": str(game["gamePk"]),
                    "game_date": slate_date,
                    "start_time": datetime.fromisoformat(game["gameDate"].replace("Z", "+00:00")),
                    "home_team": home,
                    "away_team": away,
                    "ballpark": game.get("venue", {}).get("name", "Unknown"),
                    "park_factor": 1.0,
                    "starter_home": game["teams"]["home"].get("probablePitcher", {}).get("fullName"),
                    "starter_away": game["teams"]["away"].get("probablePitcher", {}).get("fullName"),
                    "lineup_confirmed_home": bool(game["teams"]["home"].get("lineup")),
                    "lineup_confirmed_away": bool(game["teams"]["away"].get("lineup")),
                }
                self.repo.upsert_game(session, payload)
        return {"games_processed": len(games), "date": slate_date.isoformat()}

    async def evaluate_game(
        self,
        game_context: dict[str, Any],
        market_bundle: dict[str, Any],
        persist: bool = True,
    ) -> list[dict[str, Any]]:
        home_pitcher = self.pitchers.score_pitcher(game_context["home_pitcher_stats"])
        away_pitcher = self.pitchers.score_pitcher(game_context["away_pitcher_stats"])
        home_bullpen = self.bullpens.score_bullpen(game_context["home_bullpen"])
        away_bullpen = self.bullpens.score_bullpen(game_context["away_bullpen"])
        home_lineup = self.lineups.score_lineup(
            game_context["home_lineup"]["confirmed"],
            game_context["home_lineup"]["projected"],
            game_context["home_lineup"].get("top_bats", []),
        )
        away_lineup = self.lineups.score_lineup(
            game_context["away_lineup"]["confirmed"],
            game_context["away_lineup"]["projected"],
            game_context["away_lineup"].get("top_bats", []),
        )
        home_history = self.history.score_history(
            game_context.get("home_pitcher_vs_team"),
            game_context.get("recent_series_history"),
        )
        away_history = self.history.score_history(
            game_context.get("away_pitcher_vs_team"),
            game_context.get("recent_series_history"),
        )
        home_offense = self.offense.score_offense(
            game_context["home_offense"],
            home_lineup,
            game_context["away_pitcher_hand"],
            game_context["park_factor"],
        )
        away_offense = self.offense.score_offense(
            game_context["away_offense"],
            away_lineup,
            game_context["home_pitcher_hand"],
            game_context["park_factor"],
        )
        weather = self.weather_model.score_weather(game_context, game_context["weather"])
        umpire = self.umpire_model.score_umpire(game_context["umpire"])

        home_runs = self.runs.expected_runs(
            team=game_context["home_team"],
            offense_score=home_offense["offense_score"] * home_history["history_adjustment"],
            starter_quality_score=away_pitcher["quality_score"],
            bullpen_score=away_bullpen["bullpen_score"],
            park_factor=game_context["park_factor"],
            weather_multiplier=weather["weather_multiplier"],
            umpire_factor=umpire["umpire_factor"],
            starter_ip_projection=away_pitcher["starter_ip_projection"],
        )
        away_runs = self.runs.expected_runs(
            team=game_context["away_team"],
            offense_score=away_offense["offense_score"] * away_history["history_adjustment"],
            starter_quality_score=home_pitcher["quality_score"],
            bullpen_score=home_bullpen["bullpen_score"],
            park_factor=game_context["park_factor"],
            weather_multiplier=weather["weather_multiplier"],
            umpire_factor=umpire["umpire_factor"],
            starter_ip_projection=home_pitcher["starter_ip_projection"],
        )

        projected_total = home_runs.expected_runs + away_runs.expected_runs
        margin_projection = home_runs.expected_runs - away_runs.expected_runs
        results: list[dict[str, Any]] = []
        module_signals = [
            {
                "game_id": game_context["game_id"],
                "team": game_context["home_team"],
                "module_name": "pitcher",
                "score": away_pitcher["quality_score"],
                "summary": f"Away starter score {away_pitcher['quality_score']}",
                "payload": away_pitcher,
            },
            {
                "game_id": game_context["game_id"],
                "team": game_context["away_team"],
                "module_name": "pitcher",
                "score": home_pitcher["quality_score"],
                "summary": f"Home starter score {home_pitcher['quality_score']}",
                "payload": home_pitcher,
            },
            {
                "game_id": game_context["game_id"],
                "team": game_context["home_team"],
                "module_name": "bullpen",
                "score": away_bullpen["bullpen_score"],
                "summary": f"Away bullpen score {away_bullpen['bullpen_score']}",
                "payload": away_bullpen,
            },
            {
                "game_id": game_context["game_id"],
                "team": game_context["away_team"],
                "module_name": "bullpen",
                "score": home_bullpen["bullpen_score"],
                "summary": f"Home bullpen score {home_bullpen['bullpen_score']}",
                "payload": home_bullpen,
            },
            {
                "game_id": game_context["game_id"],
                "team": game_context["home_team"],
                "module_name": "offense",
                "score": home_offense["offense_score"],
                "summary": f"Home offense score {home_offense['offense_score']}",
                "payload": {**home_offense, **home_lineup, **home_history},
            },
            {
                "game_id": game_context["game_id"],
                "team": game_context["away_team"],
                "module_name": "offense",
                "score": away_offense["offense_score"],
                "summary": f"Away offense score {away_offense['offense_score']}",
                "payload": {**away_offense, **away_lineup, **away_history},
            },
            {
                "game_id": game_context["game_id"],
                "team": "both",
                "module_name": "weather",
                "score": weather["weather_stack_score"],
                "summary": f"Weather stack {weather['weather_stack_score']}",
                "payload": weather,
            },
            {
                "game_id": game_context["game_id"],
                "team": "both",
                "module_name": "umpire",
                "score": umpire["umpire_factor"] * 100,
                "summary": f"Umpire factor {umpire['umpire_factor']}",
                "payload": umpire,
            },
        ]

        for market in market_bundle["markets"]:
            side_prob = implied_probability_from_american(market["american_odds"])
            decimal_odds = american_to_decimal(market["american_odds"])
            if market.get("pair_raw_probability") is not None:
                no_vig_prob, _ = no_vig_two_sided(side_prob, market["pair_raw_probability"])
            else:
                no_vig_prob = no_vig_one_sided(side_prob)

            if market["market_type"] == "game_total":
                model_probability = self.runs.game_total_probability(projected_total, market["line"])
                pick_side = "over" if projected_total > market["line"] else "under"
            elif market["market_type"] == "first_five_total":
                first_five_projection = (
                    home_runs.expected_runs * (away_pitcher["starter_ip_projection"] / 9)
                    + away_runs.expected_runs * (home_pitcher["starter_ip_projection"] / 9)
                )
                model_probability = self.runs.game_total_probability(first_five_projection, market["line"])
                pick_side = "over" if first_five_projection > market["line"] else "under"
            elif market["market_type"] == "team_total":
                team_projection = home_runs.expected_runs if market["team"] == game_context["home_team"] else away_runs.expected_runs
                model_probability = self.runs.game_total_probability(team_projection, market["line"])
                pick_side = "over" if team_projection > market["line"] else "under"
            else:
                model_probability = self.runs.runline_cover_probability(abs(margin_projection))
                pick_side = market["side"]

            decision = classify_edge(model_probability, no_vig_prob, market["american_odds"], decimal_odds)
            market_summary = self.market_model.summarize_market(market.get("snapshots", []), decision.edge)
            top_features = sorted(
                [
                    {"feature": "Away starter vulnerability", "value": 100 - away_pitcher["quality_score"], "direction": "up"},
                    {"feature": "Home bullpen depletion", "value": 100 - home_bullpen["bullpen_score"], "direction": "up"},
                    {"feature": "Weather stack", "value": weather["weather_stack_score"], "direction": "up"},
                ],
                key=lambda item: item["value"],
                reverse=True,
            )[:3]
            thin_consensus = sum(feature["value"] >= 10 for feature in top_features) < 3
            if decision.tier in {PickTier.STRONG, PickTier.MODERATE, PickTier.MONITOR}:
                results.append(
                    {
                        "game_id": game_context["game_id"],
                        "market_type": market["market_type"],
                        "market_key": market["market_key"],
                        "pick_side": pick_side,
                        "line": market["line"],
                        "american_odds": market["american_odds"],
                        "decimal_odds": decimal_odds,
                        "model_probability": round(model_probability, 4),
                        "no_vig_probability": round(no_vig_prob, 4),
                        "edge": round(decision.edge, 4),
                        "tier": decision.tier.value,
                        "bankroll_fraction": round(decision.bankroll_fraction, 4),
                        "thin_consensus": thin_consensus,
                        "top_features": top_features,
                        "bullpen_summary": away_bullpen if market.get("team") == game_context["home_team"] else home_bullpen,
                        "weather_stack_score": weather["weather_stack_score"],
                        "line_movement_summary": market_summary,
                        "trap_warning": market_summary["trap_warning"],
                    }
                )
        if persist:
            with session_scope() as session:
                self.repo.add_module_signals(session, module_signals)
                for result in results:
                    self.repo.replace_pick(session, result)
        return results

    async def dashboard(self, slate_date: date) -> DashboardResponse:
        with session_scope() as session:
            picks = self.repo.get_today_picks(session, slate_date)
            last_50 = self.repo.recent_clv(session, 50)
            last_200 = self.repo.recent_clv(session, 200)
        bankroll = 100.0
        wins = sum(1 for pick in picks if pick.result == "win")
        losses = sum(1 for pick in picks if pick.result == "loss")
        return DashboardResponse(
            as_of=datetime.utcnow(),
            today=slate_date,
            picks=[pick.model_dump() for pick in picks],
            rolling_clv_last_50=last_50,
            rolling_clv_last_200=last_200,
            module_performance=[],
            bankroll_tracker={"starting_bankroll": bankroll, "wins": wins, "losses": losses},
        )

    async def recalibrate_weights(self) -> dict[str, Any]:
        with session_scope() as session:
            result = self.calibration.optimize(session)
        return result
