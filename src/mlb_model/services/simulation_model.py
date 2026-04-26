from __future__ import annotations

import math
import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from mlb_model.config import get_settings
from mlb_model.services.odds_engine import american_to_decimal, classify_edge, implied_probability_from_american, no_vig_two_sided
from mlb_model.utils import clamp


settings = get_settings()
model_settings = settings.load_model_settings()


BASE_EVENT_RATES = {
    "bb": 0.076,
    "k": 0.228,
    "1b": 0.132,
    "2b": 0.041,
    "3b": 0.003,
    "hr": 0.026,
}

PARK_FACTORS = {
    "Coors Field": 1.15,
    "Fenway Park": 1.05,
    "Yankee Stadium": 1.03,
    "Wrigley Field": 1.02,
    "Citizens Bank Park": 1.02,
    "Great American Ball Park": 1.04,
    "Dodger Stadium": 0.97,
    "Oracle Park": 0.92,
    "T-Mobile Park": 0.94,
    "Petco Park": 0.93,
    "loanDepot park": 0.93,
    "Kauffman Stadium": 0.98,
    "Tropicana Field": 0.94,
    "Comerica Park": 0.96,
    "Citi Field": 0.97,
    "Nationals Park": 0.99,
    "Angel Stadium": 0.98,
    "Progressive Field": 0.99,
    "Busch Stadium": 0.95,
    "Chase Field": 1.01,
}


@dataclass(slots=True)
class SimulationSummary:
    home_runs_mean: float
    away_runs_mean: float
    total_mean: float
    home_win_prob: float
    away_win_prob: float
    runline_home_cover_prob: float
    runline_away_cover_prob: float
    total_over_probabilities: dict[float, float]
    team_total_over_probabilities: dict[str, dict[float, float]]
    player_projections: dict[str, dict[int, dict[str, float]]]


class SimulationModelService:
    def __init__(self, trials: int = 10000) -> None:
        self.trials = trials
        self.random = random.Random(42)

    def simulate_game(
        self,
        home_lineup: list[dict[str, Any]],
        away_lineup: list[dict[str, Any]],
        home_pitcher: dict[str, Any],
        away_pitcher: dict[str, Any],
        total_lines: set[float] | None = None,
        home_target_runs: float = 4.4,
        away_target_runs: float = 4.4,
        home_bullpen_score: float = 65.0,
        away_bullpen_score: float = 65.0,
        home_starter_ip: float = 5.6,
        away_starter_ip: float = 5.6,
        venue: str | None = None,
    ) -> SimulationSummary:
        total_lines = total_lines or set()
        home_runs: list[int] = []
        away_runs: list[int] = []
        player_accumulators = {
            "home": defaultdict(lambda: defaultdict(float)),
            "away": defaultdict(lambda: defaultdict(float)),
        }

        home_starter_outs = int(round(home_starter_ip * 3))
        away_starter_outs = int(round(away_starter_ip * 3))
        park_factor = PARK_FACTORS.get(venue or "", 1.0)

        for _ in range(self.trials):
            away_score, away_player_stats = self._simulate_team_game(
                lineup=away_lineup,
                starter_profile=home_pitcher,
                bullpen_score=home_bullpen_score,
                target_runs=away_target_runs,
                starter_outs=home_starter_outs,
                park_factor=park_factor,
            )
            home_score, home_player_stats = self._simulate_team_game(
                lineup=home_lineup,
                starter_profile=away_pitcher,
                bullpen_score=away_bullpen_score,
                target_runs=home_target_runs,
                starter_outs=away_starter_outs,
                park_factor=park_factor,
            )
            self._accumulate_player_stats(player_accumulators["away"], away_player_stats)
            self._accumulate_player_stats(player_accumulators["home"], home_player_stats)
            away_runs.append(away_score)
            home_runs.append(home_score)

        raw_home_mean = sum(home_runs) / len(home_runs) if home_runs else 0.0
        raw_away_mean = sum(away_runs) / len(away_runs) if away_runs else 0.0
        home_scale = clamp(home_target_runs / max(raw_home_mean, 0.1), 0.45, 1.0)
        away_scale = clamp(away_target_runs / max(raw_away_mean, 0.1), 0.45, 1.0)
        adjusted_home_runs = [score * home_scale for score in home_runs]
        adjusted_away_runs = [score * away_scale for score in away_runs]
        over_counts = {line: 0 for line in total_lines}
        team_totals = {
            "home": {line / 2.0: 0 for line in total_lines},
            "away": {line / 2.0: 0 for line in total_lines},
        }
        home_wins = 0.0
        away_wins = 0.0
        home_cover = 0
        away_cover = 0

        for home_score, away_score in zip(adjusted_home_runs, adjusted_away_runs):
            total = home_score + away_score
            for line in total_lines:
                if total > line:
                    over_counts[line] += 1
                if home_score > (line / 2.0):
                    team_totals["home"][line / 2.0] += 1
                if away_score > (line / 2.0):
                    team_totals["away"][line / 2.0] += 1
            if home_score > away_score:
                home_wins += 1
            elif away_score > home_score:
                away_wins += 1
            else:
                if self.random.random() < 0.53:
                    home_wins += 1
                else:
                    away_wins += 1
            if (home_score - away_score) >= 2:
                home_cover += 1
            if (away_score - home_score) >= 2:
                away_cover += 1

        home_mean = sum(adjusted_home_runs) / len(adjusted_home_runs) if adjusted_home_runs else 0.0
        away_mean = sum(adjusted_away_runs) / len(adjusted_away_runs) if adjusted_away_runs else 0.0
        return SimulationSummary(
            home_runs_mean=round(home_mean, 3),
            away_runs_mean=round(away_mean, 3),
            total_mean=round(home_mean + away_mean, 3),
            home_win_prob=round(home_wins / self.trials, 4),
            away_win_prob=round(away_wins / self.trials, 4),
            runline_home_cover_prob=round(home_cover / self.trials, 4),
            runline_away_cover_prob=round(away_cover / self.trials, 4),
            total_over_probabilities={line: round(count / self.trials, 4) for line, count in over_counts.items()},
            team_total_over_probabilities={
                side: {line: round(count / self.trials, 4) for line, count in counts.items()}
                for side, counts in team_totals.items()
            },
            player_projections={
                "home": self._finalize_player_stats(player_accumulators["home"], home_scale),
                "away": self._finalize_player_stats(player_accumulators["away"], away_scale),
            },
        )

    def build_market_picks(
        self,
        context: dict[str, Any],
        odds_game: dict[str, Any] | None,
        simulation: SimulationSummary,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not odds_game:
            return [], []

        candidates: list[dict[str, Any]] = []
        total_lines = simulation.total_over_probabilities
        for market in odds_game.get("markets", []):
            outcomes = market.get("outcomes", [])
            if len(outcomes) != 2:
                continue
            if market["market_key"] == "h2h":
                home_outcome = next((o for o in outcomes if o["name"] == context["home_team"]), outcomes[0])
                away_outcome = next((o for o in outcomes if o["name"] == context["away_team"]), outcomes[1])
                candidates.extend(
                    self._two_sided_candidates(
                        context,
                        market_type="moneyline",
                        line=0.0,
                        left_name=context["home_team"],
                        left_odds=int(home_outcome["price"]),
                        left_prob=simulation.home_win_prob,
                        right_name=context["away_team"],
                        right_odds=int(away_outcome["price"]),
                        right_prob=simulation.away_win_prob,
                    )
                )
            elif market["market_key"] == "totals":
                first = outcomes[0]
                second = outcomes[1]
                line = float(first["point"])
                over_prob = total_lines.get(line, self._fallback_total_probability(simulation.total_mean, line))
                under_prob = 1 - over_prob
                candidates.extend(
                    self._two_sided_candidates(
                        context,
                        market_type="game_total",
                        line=line,
                        left_name="Over",
                        left_odds=int(first["price"]),
                        left_prob=over_prob,
                        right_name="Under",
                        right_odds=int(second["price"]),
                        right_prob=under_prob,
                    )
                )
            elif market["market_key"] == "spreads":
                home_outcome = next((o for o in outcomes if o["name"] == context["home_team"]), outcomes[0])
                away_outcome = next((o for o in outcomes if o["name"] == context["away_team"]), outcomes[1])
                candidates.extend(
                    self._two_sided_candidates(
                        context,
                        market_type="runline",
                        line=float(home_outcome.get("point", 0.0)),
                        left_name=context["home_team"],
                        left_odds=int(home_outcome["price"]),
                        left_prob=simulation.runline_home_cover_prob,
                        right_name=context["away_team"],
                        right_odds=int(away_outcome["price"]),
                        right_prob=simulation.runline_away_cover_prob,
                    )
                )

        eligible = [
            pick
            for pick in candidates
            if pick["edge"] > 0
            and pick["tier"] in {"strong", "moderate", "monitor"}
        ]
        ranked = sorted(
            eligible,
            key=lambda item: (
                item["market_type"] != "game_total",
                item["market_type"] != "first_five_total",
                item["market_type"] != "moneyline",
                item["tier"] != "strong",
                item["tier"] != "moderate",
                -item["edge"],
                -item["model_probability"],
            ),
        )
        daily = [
            pick
            for pick in ranked
            if pick["tier"] in {"strong", "moderate"}
            and (
                pick["market_type"] == "game_total"
                or pick["market_type"] == "first_five_total"
                or (pick["market_type"] == "moneyline" and pick["edge"] >= model_settings.edge_thresholds["moneyline_min"])
            )
            and pick["market_type"] != "runline"
        ][:10]
        matchup_ranked = sorted(
            [pick for pick in candidates if pick["tier"] != "block"],
            key=lambda item: (
                item["market_type"] != "game_total",
                item["market_type"] != "moneyline",
                item["market_type"] == "runline",
                item["edge"] <= 0,
                -item["model_probability"],
                -item["edge"],
            ),
        )
        return daily, matchup_ranked[:5]

    def _simulate_team_game(
        self,
        lineup: list[dict[str, Any]],
        starter_profile: dict[str, Any],
        bullpen_score: float,
        target_runs: float,
        starter_outs: int,
        park_factor: float,
    ) -> tuple[int, dict[int, dict[str, float]]]:
        if not lineup:
            return 0, {}
        runs = 0
        outs = 0
        bases = [None, None, None]
        batter_index = 0
        player_stats: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))

        while outs < 27:
            batter = lineup[batter_index % len(lineup)]
            batter_id = int(batter.get("batter_id", 0))
            phase = "starter" if outs < starter_outs else "bullpen"
            probs = self._plate_appearance_distribution(
                batter=batter,
                opposing_pitcher=starter_profile,
                target_runs=target_runs,
                bullpen_score=bullpen_score,
                phase=phase,
                park_factor=park_factor,
            )
            outcome = self._sample_outcome(probs)
            player_stats[batter_id]["pa"] += 1
            if outcome not in {"bb"}:
                player_stats[batter_id]["ab"] += 1
            if outcome == "k":
                player_stats[batter_id]["k"] += 1
            if outcome == "bb":
                player_stats[batter_id]["bb"] += 1
            if outcome == "hr":
                player_stats[batter_id]["hr"] += 1
            if outcome in {"1b", "2b", "3b", "hr"}:
                player_stats[batter_id]["hits"] += 1
            player_stats[batter_id]["total_bases"] += {"1b": 1, "2b": 2, "3b": 3, "hr": 4}.get(outcome, 0)

            runs_scored, new_outs, bases, batter_rbi = self._apply_outcome(outcome, bases, batter_id)
            player_stats[batter_id]["rbi"] += batter_rbi
            runs += runs_scored
            outs += new_outs
            batter_index += 1
        return runs, player_stats

    def _plate_appearance_distribution(
        self,
        batter: dict[str, Any],
        opposing_pitcher: dict[str, Any],
        target_runs: float,
        bullpen_score: float,
        phase: str,
        park_factor: float,
    ) -> dict[str, float]:
        profile = batter.get("profile", {})
        matchup = batter.get("matchup", {})
        blend_xwoba = self._blend(profile.get("xwoba", 0.315), profile.get("recent_xwoba", profile.get("xwoba", 0.315)), 0.6)
        blend_hard_hit = self._blend(profile.get("hard_hit_pct", 0.36), profile.get("recent_hard_hit_pct", profile.get("hard_hit_pct", 0.36)), 0.6)
        blend_k = self._blend(profile.get("k_pct", 0.22), profile.get("recent_k_pct", profile.get("k_pct", 0.22)), 0.6)
        blend_bb = self._blend(profile.get("bb_pct", 0.08), profile.get("recent_bb_pct", profile.get("bb_pct", 0.08)), 0.6)
        matchup_score = float(matchup.get("matchup_score", 50.0))
        pitcher_xba = float(opposing_pitcher.get("xba", 0.245))
        pitcher_barrel = float(opposing_pitcher.get("barrel_pct", 0.08))
        pitcher_hard_hit = float(opposing_pitcher.get("hard_hit_pct", 0.36))
        pitcher_k = float(opposing_pitcher.get("weighted_k_pct", 0.22))
        pitcher_bb = float(opposing_pitcher.get("weighted_bb_pct", 0.08))
        weather_multiplier = float((matchup.get("weather") or {}).get("weather_multiplier", 1.0))
        pitcher_sample = max(int(opposing_pitcher.get("sample_bbe", 0)), 1)
        shrinkage = min(pitcher_sample / (pitcher_sample + 150), 0.82)
        regressed_pitcher_xba = (pitcher_xba * shrinkage) + (0.290 * (1 - shrinkage))
        regressed_pitcher_barrel = (pitcher_barrel * shrinkage) + (0.080 * (1 - shrinkage))
        regressed_pitcher_hard_hit = (pitcher_hard_hit * shrinkage) + (0.360 * (1 - shrinkage))

        offense_factor = clamp((target_runs / 4.5) * park_factor, 0.84, 1.08)
        matchup_factor = clamp(1 + ((matchup_score - 50) / 320), 0.90, 1.10)
        xba_factor = clamp(
            1
            + ((regressed_pitcher_xba - 0.290) * 2.0)
            + ((blend_xwoba - 0.318) * 0.8),
            0.88,
            1.10,
        )
        barrel_factor = clamp(
            1
            + ((regressed_pitcher_barrel - 0.080) * 1.2)
            + ((blend_hard_hit - 0.360) * 0.45),
            0.88,
            1.10,
        )
        contact_factor = clamp(offense_factor * matchup_factor * max(xba_factor, barrel_factor), 0.82, 1.10)
        hr_factor = clamp(
            offense_factor
            * matchup_factor
            * (1 + ((blend_hard_hit - 0.36) * 0.95))
            * (1 + ((regressed_pitcher_barrel - 0.08) * 1.10))
            * (1 + ((weather_multiplier - 1.0) * 0.35)),
            0.75,
            1.15,
        )
        k_factor = clamp(
            (1 + ((blend_k - 0.22) * 1.0))
            * (1 + ((pitcher_k - 0.22) * 0.9))
            * (1 - ((matchup_score - 50) / 500)),
            0.80,
            1.22,
        )
        bb_factor = clamp(
            (1 + ((blend_bb - 0.08) * 1.3))
            * (1 + ((pitcher_bb - 0.08) * 0.8)),
            0.80,
            1.18,
        )

        if phase == "bullpen":
            bullpen_attack = clamp(1 + ((50 - bullpen_score) / 140), 0.82, 1.20)
            contact_factor *= bullpen_attack
            hr_factor *= bullpen_attack
            k_factor *= clamp(1 - ((50 - bullpen_score) / 220), 0.90, 1.08)
            bb_factor *= clamp(1 + ((50 - bullpen_score) / 240), 0.92, 1.12)

        bb_rate = BASE_EVENT_RATES["bb"] * bb_factor
        k_rate = BASE_EVENT_RATES["k"] * k_factor
        hr_rate = BASE_EVENT_RATES["hr"] * hr_factor
        single_rate = BASE_EVENT_RATES["1b"] * contact_factor
        double_rate = BASE_EVENT_RATES["2b"] * contact_factor
        triple_rate = BASE_EVENT_RATES["3b"] * contact_factor * 0.95

        total_non_out = bb_rate + k_rate + hr_rate + single_rate + double_rate + triple_rate
        if total_non_out > 0.50:
            scale = 0.50 / total_non_out
            bb_rate *= scale
            k_rate *= scale
            hr_rate *= scale
            single_rate *= scale
            double_rate *= scale
            triple_rate *= scale
        out_rate = 1 - (bb_rate + k_rate + hr_rate + single_rate + double_rate + triple_rate)
        out_rate = clamp(out_rate, 0.48, 0.76)
        total = bb_rate + k_rate + hr_rate + single_rate + double_rate + triple_rate + out_rate

        return {
            "bb": bb_rate / total,
            "k": k_rate / total,
            "1b": single_rate / total,
            "2b": double_rate / total,
            "3b": triple_rate / total,
            "hr": hr_rate / total,
            "out": out_rate / total,
        }

    def _sample_outcome(self, probabilities: dict[str, float]) -> str:
        draw = self.random.random()
        cumulative = 0.0
        for outcome, probability in probabilities.items():
            cumulative += probability
            if draw <= cumulative:
                return outcome
        return "out"

    def _apply_outcome(self, outcome: str, bases: list[int | None], batter_id: int) -> tuple[int, int, list[int | None], int]:
        first, second, third = bases
        runs = 0
        rbi = 0
        outs = 0
        if outcome in {"k", "out"}:
            return 0, 1, bases, 0
        if outcome == "bb":
            if first is not None and second is not None and third is not None:
                runs += 1
                rbi += 1
                return runs, outs, [batter_id, first, second], rbi
            if first is not None and second is not None:
                return runs, outs, [batter_id, first, second], rbi
            if first is not None:
                return runs, outs, [batter_id, first, third], rbi
            return runs, outs, [batter_id, second, third], rbi
        if outcome == "1b":
            if third is not None:
                runs += 1
                rbi += 1
            return runs, outs, [batter_id, first, second], rbi
        if outcome == "2b":
            if second is not None:
                runs += 1
                rbi += 1
            if third is not None:
                runs += 1
                rbi += 1
            return runs, outs, [None, batter_id, first], rbi
        if outcome == "3b":
            for runner in (first, second, third):
                if runner is not None:
                    runs += 1
                    rbi += 1
            return runs, outs, [None, None, batter_id], rbi
        if outcome == "hr":
            for runner in (first, second, third):
                if runner is not None:
                    runs += 1
                    rbi += 1
            runs += 1
            rbi += 1
            return runs, outs, [None, None, None], rbi
        return runs, outs, bases, rbi

    def _two_sided_candidates(
        self,
        context: dict[str, Any],
        market_type: str,
        line: float,
        left_name: str,
        left_odds: int,
        left_prob: float,
        right_name: str,
        right_odds: int,
        right_prob: float,
    ) -> list[dict[str, Any]]:
        left_raw = implied_probability_from_american(left_odds)
        right_raw = implied_probability_from_american(right_odds)
        left_no_vig, right_no_vig = no_vig_two_sided(left_raw, right_raw)
        calibrated_left = self._calibrate_market_probability(context, market_type, left_prob, left_no_vig)
        calibrated_right = self._calibrate_market_probability(context, market_type, right_prob, right_no_vig)
        left_decision = classify_edge(calibrated_left, left_no_vig, left_odds, american_to_decimal(left_odds))
        right_decision = classify_edge(calibrated_right, right_no_vig, right_odds, american_to_decimal(right_odds))
        return [
            self._candidate(context, market_type, left_name, line, left_odds, calibrated_left, left_no_vig, left_decision),
            self._candidate(context, market_type, right_name, line, right_odds, calibrated_right, right_no_vig, right_decision),
        ]

    def _calibrate_market_probability(
        self,
        context: dict[str, Any],
        market_type: str,
        raw_probability: float,
        no_vig_probability: float,
    ) -> float:
        shrink = 0.72
        weather = context.get("weather", {})
        if weather.get("weather_missing"):
            shrink -= 0.18
        if not context.get("home_lineup_confirmed") or not context.get("away_lineup_confirmed"):
            shrink -= 0.20
        if market_type in {"moneyline", "runline"}:
            shrink -= 0.05
        if context.get("home_bullpen", {}).get("bullpen_score", 65.0) == 65.0 and context.get("away_bullpen", {}).get("bullpen_score", 65.0) == 65.0:
            shrink -= 0.06
        if weather.get("weather_stack_score", 0.0) >= 2.0:
            shrink += 0.05
        if context.get("home_lineup_confirmed") and context.get("away_lineup_confirmed"):
            shrink += 0.08
        shrink = clamp(shrink, 0.22, 0.85)
        return clamp(no_vig_probability + (raw_probability - no_vig_probability) * shrink, 0.02, 0.98)

    def _candidate(
        self,
        context: dict[str, Any],
        market_type: str,
        side: str,
        line: float,
        odds: int,
        model_prob: float,
        no_vig_prob: float,
        decision: Any,
    ) -> dict[str, Any]:
        model_prob = clamp(model_prob, 0.02, 0.98)
        home_vulnerability = float(
            context.get("home_run_context", {}).get("pitcher_vulnerability")
            or context.get("home_pitcher_matchup", {}).get("vulnerability_score")
            or context.get("home_pitcher_score", {}).get("pitcher_quality_score")
            or 50.0
        )
        away_vulnerability = float(
            context.get("away_run_context", {}).get("pitcher_vulnerability")
            or context.get("away_pitcher_matchup", {}).get("vulnerability_score")
            or context.get("away_pitcher_score", {}).get("pitcher_quality_score")
            or 50.0
        )
        return {
            "matchup": context["matchup"],
            "market_type": market_type,
            "pick": side,
            "line": line,
            "american_odds": odds,
            "model_probability": round(model_prob, 4),
            "no_vig_probability": round(no_vig_prob, 4),
            "edge": round(decision.edge, 4),
            "tier": decision.tier.value,
            "bankroll_fraction": round(decision.bankroll_fraction, 4),
            "top_features": [
                {"feature": "Simulated total", "value": context["simulation"]["total_mean"], "direction": "up"},
                {"feature": "Weather stack", "value": context["weather"]["weather_stack_score"], "direction": "up"},
                {"feature": "Pitcher vulnerability", "value": max(home_vulnerability, away_vulnerability), "direction": "up"},
            ],
            "weather_stack_score": context["weather"]["weather_stack_score"],
            "start_time": context["start_time"],
            "lineup_status": "confirmed" if context.get("home_lineup_confirmed") and context.get("away_lineup_confirmed") else "projected",
            "simulation_trials": self.trials,
            "specific_blurb": self._build_specific_pick_blurb(
                context=context,
                market_type=market_type,
                side=side,
                line=line,
                model_prob=model_prob,
                no_vig_prob=no_vig_prob,
            ),
        }

    def _build_specific_pick_blurb(
        self,
        context: dict[str, Any],
        market_type: str,
        side: str,
        line: float,
        model_prob: float,
        no_vig_prob: float,
    ) -> str:
        home_leaders = self._lineup_matchup_leaders(context.get("home_lineup_matchups", []), descending=True)
        away_leaders = self._lineup_matchup_leaders(context.get("away_lineup_matchups", []), descending=True)
        home_laggards = self._lineup_matchup_leaders(context.get("home_lineup_matchups", []), descending=False)
        away_laggards = self._lineup_matchup_leaders(context.get("away_lineup_matchups", []), descending=False)

        home_team = context.get("home_team", "Home")
        away_team = context.get("away_team", "Away")
        home_pitcher = context.get("home_pitcher_name", home_team)
        away_pitcher = context.get("away_pitcher_name", away_team)
        home_offense = float((context.get("home_offense") or {}).get("offense_score", 50.0))
        away_offense = float((context.get("away_offense") or {}).get("offense_score", 50.0))
        total_mean = float((context.get("simulation") or {}).get("total_mean", 0.0))
        home_mean = float((context.get("simulation") or {}).get("home_runs_mean", 0.0))
        away_mean = float((context.get("simulation") or {}).get("away_runs_mean", 0.0))
        weather = context.get("weather", {}) or {}
        edge_pts = (model_prob - no_vig_prob) * 100
        weather_tail = self._weather_tail(weather)

        if market_type == "game_total" and side == "Over":
            attack_team = home_team if home_offense >= away_offense else away_team
            attack_leaders = home_leaders if home_offense >= away_offense else away_leaders
            target_pitcher = away_pitcher if home_offense >= away_offense else home_pitcher
            target_profile = context.get("away_pitcher_profile") if home_offense >= away_offense else context.get("home_pitcher_profile")
            leaders_text = self._leaders_text(attack_leaders)
            pitcher_text = self._pitcher_contact_text(target_pitcher, target_profile)
            return f"{leaders_text} profile best into {target_pitcher}'s pitch mix for {attack_team}, and {pitcher_text}; the sim lands at {total_mean:.1f} runs versus {line:g}{weather_tail}"

        if market_type == "game_total" and side == "Under":
            strong_arms = f"{away_pitcher} and {home_pitcher}"
            suppressors = self._leaders_text(home_laggards, fallback=home_team) + " / " + self._leaders_text(away_laggards, fallback=away_team)
            return f"{strong_arms} project to keep quality contact down early, and the softest batter matchups on the board are {suppressors}; the sim sits at {total_mean:.1f} against {line:g}{weather_tail}"

        if market_type == "moneyline":
            team = side
            is_home = team == home_team
            team_mean = home_mean if is_home else away_mean
            opp_mean = away_mean if is_home else home_mean
            starter = home_pitcher if is_home else away_pitcher
            leaders = home_leaders if is_home else away_leaders
            opp_pitcher = away_pitcher if is_home else home_pitcher
            return f"{starter} gives {team} the cleaner starter setup, and {self._leaders_text(leaders, fallback=team)} carry the best hitter-vs-arsenal edges against {opp_pitcher}; the sim has it {team_mean:.1f} to {opp_mean:.1f} with a {edge_pts:.1f}-point edge"

        if market_type == "runline":
            team = side
            is_home = team == home_team
            leaders = home_leaders if is_home else away_leaders
            margin = abs(home_mean - away_mean)
            return f"{self._leaders_text(leaders, fallback=team)} give {team} the strongest matchup cluster in this game, and the sim creates about a {margin:.1f}-run gap on average"

        return f"The model prices this side {edge_pts:.1f} percentage points above no-vig market odds based on the current hitter-pitcher matchup set."

    def _lineup_matchup_leaders(
        self,
        matchups: list[dict[str, Any]],
        descending: bool = True,
        count: int = 2,
    ) -> list[dict[str, Any]]:
        ranked: list[dict[str, Any]] = []
        for entry in matchups:
            matchup = entry.get("matchup") or {}
            pitch_matchup = entry.get("pitch_matchup") or {}
            profile = entry.get("profile") or {}
            score = float(matchup.get("matchup_score") or 0.0)
            xwoba = float(pitch_matchup.get("matchup_xwoba") or profile.get("xwoba") or 0.0)
            ranked.append(
                {
                    "name": entry.get("name", "Batter"),
                    "score": score,
                    "xwoba": xwoba,
                }
            )
        ranked.sort(key=lambda item: item["score"], reverse=descending)
        return ranked[:count]

    def _leaders_text(
        self,
        leaders: list[dict[str, Any]],
        fallback: str = "the lineup",
    ) -> str:
        if not leaders:
            return fallback
        if len(leaders) == 1:
            return f"{leaders[0]['name']} ({leaders[0]['score']:.0f} matchup)"
        first, second = leaders[0], leaders[1]
        return f"{first['name']} and {second['name']} ({first['score']:.0f}/{second['score']:.0f} matchup scores)"

    def _pitcher_contact_text(self, pitcher_name: str, profile: dict[str, Any] | None) -> str:
        profile = profile or {}
        xba = float(profile.get("xba") or 0.255)
        hh = float(profile.get("hard_hit_pct") or 0.375)
        return f"{pitcher_name} is carrying a .{int(round(xba * 1000)):03d} xBA allowed and {hh * 100:.0f}% hard-hit rate"

    def _weather_tail(self, weather: dict[str, Any]) -> str:
        wind = float(weather.get("wind_speed_mph", 0.0) or 0.0)
        stack = float(weather.get("weather_stack_score", 0.0) or 0.0)
        if wind >= 12 or stack >= 2.0:
            return f", with weather adding another push ({wind:.0f} mph wind)"
        return ""

    def _fallback_total_probability(self, mean_total: float, line: float) -> float:
        z = (mean_total - line) / 2.0
        return clamp(1 / (1 + math.exp(-z)), 0.05, 0.95)

    def _blend(self, season_value: float, recent_value: float, recent_weight: float) -> float:
        season_value = float(season_value or 0.0)
        recent_value = float(recent_value or season_value)
        return (season_value * (1 - recent_weight)) + (recent_value * recent_weight)

    def _accumulate_player_stats(self, accumulator: dict[int, dict[str, float]], stats: dict[int, dict[str, float]]) -> None:
        for batter_id, batter_stats in stats.items():
            for key, value in batter_stats.items():
                accumulator[batter_id][key] += value

    def _finalize_player_stats(self, stats: dict[int, dict[str, float]], scale: float) -> dict[int, dict[str, float]]:
        final: dict[int, dict[str, float]] = {}
        for batter_id, totals in stats.items():
            pa = totals.get("pa", 0.0) / self.trials
            hits = (totals.get("hits", 0.0) / self.trials) * scale
            hr = (totals.get("hr", 0.0) / self.trials) * scale
            rbi = (totals.get("rbi", 0.0) / self.trials) * scale
            tb = (totals.get("total_bases", 0.0) / self.trials) * scale
            final[batter_id] = {
                "pa": round(pa, 2),
                "ab": round(totals.get("ab", 0.0) / self.trials, 2),
                "hits": round(hits, 2),
                "hr": round(hr, 2),
                "bb": round((totals.get("bb", 0.0) / self.trials) * ((scale + 1) / 2), 2),
                "k": round(totals.get("k", 0.0) / self.trials, 2),
                "rbi": round(rbi, 2),
                "tb": round(tb, 2),
                "hit_prob": round(clamp(hits / max(pa, 1.0), 0.0, 1.0), 3),
                "hr_prob": round(clamp(hr / max(pa, 1.0), 0.0, 1.0), 3),
            }
        return final
