from __future__ import annotations

from mlb_model.schemas import TeamRunContext
from mlb_model.utils import clamp, logistic_probability


# 2024-2025 MLB baseline stats
# NOTE: xBA here is estimated_ba_using_speedangle on batted-ball events (not all PA).
# That metric averages ~0.290 MLB-wide, higher than traditional xBA/BA (~0.255).
_LG_XBA = 0.290
_LG_K_PCT = 0.228
_LG_BB_PCT = 0.076
_LG_HARD_HIT = 0.375
_LG_BARREL = 0.080
_LG_XWOBA = 0.318

# MLB league-average runs per team per game (2024-2025 full-season).
# This is the neutral-park baseline — park_factor scales it per venue.
_LG_RUNS = 4.50

# Empirical sample-size priors for Bayesian regression.
# At these thresholds, the weight reaches its midpoint between min and max.
# Pitcher: ~80 IP ≈ 1 200 pitches.  Batter: lineup avg PA anchor.
_PITCHER_PRIOR_PITCHES = 1_200
_BATTER_PRIOR_PA = 400


def _regress(value: float, league_avg: float, weight: float) -> float:
    return league_avg + weight * (value - league_avg)


def _sample_weight(sample: int, prior: int, min_w: float, max_w: float) -> float:
    """Return a regression weight that scales from min_w (no data) to max_w (large sample)."""
    return min_w + (max_w - min_w) * sample / (sample + prior)


class RunExpectationService:

    def expected_runs(
        self,
        team: str,
        pitcher_xba: float,
        pitcher_k_pct: float,
        pitcher_bb_pct: float,
        pitcher_hard_hit_pct: float,
        pitcher_barrel_pct: float,
        lineup_xwoba: float,
        lineup_k_pct: float,
        lineup_bb_pct: float,
        lineup_hard_hit_pct: float,
        weather_multiplier: float,
        park_factor: float,
        bullpen_score: float,
        starter_ip_projection: float,
        pitcher_sample_pitches: int = 0,
        lineup_avg_pa: int = 0,
        top_features: list[dict] | None = None,
    ) -> TeamRunContext:
        # Sample-size-aware regression: trust more of the data when sample is large.
        # Pitcher: ranges from 8% (call-up, 0 pitches) to 35% (full-season ace).
        # Batter:  ranges from 20% (no data) to 55% (600+ PA lineup).
        p_weight = _sample_weight(pitcher_sample_pitches, _PITCHER_PRIOR_PITCHES, min_w=0.08, max_w=0.35)
        b_weight = _sample_weight(lineup_avg_pa,          _BATTER_PRIOR_PA,       min_w=0.20, max_w=0.55)

        p_xba    = _regress(pitcher_xba,           _LG_XBA,      p_weight)
        p_k      = _regress(pitcher_k_pct,          _LG_K_PCT,    p_weight)
        p_bb     = _regress(pitcher_bb_pct,         _LG_BB_PCT,   p_weight)
        p_hh     = _regress(pitcher_hard_hit_pct,   _LG_HARD_HIT, p_weight)
        p_barrel = _regress(pitcher_barrel_pct,     _LG_BARREL,   p_weight)

        b_xwoba = _regress(lineup_xwoba,        _LG_XWOBA,    b_weight)
        b_k     = _regress(lineup_k_pct,        _LG_K_PCT,    b_weight)
        b_bb    = _regress(lineup_bb_pct,       _LG_BB_PCT,   b_weight)
        b_hh    = _regress(lineup_hard_hit_pct, _LG_HARD_HIT, b_weight)

        # How much of the game this starter is expected to control (0.33–1.0).
        # A projected 6.5-inning ace controls ~full game; a 3-inning opener ~one third.
        starter_share = clamp(starter_ip_projection / 6.5, 0.33, 1.0)

        # Pitcher delta: how many runs above/below average this starter allows.
        # Scaled by starter_share so a short outing limits the starter's drag/lift.
        # Cap is wider (±1.5) so elite or terrible starters can actually move the total.
        pitcher_delta = clamp(
            (p_xba    - _LG_XBA)      * 18
            - (p_k    - _LG_K_PCT)    * 14
            + (p_bb   - _LG_BB_PCT)   * 10
            + (p_hh   - _LG_HARD_HIT) * 10
            + (p_barrel - _LG_BARREL) * 14,
            -1.5, 1.5,
        ) * starter_share

        # Lineup delta: how many runs above/below average this offense scores (cap ±1.0).
        batter_delta = clamp(
            (b_xwoba - _LG_XWOBA)   * 15
            - (b_k   - _LG_K_PCT)   * 10
            + (b_bb  - _LG_BB_PCT)  * 8
            + (b_hh  - _LG_HARD_HIT) * 8,
            -1.0, 1.0,
        )

        # Bullpen adds runs whenever it covers innings the starter vacates.
        bullpen_delta = max(0.0, (60.0 - bullpen_score) / 60.0 * 0.5) * (1.0 - starter_share * 0.6)

        # Park-adjusted baseline: Coors (1.15) → ~5.18, pitcher parks (0.90) → ~4.05.
        # Weather multiplier applied multiplicatively on top.
        park_base = _LG_RUNS * park_factor * weather_multiplier

        expected = clamp(park_base + pitcher_delta + batter_delta + bullpen_delta, 1.5, 8.0)

        return TeamRunContext(
            team=team,
            pitcher_xba=round(pitcher_xba, 3),
            lineup_xwoba=round(lineup_xwoba, 3),
            weather_multiplier=weather_multiplier,
            park_factor=park_factor,
            bullpen_score=round(bullpen_score, 1),
            expected_runs=round(expected, 2),
            starter_ip_projection=starter_ip_projection,
            top_features=top_features or [],
        )

    def game_total_probability(self, projected_total: float, market_total: float) -> float:
        return clamp(logistic_probability(projected_total - market_total, scale=1.05), 0.03, 0.97)

    def runline_cover_probability(self, margin_projection: float) -> float:
        return clamp(logistic_probability(margin_projection - 1.5, scale=0.85), 0.03, 0.97)

