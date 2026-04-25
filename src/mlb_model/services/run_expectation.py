from __future__ import annotations

from mlb_model.schemas import TeamRunContext
from mlb_model.utils import clamp, logistic_probability


# 2023-2024 MLB baseline stats
# NOTE: xBA here is estimated_ba_using_speedangle on batted-ball events (not all PA).
# That metric averages ~0.290 MLB-wide, higher than traditional xBA/BA (~0.255).
_LG_XBA = 0.290
_LG_K_PCT = 0.228
_LG_BB_PCT = 0.076
_LG_HARD_HIT = 0.375
_LG_BARREL = 0.080
_LG_XWOBA = 0.318

# Regression weights — how much of the actual stat deviation to trust.
# Pitcher stats are noisier (small early-season samples), so regress more aggressively.
# Batter stats from a full lineup average out more, so allow more signal.
_PITCHER_REGRESSION = 0.18
_BATTER_REGRESSION  = 0.35

# MLB per-team average runs/game (conservative: April context, not peak-summer offense)
_BASE_RUNS = 4.30


def _regress(value: float, league_avg: float, weight: float) -> float:
    return league_avg + weight * (value - league_avg)


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
        top_features: list[dict] | None = None,
    ) -> TeamRunContext:
        # Regress pitcher stats: trust only 18% of deviation from league average.
        # This tames noisy early-season samples (small innings counts) while
        # still letting genuine quality signal through.
        p_xba    = _regress(pitcher_xba,           _LG_XBA,      _PITCHER_REGRESSION)
        p_k      = _regress(pitcher_k_pct,          _LG_K_PCT,    _PITCHER_REGRESSION)
        p_bb     = _regress(pitcher_bb_pct,         _LG_BB_PCT,   _PITCHER_REGRESSION)
        p_hh     = _regress(pitcher_hard_hit_pct,   _LG_HARD_HIT, _PITCHER_REGRESSION)
        p_barrel = _regress(pitcher_barrel_pct,     _LG_BARREL,   _PITCHER_REGRESSION)

        # Regress lineup averages: trust 35% of deviation — more stable than per-pitcher.
        b_xwoba = _regress(lineup_xwoba,        _LG_XWOBA,   _BATTER_REGRESSION)
        b_k     = _regress(lineup_k_pct,        _LG_K_PCT,   _BATTER_REGRESSION)
        b_bb    = _regress(lineup_bb_pct,       _LG_BB_PCT,  _BATTER_REGRESSION)
        b_hh    = _regress(lineup_hard_hit_pct, _LG_HARD_HIT, _BATTER_REGRESSION)

        # Expected runs this pitcher allows above/below average (capped at ±1.0)
        pitcher_runs = clamp(
            (p_xba    - _LG_XBA)      * 18
            - (p_k    - _LG_K_PCT)    * 14
            + (p_bb   - _LG_BB_PCT)   * 10
            + (p_hh   - _LG_HARD_HIT) * 10
            + (p_barrel - _LG_BARREL) * 14,
            -1.0, 1.0,
        )

        # Expected runs this lineup scores above/below average (capped at ±0.8)
        batter_runs = clamp(
            (b_xwoba - _LG_XWOBA)   * 15
            - (b_k   - _LG_K_PCT)   * 10
            + (b_bb  - _LG_BB_PCT)  * 8
            + (b_hh  - _LG_HARD_HIT) * 8,
            -0.8, 0.8,
        )

        weather_effect = (weather_multiplier - 1.0) * 0.8
        park_effect    = (park_factor - 1.0) * 0.8
        bullpen_effect = max(0.0, (60.0 - bullpen_score) / 60.0 * 0.5)

        expected = clamp(
            _BASE_RUNS + pitcher_runs + batter_runs + weather_effect + park_effect + bullpen_effect,
            1.5, 7.0,
        )
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
