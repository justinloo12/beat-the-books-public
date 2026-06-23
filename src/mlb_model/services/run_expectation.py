from __future__ import annotations

from mlb_model.schemas import TeamRunContext
from mlb_model.utils import clamp, expected_woba, logistic_probability

# ---------------------------------------------------------------------------
# League baselines (grounded in published sabermetric relationships, not feel).
# ---------------------------------------------------------------------------
# wOBA is on the same scale as OBP. League-average wOBA and the wOBA scale below
# come from FanGraphs' annual constants; PA/game is one team's plate appearances
# in a 9-inning game. These three numbers define the EXACT, non-arbitrary
# conversion from a team's expected wOBA to its expected runs:
#     runs_above_avg = ((wOBA - lgwOBA) / wOBAScale) * PA_per_game
_LG_WOBA = 0.320
_WOBA_SCALE = 1.24
_PA_PER_GAME = 38.0
_LG_TEAM_RUNS = 4.50

# xBA-against baseline, used only to derive a pitcher's wOBA-against when the
# arsenal-level est_woba is unavailable.
_LG_XBA_AGAINST = 0.245

# Empirical sample-size priors for the model's CONFIDENCE in deviating from the
# market anchor (not for regressing the stats themselves — that happens upstream
# in the split-blended profiles).
_PITCHER_PRIOR_PITCHES = 1_200
_BATTER_PRIOR_PA = 400

# Largest wOBA nudge the swing-path / pitch-movement alignment may apply, in
# wOBA points. A perfect (100) or worst (0) alignment moves expected wOBA by at
# most this much; everything in between scales linearly. Bounded on purpose so a
# secondary signal can never dominate the primary rate stats.
_MAX_SWING_WOBA = 0.008


def _sample_weight(sample: int, prior: int) -> float:
    """0 (no data) -> 1 (large sample). Used to scale model confidence."""
    if sample <= 0:
        return 0.0
    return sample / (sample + prior)


class RunExpectationService:
    """Turns a matchup into a single expected-runs number for one team.

    Pipeline (every step is in run units or wOBA, nothing random):
      1. Combine the lineup's expected wOBA with the opposing pitcher's
         wOBA-against using a Log5-style rate matchup.
      2. Apply the bounded swing/movement alignment nudge.
      3. Convert wOBA -> runs via the published wOBA scale.
      4. Apply park, weather, bullpen, and sweep effects in run units.
      5. Anchor to the market-implied team total, letting the model deviate
         only as far as its confidence (sample size, lineup certainty) and the
         configured cap allow.
    """

    def __init__(self, run_environment: dict[str, float] | None = None) -> None:
        # The wOBA->runs conversion constants ARE the run environment for the
        # season being modelled. They drift year to year (2025 scored a touch
        # lower than 2024), so they live in config and are injected here instead
        # of being frozen module constants. Defaults reproduce the prior values.
        env = run_environment or {}
        self.lg_woba = float(env.get("lg_woba", _LG_WOBA))
        self.woba_scale = float(env.get("woba_scale", _WOBA_SCALE))
        self.pa_per_game = float(env.get("pa_per_game", _PA_PER_GAME))
        self.lg_team_runs = float(env.get("lg_team_runs", _LG_TEAM_RUNS))

    def expected_runs(
        self,
        team: str,
        lineup_xwoba: float,
        pitcher_woba_against: float,
        weather_multiplier: float,
        park_factor: float,
        bullpen_score: float,
        starter_ip_projection: float,
        swing_alignment: float = 50.0,
        pitcher_sample_pitches: int = 0,
        lineup_avg_pa: int = 0,
        lineup_confirmed: bool = False,
        market_team_total: float | None = None,
        deviation_cap: float = 1.0,
        sweep_avoidance_runs: float = 0.0,
        pitcher_xba: float = _LG_XBA_AGAINST,
        park_hit_woba: float = 0.0,
        top_features: list[dict] | None = None,
    ) -> TeamRunContext:
        # 1. Log5-style rate matchup: a strong lineup against a strong pitcher
        #    lands between the two, scaled by league average.
        matchup_woba = lineup_xwoba * (pitcher_woba_against / self.lg_woba)

        # 2. Bounded secondary wOBA nudges, all on the same scale as the rate
        #    matchup above so they cooperate instead of competing:
        #      * swing-path / pitch-movement geometry alignment,
        #      * hit-type x handedness park geometry (the Green Monster turning
        #        fly balls into doubles, a short porch into homers), already
        #        weather-scaled on its air-dependent components upstream.
        swing_nudge = ((swing_alignment - 50.0) / 50.0) * _MAX_SWING_WOBA
        matchup_woba = clamp(matchup_woba + swing_nudge + park_hit_woba, 0.180, 0.520)

        # 3. wOBA -> runs (the grounded conversion).
        offense_runs = self.lg_team_runs + ((matchup_woba - self.lg_woba) / self.woba_scale) * self.pa_per_game

        # 4. Context effects in run units. Park and weather factors are already
        #    published as full-game team-scoring multipliers (a 1.15 park means a
        #    team scores 15% more runs there over a full game), so they apply at
        #    full strength — NOT halved. Halving them silently erased half of
        #    every park and weather signal, including the hit-type park factors.
        offense_runs *= park_factor
        offense_runs *= weather_multiplier
        # The bullpen is no longer a separate flat effect. The opposing pitching
        # the lineup faces (pitcher_woba_against) is already a starter/bullpen
        # blend computed upstream, weighted by the starter's projected outs — so
        # a 5-inning starter hands ~44% of the game to the pen and the pen's
        # quality is weighted accordingly. bullpen_score is retained only for the
        # TeamRunContext display below.
        model_runs = offense_runs + sweep_avoidance_runs

        # 5. Market anchor + confidence-scaled, capped deviation.
        p_conf = _sample_weight(pitcher_sample_pitches, _PITCHER_PRIOR_PITCHES)
        b_conf = _sample_weight(lineup_avg_pa, _BATTER_PRIOR_PA)
        confidence = 0.5 * p_conf + 0.5 * b_conf
        if lineup_confirmed:
            confidence = clamp(confidence + 0.15, 0.0, 1.0)

        if market_team_total is not None:
            # The cap itself scales with confidence. A thin-sample matchup is held
            # tight to the market line (~0.5x cap); a high-confidence one (big
            # pitcher + lineup samples, confirmed lineup) may disagree up to ~1.25x
            # the base cap. This is what lets genuinely strong reads SEPARATE from
            # the pack instead of every game clustering one tick off the line — the
            # "no discrimination" failure the backtest exposed.
            effective_cap = deviation_cap * (0.5 + 0.75 * confidence)
            deviation = clamp(
                (model_runs - market_team_total) * confidence, -effective_cap, effective_cap
            )
            expected = market_team_total + deviation
        else:
            expected = model_runs

        expected = clamp(expected, 1.5, 7.0)

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

    @staticmethod
    def pitcher_woba_against(profile: dict) -> float:
        """A pitcher's TRUE expected wOBA-against, folding in the two skills the
        old xBA-only proxy threw away: missing bats (K%) and free passes (BB%).

        Uses the pitcher's strikeout rate, walk rate, and xwOBA-on-contact-against
        so a high-strikeout arm is correctly rated far stingier than a
        soft-contact innings-eater with the same xBA. Falls back to the xBA proxy
        only when contact-quality data is missing entirely.
        """
        k = profile.get("k_pct")
        bb = profile.get("bb_pct")
        xwoba_contact = profile.get("xwoba_contact_against")
        if xwoba_contact:
            return expected_woba(k, bb, xwoba_contact, default_contact=0.370)
        # No contact-quality data — fall back to the xBA ratio proxy.
        return RunExpectationService.pitcher_woba_against_from_xba(
            float(profile.get("xba") or _LG_XBA_AGAINST)
        )

    @staticmethod
    def pitcher_woba_against_from_xba(pitcher_xba: float) -> float:
        """Fallback wOBA-against when arsenal-level est_woba is unavailable.

        Scales league-average wOBA by how the pitcher's xBA-against compares to
        league average — a rough but grounded proxy, not a magic constant.
        """
        ratio = (pitcher_xba or _LG_XBA_AGAINST) / _LG_XBA_AGAINST
        return clamp(_LG_WOBA * ratio, 0.250, 0.420)

    def game_total_probability(self, projected_total: float, market_total: float) -> float:
        return clamp(logistic_probability(projected_total - market_total, scale=1.05), 0.03, 0.97)

    def runline_cover_probability(self, margin_projection: float) -> float:
        return clamp(logistic_probability(margin_projection - 1.5, scale=0.85), 0.03, 0.97)
