"""Tests for the vendored empirical-Bayes rate shrinkage (rate_shrinkage.py).

Mirrors the test structure of the source implementation in the sibling
Weight Room Hero repo (tests/test_structural_model.py, test_structural_v2.py):
posterior between raw and prior, monotonic in sample size, MoM fit sanity,
and park-factor shrinkage toward 1.0 with clipping.
"""
from __future__ import annotations

import random

import pandas as pd
import pytest

from mlb_model.providers.baseball import BaseballSavantProvider
from mlb_model.services.rate_shrinkage import (
    FALLBACK_PRIOR_STRENGTH,
    PARK_FACTOR_CLIP,
    EventPriors,
    fit_beta_binomial_mom,
    park_factor_shrunk,
    shrink_rate,
    shrunk_rate,
)


class TestShrunkRate:
    def test_posterior_between_raw_and_prior(self):
        alpha, beta = 10.0, 290.0  # prior mean 0.0333
        prior_mean = alpha / (alpha + beta)
        raw = 6 / 60  # hot streak: 0.10 over 60 PA
        post = shrunk_rate(6, 60, alpha, beta)
        assert prior_mean < post < raw

    def test_posterior_monotonic_in_sample_size(self):
        alpha, beta = 10.0, 290.0
        # Same raw rate 0.10 at growing sample sizes: posterior must move
        # monotonically toward the raw rate.
        posteriors = [shrunk_rate(0.10 * n, n, alpha, beta) for n in (20, 100, 400, 2000)]
        assert posteriors == sorted(posteriors)
        assert posteriors[-1] == pytest.approx(0.10, abs=0.01)

    def test_zero_trials_gives_prior_mean(self):
        alpha, beta = 12.0, 388.0
        assert shrunk_rate(0, 0, alpha, beta) == pytest.approx(alpha / (alpha + beta))

    def test_shrink_rate_matches_count_form(self):
        alpha, beta = 8.0, 192.0
        assert shrink_rate(0.25, 120, alpha, beta) == pytest.approx(
            shrunk_rate(30.0, 120, alpha, beta)
        )

    def test_shrink_rate_none_passthrough(self):
        assert shrink_rate(None, 100, 5.0, 95.0) is None


class TestFitBetaBinomialMoM:
    def test_recovers_reasonable_prior(self):
        # Simulate a league: true talent ~ Beta(30, 970) (mean ~0.03),
        # observed counts binomial at n=450 PA.
        rng = random.Random(7)
        successes, trials = [], []
        for _ in range(300):
            p = rng.betavariate(30, 970)
            n = 450
            s = sum(1 for _ in range(n) if rng.random() < p)
            successes.append(s)
            trials.append(n)
        alpha, beta = fit_beta_binomial_mom(successes, trials)
        mean = alpha / (alpha + beta)
        assert 0.02 < mean < 0.04
        # Prior strength should land in a plausible band, not at the clamp edges.
        assert 100 < alpha + beta < 2000

    def test_fallback_prior_with_few_qualifiers(self):
        # Only 3 players qualify -> weakly-informative pooled fallback.
        alpha, beta = fit_beta_binomial_mom([10, 12, 8], [400, 380, 410])
        assert alpha + beta == pytest.approx(FALLBACK_PRIOR_STRENGTH)
        pooled = 30 / 1190
        assert alpha / (alpha + beta) == pytest.approx(pooled, rel=0.05)

    def test_small_samples_excluded_from_fit(self):
        # 20 qualified players at 0.03, plus wild 5-PA outliers that must not
        # blow up the fitted variance.
        successes = [12] * 20 + [4] * 50
        trials = [400] * 20 + [5] * 50
        alpha, beta = fit_beta_binomial_mom(successes, trials)
        assert alpha / (alpha + beta) == pytest.approx(0.03, abs=0.01)


class TestParkFactorShrunk:
    def test_small_sample_park_pulled_toward_one(self):
        assert 1.0 < park_factor_shrunk(1.20, sample_pa=500) < 1.20
        assert 0.80 < park_factor_shrunk(0.80, sample_pa=500) < 1.0

    def test_shrinkage_monotonic_in_sample_size(self):
        factors = [park_factor_shrunk(1.20, sample_pa=n) for n in (0, 500, 3000, 50000)]
        assert factors == sorted(factors)
        assert factors[0] == pytest.approx(1.0)

    def test_factors_respect_clip(self):
        lo, hi = PARK_FACTOR_CLIP
        assert park_factor_shrunk(3.0, sample_pa=10**9) == hi
        assert park_factor_shrunk(0.1, sample_pa=10**9) == lo

    def test_neutral_park_stays_neutral(self):
        assert park_factor_shrunk(1.0, sample_pa=3000) == pytest.approx(1.0)


class TestEventPriors:
    def test_fit_and_shrink(self):
        pairs = [(int(0.03 * 400), 400)] * 15 + [(int(0.08 * 400), 400)] * 15
        priors = EventPriors.fit({"hr": pairs})
        assert priors.has("hr")
        # A 20-PA hot streak at 0.25 HR/PA must be pulled hard toward league.
        post = priors.shrink("hr", 0.25, 20)
        assert post < 0.10

    def test_missing_event_passes_raw_through(self):
        priors = EventPriors()
        assert priors.shrink("hr", 0.25, 20) == 0.25
        assert priors.shrink("hr", None, 20) is None

    def test_prior_mean(self):
        priors = EventPriors({"k": (22.0, 78.0)})
        assert priors.prior_mean("k") == pytest.approx(0.22)
        assert priors.prior_mean("bb") is None


def _synthetic_statcast(n_players: int = 30, pa_per_player: int = 120, seed: int = 3) -> pd.DataFrame:
    """Minimal terminal-event Statcast frame: enough columns for the provider's
    profile builders and prior fitting."""
    rng = random.Random(seed)
    rows = []
    from datetime import date as _date

    for pid in range(1, n_players + 1):
        k_rate = 0.15 + 0.15 * rng.random()
        bb_rate = 0.05 + 0.06 * rng.random()
        hr_rate = 0.01 + 0.05 * rng.random()
        for i in range(pa_per_player):
            r = rng.random()
            if r < k_rate:
                event = "strikeout"
            elif r < k_rate + bb_rate:
                event = "walk"
            elif r < k_rate + bb_rate + hr_rate:
                event = "home_run"
            elif r < k_rate + bb_rate + hr_rate + 0.15:
                event = "single"
            else:
                event = "field_out"
            rows.append(
                {
                    "game_date": _date(2026, 4, 1 + (i % 28)),
                    "batter": pid,
                    "pitcher": 1000 + pid,
                    "events": event,
                    "description": "hit_into_play",
                    "stand": "R",
                    "p_throws": "R",
                    "pitch_type": "FF",
                    "launch_speed": 88.0 + 10.0 * rng.random(),
                    "launch_angle": 12.0,
                    "estimated_woba_using_speedangle": 0.30 + 0.08 * rng.random(),
                    "estimated_ba_using_speedangle": 0.24 + 0.06 * rng.random(),
                    "delta_run_exp": 0.0,
                    "swing_path_tilt": 30.0,
                    "attack_angle": 10.0,
                    "swing_length": 7.2,
                    "hc_x": 125.0,
                    "hc_y": 150.0,
                }
            )
    frame = pd.DataFrame(rows)
    frame["_batter_id"] = pd.to_numeric(frame["batter"])
    frame["_pitcher_id"] = pd.to_numeric(frame["pitcher"])
    return frame


class TestProviderShrinkageIntegration:
    def _provider(self, frame: pd.DataFrame) -> BaseballSavantProvider:
        provider = BaseballSavantProvider()
        provider._prepared = frame
        return provider

    def test_batter_profile_rates_are_shrunk(self):
        frame = _synthetic_statcast()
        provider = self._provider(frame)
        priors = provider._event_priors("batter")
        assert priors.has("k") and priors.has("bb") and priors.has("hr")

        profile = provider.build_batter_matchup_profile(1)
        pa = profile["sample_pa"]
        assert pa > 0
        batter_rows = frame[frame["_batter_id"] == 1]
        raw_k = (
            batter_rows["events"].isin({"strikeout", "strikeout_double_play"}).mean()
        )
        league_k = priors.prior_mean("k")
        # Posterior lies between the raw rate and the league mean.
        lo, hi = sorted((raw_k, league_k))
        assert lo - 1e-9 <= profile["k_pct"] <= hi + 1e-9
        # And is strictly pulled off the raw rate unless raw == league.
        if abs(raw_k - league_k) > 1e-6:
            assert abs(profile["k_pct"] - raw_k) > 1e-6

    def test_batter_hit_type_rates_shrunk_toward_league(self):
        frame = _synthetic_statcast()
        provider = self._provider(frame)
        priors = provider._event_priors("batter")
        profile = provider.build_batter_matchup_profile(2)
        rates = profile["bb_rates"]
        assert set(rates) == {"1B", "2B", "3B", "HR"}
        batter_rows = frame[frame["_batter_id"] == 2]
        raw_hr = (batter_rows["events"] == "home_run").mean()
        league_hr = priors.prior_mean("hr")
        lo, hi = sorted((raw_hr, league_hr))
        assert lo - 1e-9 <= rates["HR"] <= hi + 1e-9

    def test_empty_provider_degrades_to_passthrough(self):
        provider = self._provider(pd.DataFrame())
        priors = provider._event_priors("batter")
        assert not priors.has("k")
        assert priors.shrink("k", 0.31, 12) == 0.31


class TestSimulationParkFactor:
    def test_hr_rate_uses_handedness_split_park(self):
        from mlb_model.services.simulation_model import SimulationModelService

        service = SimulationModelService(trials=1)
        batter = {"profile": {"handedness": "L"}, "matchup": {}}
        pitcher = {"sample_bbe": 100}
        kwargs = dict(
            batter=batter,
            opposing_pitcher=pitcher,
            target_runs=4.5,
            bullpen_score=65.0,
            phase="starter",
            park_factor=1.0,
        )
        neutral = service._plate_appearance_distribution(**kwargs, hr_park_by_hand=None)
        boosted = service._plate_appearance_distribution(
            **kwargs, hr_park_by_hand={"L": 1.15, "R": 1.0}
        )
        suppressed = service._plate_appearance_distribution(
            **kwargs, hr_park_by_hand={"L": 0.85, "R": 1.0}
        )
        assert boosted["hr"] > neutral["hr"] > suppressed["hr"]
        # Right-handed factor must not leak onto a lefty.
        rh_only = service._plate_appearance_distribution(
            **kwargs, hr_park_by_hand={"L": 1.0, "R": 1.25}
        )
        assert rh_only["hr"] == pytest.approx(neutral["hr"])
