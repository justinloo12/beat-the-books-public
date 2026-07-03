"""Offline unit tests for the pure odds math in odds_engine.

Covers american/decimal conversions, implied probabilities, no-vig
normalization, and Kelly fraction properties. No DB, network, or app startup.
"""
from __future__ import annotations

import pytest

from mlb_model.services.odds_engine import (
    american_to_decimal,
    implied_probability_from_american,
    kelly_fraction,
    no_vig_one_sided,
    no_vig_two_sided,
)


class TestAmericanToDecimal:
    @pytest.mark.parametrize(
        ("american", "expected"),
        [
            (100, 2.0),
            (-100, 2.0),
            (150, 2.5),
            (-150, 1.0 + 100.0 / 150.0),
            (110, 2.1),
            (-110, 1.0 + 100.0 / 110.0),
            (200, 3.0),
            (-200, 1.5),
        ],
    )
    def test_known_conversions(self, american: int, expected: float) -> None:
        assert american_to_decimal(american) == pytest.approx(expected)

    @pytest.mark.parametrize("american", [100, -100, 110, -110, 250, -250, 10000, -10000])
    def test_decimal_odds_always_above_one(self, american: int) -> None:
        assert american_to_decimal(american) > 1.0


class TestImpliedProbability:
    @pytest.mark.parametrize(
        ("american", "expected"),
        [
            (100, 0.5),
            (-100, 0.5),
            (110, 100.0 / 210.0),
            (-110, 110.0 / 210.0),
            (300, 0.25),
            (-300, 0.75),
        ],
    )
    def test_known_probabilities(self, american: int, expected: float) -> None:
        assert implied_probability_from_american(american) == pytest.approx(expected)

    @pytest.mark.parametrize("american", [100, -100, 110, -110, 145, -145, 500, -500])
    def test_round_trip_probability_times_decimal_is_one(self, american: int) -> None:
        """A fair bet's implied probability times its decimal payout equals 1."""
        prob = implied_probability_from_american(american)
        decimal = american_to_decimal(american)
        assert prob * decimal == pytest.approx(1.0)

    def test_favorite_more_likely_than_underdog(self) -> None:
        assert implied_probability_from_american(-150) > implied_probability_from_american(150)


class TestNoVigTwoSided:
    @pytest.mark.parametrize(
        ("prob_a", "prob_b"),
        [
            (0.54, 0.50),
            (0.5238, 0.5238),  # both sides -110
            (0.75, 0.30),
            (0.02, 0.99),
        ],
    )
    def test_probabilities_sum_to_one(self, prob_a: float, prob_b: float) -> None:
        a, b = no_vig_two_sided(prob_a, prob_b)
        assert a + b == pytest.approx(1.0)

    def test_preserves_ratio(self) -> None:
        a, b = no_vig_two_sided(0.60, 0.30)
        assert a / b == pytest.approx(2.0)

    def test_symmetric_market_splits_evenly(self) -> None:
        a, b = no_vig_two_sided(0.5238, 0.5238)
        assert a == pytest.approx(0.5)
        assert b == pytest.approx(0.5)

    def test_degenerate_zero_market_returns_even_split(self) -> None:
        assert no_vig_two_sided(0.0, 0.0) == (0.5, 0.5)

    def test_standard_110_market_devigs_toward_half(self) -> None:
        raw = implied_probability_from_american(-110)
        a, b = no_vig_two_sided(raw, raw)
        assert a < raw  # vig removed, probability shrinks


class TestNoVigOneSided:
    def test_subtracts_default_vig(self) -> None:
        assert no_vig_one_sided(0.55) == pytest.approx(0.505)

    def test_custom_vig(self) -> None:
        assert no_vig_one_sided(0.55, vig=0.05) == pytest.approx(0.50)

    def test_clamped_at_zero(self) -> None:
        assert no_vig_one_sided(0.02) == 0.0

    def test_never_negative_or_above_one(self) -> None:
        for prob in (0.0, 0.01, 0.5, 0.99, 1.0):
            result = no_vig_one_sided(prob)
            assert 0.0 <= result <= 1.0


class TestKellyFraction:
    def test_positive_edge_gives_positive_fraction(self) -> None:
        # 60% to win an even-money bet: kelly = (1*0.6 - 0.4) / 1 = 0.2
        assert kelly_fraction(0.60, 2.0) == pytest.approx(0.20)

    def test_no_edge_gives_zero(self) -> None:
        # Model probability exactly equals implied probability -> zero edge.
        assert kelly_fraction(0.5, 2.0) == pytest.approx(0.0)

    def test_negative_edge_floored_at_zero(self) -> None:
        assert kelly_fraction(0.30, 2.0) == 0.0

    def test_zero_payout_returns_zero(self) -> None:
        # decimal odds of 1.0 (b == 0) must not divide by zero
        assert kelly_fraction(0.99, 1.0) == 0.0
        assert kelly_fraction(0.99, 0.5) == 0.0

    def test_certain_win_bets_full_bankroll(self) -> None:
        assert kelly_fraction(1.0, 2.0) == pytest.approx(1.0)

    @pytest.mark.parametrize("prob", [0.0, 0.25, 0.5, 0.75, 1.0])
    @pytest.mark.parametrize("decimal", [1.5, 1.9090909, 2.0, 3.5])
    def test_fraction_bounded_between_zero_and_one(self, prob: float, decimal: float) -> None:
        assert 0.0 <= kelly_fraction(prob, decimal) <= 1.0

    def test_monotonic_in_model_probability(self) -> None:
        fractions = [kelly_fraction(p, 2.0) for p in (0.55, 0.60, 0.65, 0.70)]
        assert fractions == sorted(fractions)
