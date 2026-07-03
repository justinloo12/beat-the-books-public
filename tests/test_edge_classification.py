"""Offline unit tests for classify_edge tier boundaries and stake sizing.

Threshold values come from config/model_config.json (loaded by the module at
import time), so these tests assert against the configured values rather than
hard-coded literals wherever possible.
"""
from __future__ import annotations

import pytest

from mlb_model.models import PickTier
from mlb_model.services.odds_engine import (
    EdgeDecision,
    american_to_decimal,
    classify_edge,
    model_settings,
)

THRESHOLDS = model_settings.edge_thresholds
UNIT_SIZES = model_settings.unit_sizes
KELLY_CAPS = model_settings.kelly_caps
JUICE_BLOCK = model_settings.juice_block_threshold

# Even-money odds keep the tier decision driven purely by the edge value.
EVEN_DECIMAL = american_to_decimal(100)


def decide(edge: float, american_odds: int = 100) -> EdgeDecision:
    """Classify with an exact edge (no_vig set to 0 avoids float subtraction noise)."""
    return classify_edge(edge, 0.0, american_odds, american_to_decimal(american_odds))


def decide_realistic(edge: float, american_odds: int = 100) -> EdgeDecision:
    """Classify with a realistic model probability (0.50 no-vig baseline).

    Needed for stake assertions: Kelly sizing depends on the actual model
    probability vs the price, not just the edge.
    """
    return classify_edge(0.50 + edge, 0.50, american_odds, american_to_decimal(american_odds))


class TestTierBoundaries:
    def test_edge_below_every_band_is_pass(self) -> None:
        decision = decide(min(THRESHOLDS["pass_below"], THRESHOLDS["watch_min"]) - 0.001)
        assert decision.tier == PickTier.PASS
        assert decision.bankroll_fraction == 0.0

    def test_negative_edge_is_pass(self) -> None:
        decision = decide(-0.05)
        assert decision.tier == PickTier.PASS
        assert decision.bankroll_fraction == 0.0

    def test_edge_below_pass_threshold_is_pass_even_inside_watch_band(self) -> None:
        # With the default config the watch band (0.035-0.06) sits entirely
        # below pass_below (0.06). Those edges historically classified MONITOR
        # and were staked; the corrected order passes on them.
        assert THRESHOLDS["watch_min"] < THRESHOLDS["pass_below"]
        for edge in (
            THRESHOLDS["watch_min"],
            (THRESHOLDS["watch_min"] + THRESHOLDS["watch_max"]) / 2,
            THRESHOLDS["pass_below"] - 0.001,
        ):
            decision = decide(edge)
            assert decision.tier == PickTier.PASS
            assert decision.bankroll_fraction == 0.0

    def test_watch_band_still_reachable_when_config_allows_it(self, monkeypatch) -> None:
        # MONITOR is only meaningful when the watch band extends at or above
        # pass_below. Lower pass_below and confirm the branch still works.
        monkeypatch.setitem(THRESHOLDS, "pass_below", 0.02)
        mid = (THRESHOLDS["watch_min"] + THRESHOLDS["watch_max"]) / 2
        assert decide(mid).tier == PickTier.MONITOR
        assert decide(THRESHOLDS["pass_below"] - 0.001).tier == PickTier.PASS

    def test_strong_min_boundary_is_moderate(self) -> None:
        # Config's "strong" band maps to the MODERATE tier.
        assert decide(THRESHOLDS["strong_min"]).tier == PickTier.MODERATE

    def test_just_below_max_bet_is_moderate(self) -> None:
        assert decide(THRESHOLDS["max_bet_min"] - 0.001).tier == PickTier.MODERATE

    def test_max_bet_min_boundary_is_strong(self) -> None:
        assert decide(THRESHOLDS["max_bet_min"]).tier == PickTier.STRONG

    def test_huge_edge_is_strong(self) -> None:
        assert decide(0.25).tier == PickTier.STRONG

    def test_edge_recorded_on_decision(self) -> None:
        decision = classify_edge(0.58, 0.50, 100, EVEN_DECIMAL)
        assert decision.edge == pytest.approx(0.08)
        assert decision.no_vig_probability == pytest.approx(0.50)


class TestJuiceBlock:
    def test_heavier_juice_than_threshold_is_blocked(self) -> None:
        odds = JUICE_BLOCK - 1  # e.g. -146 with a -145 threshold
        decision = decide(0.20, american_odds=odds)
        assert decision.tier == PickTier.BLOCK
        assert decision.bankroll_fraction == 0.0

    def test_exactly_at_threshold_is_not_blocked(self) -> None:
        # The comparison is strict (<), so odds equal to the threshold pass through.
        decision = decide(0.20, american_odds=JUICE_BLOCK)
        assert decision.tier != PickTier.BLOCK

    def test_block_wins_over_any_edge_size(self) -> None:
        decision = decide(0.50, american_odds=-300)
        assert decision.tier == PickTier.BLOCK

    def test_positive_odds_never_blocked(self) -> None:
        decision = decide(0.10, american_odds=250)
        assert decision.tier != PickTier.BLOCK


class TestStakeSizing:
    # Stakes are min(unit size, full Kelly at the quoted price, tier kelly_cap),
    # so these tests use realistic model probabilities (0.50 no-vig baseline)
    # where Kelly is comfortably above the unit sizes.

    def test_strong_gets_max_unit(self) -> None:
        decision = decide_realistic(0.12)
        assert decision.tier == PickTier.STRONG
        assert decision.bankroll_fraction == min(UNIT_SIZES["max"], KELLY_CAPS["strong"])

    def test_moderate_gets_strong_unit(self) -> None:
        decision = decide_realistic(0.08)
        assert decision.tier == PickTier.MODERATE
        assert decision.bankroll_fraction == min(UNIT_SIZES["strong"], KELLY_CAPS["moderate"])

    def test_kelly_cap_limits_strong_stake(self, monkeypatch) -> None:
        monkeypatch.setitem(KELLY_CAPS, "strong", 0.012)
        decision = decide_realistic(0.12)
        assert decision.tier == PickTier.STRONG
        assert decision.bankroll_fraction == 0.012

    def test_kelly_cap_limits_moderate_stake(self, monkeypatch) -> None:
        monkeypatch.setitem(KELLY_CAPS, "moderate", 0.005)
        decision = decide_realistic(0.08)
        assert decision.tier == PickTier.MODERATE
        assert decision.bankroll_fraction == 0.005

    def test_negative_ev_at_quoted_price_gets_zero_stake(self) -> None:
        # Edge vs the no-vig line but negative EV vs the vigged price:
        # p=0.52 at -140 (break-even 0.583) -> full Kelly is 0, stake is 0.
        decision = classify_edge(0.52, 0.45, -140, american_to_decimal(-140))
        assert decision.tier == PickTier.MODERATE
        assert decision.bankroll_fraction == 0.0

    @pytest.mark.parametrize(
        "edge", [-0.10, 0.0, 0.02, 0.04, 0.05, 0.06, 0.08, 0.09, 0.15, 0.50]
    )
    def test_bankroll_fraction_never_exceeds_max_unit(self, edge: float) -> None:
        for decision in (decide(edge), decide_realistic(edge)):
            assert 0.0 <= decision.bankroll_fraction <= UNIT_SIZES["max"]

    def test_stakes_increase_with_tier(self) -> None:
        assert UNIT_SIZES["watch"] < UNIT_SIZES["strong"] < UNIT_SIZES["max"]


class TestConfigConsistency:
    def test_tier_bands_are_ordered(self) -> None:
        # MONITOR / MODERATE / STRONG bands must be ordered and non-overlapping.
        assert THRESHOLDS["watch_min"] <= THRESHOLDS["watch_max"]
        assert THRESHOLDS["watch_max"] <= THRESHOLDS["strong_min"]
        assert THRESHOLDS["strong_min"] < THRESHOLDS["max_bet_min"]
        assert THRESHOLDS["strong_max"] == THRESHOLDS["max_bet_min"]
