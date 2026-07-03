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
JUICE_BLOCK = model_settings.juice_block_threshold

# Even-money odds keep the tier decision driven purely by the edge value.
EVEN_DECIMAL = american_to_decimal(100)


def decide(edge: float, american_odds: int = 100) -> EdgeDecision:
    """Classify with an exact edge (no_vig set to 0 avoids float subtraction noise)."""
    return classify_edge(edge, 0.0, american_odds, american_to_decimal(american_odds))


class TestTierBoundaries:
    def test_edge_below_every_band_is_pass(self) -> None:
        decision = decide(min(THRESHOLDS["pass_below"], THRESHOLDS["watch_min"]) - 0.001)
        assert decision.tier == PickTier.PASS
        assert decision.bankroll_fraction == 0.0

    def test_negative_edge_is_pass(self) -> None:
        decision = decide(-0.05)
        assert decision.tier == PickTier.PASS
        assert decision.bankroll_fraction == 0.0

    def test_watch_min_boundary_is_monitor(self) -> None:
        assert decide(THRESHOLDS["watch_min"]).tier == PickTier.MONITOR

    def test_mid_watch_band_is_monitor(self) -> None:
        mid = (THRESHOLDS["watch_min"] + THRESHOLDS["watch_max"]) / 2
        assert decide(mid).tier == PickTier.MONITOR

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
    def test_strong_gets_max_unit(self) -> None:
        assert decide(THRESHOLDS["max_bet_min"]).bankroll_fraction == UNIT_SIZES["max"]

    def test_moderate_gets_strong_unit(self) -> None:
        assert decide(THRESHOLDS["strong_min"]).bankroll_fraction == UNIT_SIZES["strong"]

    def test_monitor_gets_watch_unit(self) -> None:
        assert decide(THRESHOLDS["watch_min"]).bankroll_fraction == UNIT_SIZES["watch"]

    @pytest.mark.parametrize(
        "edge", [-0.10, 0.0, 0.02, 0.04, 0.05, 0.06, 0.08, 0.09, 0.15, 0.50]
    )
    def test_bankroll_fraction_never_exceeds_max_unit(self, edge: float) -> None:
        decision = decide(edge)
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
