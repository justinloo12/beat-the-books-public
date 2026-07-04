"""Tests for the pure backtest math in mlb_model.backtest_stats."""
from __future__ import annotations

import math

import pytest

from mlb_model.backtest_stats import (
    bootstrap_roi_ci,
    brier_score,
    longest_losing_streak,
    max_drawdown,
    profit_for,
    wilson_interval,
    zero_edge_pvalue,
)


# ---------------------------------------------------------------- Wilson

def test_wilson_no_data_is_total_ignorance():
    assert wilson_interval(0, 0) == (0.0, 1.0)


def test_wilson_known_value():
    # 8/10 with z=1.96: standard Wilson result ~ (0.490, 0.943)
    lo, hi = wilson_interval(8, 10)
    assert lo == pytest.approx(0.4902, abs=1e-3)
    assert hi == pytest.approx(0.9433, abs=1e-3)


def test_wilson_contains_point_estimate_and_bounds():
    lo, hi = wilson_interval(24, 50)
    assert lo < 24 / 50 < hi
    assert 0.0 <= lo <= hi <= 1.0
    # extremes never leave [0, 1]
    assert wilson_interval(0, 20)[0] == 0.0
    assert wilson_interval(20, 20)[1] == 1.0


def test_wilson_narrows_with_n():
    lo1, hi1 = wilson_interval(5, 10)
    lo2, hi2 = wilson_interval(500, 1000)
    assert (hi2 - lo2) < (hi1 - lo1)


def test_wilson_rejects_impossible_wins():
    with pytest.raises(ValueError):
        wilson_interval(11, 10)


# ---------------------------------------------------------------- Brier

def test_brier_perfect_and_coinflip():
    assert brier_score([1.0, 0.0], [1, 0]) == 0.0
    assert brier_score([0.5, 0.5, 0.5], [1, 0, 1]) == pytest.approx(0.25)


def test_brier_validates_inputs():
    with pytest.raises(ValueError):
        brier_score([0.5], [1, 0])
    with pytest.raises(ValueError):
        brier_score([], [])


# ---------------------------------------------------------------- streaks / drawdown

def test_longest_losing_streak_pushes_do_not_reset():
    results = ["loss", "loss", "push", "loss", "win", "loss"]
    assert longest_losing_streak(results) == 3
    assert longest_losing_streak([]) == 0
    assert longest_losing_streak(["win", "win"]) == 0


def test_max_drawdown():
    # +1, +1, -3 (peak 2 -> trough -1: dd 3), +4, -1
    assert max_drawdown([1, 1, -3, 4, -1]) == pytest.approx(3.0)
    assert max_drawdown([]) == 0.0
    assert max_drawdown([1, 2, 3]) == 0.0
    # never-above-water sequence: drawdown from the zero start
    assert max_drawdown([-1, -2]) == pytest.approx(3.0)


# ---------------------------------------------------------------- profit

def test_profit_for():
    assert profit_for("win", 1.91, 1.0) == pytest.approx(0.91)
    assert profit_for("loss", 1.91, 2.0) == -2.0
    assert profit_for("push", 1.91, 1.0) == 0.0
    assert profit_for("no_result", 1.91, 1.0) == 0.0


# ---------------------------------------------------------------- zero-edge bootstrap

def test_zero_edge_pvalue_deterministic_for_fixed_seed():
    bets = [(0.5, 1.91, 1.0)] * 30
    a = zero_edge_pvalue(bets, observed_profit=1.5, n_sims=500, seed=42)
    b = zero_edge_pvalue(bets, observed_profit=1.5, n_sims=500, seed=42)
    assert a == b
    c = zero_edge_pvalue(bets, observed_profit=1.5, n_sims=500, seed=43)
    assert c["p_value"] is not None  # different seed still valid (may or may not differ)


def test_zero_edge_pvalue_empty():
    out = zero_edge_pvalue([], observed_profit=0.0)
    assert out["p_value"] is None


def test_zero_edge_pvalue_extremes():
    # Certain winners: every simulated history earns exactly the same profit.
    bets = [(1.0, 2.0, 1.0)] * 5  # +1u each, always
    sure = zero_edge_pvalue(bets, observed_profit=5.0, n_sims=200, seed=1)
    assert sure["p_value"] == 1.0
    impossible = zero_edge_pvalue(bets, observed_profit=5.01, n_sims=200, seed=1)
    assert impossible["p_value"] == 0.0


def test_zero_edge_null_mean_is_negative_at_vigged_prices():
    # Fair prob 0.5 but paying -110 both ways: the null bettor loses on average.
    bets = [(0.5, 1.909, 1.0)] * 50
    out = zero_edge_pvalue(bets, observed_profit=0.0, n_sims=2000, seed=7)
    assert out["null_mean_profit"] < 0


# ---------------------------------------------------------------- ROI bootstrap CI

def test_bootstrap_roi_ci_deterministic_and_ordered():
    profits = [0.91, -1.0, 0.91, -1.0, 0.91]
    stakes = [1.0] * 5
    a = bootstrap_roi_ci(profits, stakes, n_sims=500, seed=9)
    b = bootstrap_roi_ci(profits, stakes, n_sims=500, seed=9)
    assert a == b
    assert a is not None and a[0] <= a[1]


def test_bootstrap_roi_ci_empty_or_zero_stake():
    assert bootstrap_roi_ci([], []) is None
    assert bootstrap_roi_ci([1.0], [0.0]) is None


def test_bootstrap_roi_ci_degenerate_all_wins():
    ci = bootstrap_roi_ci([0.5, 0.5], [1.0, 1.0], n_sims=100, seed=3)
    assert ci == (pytest.approx(0.5), pytest.approx(0.5))
