"""Pure math for the historical backtest: intervals, Brier scores, drawdown,
losing streaks, and the zero-edge bootstrap.

Everything here is deterministic given its inputs (the bootstrap takes an
explicit seed) and has no I/O, so it is unit-testable in isolation.
"""
from __future__ import annotations

import math
import random
from typing import Iterable, Sequence


# ---------------------------------------------------------------------------
# Wilson score interval
# ---------------------------------------------------------------------------

def wilson_interval(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% (by default) Wilson score interval for a binomial proportion.

    Returns (low, high). For n == 0 returns (0.0, 1.0) — total ignorance.
    """
    if n <= 0:
        return 0.0, 1.0
    if wins < 0 or wins > n:
        raise ValueError(f"wins={wins} outside [0, n={n}]")
    p_hat = wins / n
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p_hat + z2 / (2 * n)) / denom
    margin = (z / denom) * math.sqrt(p_hat * (1 - p_hat) / n + z2 / (4 * n * n))
    return max(0.0, center - margin), min(1.0, center + margin)


# ---------------------------------------------------------------------------
# Brier score
# ---------------------------------------------------------------------------

def brier_score(probs: Sequence[float], outcomes: Sequence[int]) -> float:
    """Mean squared error of probabilities vs binary outcomes (0/1).

    Lower is better; 0.25 is the score of a constant 0.5 forecast.
    """
    if len(probs) != len(outcomes):
        raise ValueError("probs and outcomes must be the same length")
    if not probs:
        raise ValueError("empty inputs")
    return sum((p - o) ** 2 for p, o in zip(probs, outcomes)) / len(probs)


# ---------------------------------------------------------------------------
# Streaks and drawdown
# ---------------------------------------------------------------------------

def longest_losing_streak(results: Iterable[str]) -> int:
    """Longest run of consecutive 'loss' entries (pushes break nothing but
    don't extend the streak)."""
    longest = current = 0
    for r in results:
        if r == "loss":
            current += 1
            longest = max(longest, current)
        elif r == "win":
            current = 0
        # push / no_result: streak neither extends nor resets
    return longest


def max_drawdown(profits: Sequence[float]) -> float:
    """Maximum peak-to-trough decline (a non-negative number, in the same
    units as the inputs) of the cumulative sum of per-bet profits."""
    peak = 0.0
    cum = 0.0
    worst = 0.0
    for p in profits:
        cum += p
        peak = max(peak, cum)
        worst = max(worst, peak - cum)
    return worst


# ---------------------------------------------------------------------------
# Staking / profit
# ---------------------------------------------------------------------------

def profit_for(result: str, decimal_odds: float, stake: float) -> float:
    """Profit in stake units for one bet. Pushes and voids return 0."""
    if result == "win":
        return stake * (decimal_odds - 1.0)
    if result == "loss":
        return -stake
    return 0.0


# ---------------------------------------------------------------------------
# Zero-edge bootstrap
# ---------------------------------------------------------------------------

def zero_edge_pvalue(
    bets: Sequence[tuple[float, float, float]],
    observed_profit: float,
    n_sims: int = 10_000,
    seed: int = 20260703,
) -> dict:
    """Monte-Carlo p-value for H0: 'the bettor has zero edge'.

    Each bet is (fair_prob, decimal_odds, stake) where fair_prob is the
    NO-VIG market probability of the picked side. Under the null the picked
    side wins exactly at the market's fair rate, and any profit is luck.
    (Note the null bettor still pays vig, so the null's mean profit is
    negative — this is the honest benchmark for someone betting real prices.)

    Returns {p_value, null_mean_profit, null_p5, null_p95, n_sims, seed}.
    p_value = fraction of simulated zero-edge histories with profit >= observed.
    Deterministic for a given seed.
    """
    if not bets:
        return {
            "p_value": None,
            "null_mean_profit": None,
            "null_p5": None,
            "null_p95": None,
            "n_sims": n_sims,
            "seed": seed,
        }
    rng = random.Random(seed)
    profits = []
    at_least = 0
    for _ in range(n_sims):
        total = 0.0
        for fair_prob, dec, stake in bets:
            if rng.random() < fair_prob:
                total += stake * (dec - 1.0)
            else:
                total -= stake
        profits.append(total)
        if total >= observed_profit - 1e-12:
            at_least += 1
    profits.sort()
    return {
        "p_value": at_least / n_sims,
        "null_mean_profit": sum(profits) / n_sims,
        "null_p5": profits[int(0.05 * n_sims)],
        "null_p95": profits[int(0.95 * n_sims) - 1],
        "n_sims": n_sims,
        "seed": seed,
    }


def bootstrap_roi_ci(
    profits: Sequence[float],
    stakes: Sequence[float],
    n_sims: int = 10_000,
    seed: int = 20260703,
) -> tuple[float, float] | None:
    """Percentile 95% CI of ROI from resampling bets with replacement.

    Deterministic for a given seed. Returns None when there are no bets or
    when total stake is zero.
    """
    n = len(profits)
    if n == 0 or len(stakes) != n or sum(stakes) <= 0:
        return None
    rng = random.Random(seed)
    rois = []
    for _ in range(n_sims):
        idx = [rng.randrange(n) for _ in range(n)]
        stake_sum = sum(stakes[i] for i in idx)
        if stake_sum <= 0:
            rois.append(0.0)
            continue
        rois.append(sum(profits[i] for i in idx) / stake_sum)
    rois.sort()
    return rois[int(0.025 * n_sims)], rois[int(0.975 * n_sims) - 1]
