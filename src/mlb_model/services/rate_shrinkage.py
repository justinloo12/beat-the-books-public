"""Empirical-Bayes shrinkage for per-player event rates.

Vendored from the sibling "Weight Room Hero" project's structural_model.py
(local repo: "Weight Room Hero", file structural_model.py), where a strict
time-based holdout (see that repo's EVALUATION.md) showed that raw
small-sample Statcast rates lose to beta-binomial shrinkage toward the
league mean. The math is copied here — with attribution — rather than
imported, so this repo has no cross-repo runtime dependency.

Core idea
---------
A player's observed event rate (HR/PA, K/PA, BB/PA, ...) over a small
sample mixes true talent with binomial noise. The empirical-Bayes fix:

    posterior rate = (successes + alpha) / (trials + alpha + beta)

where Beta(alpha, beta) is a prior over league talent, fit from the
league-wide distribution of player rates by method of moments: the
observed variance of raw rates is the sum of true talent variance and
expected binomial sampling variance, so the sampling part is subtracted
before matching moments.

Park factors get the same treatment: a raw park factor (park rate /
league rate) is shrunk toward 1.0 with a pseudo-observation count,
because one park/handedness cell only sees a few thousand PA per season.

Nothing here is tuned on this repo's holdout data; all constants are
hand-set and documented, mirroring the WRH source.
"""
from __future__ import annotations

import math
from typing import Iterable, Sequence

# Players need this many trials to enter the method-of-moments fit of the
# league talent distribution (below it, sampling noise swamps talent).
MIN_TRIALS_FOR_MOM = 100

# Bounds on the fitted prior strength (alpha + beta), i.e. how many
# pseudo-trials the league prior is worth. Guards against degenerate fits.
PRIOR_STRENGTH_BOUNDS = (30.0, 2000.0)

# Fallback prior strength when too few qualifiers exist to fit MoM
# (early season): weakly informative, worth ~200 PA — same as WRH.
FALLBACK_PRIOR_STRENGTH = 200.0
MIN_QUALIFIERS_FOR_MOM = 10

# Park factors by batter handedness are shrunk toward 1.0. A park/hand
# cell gets ~2,500-3,500 PA per season (WRH: PARK_PSEUDO_PA = 2000 with
# roughly one-plus season of data behind each factor), so one season
# moves the factor about halfway from 1.0 to its raw value.
PARK_PSEUDO_PA = 2000.0
PARK_HAND_SEASON_PA = 3000.0
PARK_FACTOR_CLIP = (0.80, 1.25)


def fit_beta_binomial_mom(
    successes: Sequence[float],
    trials: Sequence[float],
    min_trials: int = MIN_TRIALS_FOR_MOM,
) -> tuple[float, float]:
    """Fit a Beta(alpha, beta) prior over player rates by method of moments.

    Uses players with trials >= min_trials. The observed variance of raw
    rates includes binomial sampling noise E[m(1-m)/n], which is subtracted
    before matching moments. Returns (alpha, beta).

    Falls back to a weakly-informative prior centred on the pooled rate
    (strength FALLBACK_PRIOR_STRENGTH) when fewer than
    MIN_QUALIFIERS_FOR_MOM qualifiers exist.
    """
    pairs = [
        (float(s), float(n))
        for s, n in zip(successes, trials)
        if n and n >= min_trials
    ]
    total_s = sum(float(s) for s in successes)
    total_n = sum(float(n) for n in trials)
    if len(pairs) < MIN_QUALIFIERS_FOR_MOM:
        pooled = total_s / max(total_n, 1.0)
        pooled = min(max(pooled, 1e-4), 1 - 1e-4)
        return pooled * FALLBACK_PRIOR_STRENGTH, (1.0 - pooled) * FALLBACK_PRIOR_STRENGTH

    rates = [s / n for s, n in pairs]
    k = len(rates)
    m = sum(rates) / k
    m = min(max(m, 1e-4), 1 - 1e-4)
    v_obs = sum((r - m) ** 2 for r in rates) / (k - 1)
    # Expected binomial sampling variance at each player's sample size.
    v_within = sum(m * (1.0 - m) / n for _, n in pairs) / k
    v_between = max(v_obs - v_within, 1e-7)

    strength = m * (1.0 - m) / v_between - 1.0
    lo, hi = PRIOR_STRENGTH_BOUNDS
    strength = min(max(strength, lo), hi)
    return m * strength, (1.0 - m) * strength


def shrunk_rate(successes: float, trials: float, alpha: float, beta: float) -> float:
    """Beta-binomial posterior mean: (successes + alpha) / (trials + alpha + beta).

    Lies strictly between the raw rate and the prior mean
    alpha/(alpha+beta), approaching the raw rate as trials grows.
    """
    return (successes + alpha) / (trials + alpha + beta)


def shrink_rate(rate: float | None, trials: float, alpha: float, beta: float) -> float | None:
    """Convenience form for callers that hold a rate + sample size instead
    of raw counts: reconstructs successes = rate * trials. None passes
    through (missing stat stays missing)."""
    if rate is None:
        return None
    n = max(float(trials), 0.0)
    return shrunk_rate(float(rate) * n, n, alpha, beta)


def park_factor_shrunk(
    raw_factor: float,
    sample_pa: float = PARK_HAND_SEASON_PA,
    pseudo_pa: float = PARK_PSEUDO_PA,
    clip: tuple[float, float] = PARK_FACTOR_CLIP,
) -> float:
    """Shrink a raw park factor toward 1.0 with pseudo-observations, then clip.

    Algebraically identical to WRH's park_factors_by_hand: with raw factor
    r = park_rate / league_rate observed over n PA,
        shrunk = (n*park_rate + pseudo*league) / ((n + pseudo) * league)
               = 1 + (r - 1) * n / (n + pseudo).
    """
    n = max(float(sample_pa), 0.0)
    w = n / (n + float(pseudo_pa)) if (n + pseudo_pa) > 0 else 0.0
    factor = 1.0 + (float(raw_factor) - 1.0) * w
    lo, hi = clip
    return float(min(max(factor, lo), hi))


class EventPriors:
    """Beta priors per event type ('k', 'bb', '1b', '2b', '3b', 'hr', ...).

    Built once per Statcast load from the league-wide distribution of
    per-player rates; `shrink` then maps any (rate, n) to its posterior.
    An event with no fitted prior passes rates through unchanged, so a
    provider with no Statcast data behaves exactly as before.
    """

    def __init__(self, priors: dict[str, tuple[float, float]] | None = None) -> None:
        self.priors: dict[str, tuple[float, float]] = dict(priors or {})

    @classmethod
    def fit(cls, samples: dict[str, Iterable[tuple[float, float]]]) -> "EventPriors":
        """samples: event -> iterable of (successes, trials) per player."""
        priors: dict[str, tuple[float, float]] = {}
        for event, pairs in samples.items():
            pairs = list(pairs)
            if not pairs:
                continue
            succ = [p[0] for p in pairs]
            tri = [p[1] for p in pairs]
            if sum(tri) <= 0:
                continue
            priors[event] = fit_beta_binomial_mom(succ, tri)
        return cls(priors)

    def has(self, event: str) -> bool:
        return event in self.priors

    def prior_mean(self, event: str) -> float | None:
        ab = self.priors.get(event)
        if not ab:
            return None
        alpha, beta = ab
        return alpha / (alpha + beta)

    def shrink(self, event: str, rate: float | None, trials: float) -> float | None:
        """Posterior rate for one player's (rate, trials); raw rate when no
        prior is fitted for the event, None when rate is None."""
        if rate is None:
            return None
        ab = self.priors.get(event)
        if not ab:
            return float(rate)
        if not math.isfinite(float(rate)) or not math.isfinite(float(trials)):
            return float(rate)
        alpha, beta = ab
        return float(shrink_rate(rate, trials, alpha, beta))
