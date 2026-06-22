"""Deterministic run-distribution engine.

This is the heart of the sportsbook-style model: given exactly two numbers —
the home team's expected runs and the away team's expected runs — it derives
EVERY market price (moneyline, game total, run line) from a single coherent
scoring distribution. There are no random Monte-Carlo draws and no per-market
magic coefficients: change a team total and every price moves with it.

MLB single-team run scoring is over-dispersed relative to a Poisson process
(big innings cluster), so we model each team's runs with a Negative Binomial
whose variance-to-mean ratio matches the empirical ~1.25-1.30 seen in MLB
team-game data. The two team distributions are then combined exactly (no
sampling) to produce win / total / run-line probabilities.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import lgamma, log, exp

from mlb_model.utils import clamp

# Highest run total we evaluate per team. P(team scores >25 in 9 innings) is
# vanishingly small, so truncating here costs nothing and keeps the convolution
# cheap and exact.
_MAX_RUNS = 25


@dataclass(slots=True)
class MarketProbabilities:
    home_win_prob: float
    away_win_prob: float
    total_mean: float
    home_runs_mean: float
    away_runs_mean: float
    # line -> P(total runs > line)
    total_over_probabilities: dict[float, float]
    runline_home_cover_prob: float  # P(home wins by 2+)
    runline_away_cover_prob: float  # P(away wins by 2+)
    # side -> {line -> P(team runs > line)}
    team_total_over_probabilities: dict[str, dict[float, float]]


class RunDistributionService:
    def __init__(self, variance_to_mean: float = 1.28, home_extra_inning_edge: float = 0.52) -> None:
        # Empirical MLB team-game variance/mean ratio. >1 because of over-dispersion
        # (innings are not independent identical Poisson events). Tunable via the
        # backtest rather than hard-coded by feel.
        self.variance_to_mean = max(variance_to_mean, 1.0001)
        # When regulation ends tied, the game goes to extra innings. The home team
        # wins extras slightly more than half the time (last at-bat / walk-off).
        self.home_extra_inning_edge = home_extra_inning_edge

    # ------------------------------------------------------------------ #
    # Negative-binomial PMF for a single team's run total                  #
    # ------------------------------------------------------------------ #
    def _team_pmf(self, mean_runs: float) -> list[float]:
        """Return P(team scores k runs) for k = 0.._MAX_RUNS as a list.

        Negative Binomial parameterised by mean (mu) and a fixed variance/mean
        ratio c: var = c * mu = mu + mu^2 / r  =>  r = mu / (c - 1).
        """
        mu = max(mean_runs, 0.05)
        r = mu / (self.variance_to_mean - 1.0)
        # p = probability of "success"; mean = r*p/(1-p)  =>  p = mu/(mu+r)
        p = mu / (mu + r)
        log_1_minus_p = log(1.0 - p)
        log_p = log(p)
        pmf: list[float] = []
        for k in range(_MAX_RUNS + 1):
            # log C(k+r-1, k) = lgamma(k+r) - lgamma(r) - lgamma(k+1)
            log_coeff = lgamma(k + r) - lgamma(r) - lgamma(k + 1)
            log_pk = log_coeff + r * log_1_minus_p + k * log_p
            pmf.append(exp(log_pk))
        total = sum(pmf)
        # Renormalise to absorb the truncated tail beyond _MAX_RUNS.
        return [v / total for v in pmf]

    # ------------------------------------------------------------------ #
    # Public entry point                                                   #
    # ------------------------------------------------------------------ #
    def derive(
        self,
        home_runs: float,
        away_runs: float,
        total_lines: set[float] | None = None,
    ) -> MarketProbabilities:
        total_lines = total_lines or set()
        home_pmf = self._team_pmf(home_runs)
        away_pmf = self._team_pmf(away_runs)

        home_win = 0.0
        away_win = 0.0
        tie = 0.0
        home_by_2plus = 0.0
        away_by_2plus = 0.0
        # Joint over (home=h, away=a). Exact double sum, no sampling.
        for h, ph in enumerate(home_pmf):
            if ph <= 0.0:
                continue
            for a, pa in enumerate(away_pmf):
                joint = ph * pa
                if joint <= 0.0:
                    continue
                diff = h - a
                if diff > 0:
                    home_win += joint
                    if diff >= 2:
                        home_by_2plus += joint
                elif diff < 0:
                    away_win += joint
                    if -diff >= 2:
                        away_by_2plus += joint
                else:
                    tie += joint

        # Resolve regulation ties via extra innings (home edge).
        home_win += tie * self.home_extra_inning_edge
        away_win += tie * (1.0 - self.home_extra_inning_edge)

        # Total runs distribution = convolution of the two team PMFs.
        total_pmf = [0.0] * (2 * _MAX_RUNS + 1)
        for h, ph in enumerate(home_pmf):
            if ph <= 0.0:
                continue
            for a, pa in enumerate(away_pmf):
                total_pmf[h + a] += ph * pa

        home_mean = sum(k * p for k, p in enumerate(home_pmf))
        away_mean = sum(k * p for k, p in enumerate(away_pmf))
        total_mean = home_mean + away_mean

        # Run scoring is RIGHT-SKEWED, so the distribution's mean sits above its
        # median: P(total > mean) is < 0.5. The market total line is effectively
        # the 50/50 point, so pricing the over as the raw upper-tail introduces a
        # systematic UNDER lean — the model would price every over below 50% even
        # when its own mean equals the line. We remove that skew offset so that
        # when the model's mean equals the line, P(over) is exactly 0.5, and only
        # a genuine model-vs-line disagreement moves it off 50%.
        def _over(pmf: list[float], mean: float, line: float) -> float:
            raw = sum(prob for runs, prob in enumerate(pmf) if runs > line)
            skew_offset = sum(prob for runs, prob in enumerate(pmf) if runs > mean)
            return clamp(raw - skew_offset + 0.5, 0.0001, 0.9999)

        total_over: dict[float, float] = {}
        for line in total_lines:
            total_over[line] = _over(total_pmf, total_mean, line)

        team_total_over: dict[str, dict[float, float]] = {"home": {}, "away": {}}
        for line in total_lines:
            half = line / 2.0
            team_total_over["home"][half] = _over(home_pmf, home_mean, half)
            team_total_over["away"][half] = _over(away_pmf, away_mean, half)

        return MarketProbabilities(
            home_win_prob=clamp(home_win, 0.0001, 0.9999),
            away_win_prob=clamp(away_win, 0.0001, 0.9999),
            total_mean=round(home_mean + away_mean, 3),
            home_runs_mean=round(home_mean, 3),
            away_runs_mean=round(away_mean, 3),
            total_over_probabilities=total_over,
            runline_home_cover_prob=clamp(home_by_2plus, 0.0001, 0.9999),
            runline_away_cover_prob=clamp(away_by_2plus, 0.0001, 0.9999),
            team_total_over_probabilities=team_total_over,
        )
