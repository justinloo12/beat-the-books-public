from __future__ import annotations

import math
from collections.abc import Iterable


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def safe_mean(values: Iterable[float], default: float = 0.0) -> float:
    items = list(values)
    if not items:
        return default
    return sum(items) / len(items)


def shannon_entropy(distribution: dict[str, float]) -> float:
    probabilities = [value for value in distribution.values() if value > 0]
    return -sum(p * math.log(p, 2) for p in probabilities)


def logistic_probability(delta: float, scale: float = 1.0) -> float:
    return 1.0 / (1.0 + math.exp(-(delta / scale)))


# League-average wOBA value of an unintentional walk (~2023-24 linear weights,
# on the wOBA scale). Used to fold walks back into an expected-wOBA estimate.
_BB_WOBA = 0.69


def expected_woba(
    k_pct: float | None,
    bb_pct: float | None,
    xwoba_on_contact: float | None,
    default_contact: float = 0.370,
) -> float:
    """Build a TRUE expected wOBA per plate appearance from its components.

    Statcast's estimated_woba_using_speedangle is computed over batted balls
    only, so it excludes strikeouts (the most predictive pitching skill) and
    walks. A run model fed that contact-only number cannot tell a high-strikeout
    arm from a soft-contact one. This recombines the three independent pieces of
    a PA outcome into a single wOBA:

        wOBA = BB% * w_BB + (contact share) * xwOBA_on_contact + K% * 0

    where contact share = 1 - K% - BB%. Strikeouts correctly contribute zero.
    """
    k = clamp(float(k_pct or 0.0), 0.0, 0.6)
    bb = clamp(float(bb_pct or 0.0), 0.0, 0.3)
    contact_woba = float(xwoba_on_contact or default_contact)
    contact_share = clamp(1.0 - k - bb, 0.0, 1.0)
    return clamp(bb * _BB_WOBA + contact_share * contact_woba, 0.150, 0.600)

