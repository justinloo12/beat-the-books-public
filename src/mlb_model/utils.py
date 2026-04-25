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
