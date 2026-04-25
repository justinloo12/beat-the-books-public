from __future__ import annotations

from dataclasses import dataclass

from mlb_model.config import get_settings
from mlb_model.models import PickTier
from mlb_model.utils import clamp


settings = get_settings()
model_settings = settings.load_model_settings()


@dataclass(slots=True)
class EdgeDecision:
    no_vig_probability: float
    edge: float
    tier: PickTier
    bankroll_fraction: float


def american_to_decimal(odds: int) -> float:
    return 1 + (odds / 100 if odds > 0 else 100 / abs(odds))


def implied_probability_from_american(odds: int) -> float:
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def no_vig_two_sided(prob_a: float, prob_b: float) -> tuple[float, float]:
    total = prob_a + prob_b
    if total == 0:
        return 0.5, 0.5
    return prob_a / total, prob_b / total


def no_vig_one_sided(probability: float, vig: float = 0.045) -> float:
    return clamp(probability - vig, 0.0, 1.0)


def kelly_fraction(model_probability: float, decimal_odds: float) -> float:
    b = decimal_odds - 1
    p = model_probability
    q = 1 - p
    if b <= 0:
        return 0.0
    return max(0.0, (b * p - q) / b)


def classify_edge(
    model_probability: float,
    no_vig_probability: float,
    american_odds: int,
    decimal_odds: float,
) -> EdgeDecision:
    edge = model_probability - no_vig_probability
    thresholds = model_settings.edge_thresholds
    if american_odds < model_settings.juice_block_threshold:
        return EdgeDecision(no_vig_probability, edge, PickTier.BLOCK, 0.0)

    if edge >= thresholds["max_bet_min"]:
        return EdgeDecision(no_vig_probability, edge, PickTier.STRONG, model_settings.unit_sizes["max"])
    if thresholds["strong_min"] <= edge < thresholds["strong_max"]:
        return EdgeDecision(no_vig_probability, edge, PickTier.MODERATE, model_settings.unit_sizes["strong"])
    if thresholds["watch_min"] <= edge <= thresholds["watch_max"]:
        return EdgeDecision(no_vig_probability, edge, PickTier.MONITOR, model_settings.unit_sizes["watch"])
    if edge < thresholds["pass_below"]:
        return EdgeDecision(no_vig_probability, edge, PickTier.PASS, 0.0)
    if thresholds["pass_below"] <= edge < thresholds["watch_min"]:
        return EdgeDecision(no_vig_probability, edge, PickTier.PASS, 0.0)
    return EdgeDecision(no_vig_probability, edge, PickTier.PASS, 0.0)
