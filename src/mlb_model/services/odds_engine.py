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


def _stake_for(unit_key: str, kelly_cap_key: str | None, model_probability: float, decimal_odds: float) -> float:
    """Stake sizing: flat unit size, capped by full Kelly and the configured tier cap.

    The unit sizes in config are the baseline stake per tier. The stake is then
    capped at the full-Kelly fraction for the quoted price (a bet with positive
    edge vs the no-vig line can still be negative-EV vs the vigged price — Kelly
    correctly sizes those toward zero) and finally at the tier's ``kelly_caps``
    ceiling from config.
    """
    stake = model_settings.unit_sizes[unit_key]
    stake = min(stake, kelly_fraction(model_probability, decimal_odds))
    if kelly_cap_key is not None:
        cap = model_settings.kelly_caps.get(kelly_cap_key)
        if cap is not None:
            stake = min(stake, cap)
    return stake


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
        return EdgeDecision(
            no_vig_probability, edge, PickTier.STRONG, _stake_for("max", "strong", model_probability, decimal_odds)
        )
    if thresholds["strong_min"] <= edge < thresholds["strong_max"]:
        return EdgeDecision(
            no_vig_probability, edge, PickTier.MODERATE, _stake_for("strong", "moderate", model_probability, decimal_odds)
        )
    # PASS is checked before the watch band: any edge below pass_below is a
    # PASS even if it also falls inside [watch_min, watch_max]. With the
    # default config (pass_below=0.06, watch band 0.035-0.06) this closes the
    # historical gap where 0.035-0.06 edges were labeled MONITOR and given a
    # stake despite sitting below the configured pass threshold.
    if edge < thresholds["pass_below"]:
        return EdgeDecision(no_vig_probability, edge, PickTier.PASS, 0.0)
    if thresholds["watch_min"] <= edge <= thresholds["watch_max"]:
        return EdgeDecision(
            no_vig_probability, edge, PickTier.MONITOR, _stake_for("watch", None, model_probability, decimal_odds)
        )
    return EdgeDecision(no_vig_probability, edge, PickTier.PASS, 0.0)
