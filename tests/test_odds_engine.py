from mlb_model.models import PickTier
from mlb_model.services.odds_engine import (
    american_to_decimal,
    classify_edge,
    implied_probability_from_american,
    no_vig_two_sided,
)


def test_no_vig_two_sided_normalizes_pair() -> None:
    a, b = no_vig_two_sided(0.54, 0.50)
    assert round(a + b, 6) == 1.0


def test_strong_edge_classification() -> None:
    decimal = american_to_decimal(110)
    raw = implied_probability_from_american(110)
    decision = classify_edge(0.63, raw - 0.02, 110, decimal)
    assert decision.tier == PickTier.STRONG
    assert decision.bankroll_fraction > 0
