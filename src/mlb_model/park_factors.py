"""Hit-type x handedness park factors and the conversion from a batter's
batted-ball profile into an expected-wOBA adjustment for a given park.

Why this exists
---------------
A single scalar park factor (Coors = 1.15) captures the overall run environment
but misses *how* a park plays. Fenway's Green Monster is the canonical example:
it is only ~310 ft to left but 37 ft tall, so it turns line drives and lower fly
balls into DOUBLES while actually SUPPRESSING right-handed home runs (towering
flies that clear a normal fence bang off the wall). Yankee Stadium's short
right-field porch does the opposite for LEFT-handed pull power.

To model that we need park factors split by batted-ball outcome (1B/2B/3B/HR)
AND by batter handedness, combined with each hitter's own batted-ball mix and
pull tendency. A dead-pull right-handed hitter is far more exposed to the Monster
than an oppo-field lefty slap hitter standing in the same box.

The numbers below are centered at 1.0 (league-neutral) and are grounded in
publicly published Baseball Savant handedness/hit-type park factors. Parks
without a strong, well-established asymmetry are left neutral on purpose — the
scalar PARK_FACTORS table still carries their overall run environment, and the
backtest is the place to tune these rather than guessing precise values here.
"""
from __future__ import annotations

from mlb_model.utils import clamp

# ---------------------------------------------------------------------------
# wOBA linear weights (~2023-24 scale) used to convert a change in a batter's
# hit-type rates into a change in expected wOBA. A double is worth ~1.25 wOBA,
# a home run ~2.0, etc. These are the standard FanGraphs linear weights rescaled
# onto the wOBA scale, not free parameters.
# ---------------------------------------------------------------------------
_WOBA_WEIGHTS = {"1B": 0.88, "2B": 1.25, "3B": 1.58, "HR": 2.02}

# League-average per-PA rates for each hit type, used as the base a batter's
# park exposure acts on when their own rate is unknown.
_LG_RATES = {"1B": 0.150, "2B": 0.047, "3B": 0.005, "HR": 0.033}

# League-average pull rate on batted balls. A hitter above this is more exposed
# to a park's pull-side features (short porch, the Monster); below it, less.
_LG_PULL_PCT = 0.40

# The park hit-type adjustment can never move expected wOBA by more than this,
# in wOBA points (~0.37 runs at the extreme). Larger than the swing-alignment
# cap (0.008) because park effects are a stronger, far better-established signal,
# but deliberately small because the scalar PARK_FACTORS table already carries
# each park's OVERALL run environment — this delta is only the handedness /
# batted-ball SHAPE on top of it. Keeping it tight prevents double-counting the
# run environment; the exact split between the scalar and this delta is a
# backtest calibration target, not something to guess precisely here.
_MAX_PARK_WOBA = 0.012

# Neutral default returned for any park/hit-type we have not characterised.
_NEUTRAL = {"1B": 1.0, "2B": 1.0, "3B": 1.0, "HR": 1.0}

# ---------------------------------------------------------------------------
# Hit-type x handedness park factors. Keyed by venue, then batter stand ("R"/"L").
# Only meaningful deviations from 1.0 are listed; everything else defaults to 1.0.
# ---------------------------------------------------------------------------
PARK_HIT_FACTORS: dict[str, dict[str, dict[str, float]]] = {
    # Altitude inflates everything, especially gaps -> triples.
    "Coors Field": {
        "R": {"1B": 1.04, "2B": 1.08, "3B": 1.35, "HR": 1.10},
        "L": {"1B": 1.04, "2B": 1.08, "3B": 1.30, "HR": 1.10},
    },
    # The Monster: huge RHB doubles, suppressed RHB HR and 3B (balls carom back).
    "Fenway Park": {
        "R": {"1B": 1.05, "2B": 1.28, "3B": 0.85, "HR": 0.97},
        "L": {"1B": 1.03, "2B": 1.10, "3B": 0.82, "HR": 1.03},
    },
    # Short RF porch: a gift for left-handed pull power.
    "Yankee Stadium": {
        "R": {"2B": 0.97, "HR": 0.99},
        "L": {"2B": 0.98, "HR": 1.15},
    },
    # Triples Alley: deep RF death valley crushes LHB homers, inflates triples.
    "Oracle Park": {
        "R": {"3B": 1.25, "HR": 0.93},
        "L": {"3B": 1.30, "HR": 0.82},
    },
    "Petco Park": {
        "R": {"HR": 0.92},
        "L": {"HR": 0.90},
    },
    # LF wall pushed back in 2022 — RHB HR down sharply.
    "Oriole Park at Camden Yards": {
        "R": {"HR": 0.83},
        "L": {"HR": 1.04},
    },
    "Great American Ball Park": {
        "R": {"HR": 1.12},
        "L": {"HR": 1.14},
    },
    "Citizens Bank Park": {
        "R": {"HR": 1.08},
        "L": {"HR": 1.10},
    },
    # Cavernous outfield: few homers, lots of triples.
    "Kauffman Stadium": {
        "R": {"3B": 1.25, "HR": 0.92},
        "L": {"3B": 1.22, "HR": 0.90},
    },
    "Comerica Park": {
        "R": {"3B": 1.20, "HR": 0.93},
        "L": {"3B": 1.18, "HR": 0.94},
    },
    "T-Mobile Park": {
        "R": {"HR": 0.91},
        "L": {"HR": 0.90},
    },
    "loanDepot park": {
        "R": {"HR": 0.92},
        "L": {"HR": 0.92},
    },
    # Crawford Boxes in LF reward right-handed pull power.
    "Minute Maid Park": {
        "R": {"2B": 1.05, "HR": 1.10},
        "L": {"HR": 1.00},
    },
    "Dodger Stadium": {
        "R": {"HR": 1.05},
        "L": {"HR": 1.02},
    },
    "Busch Stadium": {
        "R": {"HR": 0.94},
        "L": {"HR": 0.93},
    },
}

# Venue aliases (sponsorship renames / spelling) that map to a canonical key above.
_ALIASES = {
    "Daikin Park": "Minute Maid Park",
    "PETCO Park": "Petco Park",
}


def park_hit_factors(venue: str | None, hand: str | None) -> dict[str, float]:
    """Hit-type park factors for one venue and batter handedness, neutral default."""
    if not venue:
        return dict(_NEUTRAL)
    key = _ALIASES.get(venue, venue)
    by_hand = PARK_HIT_FACTORS.get(key)
    if not by_hand:
        return dict(_NEUTRAL)
    factors = by_hand.get(hand or "R") or {}
    return {**_NEUTRAL, **factors}


def batter_park_woba_delta(
    venue: str | None,
    hand: str | None,
    bb_rates: dict[str, float] | None,
    pull_pct: float | None,
    weather_multiplier: float = 1.0,
) -> float:
    """Expected-wOBA adjustment for one batter from the park's hit-type profile.

    Combines four grounded inputs:
      * the park's factor for each hit type at this batter's handedness,
      * the batter's own rate of each hit type (falling back to league average),
      * the batter's pull tendency (directional outcomes — HR/2B/3B — only matter
        to the extent the hitter actually pulls into the short/tall features),
      * weather, applied only to the air-dependent components (HR/2B/3B), so a
        ball-carrying day in a hitter's park compounds while a 1B is untouched.

    Returns a wOBA delta bounded by +/- _MAX_PARK_WOBA.
    """
    factors = park_hit_factors(venue, hand)
    rates = bb_rates or {}
    pull = _LG_PULL_PCT if pull_pct is None else float(pull_pct)
    # Pull exposure: 1.0 at league-average pull rate, scaling up for dead-pull
    # hitters and down for oppo-field hitters. Bounded so it stays a modifier.
    pull_exposure = clamp(1.0 + (pull - _LG_PULL_PCT) * 2.5, 0.5, 1.6)
    # Air boost: only the carry-dependent outcomes ride the weather multiplier.
    air = 1.0 + (weather_multiplier - 1.0)

    delta = 0.0
    for ht in ("1B", "2B", "3B", "HR"):
        rate = rates.get(ht)
        rate = _LG_RATES[ht] if rate is None else float(rate)
        pf = factors.get(ht, 1.0)
        if ht == "1B":
            exposure = 1.0  # singles are essentially direction- and air-neutral
        else:
            exposure = pull_exposure * air
        delta += _WOBA_WEIGHTS[ht] * rate * (pf - 1.0) * exposure

    return clamp(delta, -_MAX_PARK_WOBA, _MAX_PARK_WOBA)
