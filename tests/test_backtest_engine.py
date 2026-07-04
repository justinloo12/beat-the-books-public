"""Tests for scripts/backtest.py: staking application, pick harvesting,
and offline grading against a prefilled results cache."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("backtest_script", ROOT / "scripts" / "backtest.py")
backtest = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backtest)


# ---------------------------------------------------------------- current sizing

def _entry(mp, nv, odds, **kw):
    e = {
        "date": "2026-04-25",
        "matchup": "A Team @ B Team",
        "market_type": "moneyline",
        "pick": "A Team",
        "line": 0.0,
        "model_probability": mp,
        "no_vig_probability": nv,
        "american_odds": odds,
    }
    e.update(kw)
    return e


def test_current_stake_strong_edge_hits_tier_cap():
    # edge 0.12 at +100: Kelly = 0.24, unit size (max) 0.03, cap 0.03 -> 3u
    stake, tier = backtest.current_stake_units(_entry(0.62, 0.50, 100))
    assert tier == "strong"
    assert stake == pytest.approx(3.0)


def test_current_stake_sub_threshold_edge_is_pass():
    # edge 0.05 sits below pass_below=0.06 under CURRENT rules -> no bet
    stake, tier = backtest.current_stake_units(_entry(0.55, 0.50, 100))
    assert tier == "pass"
    assert stake == 0.0


def test_current_stake_heavy_juice_is_blocked():
    stake, tier = backtest.current_stake_units(_entry(0.65, 0.55, -150))
    assert tier == "block"
    assert stake == 0.0


def test_current_stake_missing_fields_is_zero():
    stake, tier = backtest.current_stake_units({"model_probability": None})
    assert (stake, tier) == (0.0, "unknown")


# ---------------------------------------------------------------- scenario stats

def test_scenario_stats_flat_staking_math():
    entries = [
        _entry(0.55, 0.50, 100, result="win", tier="monitor"),   # +1.00
        _entry(0.55, 0.50, -110, result="loss", tier="monitor"),  # -1.00
        _entry(0.55, 0.50, -105, result="push", tier="monitor"),  # 0, still staked
        _entry(0.55, 0.50, 120, result="no_result"),              # excluded (ungraded)
    ]
    s = backtest.scenario_stats(entries, lambda e: 1.0)
    assert (s["n"], s["wins"], s["losses"], s["pushes"]) == (3, 1, 1, 1)
    assert s["staked"] == pytest.approx(3.0)
    assert s["profit"] == pytest.approx(0.0)
    assert s["roi"] == pytest.approx(0.0)
    assert s["hit_rate"] == pytest.approx(0.5)


def test_scenario_stats_skips_zero_stakes():
    entries = [_entry(0.55, 0.50, 100, result="win")]
    s = backtest.scenario_stats(entries, lambda e: 0.0)
    assert s["n"] == 0 and s["staked"] == 0.0 and s["roi"] is None


# ---------------------------------------------------------------- harvesting

def _april_board(day="2026-04-25"):
    quote = {
        "matchup": "Athletics @ Texas Rangers",
        "market_type": "game_total",
        "pick": "Over",
        "line": 8.0,
        "american_odds": -104,
        "model_probability": 0.5289,
        "no_vig_probability": 0.487,
        "edge": 0.0419,
        "tier": "monitor",
    }
    other_side = dict(quote, pick="Under", american_odds=-116, model_probability=0.4711,
                      no_vig_probability=0.513, edge=-0.0419, tier="pass")
    return {
        "date": day,
        "daily": {
            "date": day,
            "picks": [],
            "lineup_cards": [
                {"matchup": quote["matchup"], "top_game_picks": [quote, other_side, dict(quote)]}
            ],
            "skipped": [],
        },
    }


def test_harvest_reconstructs_only_positive_edge_bet_tiers_and_dedupes():
    entries = backtest.harvest_picks({"2026-04-25": _april_board()})
    assert len(entries) == 1  # Under is pass, duplicate Over quote deduped
    e = entries[0]
    assert e["pick"] == "Over" and e["is_lean"] is True and e["reconstructed"] is True
    assert e["tier"] == "monitor"


def test_harvest_prefers_published_lists_when_present():
    board = _april_board("2026-06-14")
    board["daily"]["leans"] = [
        {
            "matchup": "A @ B",
            "market_type": "moneyline",
            "pick": "A",
            "american_odds": 105,
            "model_probability": 0.55,
            "no_vig_probability": 0.46,
            "edge": 0.09,
            "tier": "monitor",
        }
    ]
    entries = backtest.harvest_picks({"2026-06-14": board})
    # published leans exist -> lineup cards are NOT mined for that day
    assert len(entries) == 1
    assert entries[0]["reconstructed"] is False and entries[0]["is_lean"] is True


# ---------------------------------------------------------------- offline grading

def test_grade_entries_uses_cache_and_prior_history():
    entries = backtest.harvest_picks({"2026-04-25": _april_board()})
    cache = {
        "2026-04-25": [
            {
                "away_team": "Athletics",
                "home_team": "Texas Rangers",
                "away_score": 5,
                "home_score": 4,
            }
        ]
    }
    backtest.grade_entries(entries, history=[], cache=cache)
    e = entries[0]
    assert e["result"] == "win"  # total 9 > 8
    assert e["final_total"] == 9
    assert e["pnl"] == pytest.approx(96.15, abs=0.01)  # $100 at -104

    # A prior grade in history wins over re-grading
    history = [
        {
            "date": "2026-04-25",
            "matchup": "Athletics @ Texas Rangers",
            "market_type": "game_total",
            "pick": "Over",
            "result": "push",
            "pnl": 0.0,
        }
    ]
    entries2 = backtest.harvest_picks({"2026-04-25": _april_board()})
    backtest.grade_entries(entries2, history=history, cache=cache)
    assert entries2[0]["result"] == "push"


def test_backfill_history_appends_only_new_graded_entries():
    entries = backtest.harvest_picks({"2026-04-25": _april_board()})
    cache = {
        "2026-04-25": [
            {"away_team": "Athletics", "home_team": "Texas Rangers", "away_score": 5, "home_score": 4}
        ]
    }
    backtest.grade_entries(entries, history=[], cache=cache)
    merged, added = backtest.backfill_history(entries, history=[])
    assert added == 1 and len(merged) == 1
    assert merged[0]["source"] == "backtest_backfill"
    assert merged[0]["result"] == "win"
    # idempotent: second run adds nothing
    merged2, added2 = backtest.backfill_history(entries, history=merged)
    assert added2 == 0 and len(merged2) == 1
