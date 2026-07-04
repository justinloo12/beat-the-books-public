"""Tests for the Odds API credit-conservation logic (snapshot script quota
guard + combined-call payload splitting in download_data)."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from mlb_model.download_data import _filter_payload_market

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "snapshot_odds_quota", ROOT / "scripts" / "snapshot_odds.py"
)
snapshot_odds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(snapshot_odds)


# ---------------------------------------------------------------- floor / parsing

def test_int_or_none():
    assert snapshot_odds._int_or_none("19488") == 19488
    assert snapshot_odds._int_or_none("19488.0") == 19488
    assert snapshot_odds._int_or_none(None) is None
    assert snapshot_odds._int_or_none("garbage") is None


def test_min_remaining_floor_env_override(monkeypatch):
    monkeypatch.delenv("ODDS_API_MIN_REMAINING", raising=False)
    assert snapshot_odds.min_remaining_floor() == snapshot_odds.DEFAULT_MIN_REMAINING
    monkeypatch.setenv("ODDS_API_MIN_REMAINING", "1200")
    assert snapshot_odds.min_remaining_floor() == 1200
    monkeypatch.setenv("ODDS_API_MIN_REMAINING", "not-a-number")
    assert snapshot_odds.min_remaining_floor() == snapshot_odds.DEFAULT_MIN_REMAINING


# ---------------------------------------------------------------- last known remaining

def test_last_known_remaining_reads_latest_snapshot(tmp_path):
    old = {
        "date": "2026-07-01",
        "snapshots": [
            {"fetched_at": "2026-07-01T16:45:00Z", "quota": {"requests_remaining": 900}, "games": []}
        ],
    }
    new = {
        "date": "2026-07-02",
        "snapshots": [
            {"fetched_at": "2026-07-02T16:45:00Z", "quota": {"requests_remaining": 850}, "games": []},
            {"fetched_at": "2026-07-02T19:45:00Z", "quota": {"requests_remaining": 848}, "games": []},
        ],
    }
    (tmp_path / "2026-07-01.json").write_text(json.dumps(old))
    (tmp_path / "2026-07-02.json").write_text(json.dumps(new))
    assert snapshot_odds.last_known_remaining(tmp_path) == 848


def test_last_known_remaining_skips_snapshots_without_quota(tmp_path):
    data = {
        "date": "2026-07-02",
        "snapshots": [
            {"fetched_at": "2026-07-02T16:45:00Z", "quota": {"requests_remaining": 700}, "games": []},
            {"fetched_at": "2026-07-02T19:45:00Z", "games": []},  # legacy: no quota block
        ],
    }
    (tmp_path / "2026-07-02.json").write_text(json.dumps(data))
    assert snapshot_odds.last_known_remaining(tmp_path) == 700


def test_last_known_remaining_none_when_never_recorded(tmp_path):
    (tmp_path / "2026-07-02.json").write_text(json.dumps({"date": "2026-07-02", "snapshots": [{"games": []}]}))
    assert snapshot_odds.last_known_remaining(tmp_path) is None
    assert snapshot_odds.last_known_remaining(tmp_path / "missing") is None


# ---------------------------------------------------------------- payload splitting

def test_filter_payload_market_keeps_only_requested_market():
    payload = {
        "timestamp": "t",
        "data": [
            {
                "away_team": "A",
                "home_team": "B",
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "markets": [
                            {"key": "h2h", "outcomes": [1]},
                            {"key": "totals", "outcomes": [2]},
                            {"key": "spreads", "outcomes": [3]},
                        ],
                    }
                ],
            }
        ],
    }
    filtered = _filter_payload_market(payload, "totals")
    markets = filtered["data"][0]["bookmakers"][0]["markets"]
    assert [m["key"] for m in markets] == ["totals"]
    # original untouched
    assert len(payload["data"][0]["bookmakers"][0]["markets"]) == 3
    # non-data keys preserved
    assert filtered["timestamp"] == "t"
