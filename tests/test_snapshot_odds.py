"""Tests for the intraday odds snapshot script and the closing-proxy
selection logic in grade_picks."""
from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import pytest

import mlb_model.grade_picks as grade_picks

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("snapshot_odds", ROOT / "scripts" / "snapshot_odds.py")
snapshot_odds = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_spec and snapshot_odds)


def _raw_game(away="Boston Red Sox", home="New York Yankees", commence="2026-07-03T23:05:00Z",
              away_price=120, home_price=-140, total_point=8.5):
    return {
        "away_team": away,
        "home_team": home,
        "commence_time": commence,
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": [
                            {"name": away, "price": away_price},
                            {"name": home, "price": home_price},
                        ],
                    },
                    {
                        "key": "totals",
                        "outcomes": [
                            {"name": "Over", "point": total_point, "price": -110},
                            {"name": "Under", "point": total_point, "price": -110},
                        ],
                    },
                ],
            }
        ],
    }


# ---------------------------------------------------------------------------
# snapshot_odds.build_games
# ---------------------------------------------------------------------------

class TestBuildGames:
    def test_extracts_moneyline_and_totals(self):
        games = snapshot_odds.build_games([_raw_game()], date(2026, 7, 3))
        assert len(games) == 1
        game = games[0]
        assert game["moneyline"]["away_odds"] == 120
        assert game["moneyline"]["home_odds"] == -140
        assert 0.99 < game["moneyline"]["away_no_vig"] + game["moneyline"]["home_no_vig"] < 1.01
        assert {o["name"] for o in game["totals"]} == {"Over", "Under"}
        assert game["totals"][0]["point"] == 8.5

    def test_filters_other_dates(self):
        # 2026-07-04 23:05 UTC is July 4 in ET — excluded from a July 3 slate
        games = snapshot_odds.build_games(
            [_raw_game(commence="2026-07-04T23:05:00Z")], date(2026, 7, 3)
        )
        assert games == []

    def test_late_utc_game_still_on_et_slate(self):
        # 2026-07-04 02:10 UTC == 2026-07-03 10:10pm ET — belongs to July 3
        games = snapshot_odds.build_games(
            [_raw_game(commence="2026-07-04T02:10:00Z")], date(2026, 7, 3)
        )
        assert len(games) == 1

    def test_skips_games_without_draftkings(self):
        raw = _raw_game()
        raw["bookmakers"][0]["key"] = "fanduel"
        assert snapshot_odds.build_games([raw], date(2026, 7, 3)) == []


# ---------------------------------------------------------------------------
# snapshot_odds.append_snapshot
# ---------------------------------------------------------------------------

class TestAppendSnapshot:
    def test_creates_and_appends(self, tmp_path):
        path = tmp_path / "2026-07-03.json"
        snap1 = {"fetched_at": "2026-07-03T16:45:00Z", "games": []}
        snap2 = {"fetched_at": "2026-07-03T19:45:00Z", "games": []}
        assert snapshot_odds.append_snapshot(path, snap1) == 1
        assert snapshot_odds.append_snapshot(path, snap2) == 2
        data = json.loads(path.read_text())
        assert data["date"] == "2026-07-03"
        assert [s["fetched_at"] for s in data["snapshots"]] == [
            "2026-07-03T16:45:00Z",
            "2026-07-03T19:45:00Z",
        ]

    def test_recovers_from_corrupt_file(self, tmp_path):
        path = tmp_path / "2026-07-03.json"
        path.write_text("{not json")
        count = snapshot_odds.append_snapshot(path, {"fetched_at": "2026-07-03T16:45:00Z", "games": []})
        assert count == 1

    def test_caps_snapshot_count_keeping_newest(self, tmp_path):
        path = tmp_path / "2026-07-03.json"
        for hour in range(10, 22):
            snapshot_odds.append_snapshot(
                path, {"fetched_at": f"2026-07-03T{hour:02d}:00:00Z", "games": []}, max_per_day=4
            )
        data = json.loads(path.read_text())
        assert len(data["snapshots"]) == 4
        assert data["snapshots"][-1]["fetched_at"] == "2026-07-03T21:00:00Z"
        assert data["snapshots"][0]["fetched_at"] == "2026-07-03T18:00:00Z"


# ---------------------------------------------------------------------------
# grade_picks closing-proxy selection
# ---------------------------------------------------------------------------

def _snap_game(commence: str, away_odds: int, home_odds: int) -> dict:
    return {
        "away_team": "Boston Red Sox",
        "home_team": "New York Yankees",
        "commence_time": commence,
        "moneyline": {"away_odds": away_odds, "home_odds": home_odds},
        "totals": [],
    }


@pytest.fixture()
def snapshot_dir(tmp_path, monkeypatch):
    snap_dir = tmp_path / "odds_snapshots"
    snap_dir.mkdir()
    monkeypatch.setattr(grade_picks, "SNAPSHOT_DIR", snap_dir)
    monkeypatch.setattr(grade_picks, "LIVE_ODDS_PATH", tmp_path / "live_odds.json")
    return snap_dir


class TestClosingProxySelection:
    def test_prefers_last_pre_pitch_snapshot(self, snapshot_dir):
        commence = "2026-07-03T23:05:00Z"
        payload = {
            "date": "2026-07-03",
            "snapshots": [
                {"fetched_at": "2026-07-03T16:45:00Z", "games": [_snap_game(commence, 110, -130)]},
                {"fetched_at": "2026-07-03T22:30:00Z", "games": [_snap_game(commence, 120, -140)]},
                # post-pitch snapshot must NOT win even though it is later
                {"fetched_at": "2026-07-04T01:30:00Z", "games": [_snap_game(commence, 200, -240)]},
            ],
        }
        (snapshot_dir / "2026-07-03.json").write_text(json.dumps(payload))
        games = grade_picks._load_closing_games(date(2026, 7, 3))
        assert len(games) == 1
        assert games[0]["moneyline"]["away_odds"] == 120

    def test_falls_back_to_earliest_post_pitch(self, snapshot_dir):
        commence = "2026-07-03T16:00:00Z"  # before every snapshot
        payload = {
            "date": "2026-07-03",
            "snapshots": [
                {"fetched_at": "2026-07-03T16:45:00Z", "games": [_snap_game(commence, 111, -131)]},
                {"fetched_at": "2026-07-03T19:45:00Z", "games": [_snap_game(commence, 150, -170)]},
            ],
        }
        (snapshot_dir / "2026-07-03.json").write_text(json.dumps(payload))
        games = grade_picks._load_closing_games(date(2026, 7, 3))
        assert games[0]["moneyline"]["away_odds"] == 111

    def test_falls_back_to_live_odds_when_no_snapshot_file(self, snapshot_dir, tmp_path):
        live = {
            "date": "2026-07-03",
            "games": [_snap_game("2026-07-03T23:05:00Z", 105, -125)],
        }
        (tmp_path / "live_odds.json").write_text(json.dumps(live))
        games = grade_picks._load_closing_games(date(2026, 7, 3))
        assert games[0]["moneyline"]["away_odds"] == 105

    def test_empty_when_nothing_matches(self, snapshot_dir):
        assert grade_picks._load_closing_games(date(2026, 7, 3)) == []

    def test_corrupt_snapshot_file_falls_back(self, snapshot_dir, tmp_path):
        (snapshot_dir / "2026-07-03.json").write_text("{broken")
        live = {
            "date": "2026-07-03",
            "games": [_snap_game("2026-07-03T23:05:00Z", 107, -127)],
        }
        (tmp_path / "live_odds.json").write_text(json.dumps(live))
        games = grade_picks._load_closing_games(date(2026, 7, 3))
        assert games[0]["moneyline"]["away_odds"] == 107
