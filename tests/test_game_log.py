"""Tests for the every-game prediction log (game_log.py) and the all-games
report card in metrics.py: extraction from boards, append-only freezing,
idempotent grading, CSV round-trips, and the Brier/calibration/rolling math."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from mlb_model.game_log import (
    FIELDNAMES,
    MODEL_VERSION,
    MODEL_VERSION_V1,
    append_rows,
    game_id_for,
    grade_pending,
    grade_rows_for_date,
    load_game_log,
    rows_from_board,
    save_game_log,
)
from mlb_model.metrics import ALL_GAMES_SAMPLE_THRESHOLD, _all_games_block, build_metrics


def _board(day="2026-07-04", with_home_ml=True, with_away_ml=False, with_total=True):
    picks = []
    if with_home_ml:
        picks.append(
            {
                "market_type": "moneyline",
                "pick": "Seattle Mariners",
                "model_probability": 0.55,
                "no_vig_probability": 0.58,
            }
        )
    if with_away_ml:
        picks.append(
            {
                "market_type": "moneyline",
                "pick": "Baltimore Orioles",
                "model_probability": 0.45,
                "no_vig_probability": 0.42,
            }
        )
    if with_total:
        picks.append({"market_type": "game_total", "pick": "Over", "line": 8.5})
    return {
        "date": day,
        "daily": {
            "lineup_cards": [
                {
                    "matchup": "Baltimore Orioles @ Seattle Mariners",
                    "home_win_prob": 0.51,
                    "simulated_total": 8.12,
                    "top_game_picks": picks,
                }
            ]
        },
    }


def _final(away="Baltimore Orioles", home="Seattle Mariners", away_score=2, home_score=5):
    return {"away_team": away, "home_team": home, "away_score": away_score, "home_score": home_score}


class TestRowsFromBoard:
    def test_home_moneyline_quote_preferred(self):
        rows = rows_from_board(_board())
        assert len(rows) == 1
        row = rows[0]
        assert row["model_home_prob"] == 0.55
        assert row["market_home_prob"] == 0.58
        assert row["prob_source"] == "moneyline_quote"
        assert row["sim_home_prob"] == 0.51
        assert row["model_total"] == 8.12
        assert row["market_total_line"] == 8.5
        assert row["status"] == "pending"
        assert row["model_version"] == MODEL_VERSION
        assert row["game_id"] == game_id_for(
            "2026-07-04", "Baltimore Orioles", "Seattle Mariners"
        )

    def test_away_only_quote_uses_complement(self):
        rows = rows_from_board(_board(with_home_ml=False, with_away_ml=True))
        row = rows[0]
        assert row["model_home_prob"] == pytest.approx(0.55)
        assert row["market_home_prob"] == pytest.approx(0.58)
        assert row["prob_source"] == "moneyline_quote"

    def test_simulation_fallback_when_no_ml_quote(self):
        rows = rows_from_board(_board(with_home_ml=False))
        row = rows[0]
        assert row["model_home_prob"] == 0.51
        assert row["market_home_prob"] is None
        assert row["prob_source"] == "simulation"

    def test_card_without_any_probability_skipped(self):
        board = _board(with_home_ml=False)
        board["daily"]["lineup_cards"][0]["home_win_prob"] = None
        assert rows_from_board(board) == []

    def test_model_version_tag(self):
        rows = rows_from_board(_board(), model_version=MODEL_VERSION_V1)
        assert rows[0]["model_version"] == MODEL_VERSION_V1


class TestAppendRows:
    def test_first_write_freezes(self):
        first = rows_from_board(_board())
        merged, added = append_rows([], first)
        assert added == 1
        # A later re-export with a different probability must NOT revise.
        revised = rows_from_board(_board())
        revised[0]["model_home_prob"] = 0.99
        merged, added = append_rows(merged, revised)
        assert added == 0
        assert merged[0]["model_home_prob"] == 0.55

    def test_new_games_append(self):
        merged, _ = append_rows([], rows_from_board(_board()))
        other = _board()
        other["daily"]["lineup_cards"][0]["matchup"] = "New York Yankees @ Boston Red Sox"
        other["daily"]["lineup_cards"][0]["top_game_picks"] = []
        merged, added = append_rows(merged, rows_from_board(other))
        assert added == 1
        assert len(merged) == 2


class TestGrading:
    def test_grades_home_win_and_brier(self):
        rows = rows_from_board(_board())
        graded = grade_rows_for_date(rows, "2026-07-04", [_final()])
        assert graded == 1
        row = rows[0]
        assert row["status"] == "final"
        assert row["home_win"] == 1
        assert row["home_score"] == 5 and row["away_score"] == 2
        assert row["model_brier"] == pytest.approx((0.55 - 1) ** 2, abs=1e-6)
        assert row["market_brier"] == pytest.approx((0.58 - 1) ** 2, abs=1e-6)

    def test_home_loss(self):
        rows = rows_from_board(_board())
        grade_rows_for_date(rows, "2026-07-04", [_final(away_score=7, home_score=3)])
        assert rows[0]["home_win"] == 0
        assert rows[0]["model_brier"] == pytest.approx(0.55**2, abs=1e-6)

    def test_idempotent(self):
        rows = rows_from_board(_board())
        grade_rows_for_date(rows, "2026-07-04", [_final()])
        first = dict(rows[0])
        again = grade_rows_for_date(rows, "2026-07-04", [_final(away_score=9, home_score=0)])
        assert again == 0
        assert rows[0] == first

    def test_unmatched_stays_pending_until_give_up(self):
        rows = rows_from_board(_board())
        graded = grade_rows_for_date(rows, "2026-07-04", [])
        assert graded == 0
        assert rows[0]["status"] == "pending"
        graded = grade_rows_for_date(rows, "2026-07-04", [], give_up=True)
        assert graded == 1
        assert rows[0]["status"] == "no_result"
        assert rows[0]["model_brier"] is None

    def test_market_brier_none_when_no_market_prob(self):
        rows = rows_from_board(_board(with_home_ml=False))
        grade_rows_for_date(rows, "2026-07-04", [_final()])
        assert rows[0]["model_brier"] is not None
        assert rows[0]["market_brier"] is None


class TestCsvRoundTrip:
    def test_round_trip(self, tmp_path: Path):
        path = tmp_path / "game_predictions.csv"
        rows = rows_from_board(_board())
        grade_rows_for_date(rows, "2026-07-04", [_final()])
        save_game_log(rows, path)
        loaded = load_game_log(path)
        assert len(loaded) == 1
        row = loaded[0]
        assert set(row) == set(FIELDNAMES)
        assert row["model_home_prob"] == 0.55
        assert row["home_win"] == 1
        assert row["home_score"] == 5
        assert row["status"] == "final"
        assert row["market_total_line"] == 8.5

    def test_save_refuses_to_shrink(self, tmp_path: Path):
        path = tmp_path / "game_predictions.csv"
        rows = rows_from_board(_board())
        save_game_log(rows, path)
        save_game_log([], path, prior_count=1)  # must refuse
        assert len(load_game_log(path)) == 1

    def test_missing_file_is_empty(self, tmp_path: Path):
        assert load_game_log(tmp_path / "nope.csv") == []


class TestGradePending:
    def test_grades_only_past_dates_and_persists(self, tmp_path: Path):
        path = tmp_path / "game_predictions.csv"
        yesterday_board = _board(day="2026-07-03")
        today_board = _board(day="2026-07-04")
        rows, _ = append_rows([], rows_from_board(yesterday_board) + rows_from_board(today_board))
        save_game_log(rows, path)

        calls: list[date] = []

        def fake_fetch(d: date):
            calls.append(d)
            return [_final()]

        graded = grade_pending(fake_fetch, today=date(2026, 7, 4), path=path)
        assert graded == 1
        assert calls == [date(2026, 7, 3)]
        loaded = {r["date"]: r for r in load_game_log(path)}
        assert loaded["2026-07-03"]["status"] == "final"
        assert loaded["2026-07-04"]["status"] == "pending"

        # Re-run: nothing new to grade, no extra writes needed.
        graded = grade_pending(fake_fetch, today=date(2026, 7, 4), path=path)
        assert graded == 0

    def test_gives_up_on_stale_dates(self, tmp_path: Path):
        path = tmp_path / "game_predictions.csv"
        old_board = _board(day="2026-06-01")
        rows, _ = append_rows([], rows_from_board(old_board))
        save_game_log(rows, path)
        graded = grade_pending(lambda d: [], today=date(2026, 7, 4), path=path)
        assert graded == 1
        assert load_game_log(path)[0]["status"] == "no_result"

    def test_fetch_failure_leaves_pending(self, tmp_path: Path):
        path = tmp_path / "game_predictions.csv"
        rows, _ = append_rows([], rows_from_board(_board(day="2026-07-03")))
        save_game_log(rows, path)

        def boom(d: date):
            raise OSError("network down")

        graded = grade_pending(boom, today=date(2026, 7, 4), path=path)
        assert graded == 0
        assert load_game_log(path)[0]["status"] == "pending"


def _graded_row(day: str, model_p: float, market_p: float | None, home_win: int, version: str = "v1-raw-rates"):
    return {
        "date": day,
        "game_id": f"{day}_x_at_y_{model_p}_{home_win}",
        "away_team": "X",
        "home_team": "Y",
        "model_version": version,
        "prob_source": "moneyline_quote",
        "model_home_prob": model_p,
        "market_home_prob": market_p,
        "sim_home_prob": model_p,
        "model_total": None,
        "market_total_line": None,
        "status": "final",
        "home_score": 5 if home_win else 2,
        "away_score": 2 if home_win else 5,
        "home_win": home_win,
        "model_brier": (model_p - home_win) ** 2,
        "market_brier": (market_p - home_win) ** 2 if market_p is not None else None,
    }


class TestAllGamesMetrics:
    def test_brier_horse_race(self):
        rows = [
            _graded_row("2026-06-01", 0.60, 0.55, 1),
            _graded_row("2026-06-01", 0.40, 0.50, 0),
        ]
        block = _all_games_block(rows)
        assert block["n_scored"] == 2
        expected_model = ((0.60 - 1) ** 2 + 0.40**2) / 2
        expected_market = ((0.55 - 1) ** 2 + 0.50**2) / 2
        assert block["model_brier"] == pytest.approx(expected_model, abs=1e-6)
        assert block["market_brier"] == pytest.approx(expected_market, abs=1e-6)
        assert block["brier_delta"] == pytest.approx(expected_model - expected_market, abs=1e-6)
        assert block["model_ahead"] is (expected_model < expected_market)

    def test_unpaired_rows_excluded_from_race(self):
        rows = [
            _graded_row("2026-06-01", 0.60, 0.55, 1),
            _graded_row("2026-06-01", 0.99, None, 0),  # no market — not in race
        ]
        block = _all_games_block(rows)
        assert block["n_final"] == 2
        assert block["n_scored"] == 1
        assert block["model_brier"] == pytest.approx((0.60 - 1) ** 2, abs=1e-6)

    def test_small_sample_flag(self):
        rows = [_graded_row("2026-06-01", 0.5, 0.5, i % 2) for i in range(10)]
        block = _all_games_block(rows)
        assert block["reliable"] is False
        assert block["threshold"] == ALL_GAMES_SAMPLE_THRESHOLD
        big = [
            dict(_graded_row("2026-06-01", 0.5, 0.5, i % 2), game_id=str(i))
            for i in range(ALL_GAMES_SAMPLE_THRESHOLD)
        ]
        assert _all_games_block(big)["reliable"] is True

    def test_calibration_buckets(self):
        rows = [
            _graded_row("2026-06-01", 0.52, 0.52, 1),
            _graded_row("2026-06-01", 0.53, 0.53, 0),
            _graded_row("2026-06-01", 0.62, 0.62, 1),
        ]
        cal = _all_games_block(rows)["calibration"]
        assert cal["n"] == 3
        by_bucket = {r["bucket"]: r for r in cal["rows"]}
        mid = by_bucket["0.50-0.55"]
        assert mid["n"] == 2
        assert mid["avg_predicted"] == pytest.approx(0.525, abs=1e-6)
        assert mid["realized"] == pytest.approx(0.5, abs=1e-6)
        assert by_bucket["0.60-0.65"]["n"] == 1
        assert by_bucket["0.00-0.35"]["n"] == 0

    def test_rolling_window_excludes_old_dates(self):
        rows = [
            _graded_row("2026-04-01", 0.9, 0.5, 0),  # bad day, far in the past
            _graded_row("2026-06-15", 0.6, 0.55, 1),
            _graded_row("2026-06-20", 0.6, 0.55, 1),
        ]
        rolling = _all_games_block(rows)["rolling"]
        assert [p["date"] for p in rolling] == ["2026-04-01", "2026-06-15", "2026-06-20"]
        last = rolling[-1]
        # The April game is outside the 30-day window of June 20.
        assert last["n"] == 2
        assert last["model_brier"] == pytest.approx((0.6 - 1) ** 2, abs=1e-6)
        first = rolling[0]
        assert first["n"] == 1
        assert first["model_brier"] == pytest.approx(0.81, abs=1e-6)

    def test_by_version_segmentation(self):
        rows = [
            _graded_row("2026-06-01", 0.6, 0.55, 1, version="v1-raw-rates"),
            _graded_row("2026-07-05", 0.6, 0.55, 1, version="v2-eb-2026-07-04"),
        ]
        block = _all_games_block(rows)
        assert set(block["by_version"]) == {"v1-raw-rates", "v2-eb-2026-07-04"}
        assert block["by_version"]["v1-raw-rates"]["n"] == 1

    def test_build_metrics_includes_all_games(self):
        metrics = build_metrics([], game_log=[_graded_row("2026-06-01", 0.6, 0.55, 1)])
        assert metrics["all_games"]["n_scored"] == 1
        # And an empty log still produces the block (dashboard never 404s).
        empty = build_metrics([])
        assert empty["all_games"]["n_logged"] == 0
        assert empty["all_games"]["model_brier"] is None
