"""Offline unit tests for the pure grading/payout logic in grade_picks.

Only pure helpers are exercised (_pnl, _grade, _find_game, _teams_match,
_american_to_decimal) -- no network calls or file IO.
"""
from __future__ import annotations

import pytest

from mlb_model.grade_picks import (
    _american_to_decimal,
    _find_game,
    _grade,
    _pnl,
    _teams_match,
)
from mlb_model.services import odds_engine


def make_game(away_score: int, home_score: int) -> dict:
    return {
        "away_team": "New York Yankees",
        "home_team": "Boston Red Sox",
        "away_score": away_score,
        "home_score": home_score,
    }


class TestPnl:
    def test_win_at_plus_odds(self) -> None:
        assert _pnl("win", 150, stake=100.0) == pytest.approx(150.0)

    def test_win_at_minus_odds(self) -> None:
        assert _pnl("win", -110, stake=110.0) == pytest.approx(100.0)

    def test_loss_costs_exactly_the_stake(self) -> None:
        assert _pnl("loss", 150, stake=100.0) == -100.0
        assert _pnl("loss", -300, stake=100.0) == -100.0

    def test_push_and_no_result_are_flat(self) -> None:
        assert _pnl("push", -110) == 0.0
        assert _pnl("no_result", -110) == 0.0

    def test_win_is_positive_loss_is_negative(self) -> None:
        for odds in (-250, -110, 100, 110, 250):
            assert _pnl("win", odds) > 0
            assert _pnl("loss", odds) < 0

    @pytest.mark.parametrize("odds", [100, -100, 110, -110, 200, -200])
    def test_matches_odds_engine_conversion(self, odds: int) -> None:
        assert _american_to_decimal(odds) == pytest.approx(odds_engine.american_to_decimal(odds))


class TestGradeGameTotal:
    def make_pick(self, side: str, line: float) -> dict:
        return {
            "market_type": "game_total",
            "pick": side,
            "line": line,
            "american_odds": -110,
        }

    def test_over_wins_when_total_exceeds_line(self) -> None:
        result, pnl = _grade(self.make_pick("Over", 8.5), make_game(5, 4))
        assert result == "win"
        assert pnl > 0

    def test_over_loses_when_total_under_line(self) -> None:
        result, pnl = _grade(self.make_pick("Over", 8.5), make_game(3, 4))
        assert result == "loss"
        assert pnl == -100.0

    def test_under_wins_when_total_below_line(self) -> None:
        result, _ = _grade(self.make_pick("Under", 8.5), make_game(3, 4))
        assert result == "win"

    def test_exact_total_is_push(self) -> None:
        result, pnl = _grade(self.make_pick("Over", 9.0), make_game(5, 4))
        assert result == "push"
        assert pnl == 0.0


class TestGradeMoneyline:
    def make_pick(self, side: str) -> dict:
        return {"market_type": "moneyline", "pick": side, "line": None, "american_odds": 120}

    def test_away_pick_wins_when_away_scores_more(self) -> None:
        result, _ = _grade(self.make_pick("New York Yankees"), make_game(6, 2))
        assert result == "win"

    def test_home_pick_loses_when_home_scores_fewer(self) -> None:
        result, _ = _grade(self.make_pick("Boston Red Sox"), make_game(6, 2))
        assert result == "loss"

    def test_unknown_team_is_no_result(self) -> None:
        result, pnl = _grade(self.make_pick("Chicago Cubs"), make_game(6, 2))
        assert result == "no_result"
        assert pnl == 0.0


class TestGradeRunline:
    def make_pick(self, side: str, line: float) -> dict:
        return {"market_type": "runline", "pick": side, "line": line, "american_odds": -110}

    def test_favorite_covers_minus_one_and_a_half(self) -> None:
        result, _ = _grade(self.make_pick("Boston Red Sox", -1.5), make_game(2, 4))
        assert result == "win"

    def test_favorite_fails_to_cover_on_one_run_win(self) -> None:
        result, _ = _grade(self.make_pick("Boston Red Sox", -1.5), make_game(3, 4))
        assert result == "loss"

    def test_underdog_plus_one_and_a_half_covers_on_one_run_loss(self) -> None:
        result, _ = _grade(self.make_pick("New York Yankees", 1.5), make_game(3, 4))
        assert result == "win"

    def test_whole_number_line_can_push(self) -> None:
        result, pnl = _grade(self.make_pick("Boston Red Sox", -1.0), make_game(3, 4))
        assert result == "push"
        assert pnl == 0.0


class TestUnknownMarket:
    def test_unknown_market_is_no_result(self) -> None:
        pick = {"market_type": "player_props", "pick": "x", "line": 1.5, "american_odds": -110}
        result, pnl = _grade(pick, make_game(3, 4))
        assert result == "no_result"
        assert pnl == 0.0


class TestTeamMatching:
    def test_exact_match(self) -> None:
        assert _teams_match("Boston Red Sox", "Boston Red Sox")

    def test_partial_name_matches(self) -> None:
        assert _teams_match("Red Sox", "Boston Red Sox")

    def test_diamondbacks_alias(self) -> None:
        assert _teams_match("D-backs", "Arizona Diamondbacks")

    def test_non_matching_teams(self) -> None:
        assert not _teams_match("New York Yankees", "Boston Red Sox")


class TestFindGame:
    def test_matches_away_at_home_format(self) -> None:
        games = [make_game(3, 4)]
        assert _find_game("Yankees @ Red Sox", games) is games[0]

    def test_returns_none_without_at_separator(self) -> None:
        assert _find_game("Yankees vs Red Sox", [make_game(3, 4)]) is None

    def test_returns_none_when_no_game_matches(self) -> None:
        assert _find_game("Cubs @ Cardinals", [make_game(3, 4)]) is None
