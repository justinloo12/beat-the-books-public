"""Tests for closing-line capture and CLV grading (DB path) using an
in-memory SQLite database — no network or filesystem state.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from mlb_model.models import Game, MarketSnapshot, Pick
from mlb_model.services.closing_line import ClosingLineService, closing_no_vig_probability
from mlb_model.services.odds_engine import (
    implied_probability_from_american,
    no_vig_one_sided,
    no_vig_two_sided,
)

START = datetime(2026, 6, 20, 23, 10)  # first pitch (UTC, naive)


@pytest.fixture()
def session():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def make_game(session: Session, game_id: str = "g1") -> Game:
    game = Game(
        game_id=game_id,
        game_date=date(2026, 6, 20),
        start_time=START,
        home_team="Boston Red Sox",
        away_team="New York Yankees",
        ballpark="Fenway Park",
    )
    session.add(game)
    session.commit()
    return game


def make_snapshot(
    session: Session,
    *,
    game_id: str = "g1",
    market_key: str = "total_8.5",
    side: str = "over",
    line: float = 8.5,
    odds: int = -110,
    captured_at: datetime,
) -> MarketSnapshot:
    snapshot = MarketSnapshot(
        game_id=game_id,
        market_type="game_total",
        market_key=market_key,
        side=side,
        line=line,
        american_odds=odds,
        decimal_odds=1 + 100 / abs(odds),
        implied_probability_raw=implied_probability_from_american(odds),
        captured_at=captured_at,
    )
    session.add(snapshot)
    session.commit()
    return snapshot


def make_pick(session: Session, *, side: str = "over", line: float = 8.5, no_vig: float = 0.50) -> Pick:
    pick = Pick(
        game_id="g1",
        market_type="game_total",
        market_key="total_8.5",
        pick_side=side,
        line=line,
        american_odds=-110,
        decimal_odds=1.909,
        model_probability=no_vig + 0.05,
        no_vig_probability=no_vig,
        edge=0.05,
        tier="moderate",
        bankroll_fraction=0.02,
    )
    session.add(pick)
    session.commit()
    return pick


class TestCaptureClosingLines:
    def test_flags_latest_pre_start_snapshot(self, session: Session) -> None:
        make_game(session)
        early = make_snapshot(session, odds=-105, captured_at=START - timedelta(hours=6))
        close = make_snapshot(session, odds=-115, captured_at=START - timedelta(minutes=30))
        post = make_snapshot(session, odds=-140, captured_at=START + timedelta(hours=1))

        flagged = ClosingLineService().capture_closing_lines(session, as_of=START + timedelta(hours=2))
        assert flagged == 1
        session.refresh(early), session.refresh(close), session.refresh(post)
        assert close.is_closing_line
        assert not early.is_closing_line
        assert not post.is_closing_line  # in-play odds never count as the close

    def test_does_not_capture_before_first_pitch(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, captured_at=START - timedelta(hours=2))
        flagged = ClosingLineService().capture_closing_lines(session, as_of=START - timedelta(hours=1))
        assert flagged == 0

    def test_idempotent_across_runs(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, captured_at=START - timedelta(hours=1))
        service = ClosingLineService()
        assert service.capture_closing_lines(session, as_of=START + timedelta(hours=1)) == 1
        assert service.capture_closing_lines(session, as_of=START + timedelta(hours=2)) == 0

    def test_each_side_gets_its_own_closing_flag(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, side="over", odds=-115, captured_at=START - timedelta(minutes=30))
        make_snapshot(session, side="under", odds=-105, captured_at=START - timedelta(minutes=30))
        flagged = ClosingLineService().capture_closing_lines(session, as_of=START + timedelta(hours=1))
        assert flagged == 2

    def test_falls_back_to_earliest_available_when_all_snapshots_post_start(self, session: Session) -> None:
        make_game(session)
        only = make_snapshot(session, captured_at=START + timedelta(minutes=10))
        ClosingLineService().capture_closing_lines(session, as_of=START + timedelta(hours=1))
        session.refresh(only)
        assert only.is_closing_line  # best available proxy rather than nothing


class TestApplyClv:
    def run_service(self, session: Session) -> None:
        service = ClosingLineService()
        service.capture_closing_lines(session, as_of=START + timedelta(hours=1))
        service.apply_clv_to_picks(session)

    def test_positive_clv_when_line_moves_toward_pick(self, session: Session) -> None:
        make_game(session)
        # Pick made at no-vig 0.50; close: over -125 / under +105 (over now favored).
        make_snapshot(session, side="over", odds=-125, captured_at=START - timedelta(minutes=20))
        make_snapshot(session, side="under", odds=105, captured_at=START - timedelta(minutes=20))
        pick = make_pick(session, side="over", no_vig=0.50)
        self.run_service(session)
        session.refresh(pick)

        expected_close, _ = no_vig_two_sided(
            implied_probability_from_american(-125), implied_probability_from_american(105)
        )
        assert pick.clv_value == pytest.approx(expected_close - 0.50, abs=1e-4)
        assert pick.clv_value > 0
        assert pick.closing_line == 8.5

    def test_negative_clv_when_line_moves_against_pick(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, side="over", odds=115, captured_at=START - timedelta(minutes=20))
        make_snapshot(session, side="under", odds=-135, captured_at=START - timedelta(minutes=20))
        pick = make_pick(session, side="over", no_vig=0.50)
        self.run_service(session)
        session.refresh(pick)
        assert pick.clv_value is not None
        assert pick.clv_value < 0

    def test_no_clv_when_total_line_moved(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, side="over", line=9.0, odds=-110, captured_at=START - timedelta(minutes=20))
        make_snapshot(session, side="under", line=9.0, odds=-110, captured_at=START - timedelta(minutes=20))
        pick = make_pick(session, side="over", line=8.5)
        self.run_service(session)
        session.refresh(pick)
        assert pick.clv_value is None  # different point: not a like-for-like probability
        assert pick.closing_line == 9.0  # but the closing point is still recorded

    def test_one_sided_fallback_when_opposite_side_missing(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, side="over", odds=-120, captured_at=START - timedelta(minutes=20))
        pick = make_pick(session, side="over", no_vig=0.50)
        self.run_service(session)
        session.refresh(pick)
        expected = no_vig_one_sided(implied_probability_from_american(-120)) - 0.50
        assert pick.clv_value == pytest.approx(expected, abs=1e-4)

    def test_pick_without_closing_snapshot_left_ungraded(self, session: Session) -> None:
        make_game(session)
        pick = make_pick(session)
        self.run_service(session)
        session.refresh(pick)
        assert pick.clv_value is None
        assert pick.closing_line is None

    def test_graded_clv_visible_to_recent_clv_query(self, session: Session) -> None:
        make_game(session)
        make_snapshot(session, side="over", odds=-125, captured_at=START - timedelta(minutes=20))
        make_snapshot(session, side="under", odds=105, captured_at=START - timedelta(minutes=20))
        make_pick(session, side="over", no_vig=0.50)
        self.run_service(session)
        values = list(session.exec(select(Pick.clv_value).where(Pick.clv_value.is_not(None))).all())
        assert len(values) == 1


class TestAdditiveMigration:
    def test_new_column_added_to_old_schema_db(self, tmp_path) -> None:
        import sqlite3

        from mlb_model.db import init_db

        db_path = tmp_path / "old_schema.db"
        con = sqlite3.connect(db_path)
        con.execute(
            "CREATE TABLE marketsnapshot ("
            "id INTEGER PRIMARY KEY, game_id VARCHAR, market_type VARCHAR, market_key VARCHAR, "
            "sportsbook VARCHAR, side VARCHAR, line FLOAT, american_odds INTEGER, decimal_odds FLOAT, "
            "implied_probability_raw FLOAT, no_vig_probability FLOAT, public_bet_percentage FLOAT, "
            "opening_line FLOAT, captured_at DATETIME)"
        )
        con.execute(
            "INSERT INTO marketsnapshot (game_id, market_type, market_key, sportsbook, side, line, "
            "american_odds, decimal_odds, implied_probability_raw, captured_at) "
            "VALUES ('g1','game_total','t','dk','over',8.5,-110,1.909,0.524,'2026-06-01 00:00:00')"
        )
        con.commit()
        con.close()

        engine = create_engine(f"sqlite:///{db_path}")
        init_db(engine)
        init_db(engine)  # idempotent

        con = sqlite3.connect(db_path)
        columns = [row[1] for row in con.execute("PRAGMA table_info(marketsnapshot)")]
        assert "is_closing_line" in columns
        # Existing rows get the non-closing default and stay readable.
        assert con.execute("SELECT is_closing_line FROM marketsnapshot").fetchall() == [(0,)]


class TestClosingNoVig:
    def test_two_sided_matches_odds_engine(self) -> None:
        expected, _ = no_vig_two_sided(
            implied_probability_from_american(-110), implied_probability_from_american(-110)
        )
        assert closing_no_vig_probability(-110, -110) == pytest.approx(expected)
        assert closing_no_vig_probability(-110, -110) == pytest.approx(0.5)

    def test_one_sided_fallback(self) -> None:
        expected = no_vig_one_sided(implied_probability_from_american(-110))
        assert closing_no_vig_probability(-110, None) == pytest.approx(expected)
