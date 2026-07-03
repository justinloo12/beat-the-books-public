"""Closing-line capture and CLV grading for database-tracked picks.

Two-step flow, both idempotent and cheap enough to run on every pipeline pass:

1. ``capture_closing_lines`` — for every game whose first pitch has passed,
   flag the most recent :class:`MarketSnapshot` per (market_key, side,
   sportsbook) as the closing line. This is a *proxy* close: the last odds we
   happened to capture before start time, not the book's true final tick.

2. ``apply_clv_to_picks`` — for every pick on a started game that has a
   flagged closing snapshot for its market and side, compute and persist
   ``Pick.clv_value``: the no-vig implied probability of the closing line
   minus the no-vig probability the pick was placed at. Positive CLV means
   the pick beat the close.

For point markets (totals, runlines) CLV is only computed when the closing
snapshot quotes the *same* line the pick was made at — comparing implied
probabilities across different points is not a like-for-like comparison. The
closing line itself is still recorded on the pick either way.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlmodel import Session, select

from mlb_model.models import Game, MarketSnapshot, Pick
from mlb_model.services.odds_engine import (
    implied_probability_from_american,
    no_vig_one_sided,
    no_vig_two_sided,
)

_LINELESS_MARKETS = {"moneyline", "h2h"}


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def closing_no_vig_probability(side_odds: int, opposite_odds: int | None) -> float:
    """No-vig probability of the picked side at the close.

    Uses the true two-sided de-vig when the opposite side's closing odds are
    available, otherwise falls back to the one-sided approximation already
    used elsewhere in the odds engine.
    """
    side_prob = implied_probability_from_american(side_odds)
    if opposite_odds is None:
        return no_vig_one_sided(side_prob)
    no_vig, _ = no_vig_two_sided(side_prob, implied_probability_from_american(opposite_odds))
    return no_vig


class ClosingLineService:
    def capture_closing_lines(self, session: Session, as_of: datetime | None = None) -> int:
        """Flag the latest pre-start snapshot per market side as the closing line.

        Returns the number of snapshots newly flagged.
        """
        as_of = _naive_utc(as_of or datetime.utcnow())
        games = list(session.exec(select(Game).where(Game.start_time <= as_of)).all())
        flagged = 0
        for game in games:
            start = _naive_utc(game.start_time)
            snapshots = list(
                session.exec(select(MarketSnapshot).where(MarketSnapshot.game_id == game.game_id)).all()
            )
            groups: dict[tuple[str, str, str], list[MarketSnapshot]] = {}
            for snapshot in snapshots:
                groups.setdefault((snapshot.market_key, snapshot.side, snapshot.sportsbook), []).append(snapshot)
            for group in groups.values():
                if any(snapshot.is_closing_line for snapshot in group):
                    continue  # already captured for this market side
                pre_start = [s for s in group if _naive_utc(s.captured_at) <= start]
                if pre_start:
                    # Last snapshot before first pitch: the closing-line proxy.
                    chosen = max(pre_start, key=lambda s: _naive_utc(s.captured_at))
                else:
                    # Nothing captured pre-start: the earliest available
                    # snapshot is the one closest to the true close.
                    chosen = min(group, key=lambda s: _naive_utc(s.captured_at))
                chosen.is_closing_line = True
                session.add(chosen)
                flagged += 1
        session.commit()
        return flagged

    def apply_clv_to_picks(self, session: Session) -> int:
        """Compute and persist clv_value for picks with a flagged closing line.

        Returns the number of picks updated.
        """
        picks: Iterable[Pick] = session.exec(select(Pick).where(Pick.clv_value.is_(None))).all()
        updated = 0
        for pick in picks:
            closing = list(
                session.exec(
                    select(MarketSnapshot).where(
                        MarketSnapshot.game_id == pick.game_id,
                        MarketSnapshot.market_key == pick.market_key,
                        MarketSnapshot.is_closing_line == True,  # noqa: E712
                    )
                ).all()
            )
            side_snapshot = next(
                (s for s in closing if s.side.lower() == pick.pick_side.lower()), None
            )
            if side_snapshot is None:
                continue
            opposite = next(
                (s for s in closing if s.side.lower() != pick.pick_side.lower()), None
            )
            pick.closing_line = side_snapshot.line
            same_line = (
                pick.market_type in _LINELESS_MARKETS
                or abs(float(side_snapshot.line) - float(pick.line)) < 1e-9
            )
            if same_line:
                close_prob = closing_no_vig_probability(
                    side_snapshot.american_odds,
                    opposite.american_odds if opposite is not None else None,
                )
                pick.clv_value = round(close_prob - pick.no_vig_probability, 4)
                updated += 1
            session.add(pick)
        session.commit()
        return updated

    def run(self, session: Session, as_of: datetime | None = None) -> dict[str, int]:
        flagged = self.capture_closing_lines(session, as_of=as_of)
        graded = self.apply_clv_to_picks(session)
        return {"closing_lines_flagged": flagged, "picks_clv_graded": graded}
