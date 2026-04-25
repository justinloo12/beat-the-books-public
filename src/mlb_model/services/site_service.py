from __future__ import annotations

from datetime import date, datetime
from typing import Any

from mlb_model.db import init_db, session_scope
from mlb_model.services.daily_model import DailyPredictionService
from mlb_model.services.repository import ModelRepository


class SiteService:
    def __init__(self) -> None:
        self.repo = ModelRepository()
        self.daily = DailyPredictionService()

    async def site_payload(self, slate_date: date) -> dict[str, Any]:
        init_db()
        board = await self.daily.daily_board(slate_date)
        with session_scope() as session:
            performance = self.repo.performance_summary(session)
            recent_picks = self.repo.get_recent_picks(session, limit=100)
            last_50_clv = self.repo.recent_clv(session, 50)
            last_200_clv = self.repo.recent_clv(session, 200)

        history = []
        for pick, game in recent_picks:
            history.append(
                {
                    "id": pick.id,
                    "placed_at": pick.placed_at.isoformat(),
                    "matchup": f"{game.away_team} @ {game.home_team}" if game else pick.game_id,
                    "market_type": pick.market_type,
                    "pick_side": pick.pick_side,
                    "line": pick.line,
                    "american_odds": pick.american_odds,
                    "edge": pick.edge,
                    "tier": pick.tier,
                    "bankroll_fraction": pick.bankroll_fraction,
                    "result": pick.result,
                    "clv_value": pick.clv_value,
                }
            )

        return {
            "as_of": datetime.utcnow().isoformat(),
            "date": slate_date.isoformat(),
            "summary": {
                **performance,
                "clv_last_50": last_50_clv,
                "clv_last_200": last_200_clv,
                "daily_pick_count": len(board["picks"]),
                "lineup_card_count": len(board["lineup_cards"]),
            },
            "daily": board,
            "history": history,
        }
