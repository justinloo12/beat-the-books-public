from __future__ import annotations

from datetime import date, datetime

from sqlmodel import Session, col, select

from mlb_model.models import Game, MarketSnapshot, ModelCalibration, ModuleSignal, Pick


class ModelRepository:
    def upsert_game(self, session: Session, payload: dict) -> Game:
        statement = select(Game).where(Game.game_id == payload["game_id"])
        game = session.exec(statement).first()
        if game is None:
            game = Game(**payload)
            session.add(game)
        else:
            for key, value in payload.items():
                setattr(game, key, value)
            game.updated_at = datetime.utcnow()
        session.commit()
        session.refresh(game)
        return game

    def add_market_snapshots(self, session: Session, snapshots: list[dict]) -> None:
        for snapshot in snapshots:
            session.add(MarketSnapshot(**snapshot))
        session.commit()

    def add_module_signals(self, session: Session, signals: list[dict]) -> None:
        for signal in signals:
            session.add(ModuleSignal(**signal))
        session.commit()

    def replace_pick(self, session: Session, payload: dict) -> Pick:
        statement = select(Pick).where(
            Pick.game_id == payload["game_id"],
            Pick.market_key == payload["market_key"],
            Pick.pick_side == payload["pick_side"],
        )
        existing = session.exec(statement).first()
        if existing is None:
            existing = Pick(**payload)
            session.add(existing)
        else:
            for key, value in payload.items():
                setattr(existing, key, value)
        session.commit()
        session.refresh(existing)
        return existing

    def get_today_picks(self, session: Session, slate_date: date) -> list[Pick]:
        statement = select(Pick).join(Game, Game.game_id == Pick.game_id).where(Game.game_date == slate_date)
        return list(session.exec(statement).all())

    def get_recent_picks(self, session: Session, limit: int = 100) -> list[tuple[Pick, Game | None]]:
        statement = (
            select(Pick, Game)
            .join(Game, Game.game_id == Pick.game_id, isouter=True)
            .order_by(col(Pick.placed_at).desc())
            .limit(limit)
        )
        return list(session.exec(statement).all())

    def recent_clv(self, session: Session, limit: int) -> float | None:
        statement = (
            select(Pick.clv_value)
            .where(Pick.clv_value.is_not(None))
            .order_by(col(Pick.placed_at).desc())
            .limit(limit)
        )
        values = list(session.exec(statement).all())
        if not values:
            return None
        return round(sum(values) / len(values), 4)

    def count_graded_bets(self, session: Session) -> int:
        statement = select(Pick).where(Pick.result.is_not(None))
        return len(list(session.exec(statement).all()))

    def performance_summary(self, session: Session) -> dict:
        picks = list(session.exec(select(Pick)).all())
        graded = [pick for pick in picks if pick.result in {"win", "loss", "push"}]
        total_staked = sum(float(pick.bankroll_fraction or 0.0) for pick in graded)
        total_profit = 0.0
        wins = 0
        losses = 0
        pushes = 0
        for pick in graded:
            stake = float(pick.bankroll_fraction or 0.0)
            if pick.result == "win":
                wins += 1
                total_profit += stake * (float(pick.decimal_odds) - 1.0)
            elif pick.result == "loss":
                losses += 1
                total_profit -= stake
            elif pick.result == "push":
                pushes += 1
        roi = (total_profit / total_staked) if total_staked else 0.0
        hit_rate = (wins / (wins + losses)) if (wins + losses) else 0.0
        return {
            "tracked_bets": len(picks),
            "graded_bets": len(graded),
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "units_risked": round(total_staked, 4),
            "units_profit": round(total_profit, 4),
            "roi": round(roi, 4),
            "hit_rate": round(hit_rate, 4),
        }

    def add_calibration(self, session: Session, sample_size: int, weights: dict, metrics: dict) -> None:
        session.add(ModelCalibration(sample_size=sample_size, weights=weights, metrics=metrics))
        session.commit()
