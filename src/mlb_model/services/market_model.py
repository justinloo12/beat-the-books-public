from __future__ import annotations

from typing import Any


class MarketSignalService:
    def summarize_market(self, snapshots: list[dict[str, Any]], model_edge: float) -> dict[str, Any]:
        if not snapshots:
            return {
                "opening_line": None,
                "current_line": None,
                "movement": "flat",
                "sharp_money_flag": False,
                "trap_warning": False,
            }
        ordered = sorted(snapshots, key=lambda row: row["captured_at"])
        opening = ordered[0]["line"]
        current = ordered[-1]["line"]
        public_pct = ordered[-1].get("public_bet_percentage") or 0.0
        cents_move = abs(int(ordered[-1]["american_odds"]) - int(ordered[0]["american_odds"]))
        sharp_money_flag = cents_move > 10
        reverse_line_movement = public_pct >= 0.65 and current != opening
        trap_warning = model_edge >= 0.12 and public_pct >= 0.75 and current != opening
        direction = "up" if current > opening else "down" if current < opening else "flat"
        return {
            "opening_line": opening,
            "current_line": current,
            "movement": direction,
            "sharp_money_flag": sharp_money_flag or reverse_line_movement,
            "trap_warning": trap_warning,
        }
