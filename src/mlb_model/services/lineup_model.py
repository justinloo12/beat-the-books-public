from __future__ import annotations

from typing import Any

from mlb_model.utils import clamp


PA_BY_SLOT = {1: 4.3, 2: 4.3, 3: 4.1, 4: 4.1, 5: 4.1, 6: 3.9, 7: 3.8, 8: 3.7, 9: 3.7}


class LineupModelService:
    def score_lineup(
        self,
        confirmed_lineup: list[dict[str, Any]],
        projected_lineup: list[dict[str, Any]],
        top_bats: list[str],
    ) -> dict[str, Any]:
        projected_by_slot = {player["slot"]: player for player in projected_lineup}
        starter_score = 0.0
        season_score = 0.0
        downgrade_flags: list[str] = []
        scratch_adjustment = 1.0

        for batter in confirmed_lineup:
            slot = int(batter["slot"])
            weight = PA_BY_SLOT.get(slot, 3.8)
            barrel = float(batter.get("barrel_pct", 6.0))
            starter_score += barrel * weight
            projected = projected_by_slot.get(slot, {})
            projected_barrel = float(projected.get("barrel_pct", barrel))
            season_score += projected_barrel * weight
            if projected and (projected_barrel - barrel) > 3:
                downgrade_flags.append(f"slot {slot} downgrade {projected_barrel - barrel:.1f} barrel pts")
            if batter.get("name") not in top_bats and projected.get("name") in top_bats:
                scratch_adjustment *= 0.88

        lineup_multiplier = clamp(starter_score / max(season_score, 0.1), 0.78, 1.08)
        return {
            "lineup_barrel_score": round(starter_score, 2),
            "season_lineup_barrel_score": round(season_score or starter_score, 2),
            "scratch_adjustment": round(scratch_adjustment, 3),
            "downgrade_flags": downgrade_flags,
            "lineup_multiplier": round(lineup_multiplier, 3),
        }
