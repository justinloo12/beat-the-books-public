from __future__ import annotations

from typing import Any

from mlb_model.utils import clamp


class BullpenModelService:
    def score_bullpen(self, relievers: list[dict[str, Any]]) -> dict[str, Any]:
        fresh_high_leverage = 0
        closer_status = "fresh"
        fatigue_flags: list[str] = []
        reliability_score = 65.0
        lhp_available = False

        for reliever in relievers:
            pitches_yesterday = int(reliever.get("pitches_yesterday", 0))
            pitches_two_days_ago = int(reliever.get("pitches_two_days_ago", 0))
            consecutive_days = int(reliever.get("consecutive_days", 0))
            days_since = int(reliever.get("days_since_last", 1))
            era = float(reliever.get("era", 4.00))
            xfip = float(reliever.get("xfip", 4.00))

            fatigued = False
            if pitches_yesterday >= 20:
                fatigue_flags.append(f"{reliever['name']}: high fatigue")
                fatigued = True
            elif 15 <= pitches_yesterday <= 19:
                fatigue_flags.append(f"{reliever['name']}: moderate fatigue")
                fatigued = True
            elif pitches_two_days_ago >= 15 and days_since == 1:
                fatigue_flags.append(f"{reliever['name']}: lingering fatigue")
                fatigued = True

            if consecutive_days >= 3:
                fatigue_flags.append(f"{reliever['name']}: 3+ consecutive days")
                fatigued = True
            if days_since >= 4:
                fatigue_flags.append(f"{reliever['name']}: inactivity review")

            if reliever.get("role") == "closer":
                closer_status = "fatigued" if fatigued else "fresh"
            if reliever.get("throws") == "L" and not fatigued:
                lhp_available = True
            if era < 3.75 and xfip < 3.90 and not fatigued:
                fresh_high_leverage += 1

            reliability_score += max(-6.0, min((xfip - era) * -4.0, 6.0))

        depleted = fresh_high_leverage < 2
        if depleted:
            reliability_score -= 12
        if closer_status != "fresh":
            reliability_score -= 8
        if not lhp_available:
            reliability_score -= 5

        return {
            "bullpen_score": round(clamp(reliability_score, 5, 95), 2),
            "fresh_high_leverage_arms": fresh_high_leverage,
            "depleted": depleted,
            "closer_status": closer_status,
            "lhp_available": lhp_available,
            "fatigue_flags": fatigue_flags[:8],
        }
