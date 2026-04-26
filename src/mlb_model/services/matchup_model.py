from __future__ import annotations

from typing import Any

from mlb_model.config import get_settings
from mlb_model.utils import clamp, safe_mean


settings = get_settings()
model_settings = settings.load_model_settings()


class MatchupModelService:
    def __init__(self) -> None:
        self.feature_weights = model_settings.feature_weights

    def score_pitcher_profile(self, pitcher_profile: dict[str, Any]) -> dict[str, Any]:
        weights = self.feature_weights["pitcher"]
        group_weight = sum(weights.values()) or 1.0
        arsenal = pitcher_profile.get("pitch_arsenal") or []
        primary_pitch = arsenal[0] if arsenal else {}
        xba_score = self._inverse_rate_score(pitcher_profile.get("xba"), baseline=0.290, spread=0.08)
        run_value_score = self._inverse_rate_score(pitcher_profile.get("weighted_run_value"), baseline=0.0, spread=1.5)
        hard_hit_score = self._inverse_rate_score(pitcher_profile.get("hard_hit_pct"), baseline=0.375, spread=0.14)
        k_minus_bb = float(pitcher_profile.get("weighted_k_pct", 0.22)) - float(pitcher_profile.get("weighted_bb_pct", 0.08))
        k_minus_bb_score = self._rate_score(k_minus_bb, baseline=0.14, spread=0.18)
        barrel_score = self._inverse_rate_score(pitcher_profile.get("barrel_pct"), baseline=0.08, spread=0.08)
        ev50_score = self._inverse_rate_score(pitcher_profile.get("ev50"), baseline=89.0, spread=12.0)
        movement_raw = float(pitcher_profile.get("movement_score", 0.0))
        spin_modifier = 1 + min(abs(float(primary_pitch.get("spin_axis", 180.0)) - 180.0) / 360.0, 0.18)
        extension_modifier = 1 + min(max(float(pitcher_profile.get("extension", 6.1)) - 6.0, -0.5) * 0.08, 0.12)
        movement_score = clamp((movement_raw * spin_modifier * extension_modifier) * 2.6, 20, 90)
        quality_score = (
            xba_score * (weights["xba_split"] / group_weight)
            + run_value_score * (weights["run_value_per_pitch"] / group_weight)
            + hard_hit_score * (weights["hard_hit_allowed"] / group_weight)
            + k_minus_bb_score * (weights["k_minus_bb"] / group_weight)
            + barrel_score * (weights["barrel_allowed"] / group_weight)
            + ev50_score * (weights["ev50_allowed"] / group_weight)
            + movement_score * (weights["movement_profile"] / group_weight)
        )
        recent_delta = (
            (float(pitcher_profile.get("recent_xba", pitcher_profile.get("xba", 0.245))) - float(pitcher_profile.get("xba", 0.245))) * 180
            + (float(pitcher_profile.get("recent_hard_hit_pct", pitcher_profile.get("hard_hit_pct", 0.36))) - float(pitcher_profile.get("hard_hit_pct", 0.36))) * 75
            + (float(pitcher_profile.get("recent_barrel_pct", pitcher_profile.get("barrel_pct", 0.08))) - float(pitcher_profile.get("barrel_pct", 0.08))) * 130
            - (float(pitcher_profile.get("recent_k_pct", pitcher_profile.get("weighted_k_pct", 0.22))) - float(pitcher_profile.get("weighted_k_pct", 0.22))) * 50
        )
        vulnerability = clamp(100 - quality_score, 12, 92)
        return {
            "quality_score": round(quality_score, 2),
            "vulnerability_score": round(vulnerability, 2),
            "recent_delta": round(recent_delta, 2),
            "components": {
                "xba_split": round(xba_score, 2),
                "run_value_per_pitch": round(run_value_score, 2),
                "hard_hit_allowed": round(hard_hit_score, 2),
                "k_minus_bb": round(k_minus_bb_score, 2),
                "barrel_allowed": round(barrel_score, 2),
                "ev50_allowed": round(ev50_score, 2),
                "movement_profile": round(movement_score, 2),
            },
        }

    def score_batter_vs_pitcher(
        self,
        batter_profile: dict[str, Any],
        pitcher_profile: dict[str, Any],
        lineup_slot: int | None = None,
    ) -> dict[str, Any]:
        weights = self.feature_weights["batter"]
        group_weight = sum(weights.values()) or 1.0
        arsenal = pitcher_profile.get("pitch_arsenal", [])
        batter_by_pitch = {item["pitch_type"]: item for item in batter_profile.get("pitch_profiles", [])}
        pitch_scores: list[dict[str, Any]] = []
        pitch_value_match = 50.0

        if arsenal:
            weighted_pitch_scores = []
            for pitch in arsenal:
                batter_pitch = batter_by_pitch.get(pitch["pitch_type"], {})
                usage = float(pitch.get("usage_pct", 0.0))
                run_value_score = self._rate_score(
                    float(batter_pitch.get("run_value", 0.0)) + float(pitch.get("run_value", 0.0)),
                    baseline=0.0,
                    spread=1.8,
                )
                qoc_score = self._rate_score(
                    float(batter_pitch.get("quality_of_contact", batter_profile.get("quality_of_contact", 0.32)))
                    - float(pitch.get("quality_of_contact", pitcher_profile.get("xba", 0.245))),
                    baseline=0.04,
                    spread=0.20,
                )
                swing_fit = self._swing_fit(batter_profile, pitcher_profile, pitch)
                pitch_score = clamp((run_value_score * 0.55) + (qoc_score * 0.25) + (swing_fit * 0.20), 20, 92)
                weighted_pitch_scores.append(pitch_score * usage)
                pitch_scores.append(
                    {
                        "pitch_type": pitch["pitch_type"],
                        "usage_pct": usage,
                        "pitch_score": round(pitch_score, 3),
                        "swing_fit": round(swing_fit, 3),
                    }
                )
            pitch_value_match = sum(weighted_pitch_scores)

        xwoba_score = self._rate_score(batter_profile.get("xwoba"), baseline=0.315, spread=0.16)
        contact_score = (
            self._rate_score(batter_profile.get("hard_hit_pct"), baseline=0.375, spread=0.16) * 0.5
            + self._rate_score(batter_profile.get("ev50"), baseline=89.0, spread=12.0) * 0.5
        )
        swing_score = self._swing_fit(batter_profile, pitcher_profile, None)
        discipline_edge = float(batter_profile.get("bb_pct", 0.08)) - float(batter_profile.get("k_pct", 0.22))
        discipline_score = self._rate_score(discipline_edge, baseline=-0.14, spread=0.20)
        qoc_score = self._rate_score(batter_profile.get("quality_of_contact", batter_profile.get("xwoba", 0.315)), baseline=0.315, spread=0.15)
        handedness_bonus = 2.5 if batter_profile.get("handedness") != pitcher_profile.get("handedness") else -1.0
        pa_weight = 1.06 if lineup_slot in {1, 2} else 1.03 if lineup_slot in {3, 4, 5} else 0.98
        batter_score = (
            xwoba_score * (weights["xwoba_split"] / group_weight)
            + pitch_value_match * (weights["pitch_type_run_value"] / group_weight)
            + contact_score * (weights["contact_quality"] / group_weight)
            + swing_score * (weights["swing_path"] / group_weight)
            + discipline_score * (weights["discipline"] / group_weight)
            + qoc_score * (weights["quality_of_contact_profile"] / group_weight)
        )
        matchup_score = clamp((batter_score + handedness_bonus) * pa_weight, 18, 92)
        recent_delta = (
            (float(batter_profile.get("recent_xwoba", batter_profile.get("xwoba", 0.315))) - float(batter_profile.get("xwoba", 0.315))) * 170
            + (float(batter_profile.get("recent_hard_hit_pct", batter_profile.get("hard_hit_pct", 0.36))) - float(batter_profile.get("hard_hit_pct", 0.36))) * 55
            + (float(batter_profile.get("recent_ev50", batter_profile.get("ev50", 89.0))) - float(batter_profile.get("ev50", 89.0))) * 1.4
            - (float(batter_profile.get("recent_k_pct", batter_profile.get("k_pct", 0.22))) - float(batter_profile.get("k_pct", 0.22))) * 42
        )
        return {
            "batter_id": batter_profile.get("batter_id"),
            "matchup_score": round(matchup_score, 2),
            "batter_score": round(batter_score, 2),
            "recent_delta": round(recent_delta, 2),
            "handedness_bonus": handedness_bonus,
            "pitch_scores": sorted(pitch_scores, key=lambda item: item["pitch_score"], reverse=True),
        }

    def lineup_offense_score(
        self,
        lineup_matchups: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not lineup_matchups:
            return {"offense_score": 50.0, "batter_score": 50.0, "recency_score": 50.0, "top_batter_edges": []}

        weighted_matchups = []
        recent_deltas = []
        for entry in lineup_matchups:
            slot = entry.get("slot", 9)
            try:
                slot_num = int(slot)
            except (TypeError, ValueError):
                slot_num = 9
            pa_weight = 1.06 if slot_num in {1, 2} else 1.03 if slot_num in {3, 4, 5} else 0.98
            weighted_matchups.append(entry["matchup"]["matchup_score"] * pa_weight)
            recent_deltas.append(entry["matchup"].get("recent_delta", 0.0))

        offense_score = clamp(sum(weighted_matchups) / len(weighted_matchups), 20, 90)
        recency_score = clamp(50 + safe_mean(recent_deltas), 20, 90)
        top_edges = sorted(
            [
                {
                    "player_id": entry.get("batter_id"),
                    "slot": entry.get("slot"),
                    "matchup_score": entry["matchup"]["matchup_score"],
                }
                for entry in lineup_matchups
            ],
            key=lambda item: item["matchup_score"],
            reverse=True,
        )[:3]
        return {
            "offense_score": round(offense_score, 2),
            "batter_score": round(offense_score, 2),
            "recency_score": round(recency_score, 2),
            "top_batter_edges": top_edges,
        }

    def _swing_fit(
        self,
        batter_profile: dict[str, Any],
        pitcher_profile: dict[str, Any],
        pitch_profile: dict[str, Any] | None,
    ) -> float:
        swing_path = float(batter_profile.get("swing_path_score", 0.0))
        attack_angle = float(batter_profile.get("attack_angle", 0.0))
        arsenal = pitcher_profile.get("pitch_arsenal") or []
        primary_pitch = arsenal[0] if arsenal else {}
        arm_angle = float((pitch_profile or {}).get("arm_angle", pitcher_profile.get("arm_angle", 45.0)))
        vertical = float((pitch_profile or {}).get("vertical_movement", pitcher_profile.get("movement_score", 1.0) / 30))
        horizontal = float((pitch_profile or {}).get("horizontal_movement", pitcher_profile.get("movement_score", 1.0) / 30))
        spin_dir = float((pitch_profile or {}).get("spin_dir", primary_pitch.get("spin_dir", 180.0)))
        swing_plane_alignment = 58 - min(abs((attack_angle + swing_path * 0.04) - (vertical * 10)) * 4.5, 24)
        angle_disruption = 14 - min(abs((arm_angle * 0.32) - abs(horizontal * 10)) * 1.8, 14)
        spin_fit = 12 - min(abs((spin_dir % 180) - (swing_path % 180)) / 6, 12)
        # subtract ~20 to center the neutral default at 50 (raw neutral ≈ 58+0+12=70)
        return clamp(swing_plane_alignment + angle_disruption + spin_fit - 20, 18, 88)

    def _rate_score(self, value: Any, baseline: float, spread: float) -> float:
        numeric = float(value or 0.0)
        return clamp(50 + ((numeric - baseline) / max(spread, 0.0001)) * 25, 10, 95)

    def _inverse_rate_score(self, value: Any, baseline: float, spread: float) -> float:
        numeric = float(value or 0.0)
        return clamp(50 - ((numeric - baseline) / max(spread, 0.0001)) * 25, 10, 95)
