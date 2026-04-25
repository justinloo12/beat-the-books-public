from __future__ import annotations

from typing import Any

from mlb_model.utils import clamp, safe_mean, shannon_entropy


class PitcherModelService:
    def score_pitcher(self, stats: dict[str, Any]) -> dict[str, Any]:
        xera = float(stats.get("xERA", stats.get("ERA", 4.25)))
        era = float(stats.get("ERA", xera))
        xba = float(stats.get("xBA", 0.245))
        xslg = float(stats.get("xSLG", 0.400))
        hard_hit = float(stats.get("HardHit%", 38.0))
        barrel = float(stats.get("Barrel%", 8.5))
        extension_pct = float(stats.get("ExtensionPercentile", 50.0))
        chase = float(stats.get("Chase%", 28.0))
        whiff = float(stats.get("Whiff%", 24.0))
        gb = float(stats.get("GB%", 42.0))
        season_velo = float(stats.get("FBv", 93.0))
        last5_velo = safe_mean(stats.get("Last5FBv", [season_velo]), season_velo)
        pitch_mix = stats.get("PitchMix", {})
        days_rest = int(stats.get("DaysRest", 4))
        innings_pitched = float(stats.get("IP", 120.0))
        rolling_xera = float(stats.get("Last3xERA", xera))

        regression_gap = era - xera
        contact_multiplier = 1.1 if extension_pct < 20 else 1.0
        fly_ball_penalty = 1.08 if gb < 35 else 1.0
        velo_penalty = 0.92 if season_velo - last5_velo > 1.5 else 1.0
        rest_penalty = 0.85 if days_rest == 3 else 0.95 if days_rest >= 6 else 1.0
        fatigue_penalty = 1.0 - max(0.0, min((innings_pitched - 130.0) * 0.0025, 0.15))

        recent_component = clamp((6.0 - rolling_xera) / 6.0, 0.0, 1.0)
        season_component = clamp((6.0 - xera) / 6.0, 0.0, 1.0)
        quality = (
            season_component * 0.30
            + recent_component * 0.35
            + clamp((0.300 - xba) / 0.120, 0.0, 1.0) * 0.08
            + clamp((0.500 - xslg) / 0.250, 0.0, 1.0) * 0.08
            + clamp((35 - hard_hit) / 20, 0.0, 1.0) * 0.06
            + clamp((10 - barrel) / 10, 0.0, 1.0) * 0.05
            + clamp(chase / 40, 0.0, 1.0) * 0.04
            + clamp(whiff / 40, 0.0, 1.0) * 0.02
            + clamp(gb / 55, 0.0, 1.0) * 0.02
        )
        quality *= contact_multiplier * fly_ball_penalty * velo_penalty * rest_penalty * fatigue_penalty
        quality = clamp(quality * 100, 0.0, 100.0)

        entropy = shannon_entropy(pitch_mix) if pitch_mix else 0.0
        manual_review = abs(rolling_xera - xera) > 1.0
        flag = "Elite" if quality >= 75 else "Low" if quality >= 58 else "Medium" if quality >= 42 else "High"
        if regression_gap < -1.0:
            flag = "High"
        elif regression_gap > 1.0 and quality > 60:
            flag = "Elite"

        return {
            "quality_score": round(quality, 2),
            "vulnerability_flag": flag,
            "regression_gap": round(regression_gap, 2),
            "pitch_mix_entropy": round(entropy, 3),
            "manual_review": manual_review,
            "starter_ip_projection": round(clamp(4.5 + (quality / 40), 3.5, 7.5), 2),
        }
