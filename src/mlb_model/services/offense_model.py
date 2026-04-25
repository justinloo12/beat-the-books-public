from __future__ import annotations

from typing import Any

from mlb_model.utils import clamp


class OffenseModelService:
    def score_offense(
        self,
        team_profile: dict[str, Any],
        lineup_profile: dict[str, Any],
        pitcher_hand: str,
        park_factor: float,
    ) -> dict[str, Any]:
        suffix = "vs_lhp" if pitcher_hand == "L" else "vs_rhp"
        wrc_plus = float(team_profile.get(f"wrc_plus_{suffix}", 100.0))
        barrel = float(team_profile.get(f"barrel_pct_{suffix}", 7.5))
        hard_hit = float(team_profile.get(f"hard_hit_pct_{suffix}", 37.0))
        strikeout = float(team_profile.get(f"k_pct_{suffix}", 22.0))
        walk = float(team_profile.get(f"bb_pct_{suffix}", 8.0))
        recent_runs = float(team_profile.get("recent_runs_per_game", 4.4))
        season_runs = float(team_profile.get("season_runs_per_game", 4.4))
        lineup_barrel = float(lineup_profile.get("lineup_barrel_score", barrel))
        baseline_lineup_barrel = float(lineup_profile.get("season_lineup_barrel_score", barrel))
        scratch_adjustment = float(lineup_profile.get("scratch_adjustment", 1.0))

        recent_multiplier = 0.7 + 0.3 * clamp(recent_runs / max(season_runs, 0.1), 0.7, 1.3)
        lineup_multiplier = clamp(lineup_barrel / max(baseline_lineup_barrel, 0.1), 0.75, 1.15)

        offense = (
            clamp(wrc_plus / 140, 0.55, 1.2) * 0.40
            + clamp(barrel / 12, 0.4, 1.2) * 0.15
            + clamp(hard_hit / 45, 0.5, 1.1) * 0.12
            + clamp((30 - strikeout) / 12, 0.4, 1.1) * 0.13
            + clamp(walk / 12, 0.4, 1.0) * 0.10
            + clamp(park_factor, 0.9, 1.12) * 0.10
        )
        score = clamp(offense * 100 * recent_multiplier * lineup_multiplier * scratch_adjustment, 20, 95)
        return {
            "offense_score": round(score, 2),
            "lineup_multiplier": round(lineup_multiplier, 3),
            "recent_multiplier": round(recent_multiplier, 3),
        }
