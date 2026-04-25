from __future__ import annotations

from typing import Any

from mlb_model.utils import clamp


class UmpireModelService:
    def score_umpire(self, profile: dict[str, Any]) -> dict[str, Any]:
        k_delta = float(profile.get("k_rate_delta", 0.0))
        walk_delta = float(profile.get("walk_rate_delta", 0.0))
        run_delta = float(profile.get("run_env_delta", 0.0))
        zone_size = float(profile.get("zone_size_delta", 0.0))

        factor = 1.0 + run_delta * 0.5 - zone_size * 0.2 - k_delta * 0.15 + walk_delta * 0.05
        return {"umpire_factor": round(clamp(factor, 0.9, 1.1), 3)}
