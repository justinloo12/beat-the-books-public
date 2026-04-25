from __future__ import annotations

from typing import Any


class HistoryModelService:
    def score_history(
        self,
        pitcher_vs_team: dict[str, Any] | None = None,
        team_vs_team: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        pitcher_vs_team = pitcher_vs_team or {}
        team_vs_team = team_vs_team or {}
        adjustment = 1.0
        notes: list[str] = []

        pa = int(pitcher_vs_team.get("pa", 0))
        if pa >= 50:
            team_woba = float(pitcher_vs_team.get("woba", 0.320))
            adjustment += (team_woba - 0.315) * 0.15
            notes.append(f"pitcher/team sample {pa} PA")

        meetings = int(team_vs_team.get("games", 0))
        if meetings >= 10:
            run_delta = float(team_vs_team.get("run_total_delta", 0.0))
            adjustment += run_delta * 0.05
            notes.append(f"last {meetings} meetings delta {run_delta:+.2f}")

        return {"history_adjustment": round(adjustment, 3), "history_notes": notes}
