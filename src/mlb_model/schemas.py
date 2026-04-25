from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class TeamRunContext(BaseModel):
    team: str
    pitcher_xba: float
    lineup_xwoba: float
    weather_multiplier: float
    park_factor: float
    bullpen_score: float
    expected_runs: float
    starter_ip_projection: float
    top_features: list[dict[str, Any]] = Field(default_factory=list)


class MarketEvaluation(BaseModel):
    game_id: str
    market_type: str
    market_key: str
    pick_side: str
    model_probability: float
    no_vig_probability: float
    edge: float
    tier: str
    bankroll_fraction: float
    thin_consensus: bool
    top_features: list[dict[str, Any]]
    bullpen_summary: dict[str, Any]
    weather_stack_score: float
    line_movement_summary: dict[str, Any]
    trap_warning: bool


class DashboardResponse(BaseModel):
    as_of: datetime
    today: date
    picks: list[dict[str, Any]]
    rolling_clv_last_50: float | None
    rolling_clv_last_200: float | None
    module_performance: list[dict[str, Any]]
    bankroll_tracker: dict[str, Any]
