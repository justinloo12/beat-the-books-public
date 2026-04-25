from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Any

from sqlmodel import JSON, Column, Field, SQLModel


class MarketType(StrEnum):
    GAME_TOTAL = "game_total"
    FIRST_FIVE_TOTAL = "first_five_total"
    TEAM_TOTAL = "team_total"
    RUNLINE = "runline"


class PickTier(StrEnum):
    STRONG = "strong"
    MODERATE = "moderate"
    MONITOR = "monitor"
    PASS = "pass"
    BLOCK = "block"


class Game(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    game_id: str = Field(index=True, unique=True)
    game_date: date = Field(index=True)
    start_time: datetime
    home_team: str = Field(index=True)
    away_team: str = Field(index=True)
    ballpark: str
    park_factor: float = 1.0
    is_indoor: bool = False
    starter_home: str | None = None
    starter_away: str | None = None
    lineup_confirmed_home: bool = False
    lineup_confirmed_away: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class MarketSnapshot(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    game_id: str = Field(index=True)
    market_type: str = Field(index=True)
    market_key: str = Field(index=True)
    sportsbook: str = Field(default="draftkings", index=True)
    side: str
    line: float
    american_odds: int
    decimal_odds: float
    implied_probability_raw: float
    no_vig_probability: float | None = None
    public_bet_percentage: float | None = None
    opening_line: float | None = None
    captured_at: datetime = Field(default_factory=datetime.utcnow, index=True)


class ModuleSignal(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    game_id: str = Field(index=True)
    team: str = Field(index=True)
    module_name: str = Field(index=True)
    score: float
    confidence: float = 0.5
    summary: str
    payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Pick(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    game_id: str = Field(index=True)
    market_type: str = Field(index=True)
    market_key: str = Field(index=True)
    pick_side: str
    line: float
    american_odds: int
    decimal_odds: float
    model_probability: float
    no_vig_probability: float
    edge: float = Field(index=True)
    tier: str = Field(index=True)
    bankroll_fraction: float
    thin_consensus: bool = False
    top_features: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    bullpen_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    weather_stack_score: float = 0.0
    line_movement_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    trap_warning: bool = False
    placed_at: datetime = Field(default_factory=datetime.utcnow)
    closing_line: float | None = None
    clv_value: float | None = None
    result: str | None = None


class ModelCalibration(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    sample_size: int
    weights: dict[str, float] = Field(default_factory=dict, sa_column=Column(JSON))
    metrics: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)
