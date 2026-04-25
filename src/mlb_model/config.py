from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT = Path(__file__).resolve().parents[2]


class ModelSettings(BaseModel):
    module_toggles: dict[str, bool]
    weights: dict[str, float]
    feature_weights: dict[str, dict[str, float]]
    market_type_weights: dict[str, dict[str, float]]
    edge_thresholds: dict[str, float]
    kelly_caps: dict[str, float]
    unit_sizes: dict[str, float]
    juice_block_threshold: int
    simulation: dict[str, int]
    recalibration: dict[str, int]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MLB_MODEL_", env_file=".env", extra="ignore")

    app_name: str = "MLB Odds Model"
    database_url: str = f"sqlite:///{ROOT / 'mlb_model.db'}"
    model_config_path: Path = ROOT / "config" / "model_config.json"
    tomorrow_api_key: str | None = None
    odds_api_key: str | None = None
    oddsjam_api_key: str | None = None
    umpire_api_key: str | None = None
    pinnacle_base_url: str | None = None
    bankroll_units: float = 100.0
    data_dir: Path = ROOT / "data"
    lineup_poll_hour_et: int = 15
    morning_refresh_hour_et: int = 9
    pregame_refresh_minutes: int = 30

    def load_model_settings(self) -> ModelSettings:
        with self.model_config_path.open("r", encoding="utf-8") as file:
            data: dict[str, Any] = json.load(file)
        return ModelSettings.model_validate(data)


@lru_cache
def get_settings() -> Settings:
    return Settings()
