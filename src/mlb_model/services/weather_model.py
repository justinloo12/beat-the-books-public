from __future__ import annotations

from typing import Any

from mlb_model.ballparks import ballpark_meta
from mlb_model.utils import clamp


class WeatherModelService:
    def _wind_vector(self, venue: str | None, wind_direction: float | None) -> tuple[bool, bool]:
        if wind_direction is None or venue is None:
            return False, False
        meta = ballpark_meta(venue)
        bearing = meta.get("center_field_bearing")
        if bearing is None:
            return False, False
        # Meteorological direction indicates where wind comes from.
        # If it is within ~35 degrees of center-field bearing, the ball gets help out.
        # If it is near the inverse direction, it is blowing in.
        out_delta = abs(((float(wind_direction) - float(bearing) + 180) % 360) - 180)
        in_delta = abs(((float(wind_direction) - ((float(bearing) + 180) % 360) + 180) % 360) - 180)
        return out_delta <= 35, in_delta <= 35

    def score_weather(self, game: dict[str, Any], forecast: dict[str, Any]) -> dict[str, Any]:
        venue = game.get("ballpark")
        if game.get("is_indoor", False) or ballpark_meta(venue).get("indoor", False):
            return {"weather_multiplier": 1.0, "weather_stack_score": 0.0}

        temp = float(forecast.get("temperature_f", 72.0))
        wind_speed = float(forecast.get("wind_speed_mph", 0.0))
        humidity = float(forecast.get("humidity", 50.0))
        wind_direction = forecast.get("wind_direction")
        wind_out, wind_in = self._wind_vector(venue, float(wind_direction) if wind_direction not in {None, "neutral"} else None)

        temp_adj = 1 + ((temp - 72) / 10) * 0.02
        wind_adj = 1.0
        if wind_out and wind_speed > 8:
            wind_adj += min((wind_speed - 8) * 0.01, 0.08)
        if wind_in and wind_speed > 8:
            wind_adj -= min((wind_speed - 8) * 0.01, 0.08)
        humidity_adj = 1 + clamp((humidity - 55) / 100 * 0.015, -0.015, 0.02)

        both_trigger = (abs(temp - 72) >= 10) and (wind_speed > 8)
        multiplier = 1 + ((temp_adj - 1) + (wind_adj - 1)) * (1.0 if both_trigger else 0.4)
        multiplier *= humidity_adj
        stack_score = clamp(abs(multiplier - 1) * 25, 0, 5)
        return {
            "weather_multiplier": round(multiplier, 3),
            "weather_stack_score": round(stack_score, 2),
            "wind_blowing_out": wind_out,
            "wind_blowing_in": wind_in,
        }
