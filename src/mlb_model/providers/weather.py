from __future__ import annotations

from typing import Any

import httpx

from mlb_model.ballparks import ballpark_meta
from mlb_model.config import get_settings


class TomorrowWeatherProvider:
    base_url = "https://api.tomorrow.io/v4/weather/forecast"
    open_meteo_url = "https://api.open-meteo.com/v1/forecast"

    def __init__(self) -> None:
        self.settings = get_settings()

    async def healthcheck(self) -> dict[str, Any]:
        return {"provider": "tomorrow", "configured": bool(self.settings.tomorrow_api_key)}

    async def fetch_forecast(self, location: str, start_time: str | None = None) -> dict[str, Any]:
        forecast = {}
        if self.settings.tomorrow_api_key:
            forecast = await self._fetch_tomorrow(location)
        if forecast:
            return forecast
        return await self._fetch_open_meteo(location, start_time)

    async def _fetch_tomorrow(self, location: str) -> dict[str, Any]:
        params = {"location": location, "apikey": self.settings.tomorrow_api_key}
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            return {}

        hourly = []
        for entry in (((payload or {}).get("timelines") or {}).get("hourly") or []):
            values = entry.get("values", {})
            hourly.append(
                {
                    "time": entry.get("time"),
                    "values": {
                        "temperature": values.get("temperature"),
                        "windSpeed": values.get("windSpeed"),
                        "windDirection": values.get("windDirection"),
                        "humidity": values.get("humidity"),
                    },
                }
            )
        if not hourly:
            return {}
        return {"provider": "tomorrow", "timelines": {"hourly": hourly}}

    async def _fetch_open_meteo(self, location: str, start_time: str | None = None) -> dict[str, Any]:
        meta = ballpark_meta(location)
        lat = meta.get("lat")
        lon = meta.get("lon")
        if lat is None or lon is None:
            return {}

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m",
            "forecast_days": 2,
            "timezone": "America/New_York",
        }
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(self.open_meteo_url, params=params)
                response.raise_for_status()
                payload = response.json()
        except Exception:
            return {}

        hourly_payload = payload.get("hourly") or {}
        times = hourly_payload.get("time") or []
        temps = hourly_payload.get("temperature_2m") or []
        humidities = hourly_payload.get("relative_humidity_2m") or []
        wind_speeds = hourly_payload.get("wind_speed_10m") or []
        wind_dirs = hourly_payload.get("wind_direction_10m") or []
        hourly = []
        for index, time_value in enumerate(times):
            hourly.append(
                {
                    "time": time_value,
                    "values": {
                        "temperature": temps[index] if index < len(temps) else None,
                        "windSpeed": wind_speeds[index] if index < len(wind_speeds) else None,
                        "windDirection": wind_dirs[index] if index < len(wind_dirs) else None,
                        "humidity": humidities[index] if index < len(humidities) else None,
                    },
                }
            )
        if not hourly:
            return {}
        return {"provider": "open-meteo", "timelines": {"hourly": hourly}}
