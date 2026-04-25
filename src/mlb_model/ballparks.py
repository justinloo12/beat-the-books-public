from __future__ import annotations

from typing import Any


BALLPARKS: dict[str, dict[str, Any]] = {
    "Angel Stadium": {"lat": 33.8003, "lon": -117.8827, "center_field_bearing": 40, "indoor": False},
    "Busch Stadium": {"lat": 38.6226, "lon": -90.1928, "center_field_bearing": 25, "indoor": False},
    "Chase Field": {"lat": 33.4453, "lon": -112.0667, "center_field_bearing": 20, "indoor": False},
    "Citi Field": {"lat": 40.7571, "lon": -73.8458, "center_field_bearing": 20, "indoor": False},
    "Citizens Bank Park": {"lat": 39.9061, "lon": -75.1665, "center_field_bearing": 22, "indoor": False},
    "Comerica Park": {"lat": 42.3390, "lon": -83.0485, "center_field_bearing": 35, "indoor": False},
    "Coors Field": {"lat": 39.7559, "lon": -104.9942, "center_field_bearing": 18, "indoor": False},
    "Dodger Stadium": {"lat": 34.0739, "lon": -118.2400, "center_field_bearing": 35, "indoor": False},
    "Fenway Park": {"lat": 42.3467, "lon": -71.0972, "center_field_bearing": 37, "indoor": False},
    "Globe Life Field": {"lat": 32.7473, "lon": -97.0847, "center_field_bearing": 36, "indoor": False},
    "Great American Ball Park": {"lat": 39.0974, "lon": -84.5066, "center_field_bearing": 50, "indoor": False},
    "Guaranteed Rate Field": {"lat": 41.8299, "lon": -87.6338, "center_field_bearing": 35, "indoor": False},
    "Rate Field": {"lat": 41.8299, "lon": -87.6338, "center_field_bearing": 35, "indoor": False},
    "Kauffman Stadium": {"lat": 39.0517, "lon": -94.4803, "center_field_bearing": 48, "indoor": False},
    "loanDepot park": {"lat": 25.7781, "lon": -80.2197, "center_field_bearing": 15, "indoor": True},
    "Minute Maid Park": {"lat": 29.7573, "lon": -95.3555, "center_field_bearing": 32, "indoor": False},
    "Daikin Park": {"lat": 29.7573, "lon": -95.3555, "center_field_bearing": 32, "indoor": False},
    "Nationals Park": {"lat": 38.8730, "lon": -77.0074, "center_field_bearing": 60, "indoor": False},
    "Oriole Park at Camden Yards": {"lat": 39.2839, "lon": -76.6217, "center_field_bearing": 45, "indoor": False},
    "Oracle Park": {"lat": 37.7786, "lon": -122.3893, "center_field_bearing": 55, "indoor": False},
    "PETCO Park": {"lat": 32.7073, "lon": -117.1566, "center_field_bearing": 40, "indoor": False},
    "Petco Park": {"lat": 32.7073, "lon": -117.1566, "center_field_bearing": 40, "indoor": False},
    "PNC Park": {"lat": 40.4469, "lon": -80.0057, "center_field_bearing": 42, "indoor": False},
    "Progressive Field": {"lat": 41.4962, "lon": -81.6852, "center_field_bearing": 32, "indoor": False},
    "Rogers Centre": {"lat": 43.6414, "lon": -79.3894, "center_field_bearing": 35, "indoor": True},
    "Sutter Health Park": {"lat": 38.5806, "lon": -121.5136, "center_field_bearing": 28, "indoor": False},
    "T-Mobile Park": {"lat": 47.5914, "lon": -122.3325, "center_field_bearing": 10, "indoor": False},
    "Target Field": {"lat": 44.9817, "lon": -93.2776, "center_field_bearing": 22, "indoor": False},
    "Tropicana Field": {"lat": 27.7683, "lon": -82.6534, "center_field_bearing": 25, "indoor": True},
    "Truist Park": {"lat": 33.8907, "lon": -84.4677, "center_field_bearing": 40, "indoor": False},
    "American Family Field": {"lat": 43.0280, "lon": -87.9712, "center_field_bearing": 25, "indoor": True},
    "Wrigley Field": {"lat": 41.9484, "lon": -87.6553, "center_field_bearing": 45, "indoor": False},
    "Yankee Stadium": {"lat": 40.8296, "lon": -73.9262, "center_field_bearing": 55, "indoor": False},
    "UNIQLO Field at Dodger Stadium": {"lat": 34.0739, "lon": -118.2400, "center_field_bearing": 35, "indoor": False},
}


def ballpark_meta(venue: str | None) -> dict[str, Any]:
    if not venue:
        return {}
    return BALLPARKS.get(venue, {})
