from __future__ import annotations

import argparse
import asyncio
import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from mlb_model.config import get_settings


ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
ODDS_DIR = DATA_DIR / "odds_api"
SAVANT_DIR = DATA_DIR / "baseball_savant"


@dataclass(slots=True)
class DownloadSummary:
    files_written: list[Path]
    requests_made: int
    errors: list[str] | None = None


def ensure_dirs() -> None:
    for path in (DATA_DIR, ODDS_DIR, SAVANT_DIR):
        path.mkdir(parents=True, exist_ok=True)


def month_ranges(start_date: date, end_date: date) -> list[tuple[date, date]]:
    ranges: list[tuple[date, date]] = []
    current = start_date.replace(day=1)
    while current <= end_date:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        chunk_end = min(end_date, next_month - timedelta(days=1))
        chunk_start = max(start_date, current)
        ranges.append((chunk_start, chunk_end))
        current = next_month
    return ranges


async def fetch_json(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> Any:
    response = await client.get(url, params=params)
    if response.is_error:
        raise RuntimeError(f"{response.status_code} for {response.url}: {response.text[:500]}")
    return response.json()


async def fetch_text(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> str:
    response = await client.get(url, params=params)
    if response.is_error:
        raise RuntimeError(f"{response.status_code} for {response.url}: {response.text[:500]}")
    return response.text


async def download_odds(target_date: date, markets: list[str]) -> DownloadSummary:
    settings = get_settings()
    if not settings.odds_api_key:
        raise RuntimeError("MLB_MODEL_ODDS_API_KEY is not configured.")

    ensure_dirs()
    files_written: list[Path] = []
    requests_made = 0
    today_et = datetime.now().astimezone().date()
    use_current_endpoint = target_date >= today_et
    current_url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    historical_url = "https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds"
    async with httpx.AsyncClient(timeout=60.0) as client:
        for market in markets:
            path = ODDS_DIR / f"{target_date.isoformat()}_{market}.json"
            if path.exists():
                files_written.append(path)
                continue
            params = {
                "apiKey": settings.odds_api_key,
                "regions": "us",
                "markets": market,
                "oddsFormat": "american",
                "dateFormat": "iso",
                "bookmakers": "draftkings",
            }
            if use_current_endpoint:
                payload = await fetch_json(client, current_url, params=params)
                payload = {"timestamp": datetime.utcnow().isoformat(), "data": payload}
            else:
                historical_params = {
                    **params,
                    "date": f"{target_date.isoformat()}T12:00:00Z",
                }
                payload = await fetch_json(client, historical_url, params=historical_params)
            requests_made += 1
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            files_written.append(path)
    return DownloadSummary(files_written=files_written, requests_made=requests_made, errors=[])


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_statcast_csv(payload: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(payload.splitlines())
    return [dict(row) for row in reader]


async def download_savant_statcast(
    start_date: date,
    end_date: date,
) -> DownloadSummary:
    ensure_dirs()
    files_written: list[Path] = []
    requests_made = 0
    async with httpx.AsyncClient(timeout=180.0) as client:
        for chunk_start, chunk_end in month_ranges(start_date, end_date):
            path = SAVANT_DIR / f"statcast_{chunk_start.isoformat()}_{chunk_end.isoformat()}.csv"
            if path.exists():
                files_written.append(path)
                continue
            params = {
                "all": "true",
                "hfPT": "",
                "hfAB": "",
                "hfGT": "R|",
                "hfPR": "",
                "hfZ": "",
                "stadium": "",
                "hfBBT": "",
                "hfPull": "",
                "metric_1": "",
                "group_by": "name",
                "type": "details",
                "player_event_sort": "api_p_release_speed",
                "sort_col": "pitches",
                "min_pitches": "0",
                "min_results": "0",
                "game_date_gt": chunk_start.isoformat(),
                "game_date_lt": chunk_end.isoformat(),
            }
            url = "https://baseballsavant.mlb.com/statcast_search/csv"
            payload = await fetch_text(client, url, params=params)
            requests_made += 1
            rows = parse_statcast_csv(payload)
            write_csv(path, rows)
            files_written.append(path)
    return DownloadSummary(files_written=files_written, requests_made=requests_made, errors=[])


async def download_savant_leaderboards(seasons: list[int]) -> DownloadSummary:
    from pybaseball import (
        batting_stats_bref,
        pitching_stats_bref,
        statcast_batter_expected_stats,
        statcast_batter_percentile_ranks,
        statcast_pitcher_arsenal_stats,
        statcast_pitcher_exitvelo_barrels,
        statcast_pitcher_expected_stats,
        statcast_pitcher_percentile_ranks,
    )

    ensure_dirs()
    files_written: list[Path] = []
    requests_made = 0
    errors: list[str] = []
    for season in seasons:
        jobs = {
            f"statcast_pitcher_expected_stats_{season}.csv": lambda season=season: statcast_pitcher_expected_stats(season, minPA=0),
            f"statcast_pitcher_percentile_ranks_{season}.csv": lambda season=season: statcast_pitcher_percentile_ranks(season),
            f"statcast_pitcher_exitvelo_barrels_{season}.csv": lambda season=season: statcast_pitcher_exitvelo_barrels(season, minBBE=0),
            f"statcast_pitcher_arsenal_stats_{season}.csv": lambda season=season: statcast_pitcher_arsenal_stats(season, minPA=0),
            f"statcast_batter_expected_stats_{season}.csv": lambda season=season: statcast_batter_expected_stats(season, minPA=0),
            f"statcast_batter_percentile_ranks_{season}.csv": lambda season=season: statcast_batter_percentile_ranks(season),
            f"pitching_stats_bref_{season}.csv": lambda season=season: pitching_stats_bref(season),
            f"batting_stats_bref_{season}.csv": lambda season=season: batting_stats_bref(season),
        }
        for filename, job in jobs.items():
            path = SAVANT_DIR / filename
            if path.exists():
                files_written.append(path)
                continue
            try:
                frame = await asyncio.to_thread(job)
                frame.to_csv(path, index=False)
                files_written.append(path)
                requests_made += 1
            except Exception as exc:
                errors.append(f"{filename}: {exc}")
    return DownloadSummary(files_written=files_written, requests_made=requests_made, errors=errors)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Download MLB odds and Baseball Savant data.")
    parser.add_argument("--odds-date", default=date.today().isoformat())
    parser.add_argument("--start-date", default=(date.today() - timedelta(days=730)).isoformat())
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--seasons", nargs="+", type=int, default=[date.today().year - 2, date.today().year - 1, date.today().year])
    parser.add_argument(
        "--markets",
        nargs="+",
        default=["totals", "spreads"],
        help="Odds API markets to pull for MLB.",
    )
    args = parser.parse_args()

    odds_date = date.fromisoformat(args.odds_date)
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    odds = await download_odds(odds_date, args.markets)
    statcast = await download_savant_statcast(start_date, end_date)
    leaderboards = await download_savant_leaderboards(args.seasons)

    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "odds_files": [str(path) for path in odds.files_written],
        "statcast_files": [str(path) for path in statcast.files_written],
        "leaderboard_files": [str(path) for path in leaderboards.files_written],
        "leaderboard_errors": leaderboards.errors or [],
        "requests_made": odds.requests_made + statcast.requests_made + leaderboards.requests_made,
    }
    path = DATA_DIR / "download_summary.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
