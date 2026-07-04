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
QUOTA_PATH = ODDS_DIR / "quota.json"

# Credit floor: when The Odds API reports fewer remaining credits than this,
# odds fetches are skipped (gracefully — never crash a workflow) so the last
# credits of the month stay available for manual use. Override with
# ODDS_API_MIN_REMAINING.
DEFAULT_MIN_REMAINING = 500


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


def _quota_floor() -> int:
    import os

    try:
        return int(os.environ.get("ODDS_API_MIN_REMAINING", DEFAULT_MIN_REMAINING))
    except ValueError:
        return DEFAULT_MIN_REMAINING


def read_last_known_quota() -> dict[str, Any]:
    """Last x-requests-remaining/used reported by The Odds API (persisted by
    the previous fetch). {} when never recorded."""
    if QUOTA_PATH.exists():
        try:
            return json.loads(QUOTA_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def write_quota(headers: Any) -> dict[str, Any]:
    """Persist the quota headers from an Odds API response and return them."""

    def _int(name: str) -> int | None:
        raw = headers.get(name)
        try:
            return int(float(raw)) if raw is not None else None
        except (TypeError, ValueError):
            return None

    quota = {
        "requests_remaining": _int("x-requests-remaining"),
        "requests_used": _int("x-requests-used"),
        "recorded_at": datetime.utcnow().isoformat(),
    }
    ensure_dirs()
    QUOTA_PATH.write_text(json.dumps(quota, indent=2), encoding="utf-8")
    return quota


async def fetch_json_with_headers(
    client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None
) -> tuple[Any, Any]:
    response = await client.get(url, params=params)
    if response.is_error:
        raise RuntimeError(f"{response.status_code} for {response.url}: {response.text[:500]}")
    return response.json(), response.headers


async def fetch_text(client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> str:
    response = await client.get(url, params=params)
    if response.is_error:
        raise RuntimeError(f"{response.status_code} for {response.url}: {response.text[:500]}")
    return response.text


async def download_odds(target_date: date, markets: list[str]) -> DownloadSummary:
    """Fetch odds for target_date with the MINIMUM Odds API usage.

    Credit conservation (20k credits/month budget):
    - ONE request per run: all markets combined (cost = #markets x #regions
      credits either way, but no per-market/per-event fan-out), one region
      (us), one bookmaker filter (DraftKings).
    - The x-requests-remaining / x-requests-used response headers are
      persisted to data/odds_api/quota.json and echoed in the logs so quota
      burn is visible over time.
    - Hard floor: if the API reported fewer than ODDS_API_MIN_REMAINING
      (default 500) remaining credits on the previous call, the fetch is
      SKIPPED gracefully instead of burning the reserve.
    """
    settings = get_settings()
    if not settings.odds_api_key:
        raise RuntimeError("MLB_MODEL_ODDS_API_KEY is not configured.")

    ensure_dirs()
    files_written: list[Path] = []
    today_et = datetime.now().astimezone().date()
    use_current_endpoint = target_date >= today_et
    paths = {market: ODDS_DIR / f"{target_date.isoformat()}_{market}.json" for market in markets}

    # Historical dates: reuse any files already on disk, never re-fetch them.
    if not use_current_endpoint and all(path.exists() for path in paths.values()):
        return DownloadSummary(files_written=list(paths.values()), requests_made=0, errors=[])

    floor = _quota_floor()
    last_quota = read_last_known_quota()
    remaining = last_quota.get("requests_remaining")
    if remaining is not None and remaining < floor:
        message = (
            f"Odds fetch skipped: only {remaining} Odds API credits remaining "
            f"(< ODDS_API_MIN_REMAINING={floor}). Preserving the reserve."
        )
        print(message)
        return DownloadSummary(files_written=[], requests_made=0, errors=[message])

    current_url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
    historical_url = "https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds"
    params = {
        "apiKey": settings.odds_api_key,
        "regions": "us",
        "markets": ",".join(markets),  # single combined call — no fan-out
        "oddsFormat": "american",
        "dateFormat": "iso",
        "bookmakers": "draftkings",
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        if use_current_endpoint:
            raw, headers = await fetch_json_with_headers(client, current_url, params=params)
            payload = {"timestamp": datetime.utcnow().isoformat(), "data": raw}
        else:
            payload, headers = await fetch_json_with_headers(
                client, historical_url, params={**params, "date": f"{target_date.isoformat()}T12:00:00Z"}
            )
    quota = write_quota(headers)
    print(
        f"Odds API quota: used={quota.get('requests_used')} "
        f"remaining={quota.get('requests_remaining')}"
    )

    # The combined response carries every requested market per game. Split it
    # back into the per-market files downstream consumers expect (each file
    # keeps ONLY its own market so MarketProvider's file merge stays
    # duplicate-free).
    for market, path in paths.items():
        path.write_text(json.dumps(_filter_payload_market(payload, market), indent=2), encoding="utf-8")
        files_written.append(path)
    return DownloadSummary(files_written=files_written, requests_made=1, errors=[])


def _filter_payload_market(payload: dict[str, Any], market_key: str) -> dict[str, Any]:
    """Copy of an Odds API payload with each bookmaker's markets filtered to
    market_key only (games without that market are kept, with empty markets)."""
    filtered_games = []
    for game in payload.get("data", []) or []:
        game_copy = dict(game)
        game_copy["bookmakers"] = [
            {**book, "markets": [m for m in book.get("markets", []) if m.get("key") == market_key]}
            for book in game.get("bookmakers", []) or []
        ]
        filtered_games.append(game_copy)
    return {**payload, "data": filtered_games}


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
        statcast_batter_exitvelo_barrels,
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
            f"statcast_batter_exitvelo_barrels_{season}.csv": lambda season=season: statcast_batter_exitvelo_barrels(season, minBBE=0),
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
        default=["totals"],
        help="Odds API markets to pull for MLB (totals only — we no longer bet ML/runline).",
    )
    parser.add_argument(
        "--odds-only",
        action="store_true",
        help="Skip Statcast/leaderboard downloads — only refresh odds. Use for mid-day re-runs.",
    )
    args = parser.parse_args()

    odds_date = date.fromisoformat(args.odds_date)
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)

    # The upfront odds fetch is a convenience seed; the backtest fetches odds
    # per-day on its own. When NOT running odds-only, a dead/empty Odds API quota
    # must not abort the (free) Statcast + leaderboard downloads, otherwise an
    # exhausted quota blocks the whole pipeline — including building the cache
    # that makes future runs free. So we swallow odds errors here unless the run
    # is specifically an odds-only refresh, where odds are the entire point.
    try:
        odds = await download_odds(odds_date, args.markets)
    except Exception as exc:  # noqa: BLE001 — degrade gracefully on quota/auth errors
        if args.odds_only:
            raise
        print(f"WARNING: upfront odds fetch failed ({exc}); continuing with Statcast only.")
        odds = DownloadSummary(files_written=[], requests_made=0, errors=[str(exc)])
    if args.odds_only:
        statcast = DownloadSummary(files_written=[], requests_made=0, errors=[])
        leaderboards = DownloadSummary(files_written=[], requests_made=0, errors=[])
    else:
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
