"""Capture one intraday odds snapshot and append it to docs/data/odds_snapshots/.

Deliberately slim: stdlib only (no package install needed in CI), ONE request
to The Odds API (h2h + totals in a single call), and one small JSON append.
Runs several times a day from .github/workflows/snapshot-odds.yml so that
grade_picks.py can use the last pre-first-pitch snapshot per game as the
closing-line proxy instead of a single end-of-day file.

Usage:
    MLB_MODEL_ODDS_API_KEY=... python scripts/snapshot_odds.py
    python scripts/snapshot_odds.py --date 2026-07-03   # override slate date
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = ROOT / "docs" / "data" / "odds_snapshots"
ODDS_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
ET = ZoneInfo("America/New_York")

# Safety valve: never let a runaway schedule bloat one day's file.
MAX_SNAPSHOTS_PER_DAY = 16


def _implied(american: int) -> float:
    if american < 0:
        return (-american) / (-american + 100)
    return 100 / (american + 100)


def _no_vig(a: int, b: int) -> tuple[float, float]:
    ra, rb = _implied(a), _implied(b)
    total = ra + rb
    return ra / total, rb / total


def fetch_raw_odds(api_key: str) -> list[dict]:
    """One API call: h2h + totals, DraftKings, american odds."""
    params = urllib.parse.urlencode(
        {
            "apiKey": api_key,
            "regions": "us",
            "markets": "h2h,totals",
            "oddsFormat": "american",
            "dateFormat": "iso",
            "bookmakers": "draftkings",
        }
    )
    req = urllib.request.Request(
        f"{ODDS_URL}?{params}",
        headers={"User-Agent": "beat-the-books/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def build_games(raw_games: list[dict], slate_date: date) -> list[dict]:
    """Reduce the raw Odds API payload to the same per-game shape used by
    docs/data/live_odds.json (moneyline w/ no-vig probs + totals outcomes).

    Only games whose first pitch falls on slate_date in ET are kept — the
    current-odds endpoint also returns tomorrow's early listings.
    """
    games: list[dict] = []
    for game in raw_games:
        commence = game.get("commence_time")
        if commence:
            try:
                start_et = datetime.fromisoformat(commence.replace("Z", "+00:00")).astimezone(ET)
                if start_et.date() != slate_date:
                    continue
            except ValueError:
                pass
        away = game.get("away_team", "")
        home = game.get("home_team", "")
        dk = next((b for b in game.get("bookmakers", []) if b.get("key") == "draftkings"), None)
        if not dk:
            continue

        moneyline: dict = {}
        h2h = next((m for m in dk.get("markets", []) if m.get("key") == "h2h"), None)
        if h2h:
            outcomes = h2h.get("outcomes", [])
            away_odds = next((int(o["price"]) for o in outcomes if o.get("name") == away), None)
            home_odds = next((int(o["price"]) for o in outcomes if o.get("name") == home), None)
            if away_odds is not None and home_odds is not None:
                away_nv, home_nv = _no_vig(away_odds, home_odds)
                moneyline = {
                    "away_odds": away_odds,
                    "home_odds": home_odds,
                    "away_no_vig": round(away_nv, 4),
                    "home_no_vig": round(home_nv, 4),
                }

        totals_market = next((m for m in dk.get("markets", []) if m.get("key") == "totals"), None)
        totals = [
            {"name": o.get("name"), "point": o.get("point"), "price": int(o["price"])}
            for o in (totals_market.get("outcomes", []) if totals_market else [])
            if o.get("price") is not None
        ]

        games.append(
            {
                "away_team": away,
                "home_team": home,
                "commence_time": commence,
                "moneyline": moneyline,
                "totals": totals,
            }
        )
    return games


def append_snapshot(path: Path, snapshot: dict, max_per_day: int = MAX_SNAPSHOTS_PER_DAY) -> int:
    """Append a timestamped snapshot to the per-date file. Returns the new
    snapshot count. Corrupt/legacy files are replaced rather than crashing."""
    data: dict = {}
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    snapshots = data.get("snapshots") if isinstance(data, dict) else None
    if not isinstance(snapshots, list):
        snapshots = []
    snapshots.append(snapshot)
    # Keep chronological order and enforce the per-day cap (drop oldest —
    # the LAST pre-pitch snapshot is what matters for the closing proxy).
    snapshots.sort(key=lambda s: s.get("fetched_at", ""))
    snapshots = snapshots[-max_per_day:]
    payload = {"date": path.stem, "snapshots": snapshots}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return len(snapshots)


def main() -> int:
    parser = argparse.ArgumentParser(description="Append one intraday odds snapshot.")
    parser.add_argument(
        "--date",
        default=datetime.now(ET).date().isoformat(),
        help="Slate date in ET (YYYY-MM-DD). Defaults to today in ET.",
    )
    args = parser.parse_args()
    slate_date = date.fromisoformat(args.date)

    api_key = os.environ.get("MLB_MODEL_ODDS_API_KEY")
    if not api_key:
        print("MLB_MODEL_ODDS_API_KEY is not set — nothing to do.")
        return 1

    raw = fetch_raw_odds(api_key)
    games = build_games(raw, slate_date)
    if not games:
        print(f"No DraftKings MLB odds returned for {slate_date} — skipping append.")
        return 0

    snapshot = {
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "games": games,
    }
    out_path = SNAPSHOT_DIR / f"{slate_date.isoformat()}.json"
    count = append_snapshot(out_path, snapshot)
    print(f"Appended snapshot #{count} ({len(games)} games) to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
