# Beat the Books

MLB forecasting and simulation platform that combines Statcast features, matchup modeling, market odds, and daily automation into a live web dashboard.

## Stack

This implementation uses:

- Python 3.11
- FastAPI for APIs and dashboard payloads
- SQLModel + SQLite for local persistence
- APScheduler for recurring refresh and recalibration jobs
- `pybaseball` for Statcast / Savant data pulls
- MLB Stats API, weather providers, and market clients behind provider interfaces

All module weights and feature toggles live in `config/model_config.json`.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn mlb_model.main:app --reload
```

Then open `http://127.0.0.1:8000/` for the local dashboard.

## Refresh Today’s Snapshot

```bash
.venv/bin/python -m mlb_model.export_site --date 2026-04-25
```

This writes a GitHub Pages-friendly JSON snapshot to `docs/data/latest.json`.

## GitHub Pages Auto Refresh

The repo includes `.github/workflows/refresh-site.yml`, which refreshes the live board automatically during the late-morning ET window.

Set these GitHub repository secrets before relying on the workflow:

- `MLB_MODEL_ODDS_API_KEY`
- `MLB_MODEL_TOMORROW_API_KEY`

The workflow runs on a schedule and on manual dispatch, downloads the latest `h2h`, `totals`, and `spreads` markets, exports `docs/data/latest.json`, and pushes the updated snapshot back to `main`.

## What Is Implemented

- Independent module services for pitchers, bullpens, offense, lineups, weather, umpires, market movement, and synthesis
- Probabilistic pricing engine with no-vig conversion, edge scoring, and bankroll sizing
- Local persistence for games, market snapshots, picks, CLV, module signals, and results
- Scheduler hooks for daily refresh, lineup refresh, and pregame recalculation
- Static-site export for GitHub Pages plus a local FastAPI dashboard

## What Needs API Keys / Wiring

- Tomorrow.io weather key
- Optional OddsJam / market feed credentials
- Umpire source credentials if required

Those integrations already have provider stubs and parsing contracts so they can be swapped cleanly.
