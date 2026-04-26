from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path

from mlb_model.services.site_service import SiteService

_PICK_HISTORY_PATH = Path(__file__).resolve().parents[2] / "docs" / "data" / "pick_history.json"


def _load_pick_history() -> list[dict]:
    if _PICK_HISTORY_PATH.exists():
        try:
            return json.loads(_PICK_HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _history_summary(entries: list[dict]) -> dict:
    graded = [e for e in entries if e.get("result") in {"win", "loss", "push"}]
    wins = sum(1 for e in graded if e.get("result") == "win")
    losses = sum(1 for e in graded if e.get("result") == "loss")
    pushes = sum(1 for e in graded if e.get("result") == "push")
    units_risked = float(len(graded))
    units_profit = round(sum(float(e.get("pnl", 0.0)) / 100.0 for e in graded), 4)
    tracked = len(graded)
    hit_rate = (wins / tracked) if tracked else 0.0
    roi = (units_profit / units_risked) if units_risked else 0.0
    return {
        "tracked_bets": tracked,
        "graded_bets": tracked,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "units_risked": units_risked,
        "units_profit": units_profit,
        "roi": roi,
        "hit_rate": hit_rate,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Export a static site snapshot for GitHub Pages.")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    slate_date = date.fromisoformat(args.date)
    service = SiteService()
    payload = await service.site_payload(slate_date)

    # Merge file-based graded pick history (overrides SQLite history which lacks results)
    graded_history = _load_pick_history()
    if graded_history:
        payload["history"] = list(reversed(graded_history))
        payload["summary"].update(_history_summary(graded_history))

    docs_data = Path(__file__).resolve().parents[2] / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)
    dated_path = docs_data / f"{slate_date.isoformat()}.json"
    latest_path = docs_data / "latest.json"
    dated_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Rebuild archive index from all dated files so the UI can list available days
    index_entries = []
    for p in sorted(docs_data.glob("20??-??-??.json"), reverse=True):
        try:
            day_data = json.loads(p.read_text(encoding="utf-8"))
            day_picks = day_data.get("daily", {}).get("picks", [])
            index_entries.append({
                "date": p.stem,
                "picks": len(day_picks),
                "strong": sum(1 for pk in day_picks if pk.get("tier") == "strong"),
                "games": len(day_data.get("daily", {}).get("lineup_cards", [])),
            })
        except Exception:
            continue
    archive_path = docs_data / "archive_index.json"
    archive_path.write_text(json.dumps({"dates": index_entries}, indent=2), encoding="utf-8")

    print(str(latest_path))


if __name__ == "__main__":
    asyncio.run(main())
