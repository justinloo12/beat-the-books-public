from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path

from mlb_model.metrics import build_metrics, write_metrics
from mlb_model.services.meta_model import MetaModel
from mlb_model.services.site_service import SiteService

_PICK_HISTORY_PATH = Path(__file__).resolve().parents[2] / "docs" / "data" / "pick_history.json"


def _load_pick_history() -> list[dict]:
    if _PICK_HISTORY_PATH.exists():
        try:
            return json.loads(_PICK_HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _stats_for(entries: list[dict]) -> dict:
    graded = [e for e in entries if e.get("result") in {"win", "loss", "push"}]
    wins = sum(1 for e in graded if e.get("result") == "win")
    losses = sum(1 for e in graded if e.get("result") == "loss")
    pushes = sum(1 for e in graded if e.get("result") == "push")
    units_risked = float(len(graded))
    units_profit = round(sum(float(e.get("pnl", 0.0)) / 100.0 for e in graded), 4)
    tracked = len(graded)
    hit_rate = (wins / (wins + losses)) if (wins + losses) else 0.0
    roi = (units_profit / units_risked) if units_risked else 0.0
    return {
        "tracked_bets": tracked,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "units_risked": units_risked,
        "units_profit": units_profit,
        "roi": roi,
        "hit_rate": hit_rate,
    }


def _split_stats(entries: list[dict], prefix: str) -> dict:
    ml = _stats_for([e for e in entries if e.get("market_type") == "moneyline"])
    totals = _stats_for([e for e in entries if e.get("market_type") == "game_total"])
    return {
        f"{prefix}_ml_wins": ml["wins"],
        f"{prefix}_ml_losses": ml["losses"],
        f"{prefix}_ml_pushes": ml["pushes"],
        f"{prefix}_ml_hit_rate": ml["hit_rate"],
        f"{prefix}_ml_roi": ml["roi"],
        f"{prefix}_ml_units_profit": ml["units_profit"],
        f"{prefix}_total_wins": totals["wins"],
        f"{prefix}_total_losses": totals["losses"],
        f"{prefix}_total_pushes": totals["pushes"],
        f"{prefix}_total_hit_rate": totals["hit_rate"],
        f"{prefix}_total_roi": totals["roi"],
        f"{prefix}_total_units_profit": totals["units_profit"],
    }


def _history_summary(entries: list[dict]) -> dict:
    # is_lean=None means the entry pre-dates the field — treat as a pick
    picks = [e for e in entries if not e.get("is_lean")]
    leans = [e for e in entries if e.get("is_lean")]
    pick_stats = _stats_for(picks)
    lean_stats = _stats_for(leans)
    return {
        **pick_stats,
        "graded_bets": pick_stats["tracked_bets"],
        **_split_stats(picks, "pick"),
        "lean_tracked_bets": lean_stats["tracked_bets"],
        "lean_wins": lean_stats["wins"],
        "lean_losses": lean_stats["losses"],
        "lean_pushes": lean_stats["pushes"],
        "lean_units_risked": lean_stats["units_risked"],
        "lean_units_profit": lean_stats["units_profit"],
        "lean_roi": lean_stats["roi"],
        "lean_hit_rate": lean_stats["hit_rate"],
        **_split_stats(leans, "lean"),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Export a static site snapshot for GitHub Pages.")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()

    slate_date = date.fromisoformat(args.date)
    service = SiteService()
    payload = await service.site_payload(slate_date)

    docs_data = Path(__file__).resolve().parents[2] / "docs" / "data"
    docs_data.mkdir(parents=True, exist_ok=True)
    dated_path = docs_data / f"{slate_date.isoformat()}.json"
    latest_path = docs_data / "latest.json"

    # Preserve picks/leans already locked from an earlier run today.
    # New picks from this run are merged in (deduplicated by market+pick+matchup).
    # This means the 11am picks survive the 3pm re-run, and the 3pm picks
    # survive the 6pm re-run — picks only accumulate, never disappear.
    if dated_path.exists():
        try:
            existing = json.loads(dated_path.read_text(encoding="utf-8"))
            if existing.get("date") == slate_date.isoformat():
                existing_picks = existing.get("daily", {}).get("picks", [])
                existing_leans = existing.get("daily", {}).get("leans", [])
                new_picks = payload["daily"].get("picks", [])
                new_leans = payload["daily"].get("leans", [])

                def _merge(existing_list, new_list):
                    seen = {(p["market_type"], p["pick"], p.get("matchup", "")) for p in existing_list}
                    merged = list(existing_list)
                    for p in new_list:
                        key = (p["market_type"], p["pick"], p.get("matchup", ""))
                        if key not in seen:
                            merged.append(p)
                            seen.add(key)
                    return merged

                payload["daily"]["picks"] = _merge(existing_picks, new_picks)
                payload["daily"]["leans"] = _merge(existing_leans, new_leans)
        except Exception:
            pass

    # Merge file-based graded pick history (overrides SQLite history which lacks results)
    graded_history = _load_pick_history()
    if graded_history:
        payload["history"] = list(reversed(graded_history))
        payload["summary"].update(_history_summary(graded_history))

    # Meta-model: retrain (or arm the fallback) from the graded history and
    # surface the status in the payload so the dashboard can display it.
    meta = MetaModel()
    meta_status = meta.train_from_history(graded_history)
    payload["meta_model"] = meta_status

    # Performance metrics for the dashboard's Model Performance section.
    write_metrics(build_metrics(graded_history, meta_status))

    dated_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Rebuild archive index from all dated files so the UI can list available days
    index_entries = []
    for p in sorted(docs_data.glob("20??-??-??.json"), reverse=True):
        try:
            day_data = json.loads(p.read_text(encoding="utf-8"))
            day_picks = day_data.get("daily", {}).get("picks", [])
            day_leans = day_data.get("daily", {}).get("leans", [])
            index_entries.append({
                "date": p.stem,
                "picks": len(day_picks),
                "leans": len(day_leans),
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
