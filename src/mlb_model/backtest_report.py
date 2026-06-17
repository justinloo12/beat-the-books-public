"""Analyse backtest/results.json: performance by tier, edge band, market, and a
calibration curve — plus a head-to-head of the new vs. legacy calibration.

Pure-Python (no network, no heavy deps), so it runs anywhere.

Usage:
  python -m mlb_model.backtest_report [--results path/to/results.json]
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

_DEFAULT_RESULTS = Path(__file__).resolve().parents[2] / "backtest" / "results.json"


def _american_to_decimal(odds: float) -> float:
    odds = float(odds)
    return 1.0 + (odds / 100.0 if odds > 0 else 100.0 / abs(odds))


def _summarize(rows: list[dict]) -> dict:
    graded = [r for r in rows if r.get("result") in {"win", "loss", "push"}]
    wins = sum(1 for r in graded if r["result"] == "win")
    losses = sum(1 for r in graded if r["result"] == "loss")
    pushes = sum(1 for r in graded if r["result"] == "push")
    decided = wins + losses
    units_profit = sum(float(r.get("pnl", 0.0)) / 100.0 for r in graded)
    staked = float(len([r for r in graded if r["result"] != "push"]))
    return {
        "n": len(graded),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "hit_rate": (wins / decided) if decided else 0.0,
        "units_profit": units_profit,
        "roi": (units_profit / staked) if staked else 0.0,
    }


def _print_table(title: str, groups: dict[str, list[dict]]) -> None:
    print(f"\n=== {title} ===")
    print(f"{'bucket':<22}{'n':>5}{'W-L':>10}{'hit%':>8}{'units':>9}{'ROI%':>8}")
    for key in sorted(groups):
        s = _summarize(groups[key])
        if s["n"] == 0:
            continue
        wl = f"{s['wins']}-{s['losses']}"
        print(f"{key:<22}{s['n']:>5}{wl:>10}{s['hit_rate']*100:>7.1f}%{s['units_profit']:>9.2f}{s['roi']*100:>7.1f}%")


def _calibration(rows: list[dict], prob_key: str) -> None:
    print(f"\n=== Calibration curve ({prob_key}) ===")
    print(f"{'predicted':<14}{'n':>5}{'actual win%':>13}")
    bands = defaultdict(list)
    for r in rows:
        if r.get("result") not in {"win", "loss"}:
            continue
        p = r.get(prob_key)
        if p is None:
            continue
        lo = int(float(p) * 10) * 10
        bands[lo].append(1 if r["result"] == "win" else 0)
    for lo in sorted(bands):
        outcomes = bands[lo]
        actual = sum(outcomes) / len(outcomes)
        print(f"{lo:>3}-{lo+10:<9}{len(outcomes):>5}{actual*100:>12.1f}%")


def _reclassify_legacy(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split rows by what the NEW vs LEGACY calibration would have called a pick
    (moderate/strong tier), to compare the two formulas on identical games."""
    new_picks = [r for r in rows if r.get("tier") in {"strong", "moderate"}]
    legacy_picks = [r for r in rows if r.get("legacy_tier") in {"strong", "moderate"}]
    return new_picks, legacy_picks


def main() -> None:
    parser = argparse.ArgumentParser(description="Report backtest performance.")
    parser.add_argument("--results", default=str(_DEFAULT_RESULTS))
    args = parser.parse_args()
    rows = json.loads(Path(args.results).read_text(encoding="utf-8"))

    overall = _summarize(rows)
    print("=== Overall ===")
    print(f"graded={overall['n']}  record={overall['wins']}-{overall['losses']}"
          f" (pushes {overall['pushes']})  hit%={overall['hit_rate']*100:.1f}"
          f"  units={overall['units_profit']:+.2f}  ROI={overall['roi']*100:+.1f}%")

    picks = [r for r in rows if not r.get("is_lean")]
    leans = [r for r in rows if r.get("is_lean")]
    _print_table("Picks vs Leans", {"picks": picks, "leans": leans})

    by_tier = defaultdict(list)
    for r in rows:
        by_tier[str(r.get("tier"))].append(r)
    _print_table("By tier", by_tier)

    by_market = defaultdict(list)
    for r in rows:
        by_market[str(r.get("market_type"))].append(r)
    _print_table("By market", by_market)

    by_edge = defaultdict(list)
    for r in rows:
        e = r.get("edge")
        if e is None:
            continue
        lo = int(float(e) * 100 // 2 * 2)  # 2% bands
        by_edge[f"{lo}-{lo+2}%"].append(r)
    _print_table("By edge band", by_edge)

    by_lineup = defaultdict(list)
    for r in rows:
        by_lineup[str(r.get("lineup_status"))].append(r)
    _print_table("By lineup status", by_lineup)

    _calibration(rows, "model_probability")

    new_picks, legacy_picks = _reclassify_legacy(rows)
    print("\n=== New vs Legacy calibration (rows each would tier as a PICK) ===")
    _print_table("formula", {"new (capped)": new_picks, "legacy (uncapped)": legacy_picks})


if __name__ == "__main__":
    main()
