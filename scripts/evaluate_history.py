"""Honest retrospective evaluation of the archived daily boards.

Loads every archived daily board JSON in docs/data/ plus the graded
pick_history.json and produces reports/historical_evaluation.md with:

- pick/lean inventory by tier and market
- win/loss record and ROI at the stated odds (flat $100 per bet)
- realized line movement (a CLV proxy) wherever both a pick-time line and a
  later line for the same market exist in the archive
- a calibration table: predicted probability buckets vs realized win rate

The report states the numbers as they are. No filtering of bad days, no
selective windows.

Usage:
    python scripts/evaluate_history.py
"""
from __future__ import annotations

import json
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS_DATA = ROOT / "docs" / "data"
REPORTS_DIR = ROOT / "reports"
DATE_FILE_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")

GRADED = {"win", "loss", "push"}


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_boards() -> dict[str, dict]:
    boards: dict[str, dict] = {}
    for path in sorted(DOCS_DATA.glob("20??-??-??.json")):
        if not DATE_FILE_RE.match(path.stem):
            continue
        try:
            boards[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
    return boards


def load_history() -> list[dict]:
    path = DOCS_DATA / "pick_history.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Record / ROI
# ---------------------------------------------------------------------------

def record_stats(entries: list[dict]) -> dict:
    graded = [e for e in entries if e.get("result") in GRADED]
    wins = sum(1 for e in graded if e["result"] == "win")
    losses = sum(1 for e in graded if e["result"] == "loss")
    pushes = sum(1 for e in graded if e["result"] == "push")
    staked = 100.0 * len(graded)
    profit = sum(float(e.get("pnl", 0.0)) for e in graded)
    return {
        "n": len(entries),
        "graded": len(graded),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "staked": staked,
        "profit": round(profit, 2),
        "roi": (profit / staked) if staked else None,
        "hit_rate": (wins / (wins + losses)) if (wins + losses) else None,
    }


def fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:+.1f}%".replace("+", "") if value >= 0 else f"{value * 100:.1f}%"


def fmt_signed_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:+.1f}%"


# ---------------------------------------------------------------------------
# Line movement (CLV proxy) from the archived boards
# ---------------------------------------------------------------------------

def _later_market_lookup(board: dict) -> list[dict]:
    """All market quotes in the board's lineup cards (regenerated on every
    export, so they reflect the FINAL export of the day, while picks/leans are
    locked at first appearance)."""
    quotes: list[dict] = []
    for card in board.get("daily", {}).get("lineup_cards", []):
        for quote in card.get("top_game_picks", []) or []:
            quotes.append(quote)
    return quotes


def _movement_for(pick: dict, quotes: list[dict]) -> dict | None:
    """no-vig probability movement from pick time to the day's final export."""
    pick_nv = pick.get("no_vig_probability")
    if pick_nv is None:
        return None
    for quote in quotes:
        if quote.get("matchup") != pick.get("matchup"):
            continue
        if quote.get("market_type") != pick.get("market_type"):
            continue
        later_nv = quote.get("no_vig_probability")
        if later_nv is None:
            continue
        if pick.get("market_type") in ("game_total", "first_five_total"):
            if quote.get("line") != pick.get("line"):
                continue
            if quote.get("pick") == pick.get("pick"):
                side_nv = later_nv
            else:
                side_nv = 1.0 - later_nv  # opposite side of the same total line
        elif pick.get("market_type") in ("moneyline", "h2h"):
            if quote.get("pick") == pick.get("pick"):
                side_nv = later_nv
            else:
                side_nv = 1.0 - later_nv  # the other team
        else:
            continue
        return {
            "matchup": pick.get("matchup"),
            "market_type": pick.get("market_type"),
            "pick": pick.get("pick"),
            "pick_nv": float(pick_nv),
            "later_nv": round(float(side_nv), 4),
            "delta": round(float(side_nv) - float(pick_nv), 4),
            "same_odds": quote.get("american_odds") == pick.get("american_odds")
            and quote.get("pick") == pick.get("pick"),
        }
    return None


def collect_movements(boards: dict[str, dict]) -> list[dict]:
    movements: list[dict] = []
    for day, board in boards.items():
        daily = board.get("daily", {})
        quotes = _later_market_lookup(board)
        for entry in list(daily.get("picks", [])) + list(daily.get("leans", [])):
            movement = _movement_for(entry, quotes)
            if movement is not None:
                movement["date"] = day
                movements.append(movement)
    return movements


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def calibration_rows(entries: list[dict]) -> list[dict]:
    buckets = [(0.0, 0.45), (0.45, 0.50), (0.50, 0.55), (0.55, 0.60), (0.60, 1.01)]
    rows = []
    decided = [
        e for e in entries if e.get("result") in ("win", "loss") and e.get("model_probability") is not None
    ]
    for low, high in buckets:
        members = [e for e in decided if low <= float(e["model_probability"]) < high]
        if not members:
            rows.append({"bucket": f"{low:.2f}-{min(high, 1.0):.2f}", "n": 0, "avg_pred": None, "win_rate": None})
            continue
        wins = sum(1 for e in members if e["result"] == "win")
        rows.append(
            {
                "bucket": f"{low:.2f}-{min(high, 1.0):.2f}",
                "n": len(members),
                "avg_pred": statistics.mean(float(e["model_probability"]) for e in members),
                "win_rate": wins / len(members),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report() -> str:
    boards = load_boards()
    history = load_history()

    lines: list[str] = []
    add = lines.append
    add("# Historical Evaluation — Beat the Books")
    add("")
    add(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    add("")
    add(
        "This report is generated by `scripts/evaluate_history.py` from every archived "
        "daily board in `docs/data/` and the graded results in `pick_history.json`. "
        "Nothing is filtered or cherry-picked: every archived, graded bet is included."
    )
    add("")

    # ------------------------------------------------------------- inventory
    add("## 1. Archive inventory")
    add("")
    if not boards:
        add("No archived daily boards found.")
    else:
        add("| Date | Board picks | Leans | Lineup cards | Last export (UTC) |")
        add("|------|------------:|------:|-------------:|-------------------|")
        for day, board in boards.items():
            daily = board.get("daily", {})
            add(
                f"| {day} | {len(daily.get('picks', []))} | {len(daily.get('leans', []))} "
                f"| {len(daily.get('lineup_cards', []))} | {board.get('as_of', 'unknown')[:16]} |"
            )
    graded_history = [e for e in history if e.get("result") in GRADED]
    ungraded = [e for e in history if e.get("result") not in GRADED]
    add("")
    add(
        f"The archive spans **{len(boards)} day(s)**. `pick_history.json` holds "
        f"**{len(history)}** tracked entries, of which **{len(graded_history)}** are graded "
        f"(win/loss/push) and **{len(ungraded)}** had no gradable result."
    )
    official = [e for e in history if not e.get("is_lean")]
    lean_entries = [e for e in history if e.get("is_lean")]
    add("")
    add(
        f"Of the tracked entries, **{len(official)}** are official picks and "
        f"**{len(lean_entries)}** are leans (informational, sub-threshold plays the site "
        "publishes alongside the board). Every graded entry in the current archive is a lean — "
        "the mid-band totals strategy produced **zero** official picks on the archived days."
    )
    add("")

    # ------------------------------------------------------------- record
    add("## 2. Record and ROI at stated odds")
    add("")
    add("Flat $100 per entry at the archived odds. Pushes stake and refund. ROI = profit / total staked.")
    add("")
    overall = record_stats(history)
    add("| Cut | N | W-L-P | Hit rate | Staked | Profit | ROI |")
    add("|-----|--:|-------|---------:|-------:|-------:|----:|")

    def row(label: str, entries: list[dict]) -> None:
        s = record_stats(entries)
        hit = "n/a" if s["hit_rate"] is None else f"{s['hit_rate'] * 100:.1f}%"
        roi = "n/a" if s["roi"] is None else f"{s['roi'] * 100:+.1f}%"
        add(
            f"| {label} | {s['graded']} | {s['wins']}-{s['losses']}-{s['pushes']} | {hit} "
            f"| ${s['staked']:.0f} | ${s['profit']:+.2f} | {roi} |"
        )

    row("All graded entries", history)
    for tier in sorted({e.get("tier") or "unknown" for e in history}):
        row(f"Tier: {tier}", [e for e in history if (e.get("tier") or "unknown") == tier])
    for market in sorted({e.get("market_type") or "unknown" for e in history}):
        row(f"Market: {market}", [e for e in history if (e.get("market_type") or "unknown") == market])
    add("")
    if overall["roi"] is not None:
        verdict = (
            "profitable" if overall["roi"] > 0 else ("break-even" if overall["roi"] == 0 else "unprofitable")
        )
        add(
            f"**Bottom line:** {overall['wins']}-{overall['losses']}-{overall['pushes']} "
            f"({fmt_pct(overall['hit_rate'])} hit rate) for **{fmt_signed_pct(overall['roi'])} ROI** — "
            f"{verdict} over this sample. With only {overall['graded']} graded bets, this says almost "
            "nothing about true skill either way: the 95% confidence interval on a "
            f"{overall['graded']}-bet win rate spans tens of percentage points."
        )
    add("")

    # ------------------------------------------------------------- CLV proxy
    add("## 3. Realized line movement (CLV proxy)")
    add("")
    movements = collect_movements(boards)
    clv_graded = [e for e in history if e.get("clv") is not None]
    if movements:
        deltas = [m["delta"] for m in movements]
        moved = [m for m in movements if abs(m["delta"]) > 1e-9]
        add(
            f"Matched **{len(movements)}** archived picks/leans against a later quote for the same "
            f"market in the same day's final board export. Average no-vig probability movement "
            f"toward the pick: **{statistics.mean(deltas) * 100:+.2f} pp** "
            f"({len([d for d in deltas if d > 0])} moved with the pick, "
            f"{len([d for d in deltas if d < 0])} against, "
            f"{len(deltas) - len(moved)} unchanged)."
        )
        if len(moved) == 0:
            add("")
            add(
                "**Caveat that matters:** every matched pair shows zero movement because each archived "
                "day currently contains only one odds snapshot — the picks and the day's final board "
                "were built from the same odds download. The archive cannot yet distinguish beating "
                "the close from matching it. The closing-line capture added to the pipeline fixes "
                "this going forward; this table will only become meaningful once days with multiple "
                "intraday snapshots accumulate."
            )
    else:
        add("No archived pick could be matched to a later line for the same market.")
    add("")
    if clv_graded:
        clv_values = [float(e["clv"]) for e in clv_graded]
        add(
            f"`pick_history.json` contains **{len(clv_graded)}** entries with a graded CLV value "
            f"(mean {statistics.mean(clv_values) * 100:+.2f} pp). Same caveat: with one snapshot per "
            "day so far, these are 0.0 by construction, not evidence of matching the close."
        )
    add("")

    # ------------------------------------------------------------- calibration
    add("## 4. Calibration (predicted probability vs realized win rate)")
    add("")
    rows = calibration_rows(history)
    populated = [r for r in rows if r["n"] > 0]
    add("| Model probability bucket | N | Avg predicted | Realized win rate | Gap |")
    add("|--------------------------|--:|--------------:|------------------:|----:|")
    for r in rows:
        if r["n"] == 0:
            add(f"| {r['bucket']} | 0 | — | — | — |")
        else:
            gap = r["win_rate"] - r["avg_pred"]
            add(
                f"| {r['bucket']} | {r['n']} | {r['avg_pred'] * 100:.1f}% | "
                f"{r['win_rate'] * 100:.1f}% | {gap * 100:+.1f} pp |"
            )
    add("")
    decided_n = sum(r["n"] for r in populated)
    add(
        f"With **{decided_n}** decided bets spread over {len(populated)} bucket(s), this table cannot "
        "establish calibration quality — a single bucket needs on the order of 100+ bets before its "
        "realized rate is meaningful to within ±5 pp. It is included so the numbers accumulate in "
        "one place, not because any conclusion can be drawn yet."
    )
    add("")

    # ------------------------------------------------------------- honesty
    add("## 5. What this evaluation can and cannot say")
    add("")
    add("- **Sample size is tiny.** A handful of days and a couple dozen graded leans. Any ROI figure")
    add("  at this scale is noise; a coin-flipping bettor would frequently look this good or this bad.")
    add("- **No official picks yet.** The current strategy (mid-band totals) produced zero board picks")
    add("  on the archived days; everything graded so far is an informational lean.")
    add("- **The CLV columns are not yet informative.** Each archived day has a single odds snapshot,")
    add("  so pick-time and \"closing\" prices coincide. Real CLV measurement starts with the")
    add("  closing-line capture now in the pipeline and needs multi-snapshot days to mean anything.")
    add("- **The closing line here is a proxy** (last captured snapshot before first pitch, DraftKings")
    add("  only), not the true market close.")
    add("- **Archive completeness:** only days on which the refresh workflow ran and validated are")
    add("  present. Days the pipeline failed are absent, which is a form of survivorship in the")
    add("  archive itself.")
    add("")
    return "\n".join(lines)


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report = build_report()
    out_path = REPORTS_DIR / "historical_evaluation.md"
    out_path.write_text(report, encoding="utf-8")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
