"""Full historical backtest over every archived daily board.

Collects every pick/lean the model ever published (including the April-era
boards, where candidate bets lived inside lineup_cards[].top_game_picks and
were never copied into a picks/leans list), grades ungraded entries against
final scores from the free MLB Stats API, and produces reports/backtest.md
with record/ROI under historical flat staking AND current Kelly-capped
sizing, calibration with Wilson intervals, model-vs-market Brier scores,
line movement, drawdown/losing-streak analysis, and a zero-edge bootstrap
p-value.

Optionally backfills the graded April entries into docs/data/pick_history.json
(--update-history) so metrics.json / the live dashboard reflect the full
archive, and rebuilds docs/data/archive_index.json.

Usage:
    .venv/bin/python scripts/backtest.py                  # report only
    .venv/bin/python scripts/backtest.py --update-history # also backfill history + archive index

Results fetched from statsapi.mlb.com are cached in backtest/results_cache.json
so re-runs (and CI) are deterministic and offline.
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mlb_model.backtest_stats import (  # noqa: E402
    bootstrap_roi_ci,
    brier_score,
    longest_losing_streak,
    max_drawdown,
    profit_for,
    wilson_interval,
    zero_edge_pvalue,
)
from mlb_model.grade_picks import _fetch_results, _find_game, _grade, _teams_match  # noqa: E402
from mlb_model.services.odds_engine import american_to_decimal, classify_edge  # noqa: E402

DOCS_DATA = ROOT / "docs" / "data"
REPORTS_DIR = ROOT / "reports"
RESULTS_CACHE = ROOT / "backtest" / "results_cache.json"
DATE_RE = re.compile(r"^20\d{2}-\d{2}-\d{2}$")

GRADED = {"win", "loss", "push"}
DECIDED = {"win", "loss"}
BET_TIERS = {"monitor", "moderate", "strong"}

BOOTSTRAP_SEED = 20260703
BOOTSTRAP_SIMS = 10_000


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_boards() -> dict[str, dict]:
    boards: dict[str, dict] = {}
    for path in sorted(DOCS_DATA.glob("20??-??-??.json")):
        if not DATE_RE.match(path.stem):
            continue
        try:
            boards[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
    return boards


def load_pick_history() -> list[dict]:
    path = DOCS_DATA / "pick_history.json"
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Results (MLB Stats API, cached)
# ---------------------------------------------------------------------------

def load_results_cache() -> dict[str, list[dict]]:
    if RESULTS_CACHE.exists():
        try:
            return json.loads(RESULTS_CACHE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def results_for(day: str, cache: dict[str, list[dict]]) -> list[dict]:
    if day in cache:
        return cache[day]
    try:
        games = _fetch_results(date.fromisoformat(day))
    except Exception as exc:  # network failure — grade what we can
        print(f"WARNING: could not fetch results for {day}: {exc}")
        return []
    cache[day] = games
    RESULTS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_CACHE.write_text(json.dumps(cache, indent=1), encoding="utf-8")
    return games


# ---------------------------------------------------------------------------
# Pick harvesting
# ---------------------------------------------------------------------------

def harvest_picks(boards: dict[str, dict]) -> list[dict]:
    """Every pick/lean the model published, one entry per market side.

    Modern boards (June onward) publish daily.picks and daily.leans. April-era
    boards published neither; their candidate bets exist only as the
    positive-edge, non-pass sides inside lineup_cards[].top_game_picks. Those
    are reconstructed here and flagged reconstructed=True / is_lean=True
    (they were never published as official picks).
    """
    entries: list[dict] = []
    for day, board in boards.items():
        daily = board.get("daily", {})
        published = [(p, False) for p in daily.get("picks", []) or []]
        published += [(p, True) for p in daily.get("leans", []) or []]
        if published:
            for pk, is_lean in published:
                entries.append(_entry_from(pk, day, is_lean=is_lean, reconstructed=False))
            continue
        # April-era reconstruction
        seen: set[tuple] = set()
        for card in daily.get("lineup_cards", []) or []:
            for pk in card.get("top_game_picks", []) or []:
                tier = pk.get("tier")
                edge = pk.get("edge")
                if tier not in BET_TIERS or edge is None or float(edge) <= 0:
                    continue
                key = (pk.get("matchup"), pk.get("market_type"), pk.get("pick"), pk.get("line"))
                if key in seen:
                    continue
                seen.add(key)
                entries.append(_entry_from(pk, day, is_lean=True, reconstructed=True))
    entries.sort(key=lambda e: (e["date"], e["matchup"], e["market_type"], e["pick"]))
    return entries


def _entry_from(pk: dict, day: str, is_lean: bool, reconstructed: bool) -> dict:
    return {
        "date": day,
        "matchup": pk.get("matchup"),
        "market_type": pk.get("market_type"),
        "pick": pk.get("pick"),
        "line": pk.get("line"),
        "american_odds": pk.get("american_odds"),
        "model_probability": pk.get("model_probability"),
        "no_vig_probability": pk.get("no_vig_probability"),
        "edge": pk.get("edge"),
        "tier": pk.get("tier"),
        "bankroll_fraction": pk.get("bankroll_fraction"),
        "is_lean": is_lean,
        "reconstructed": reconstructed,
    }


def grade_entries(entries: list[dict], history: list[dict], cache: dict[str, list[dict]]) -> None:
    """Attach result/pnl/scores to every entry, in place.

    Grades already present in pick_history.json are reused verbatim;
    everything else is graded from final scores.
    """
    by_key = {
        (h.get("date"), h.get("matchup"), h.get("market_type"), h.get("pick")): h
        for h in history
    }
    for e in entries:
        key = (e["date"], e["matchup"], e["market_type"], e["pick"])
        prior = by_key.get(key)
        if prior is not None and prior.get("result") in GRADED:
            e["result"] = prior["result"]
            e["pnl"] = prior.get("pnl")
            for f in ("away_score", "home_score", "final_total", "clv", "closing_odds", "closing_line"):
                if prior.get(f) is not None:
                    e[f] = prior[f]
            continue
        games = results_for(e["date"], cache)
        game = _find_game(e["matchup"] or "", games)
        if game is None:
            e["result"], e["pnl"] = "no_result", 0.0
            continue
        result, pnl = _grade(e, game)
        e["result"], e["pnl"] = result, pnl
        e["away_score"], e["home_score"] = game["away_score"], game["home_score"]
        e["final_total"] = game["away_score"] + game["home_score"]


# ---------------------------------------------------------------------------
# Staking
# ---------------------------------------------------------------------------

def current_stake_units(e: dict) -> tuple[float, str]:
    """(stake in units, tier) under the CURRENT sizing rules: classify_edge
    with today's thresholds, unit sizes and Kelly caps. 1u = 1% of bankroll."""
    mp, nv, odds = e.get("model_probability"), e.get("no_vig_probability"), e.get("american_odds")
    if mp is None or nv is None or odds is None:
        return 0.0, "unknown"
    decision = classify_edge(float(mp), float(nv), int(odds), american_to_decimal(int(odds)))
    return decision.bankroll_fraction * 100.0, decision.tier.value


def scenario_stats(entries: list[dict], stake_fn) -> dict:
    """Record/ROI over graded entries for a staking function entry -> units."""
    graded = [e for e in entries if e.get("result") in GRADED]
    rows = []
    for e in graded:
        stake = stake_fn(e)
        if stake <= 0:
            continue
        dec = american_to_decimal(int(e["american_odds"]))
        rows.append((e, stake, profit_for(e["result"], dec, stake)))
    wins = sum(1 for e, _, _ in rows if e["result"] == "win")
    losses = sum(1 for e, _, _ in rows if e["result"] == "loss")
    pushes = sum(1 for e, _, _ in rows if e["result"] == "push")
    staked = sum(s for _, s, _ in rows)
    profit = sum(p for _, _, p in rows)
    return {
        "n": len(rows),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "hit_rate": wins / (wins + losses) if (wins + losses) else None,
        "staked": staked,
        "profit": profit,
        "roi": profit / staked if staked else None,
    }


# ---------------------------------------------------------------------------
# Market universe for Brier (one canonical side per market)
# ---------------------------------------------------------------------------

def market_universe(boards: dict[str, dict], cache: dict[str, list[dict]]) -> list[dict]:
    """Every market quoted in the archived lineup cards, one CANONICAL side
    each (home team for moneyline, Over for totals) so complementary sides
    are not double counted, graded against the final score."""
    rows: list[dict] = []
    for day, board in boards.items():
        games = results_for(day, cache)
        if not games:
            continue
        seen: set[tuple] = set()
        for card in board.get("daily", {}).get("lineup_cards", []) or []:
            matchup = card.get("matchup", "")
            if " @ " not in matchup:
                continue
            home_team = matchup.split(" @ ", 1)[1].strip()
            for pk in card.get("top_game_picks", []) or []:
                mp, nv = pk.get("model_probability"), pk.get("no_vig_probability")
                if mp is None or nv is None:
                    continue
                market = pk.get("market_type")
                side = pk.get("pick", "")
                if market in ("moneyline", "h2h"):
                    if not _teams_match(side, home_team):
                        continue  # canonical side: home
                    key = (day, matchup, "moneyline")
                elif market == "game_total":
                    if side != "Over":
                        continue  # canonical side: Over
                    key = (day, matchup, "game_total", pk.get("line"))
                else:
                    continue
                if key in seen:
                    continue
                game = _find_game(matchup, games)
                if game is None:
                    continue
                result, _ = _grade(pk, game)
                if result not in DECIDED:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "date": day,
                        "market_type": "moneyline" if market in ("moneyline", "h2h") else market,
                        "model_probability": float(mp),
                        "no_vig_probability": float(nv),
                        "outcome": 1 if result == "win" else 0,
                    }
                )
    return rows


# ---------------------------------------------------------------------------
# Line movement (multi-snapshot days only)
# ---------------------------------------------------------------------------

def line_movements(boards: dict[str, dict], entries: list[dict]) -> list[dict]:
    """no-vig movement from pick time to the day's final board export, for
    picks whose pick-time quote differs from the final export (i.e. days with
    more than one odds snapshot — June onward)."""
    moves: list[dict] = []
    by_day: dict[str, list[dict]] = {}
    for e in entries:
        if not e.get("reconstructed"):
            by_day.setdefault(e["date"], []).append(e)
    for day, day_entries in by_day.items():
        board = boards.get(day, {})
        quotes = [
            q
            for card in board.get("daily", {}).get("lineup_cards", []) or []
            for q in card.get("top_game_picks", []) or []
        ]
        for e in day_entries:
            nv0 = e.get("no_vig_probability")
            if nv0 is None:
                continue
            for q in quotes:
                if q.get("matchup") != e.get("matchup") or q.get("market_type") != e.get("market_type"):
                    continue
                nv1 = q.get("no_vig_probability")
                if nv1 is None:
                    continue
                if e["market_type"] in ("game_total", "first_five_total"):
                    if q.get("line") != e.get("line"):
                        continue
                    side_nv = nv1 if q.get("pick") == e.get("pick") else 1.0 - nv1
                elif e["market_type"] in ("moneyline", "h2h"):
                    side_nv = nv1 if q.get("pick") == e.get("pick") else 1.0 - nv1
                else:
                    continue
                moves.append(
                    {
                        "date": day,
                        "matchup": e["matchup"],
                        "market_type": e["market_type"],
                        "pick": e["pick"],
                        "delta": round(float(side_nv) - float(nv0), 4),
                    }
                )
                break
    return moves


# ---------------------------------------------------------------------------
# History backfill
# ---------------------------------------------------------------------------

def backfill_history(entries: list[dict], history: list[dict]) -> tuple[list[dict], int]:
    """Append graded reconstructed entries missing from pick_history.json."""
    existing = {(h.get("date"), h.get("matchup"), h.get("market_type"), h.get("pick")) for h in history}
    added = 0
    merged = list(history)
    for e in entries:
        key = (e["date"], e["matchup"], e["market_type"], e["pick"])
        if key in existing or e.get("result") not in GRADED:
            continue
        record = {
            "date": e["date"],
            "matchup": e["matchup"],
            "market_type": e["market_type"],
            "pick": e["pick"],
            "line": e.get("line"),
            "american_odds": e.get("american_odds"),
            "edge": e.get("edge"),
            "tier": e.get("tier"),
            "model_probability": e.get("model_probability"),
            "no_vig_probability": e.get("no_vig_probability"),
            "closing_odds": None,
            "closing_line": None,
            "clv": None,
            "legacy_edge": None,
            "legacy_tier": None,
            "legacy_model_probability": None,
            "module_signals": None,
            "is_lean": bool(e.get("is_lean", True)),
            "reconstructed": bool(e.get("reconstructed", False)),
            "source": "backtest_backfill",
            "result": e["result"],
            "pnl": e.get("pnl", 0.0),
        }
        for f in ("away_score", "home_score", "final_total"):
            if e.get(f) is not None:
                record[f] = e[f]
        merged.append(record)
        existing.add(key)
        added += 1
    merged.sort(key=lambda h: (h.get("date") or "", h.get("matchup") or "", h.get("market_type") or "", h.get("pick") or ""))
    return merged, added


def rebuild_archive_index(boards: dict[str, dict]) -> None:
    index_entries = []
    for day in sorted(boards, reverse=True):
        daily = boards[day].get("daily", {})
        picks = daily.get("picks", []) or []
        leans = daily.get("leans", []) or []
        index_entries.append(
            {
                "date": day,
                "picks": len(picks),
                "leans": len(leans),
                "strong": sum(1 for p in picks if p.get("tier") == "strong"),
                "games": len(daily.get("lineup_cards", []) or []),
            }
        )
    (DOCS_DATA / "archive_index.json").write_text(
        json.dumps({"dates": index_entries}, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def _fmt_roi(v: float | None) -> str:
    return "n/a" if v is None else f"{v * 100:+.1f}%"


def _fmt_units(v: float) -> str:
    return f"{v:+.2f}u"


def _scenario_row(label: str, s: dict) -> str:
    hit = "n/a" if s["hit_rate"] is None else f"{s['hit_rate'] * 100:.1f}%"
    return (
        f"| {label} | {s['n']} | {s['wins']}-{s['losses']}-{s['pushes']} | {hit} "
        f"| {s['staked']:.2f}u | {_fmt_units(s['profit'])} | {_fmt_roi(s['roi'])} |"
    )


def build_report(
    boards: dict[str, dict],
    entries: list[dict],
    universe: list[dict],
    moves: list[dict],
) -> str:
    graded = [e for e in entries if e.get("result") in GRADED]
    decided = [e for e in graded if e["result"] in DECIDED]

    flat = lambda e: 1.0  # noqa: E731 — historical flat staking: 1u per bet
    current = lambda e: current_stake_units(e)[0]  # noqa: E731

    overall_flat = scenario_stats(entries, flat)
    overall_current = scenario_stats(entries, current)

    # Zero-edge bootstrap on flat stakes over decided bets with a no-vig prob
    boot_bets = [
        (float(e["no_vig_probability"]), american_to_decimal(int(e["american_odds"])), 1.0)
        for e in decided
        if e.get("no_vig_probability") is not None and e.get("american_odds") is not None
    ]
    boot_observed = sum(
        profit_for(e["result"], american_to_decimal(int(e["american_odds"])), 1.0)
        for e in decided
        if e.get("no_vig_probability") is not None and e.get("american_odds") is not None
    )
    boot = zero_edge_pvalue(boot_bets, boot_observed, BOOTSTRAP_SIMS, BOOTSTRAP_SEED)

    flat_profits = [
        profit_for(e["result"], american_to_decimal(int(e["american_odds"])), 1.0) for e in graded
    ]
    roi_ci = bootstrap_roi_ci(flat_profits, [1.0] * len(graded), BOOTSTRAP_SIMS, BOOTSTRAP_SEED)

    # Brier: model vs market over the full graded market universe
    model_brier = market_brier = None
    if universe:
        model_brier = brier_score(
            [r["model_probability"] for r in universe], [r["outcome"] for r in universe]
        )
        market_brier = brier_score(
            [r["no_vig_probability"] for r in universe], [r["outcome"] for r in universe]
        )
    # Brier restricted to actual picks
    pick_rows = [e for e in decided if e.get("model_probability") is not None and e.get("no_vig_probability") is not None]
    pick_model_brier = pick_market_brier = None
    if pick_rows:
        outcomes = [1 if e["result"] == "win" else 0 for e in pick_rows]
        pick_model_brier = brier_score([float(e["model_probability"]) for e in pick_rows], outcomes)
        pick_market_brier = brier_score([float(e["no_vig_probability"]) for e in pick_rows], outcomes)

    # Drawdown / streaks on flat staking, chronological
    chron = sorted(graded, key=lambda e: (e["date"], e["matchup"], e["market_type"], e["pick"]))
    chron_profits = [
        profit_for(e["result"], american_to_decimal(int(e["american_odds"])), 1.0) for e in chron
    ]
    dd = max_drawdown(chron_profits)
    streak = longest_losing_streak([e["result"] for e in chron])

    lines: list[str] = []
    add = lines.append
    add("# Backtest — Beat the Books (full archive)")
    add("")
    add(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} by `scripts/backtest.py`.")
    add("")
    add(
        "Supersedes the day-by-day view in [historical_evaluation.md](historical_evaluation.md) "
        "by extending it over the FULL recovered archive (April-era boards restored from a local "
        "checkout). Every archived pick/lean is included; nothing is filtered."
    )
    add("")

    # ------------------------------------------------------------- verdict
    add("## Verdict")
    add("")
    p = boot["p_value"]
    n_dec = len(boot_bets)
    roi = overall_flat["roi"]
    if p is None or roi is None:
        verdict = "No graded bets — no verdict possible."
    else:
        brier_clause = ""
        if model_brier is not None and market_brier is not None:
            if model_brier >= market_brier:
                brier_clause = (
                    f" Moreover, across {len(universe)} archived markets the model's Brier score "
                    f"({model_brier:.4f}) is {'worse than' if model_brier > market_brier else 'no better than'} "
                    f"the no-vig market's ({market_brier:.4f}): the model does NOT out-predict the market "
                    "it bets into, so its picks are expected losers after vig regardless of the observed record."
                )
            else:
                brier_clause = (
                    f" Across {len(universe)} archived markets the model's Brier score ({model_brier:.4f}) "
                    f"beats the no-vig market's ({market_brier:.4f}) — necessary (not sufficient) for a real edge."
                )
        if p < 0.05 and roi > 0 and model_brier is not None and market_brier is not None and model_brier < market_brier:
            call = "**profitable edge** on this sample"
        elif roi is not None and roi < 0 and model_brier is not None and market_brier is not None and model_brier > market_brier:
            call = "**evidence of negative edge**"
        else:
            call = "**no evidence of edge**"
        verdict = (
            f"**Bootstrap p-value: {p:.3f}** — under a zero-edge process (each pick winning exactly at "
            f"its no-vig market probability, still paying vig), {p * 100:.1f}% of {boot['n_sims']:,} simulated "
            f"histories of these same {n_dec} bets end with at least the observed profit "
            f"({boot_observed:+.2f}u flat-staked). The observed record is "
            f"{overall_flat['wins']}-{overall_flat['losses']}-{overall_flat['pushes']} "
            f"({_fmt_roi(roi)} ROI).{brier_clause} Verdict: {call}. "
            f"With only {len(graded)} graded bets, no result at this scale can establish skill — "
            "the sample is far below the several hundred bets needed to separate edge from variance."
        )
    add(verdict)
    add("")

    # ------------------------------------------------------------- inventory
    add("## 1. Archive inventory")
    add("")
    add("| Date | Published picks | Published leans | Reconstructed bets | Lineup cards |")
    add("|------|---------------:|----------------:|-------------------:|-------------:|")
    for day in sorted(boards):
        daily = boards[day].get("daily", {})
        recon = sum(1 for e in entries if e["date"] == day and e.get("reconstructed"))
        add(
            f"| {day} | {len(daily.get('picks', []) or [])} | {len(daily.get('leans', []) or [])} "
            f"| {recon} | {len(daily.get('lineup_cards', []) or [])} |"
        )
    recon_total = sum(1 for e in entries if e.get("reconstructed"))
    add("")
    add(
        f"**{len(boards)} archived days, {len(entries)} bet entries** ({len(entries) - recon_total} "
        f"published picks/leans + {recon_total} reconstructed from April-era lineup cards, where the "
        "board format kept candidate bets only inside `top_game_picks` and published no picks/leans "
        "lists). Reconstructed entries use the era's own tiers (monitor/moderate/strong at the "
        f"archived odds). {len(graded)} graded, "
        f"{sum(1 for e in entries if e.get('result') not in GRADED)} without a gradable result."
    )
    add("")

    # ------------------------------------------------------------- record
    add("## 2. Record and ROI")
    add("")
    add("### (a) Historical flat staking — 1u per bet at archived odds")
    add("")
    add("| Cut | N | W-L-P | Hit rate | Staked | Profit | ROI |")
    add("|-----|--:|-------|---------:|-------:|-------:|----:|")
    add(_scenario_row("All graded", overall_flat))
    for tier in sorted({e.get("tier") or "unknown" for e in graded}):
        sub = [e for e in entries if (e.get("tier") or "unknown") == tier]
        add(_scenario_row(f"Tier: {tier}", scenario_stats(sub, flat)))
    for market in sorted({e.get("market_type") or "unknown" for e in graded}):
        sub = [e for e in entries if (e.get("market_type") or "unknown") == market]
        add(_scenario_row(f"Market: {market}", scenario_stats(sub, flat)))
    for month in sorted({e["date"][:7] for e in graded}):
        sub = [e for e in entries if e["date"][:7] == month]
        add(_scenario_row(f"Month: {month}", scenario_stats(sub, flat)))
    if roi_ci:
        add("")
        add(
            f"Resampling the {len(graded)} graded bets 10,000 times gives a 95% CI on flat-stake ROI of "
            f"**[{roi_ci[0] * 100:+.1f}%, {roi_ci[1] * 100:+.1f}%]** — a range this wide is what a small "
            "sample looks like."
        )
    add("")
    add("### (b) Current Kelly-capped sizing applied retroactively")
    add("")
    add(
        "Every archived bet re-sized by today's rules (`classify_edge`: current edge thresholds, "
        "unit sizes, full-Kelly cap and tier caps; 1u = 1% of bankroll). Bets today's rules would "
        "not place (PASS/BLOCK, or Kelly ≤ 0 at the quoted price) drop to zero stake — the N column "
        "shows how many survive."
    )
    add("")
    add("| Cut | N | W-L-P | Hit rate | Staked | Profit | ROI |")
    add("|-----|--:|-------|---------:|-------:|-------:|----:|")
    add(_scenario_row("All graded", overall_current))
    for market in sorted({e.get("market_type") or "unknown" for e in graded}):
        sub = [e for e in entries if (e.get("market_type") or "unknown") == market]
        add(_scenario_row(f"Market: {market}", scenario_stats(sub, current)))
    for month in sorted({e["date"][:7] for e in graded}):
        sub = [e for e in entries if e["date"][:7] == month]
        add(_scenario_row(f"Month: {month}", scenario_stats(sub, current)))
    add("")

    # ------------------------------------------------------------- brier
    add("## 3. Model vs market — Brier scores (the number that matters)")
    add("")
    if universe and model_brier is not None:
        add(
            f"Across **{len(universe)}** archived markets (one canonical side each: home moneyline / "
            "Over total; both April-era and current boards), scored against final results:"
        )
        add("")
        add("| Forecaster | Brier score (lower = better) |")
        add("|------------|------------------------------:|")
        add(f"| Model probability | {model_brier:.4f} |")
        add(f"| No-vig market probability | {market_brier:.4f} |")
        add(f"| Coin flip (0.5 constant) | 0.2500 |")
        add("")
        diff = model_brier - market_brier
        if diff > 0:
            add(
                f"**The model is {diff:.4f} Brier points WORSE than the market.** A bettor whose "
                "probabilities are worse than the no-vig line loses in expectation on every bet after "
                "vig — any winning stretch is variance. This is the single most important number in "
                "this report."
            )
        else:
            add(
                f"**The model is {-diff:.4f} Brier points better than the market** on this sample. "
                "Necessary but not sufficient for profit: the margin must also exceed the vig, and "
                f"{len(universe)} markets is a small sample for a Brier comparison."
            )
    else:
        add("No gradable market universe available.")
    if pick_model_brier is not None:
        add("")
        add(
            f"Restricted to the {len(pick_rows)} decided bets actually placed: model Brier "
            f"{pick_model_brier:.4f} vs market {pick_market_brier:.4f}. (Selection-biased subset — "
            "these are exactly the markets where model and market disagree most.)"
        )
    add("")

    # ------------------------------------------------------------- calibration
    add("## 4. Calibration (predicted probability vs realized, Wilson 95% CIs)")
    add("")
    buckets = [(0.0, 0.45), (0.45, 0.50), (0.50, 0.55), (0.55, 0.60), (0.60, 1.01)]
    cal_rows = [e for e in decided if e.get("model_probability") is not None]
    add("| Model prob bucket | N | Avg predicted | Realized | Wilson 95% CI | Market says |")
    add("|-------------------|--:|--------------:|---------:|---------------|------------:|")
    for low, high in buckets:
        members = [e for e in cal_rows if low <= float(e["model_probability"]) < high]
        label = f"{low:.2f}-{min(high, 1.0):.2f}"
        if not members:
            add(f"| {label} | 0 | — | — | — | — |")
            continue
        wins = sum(1 for e in members if e["result"] == "win")
        lo, hi = wilson_interval(wins, len(members))
        avg_pred = statistics.mean(float(e["model_probability"]) for e in members)
        nv_members = [float(e["no_vig_probability"]) for e in members if e.get("no_vig_probability") is not None]
        nv_avg = f"{statistics.mean(nv_members) * 100:.1f}%" if nv_members else "n/a"
        add(
            f"| {label} | {len(members)} | {avg_pred * 100:.1f}% | {wins / len(members) * 100:.1f}% "
            f"| [{lo * 100:.1f}%, {hi * 100:.1f}%] | {nv_avg} |"
        )
    add("")
    add(
        "Every populated bucket's Wilson interval is tens of points wide at these sample sizes; "
        "the table records the data, it cannot certify calibration."
    )
    add("")

    # ------------------------------------------------------------- movement
    add("## 5. Line movement (pick-time vs final board export)")
    add("")
    if moves:
        deltas = [m["delta"] for m in moves]
        moved = [d for d in deltas if abs(d) > 1e-9]
        add(
            f"**{len(moves)}** published picks/leans matched to a later quote for the same market in "
            f"the same day's final export. Mean no-vig movement toward the pick: "
            f"**{statistics.mean(deltas) * 100:+.2f} pp** "
            f"({len([d for d in deltas if d > 0])} with, {len([d for d in deltas if d < 0])} against, "
            f"{len(deltas) - len(moved)} unchanged)."
        )
        if not moved:
            add("")
            add(
                "All matched pairs show zero movement: each archived day so far contains a single odds "
                "snapshot, so pick-time and final-export prices coincide by construction. Multi-snapshot "
                "days (the intraday snapshot workflow, June onward) are needed before this measures anything."
            )
    else:
        add("No pick could be matched to a later same-day quote.")
    add("")

    # ------------------------------------------------------------- risk
    add("## 6. Drawdown and losing streaks (flat 1u staking)")
    add("")
    add(f"- Cumulative profit: **{_fmt_units(sum(chron_profits))}** over {len(chron)} graded bets")
    add(f"- Maximum drawdown: **{dd:.2f}u**")
    add(f"- Longest losing streak: **{streak}** consecutive losses")
    add(
        f"- Zero-edge simulation of the same bet sequence: mean final profit "
        f"{boot['null_mean_profit']:+.2f}u, 5th-95th percentile "
        f"[{boot['null_p5']:+.2f}u, {boot['null_p95']:+.2f}u]"
        if boot["p_value"] is not None
        else "- Zero-edge simulation unavailable (no bets with market probabilities)"
    )
    add("")

    # ------------------------------------------------------------- honesty
    add("## 7. Caveats — read before quoting any number above")
    add("")
    add("- **The April-era entries were reconstructed**, not published as picks: the era's board format")
    add("  kept candidate bets inside lineup cards only. They use the era's own tier labels and odds,")
    add("  but no bettor could have followed them from the public site at the time.")
    add("- **Gaps are survivorship in the archive itself**: 2026-04-30 and 2026-05-02 → 2026-06-13 have")
    add("  no boards (pipeline not running / not archived). Days the pipeline failed are missing, and")
    add("  there is no way to know how those days would have graded.")
    add("- **Odds are DraftKings-only single snapshots** for most days; ROI at 'archived odds' assumes")
    add("  fills at those prices.")
    add("- **The zero-edge bootstrap conditions on the same bets** — it answers 'is this record luck?',")
    add("  not 'will the process profit going forward?'.")
    add("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Full historical backtest over archived boards.")
    parser.add_argument(
        "--update-history",
        action="store_true",
        help="Backfill graded reconstructed entries into docs/data/pick_history.json and rebuild archive_index.json.",
    )
    args = parser.parse_args()

    boards = load_boards()
    history = load_pick_history()
    cache = load_results_cache()

    entries = harvest_picks(boards)
    grade_entries(entries, history, cache)
    universe = market_universe(boards, cache)
    moves = line_movements(boards, entries)

    report = build_report(boards, entries, universe, moves)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORTS_DIR / "backtest.md"
    out.write_text(report, encoding="utf-8")
    print(f"Wrote {out}")

    if args.update_history:
        merged, added = backfill_history(entries, history)
        if added:
            (DOCS_DATA / "pick_history.json").write_text(json.dumps(merged, indent=2), encoding="utf-8")
        rebuild_archive_index(boards)
        print(f"Backfilled {added} graded entries into pick_history.json (now {len(merged)}); archive index rebuilt.")

    graded = sum(1 for e in entries if e.get("result") in GRADED)
    print(f"{len(boards)} days, {len(entries)} bets, {graded} graded, {len(universe)} markets in Brier universe.")


if __name__ == "__main__":
    main()
