"""Pick the betting edge thresholds on the VALIDATION split, then report how
those exact thresholds perform on the untouched TEST/holdout split.

This closes the loop the earlier backtest left open: the lean/pick edge cutoffs
were hand-set by feel. Hand-tuning a threshold on the same data you grade is how
a model fools itself. Here we sweep the edge cutoff on validation only, keep the
cutoff that maximises validation ROI (subject to a minimum bet count so we don't
chase a 3-2 fluke), and then apply that frozen cutoff to the holdout split. If
the holdout ROI holds up, the edge is real; if it collapses, the validation peak
was overfit and we learn that BEFORE risking money.

Pure-Python, no network. Run after backtest_grade has produced the split files.

Usage:
  python -m mlb_model.backtest_sweep \
      --val  backtest/results_val.json \
      --test backtest/results_test.json
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def _roi(rows: list[dict]) -> tuple[int, float, float]:
    """Return (n_decided, units_profit, roi) over graded, non-push rows."""
    graded = [r for r in rows if r.get("result") in {"win", "loss", "push"}]
    units = sum(float(r.get("pnl", 0.0)) / 100.0 for r in graded)
    staked = float(len([r for r in graded if r.get("result") != "push"]))
    roi = (units / staked) if staked else 0.0
    return int(staked), units, roi


def _select(rows: list[dict], cutoff: float) -> list[dict]:
    out = []
    for r in rows:
        e = r.get("edge")
        if e is None:
            continue
        if float(e) >= cutoff:
            out.append(r)
    return out


def _sweep(
    val_rows: list[dict],
    test_rows: list[dict],
    min_bets: int,
    cutoffs: list[float],
) -> dict:
    """Choose the cutoff that maximises validation ROI with >= min_bets bets,
    then evaluate it on the holdout test rows."""
    best = None
    for cutoff in cutoffs:
        n, units, roi = _roi(_select(val_rows, cutoff))
        if n < min_bets:
            continue
        if best is None or roi > best["val_roi"]:
            best = {"cutoff": cutoff, "val_n": n, "val_units": units, "val_roi": roi}
    if best is None:
        return {"cutoff": None}
    tn, tunits, troi = _roi(_select(test_rows, best["cutoff"]))
    best.update({"test_n": tn, "test_units": tunits, "test_roi": troi})
    return best


def _by_market(rows: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        groups[str(r.get("market_type"))].append(r)
    return groups


def _print_result(label: str, res: dict) -> None:
    if res.get("cutoff") is None:
        print(f"{label:<18} no cutoff cleared the minimum bet count")
        return
    print(
        f"{label:<18} cutoff>={res['cutoff']*100:>4.1f}%  "
        f"VAL {res['val_n']:>4} bets {res['val_roi']*100:>+6.1f}% ROI ({res['val_units']:>+6.2f}u)   "
        f"HOLDOUT {res['test_n']:>4} bets {res['test_roi']*100:>+6.1f}% ROI ({res['test_units']:>+6.2f}u)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Validation-tuned edge threshold sweep.")
    parser.add_argument("--val", required=True, help="validation split results.json")
    parser.add_argument("--test", required=True, help="test/holdout split results.json")
    parser.add_argument("--min-bets", type=int, default=30, help="min bets a cutoff must clear on validation")
    parser.add_argument("--max-cutoff", type=float, default=0.10, help="highest edge cutoff to scan")
    parser.add_argument("--step", type=float, default=0.005, help="cutoff step")
    args = parser.parse_args()

    val_rows = json.loads(Path(args.val).read_text(encoding="utf-8"))
    test_rows = json.loads(Path(args.test).read_text(encoding="utf-8"))

    steps = int(round(args.max_cutoff / args.step)) + 1
    cutoffs = [round(i * args.step, 4) for i in range(steps)]

    print("=== Edge-threshold sweep (tuned on VALIDATION, reported on HOLDOUT) ===")
    print(f"validation rows={len(val_rows)}  holdout rows={len(test_rows)}  "
          f"min_bets={args.min_bets}  cutoffs=0..{args.max_cutoff*100:.1f}% step {args.step*100:.1f}%\n")

    overall = _sweep(val_rows, test_rows, args.min_bets, cutoffs)
    _print_result("ALL MARKETS", overall)

    print()
    val_by = _by_market(val_rows)
    test_by = _by_market(test_rows)
    for market in sorted(val_by):
        res = _sweep(val_by[market], test_by.get(market, []), args.min_bets, cutoffs)
        _print_result(market, res)

    print(
        "\nRead: a cutoff whose HOLDOUT ROI stays positive and near its VALIDATION ROI is a"
        "\nreal, deployable edge. A cutoff that is strongly positive on validation but flips"
        "\nnegative on holdout was overfit — do not ship it."
    )


if __name__ == "__main__":
    main()
