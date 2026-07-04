# Backtest — Beat the Books (full archive)

Generated: 2026-07-04 02:10 UTC by `scripts/backtest.py`.

Supersedes the day-by-day view in [historical_evaluation.md](historical_evaluation.md) by extending it over the FULL recovered archive (April-era boards restored from a local checkout). Every archived pick/lean is included; nothing is filtered.

## Verdict

**Bootstrap p-value: 0.289** — under a zero-edge process (each pick winning exactly at its no-vig market probability, still paying vig), 28.9% of 10,000 simulated histories of these same 48 bets end with at least the observed profit (+2.00u flat-staked). The observed record is 24-24-2 (+4.0% ROI). Moreover, across 166 archived markets the model's Brier score (0.2554) is worse than the no-vig market's (0.2508): the model does NOT out-predict the market it bets into, so its picks are expected losers after vig regardless of the observed record. Verdict: **no evidence of edge**. With only 50 graded bets, no result at this scale can establish skill — the sample is far below the several hundred bets needed to separate edge from variance.

## 1. Archive inventory

| Date | Published picks | Published leans | Reconstructed bets | Lineup cards |
|------|---------------:|----------------:|-------------------:|-------------:|
| 2026-04-25 | 0 | 0 | 12 | 15 |
| 2026-04-26 | 0 | 0 | 11 | 15 |
| 2026-04-27 | 0 | 0 | 1 | 8 |
| 2026-04-28 | 0 | 0 | 6 | 11 |
| 2026-04-29 | 0 | 0 | 0 | 15 |
| 2026-05-01 | 0 | 0 | 0 | 0 |
| 2026-06-14 | 0 | 8 | 0 | 14 |
| 2026-06-15 | 0 | 2 | 0 | 8 |
| 2026-06-16 | 0 | 4 | 0 | 15 |
| 2026-06-17 | 0 | 8 | 0 | 14 |

**10 archived days, 52 bet entries** (22 published picks/leans + 30 reconstructed from April-era lineup cards, where the board format kept candidate bets only inside `top_game_picks` and published no picks/leans lists). Reconstructed entries use the era's own tiers (monitor/moderate/strong at the archived odds). 50 graded, 2 without a gradable result.

## 2. Record and ROI

### (a) Historical flat staking — 1u per bet at archived odds

| Cut | N | W-L-P | Hit rate | Staked | Profit | ROI |
|-----|--:|-------|---------:|-------:|-------:|----:|
| All graded | 50 | 24-24-2 | 50.0% | 50.00u | +2.00u | +4.0% |
| Tier: moderate | 8 | 3-5-0 | 37.5% | 8.00u | -0.71u | -8.9% |
| Tier: monitor | 27 | 12-13-2 | 48.0% | 27.00u | -0.73u | -2.7% |
| Tier: pass | 12 | 8-4-0 | 66.7% | 12.00u | +4.56u | +38.0% |
| Tier: strong | 3 | 1-2-0 | 33.3% | 3.00u | -1.12u | -37.4% |
| Market: game_total | 16 | 6-8-2 | 42.9% | 16.00u | -2.65u | -16.6% |
| Market: moneyline | 34 | 18-16-0 | 52.9% | 34.00u | +4.65u | +13.7% |
| Month: 2026-04 | 28 | 12-14-2 | 46.2% | 28.00u | -0.58u | -2.1% |
| Month: 2026-06 | 22 | 12-10-0 | 54.5% | 22.00u | +2.57u | +11.7% |

Resampling the 50 graded bets 10,000 times gives a 95% CI on flat-stake ROI of **[-24.5%, +32.9%]** — a range this wide is what a small sample looks like.

### (b) Current Kelly-capped sizing applied retroactively

Every archived bet re-sized by today's rules (`classify_edge`: current edge thresholds, unit sizes, full-Kelly cap and tier caps; 1u = 1% of bankroll). Bets today's rules would not place (PASS/BLOCK, or Kelly ≤ 0 at the quoted price) drop to zero stake — the N column shows how many survive.

| Cut | N | W-L-P | Hit rate | Staked | Profit | ROI |
|-----|--:|-------|---------:|-------:|-------:|----:|
| All graded | 25 | 12-13-0 | 48.0% | 52.00u | +1.31u | +2.5% |
| Market: game_total | 6 | 2-4-0 | 33.3% | 14.00u | -4.55u | -32.5% |
| Market: moneyline | 19 | 10-9-0 | 52.6% | 38.00u | +5.86u | +15.4% |
| Month: 2026-04 | 11 | 4-7-0 | 36.4% | 24.00u | -3.79u | -15.8% |
| Month: 2026-06 | 14 | 8-6-0 | 57.1% | 28.00u | +5.10u | +18.2% |

## 3. Model vs market — Brier scores (the number that matters)

Across **166** archived markets (one canonical side each: home moneyline / Over total; both April-era and current boards), scored against final results:

| Forecaster | Brier score (lower = better) |
|------------|------------------------------:|
| Model probability | 0.2554 |
| No-vig market probability | 0.2508 |
| Coin flip (0.5 constant) | 0.2500 |

**The model is 0.0046 Brier points WORSE than the market.** A bettor whose probabilities are worse than the no-vig line loses in expectation on every bet after vig — any winning stretch is variance. This is the single most important number in this report.

Restricted to the 48 decided bets actually placed: model Brier 0.2497 vs market 0.2481. (Selection-biased subset — these are exactly the markets where model and market disagree most.)

## 4. Calibration (predicted probability vs realized, Wilson 95% CIs)

| Model prob bucket | N | Avg predicted | Realized | Wilson 95% CI | Market says |
|-------------------|--:|--------------:|---------:|---------------|------------:|
| 0.00-0.45 | 6 | 40.0% | 33.3% | [9.7%, 70.0%] | 33.8% |
| 0.45-0.50 | 7 | 48.4% | 42.9% | [15.8%, 75.0%] | 43.0% |
| 0.50-0.55 | 19 | 53.2% | 57.9% | [36.3%, 76.9%] | 46.7% |
| 0.55-0.60 | 12 | 57.1% | 50.0% | [25.4%, 74.6%] | 50.6% |
| 0.60-1.00 | 4 | 63.5% | 50.0% | [15.0%, 85.0%] | 51.3% |

Every populated bucket's Wilson interval is tens of points wide at these sample sizes; the table records the data, it cannot certify calibration.

## 5. Line movement (pick-time vs final board export)

**22** published picks/leans matched to a later quote for the same market in the same day's final export. Mean no-vig movement toward the pick: **+0.00 pp** (0 with, 0 against, 22 unchanged).

All matched pairs show zero movement: each archived day so far contains a single odds snapshot, so pick-time and final-export prices coincide by construction. Multi-snapshot days (the intraday snapshot workflow, June onward) are needed before this measures anything.

## 6. Drawdown and losing streaks (flat 1u staking)

- Cumulative profit: **+2.00u** over 50 graded bets
- Maximum drawdown: **7.81u**
- Longest losing streak: **5** consecutive losses
- Zero-edge simulation of the same bet sequence: mean final profit -2.14u, 5th-95th percentile [-14.09u, +9.88u]

## 7. Caveats — read before quoting any number above

- **The April-era entries were reconstructed**, not published as picks: the era's board format
  kept candidate bets inside lineup cards only. They use the era's own tier labels and odds,
  but no bettor could have followed them from the public site at the time.
- **Gaps are survivorship in the archive itself**: 2026-04-30 and 2026-05-02 → 2026-06-13 have
  no boards (pipeline not running / not archived). Days the pipeline failed are missing, and
  there is no way to know how those days would have graded.
- **Odds are DraftKings-only single snapshots** for most days; ROI at 'archived odds' assumes
  fills at those prices.
- **The zero-edge bootstrap conditions on the same bets** — it answers 'is this record luck?',
  not 'will the process profit going forward?'.
