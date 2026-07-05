# Pre-Registered Experiment Protocol

**Frozen: 2026-07-04.** This document fixes, in advance, how the model is
evaluated for the remainder of the 2026 MLB season. It exists so that the
verdict — whatever it is — cannot be moved after the fact. Changes are
permitted only as dated amendments appended to the bottom; the original text
above the amendment line is never edited.

A machine-readable copy lives at [`data/protocol.json`](data/protocol.json).

## 1. What is being tested

Whether a fully automated MLB model can demonstrate forecasting skill
against the betting market, measured two ways:

1. **All-games probability race (primary).** Every game the model prices
   (~15/day) is logged with the model's home-win probability and the
   market's no-vig home probability, taken from the same odds download.
   Both are scored by Brier against the final result. The question:
   is the model's Brier lower than the market's?
2. **Closing line value on picks (secondary).** For the small subset of
   games that become official picks, CLV against the last pre-first-pitch
   odds snapshot.

## 2. Data and logging rules

- **Log:** `docs/data/game_predictions.csv`, append-only, committed by the
  existing refresh workflow. One row per (date, game).
- **Freezing:** the first prediction exported for a game each day is
  frozen. Later intraday re-exports never revise a logged row.
- **Market baseline:** the no-vig (vig-removed) DraftKings home probability
  from the day's single odds download. No additional odds API calls are
  made for this experiment.
- **Grading:** nightly, automated, from final scores on the free MLB Stats
  API. Games with no final score after 3 days are closed as `no_result`
  and excluded from all metrics. Grading is idempotent; a graded row is
  never regraded.
- **Backfill:** boards archived before 2026-07-04 were extracted and graded
  under the same rules and tagged `model_version = v1-raw-rates`. They were
  produced by the pre-registration model and are reported, not hidden.

## 3. Metrics

- **Brier score** (mean squared error of probability vs outcome, lower is
  better) for model and market, computed over the *paired* set: games where
  both a model and a market probability were logged and a final score exists.
- **Calibration table:** predicted home-win probability buckets vs realized
  home-win rate, with per-bucket counts.
- **30-day rolling Brier** for model and market over the same paired games.
- **CLV on picks:** no-vig probability at the closing proxy minus no-vig
  probability at pick time (as already defined on the dashboard).

## 4. Success criteria (stated before the data can speak)

- **Evidence of skill** requires *both*:
  - model Brier below market Brier over **at least 1,000 graded games**, and
  - mean CLV above zero over **at least 300 graded picks**.
- **Evidence of no skill:** model Brier at or above market Brier over 1,000+
  graded games. That is the expected outcome for most models against a
  closing-line-efficient market, and it will be published just as loudly.
- Below **200 graded games**, the dashboard labels the all-games comparison
  as noise and draws no conclusion in either direction.

## 5. No-peeking commitments

- The robot publishes the numbers on every refresh, good or bad. There is
  no manual step between grading and publication.
- Metrics, thresholds, and success criteria above do not change mid-season.
  Any change requires a dated amendment below, and amendments cannot be
  retroactive (they apply only to data logged after the amendment date).
- Model changes are allowed (the model is the thing being developed), but
  every logged prediction carries the `model_version` that produced it, so
  the report card can always be segmented by version. Versions to date:
  - `v1-raw-rates` — raw Statcast aggregate event rates (boards through 2026-07-03).
  - `v2-eb-2026-07-04` — simulation event rates empirical-Bayes-shrunk with
    method-of-moments league priors, handedness-split park factors on the HR
    component — unified with the weight-room-hero-sim methodology, 2026-07-04.
- No cherry-picked windows: the headline numbers are cumulative from the
  first logged game; the only windowed view is the pre-declared 30-day
  rolling Brier.

## 6. Current verdict at freeze time

As of 2026-07-04, over the 98 backfilled games with market odds:
model Brier 0.2442 vs market Brier 0.2402 — the market is ahead, and the
sample is far below the 200-game noise floor. **No evidence of edge.**
The experiment exists to settle that with ~1,300 more games by October.

---

*Amendments (dated, append-only): none.*
