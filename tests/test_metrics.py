"""Tests for the dashboard metrics builder."""
from __future__ import annotations

from mlb_model.metrics import SMALL_SAMPLE_THRESHOLD, build_metrics


def _entry(date="2026-06-15", result="win", pnl=100.0, tier="monitor",
           market="game_total", is_lean=True, clv=None, model_probability=0.55):
    return {
        "date": date,
        "matchup": "A @ B",
        "market_type": market,
        "pick": "Over",
        "result": result,
        "pnl": pnl,
        "tier": tier,
        "is_lean": is_lean,
        "clv": clv,
        "model_probability": model_probability,
    }


class TestRecordBlocks:
    def test_overall_record_and_roi(self):
        history = [
            _entry(result="win", pnl=100.0),
            _entry(result="loss", pnl=-100.0),
            _entry(result="win", pnl=91.0),
            _entry(result="push", pnl=0.0),
            _entry(result="no_result", pnl=0.0),  # excluded
        ]
        metrics = build_metrics(history)
        overall = metrics["overall"]
        assert overall["n"] == 4
        assert (overall["wins"], overall["losses"], overall["pushes"]) == (2, 1, 1)
        assert overall["hit_rate"] == round(2 / 3, 4)
        assert overall["profit_units"] == 0.91
        assert overall["roi"] == round(0.91 / 4, 4)
        assert overall["reliable"] is False

    def test_picks_and_leans_split(self):
        history = [
            _entry(is_lean=False, result="win", pnl=95.0),
            _entry(is_lean=True, result="loss", pnl=-100.0),
        ]
        metrics = build_metrics(history)
        assert metrics["official_picks"]["n"] == 1
        assert metrics["leans"]["n"] == 1
        assert metrics["official_picks"]["wins"] == 1

    def test_by_tier_and_market_have_sample_sizes(self):
        history = [
            _entry(tier="monitor", market="moneyline"),
            _entry(tier="pass", market="game_total"),
            _entry(tier="pass", market="game_total", result="loss", pnl=-100.0),
        ]
        metrics = build_metrics(history)
        assert metrics["by_tier"]["monitor"]["n"] == 1
        assert metrics["by_tier"]["pass"]["n"] == 2
        assert metrics["by_market"]["moneyline"]["n"] == 1
        assert metrics["by_market"]["game_total"]["n"] == 2

    def test_reliable_flag_flips_at_threshold(self):
        history = [_entry() for _ in range(SMALL_SAMPLE_THRESHOLD)]
        metrics = build_metrics(history)
        assert metrics["overall"]["reliable"] is True


class TestDailySeries:
    def test_cumulative_units(self):
        history = [
            _entry(date="2026-06-14", result="loss", pnl=-100.0),
            _entry(date="2026-06-15", result="win", pnl=120.0),
            _entry(date="2026-06-15", result="win", pnl=80.0),
        ]
        daily = build_metrics(history)["daily"]
        assert [d["date"] for d in daily] == ["2026-06-14", "2026-06-15"]
        assert daily[0]["cum_units"] == -1.0
        assert daily[1]["profit_units"] == 2.0
        assert daily[1]["cum_units"] == 1.0
        assert daily[1]["n"] == 2


class TestClvBlock:
    def test_cumulative_average_series(self):
        history = [
            _entry(date="2026-06-14", clv=0.01),
            _entry(date="2026-06-15", clv=-0.02),
            _entry(date="2026-06-16", clv=0.04),
            _entry(date="2026-06-17", clv=None),  # ignored
        ]
        clv = build_metrics(history)["clv"]
        assert clv["n"] == 3
        assert clv["positive"] == 2 and clv["negative"] == 1 and clv["zero"] == 0
        assert clv["mean_clv"] == round((0.01 - 0.02 + 0.04) / 3, 5)
        cum = clv["cumulative"]
        assert cum[-1]["n"] == 3
        assert cum[0]["cum_avg_clv"] == 0.01
        assert cum[1]["cum_avg_clv"] == round((0.01 - 0.02) / 2, 5)

    def test_empty_clv(self):
        clv = build_metrics([_entry(clv=None)])["clv"]
        assert clv["n"] == 0
        assert clv["mean_clv"] is None
        assert clv["cumulative"] == []


class TestCalibration:
    def test_buckets_and_gap(self):
        history = [
            _entry(model_probability=0.52, result="win"),
            _entry(model_probability=0.53, result="loss"),
            _entry(model_probability=0.62, result="win"),
        ]
        cal = build_metrics(history)["calibration"]
        assert cal["n"] == 3
        mid = next(r for r in cal["rows"] if r["bucket"] == "0.50-0.55")
        assert mid["n"] == 2
        assert mid["win_rate"] == 0.5
        assert mid["avg_predicted"] == round((0.52 + 0.53) / 2, 4)
        top = next(r for r in cal["rows"] if r["bucket"] == "0.60-1.00")
        assert top["n"] == 1 and top["win_rate"] == 1.0

    def test_pushes_excluded_from_calibration(self):
        cal = build_metrics([_entry(result="push", model_probability=0.52)])["calibration"]
        assert cal["n"] == 0


class TestMetaStatusPassthrough:
    def test_status_embedded(self):
        status = {"state": "fallback", "message": "insufficient data (n=22/150)"}
        metrics = build_metrics([], meta_model_status=status)
        assert metrics["meta_model"] == status

    def test_threshold_exposed(self):
        assert build_metrics([])["small_sample_threshold"] == SMALL_SAMPLE_THRESHOLD
