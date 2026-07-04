"""Tests for the stacked meta-model: recovery of known coefficients on
synthetic data, and the strict small-sample fallback path."""
from __future__ import annotations

import math
import random

import numpy as np
import pytest

from mlb_model.services.meta_model import (
    FEATURE_NAMES,
    MIN_TRAIN_SAMPLES,
    MetaModel,
    fit_logistic_irls,
)

# True coefficients on the STANDARDIZED feature scale used to generate data.
TRUE_COEFFS = {
    "pitcher_home": 0.9,
    "pitcher_away": -0.7,
    "bullpen_home": 0.4,
    "bullpen_away": -0.3,
    "offense_home": 0.6,
    "offense_away": -0.5,
    "weather_stack": 0.2,
    "market_no_vig": 1.1,
}
TRUE_INTERCEPT = 0.15

# Raw scales the synthetic signals are drawn on (mirrors real module output).
RAW_SCALES = {
    "pitcher_home": (50.0, 12.0),
    "pitcher_away": (50.0, 12.0),
    "bullpen_home": (65.0, 10.0),
    "bullpen_away": (65.0, 10.0),
    "offense_home": (50.0, 11.0),
    "offense_away": (50.0, 11.0),
    "weather_stack": (0.5, 0.8),
    "market_no_vig": (0.5, 0.06),
}


def make_synthetic_history(n: int, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    entries = []
    for i in range(n):
        z_values = {name: rng.gauss(0.0, 1.0) for name in FEATURE_NAMES}
        logit = TRUE_INTERCEPT + sum(TRUE_COEFFS[name] * z for name, z in z_values.items())
        prob = 1.0 / (1.0 + math.exp(-logit))
        result = "win" if rng.random() < prob else "loss"
        signals = {}
        market_nv = None
        for name, z in z_values.items():
            mean, sd = RAW_SCALES[name]
            raw = mean + sd * z
            if name == "market_no_vig":
                market_nv = raw
            else:
                signals[name] = raw
        entries.append(
            {
                "date": f"2026-07-{(i % 28) + 1:02d}",
                "module_signals": signals,
                "no_vig_probability": market_nv,
                "result": result,
            }
        )
    return entries


class TestIRLSFitter:
    def test_recovers_simple_coefficients(self):
        rng = np.random.default_rng(3)
        n = 4000
        x = rng.normal(size=(n, 2))
        true_beta = np.array([1.2, -0.8])
        probs = 1.0 / (1.0 + np.exp(-(0.3 + x @ true_beta)))
        y = (rng.random(n) < probs).astype(float)
        fit = fit_logistic_irls(x, y, l2=0.5)
        assert fit["converged"]
        assert abs(fit["intercept"] - 0.3) < 0.15
        assert np.allclose(fit["coefficients"], true_beta, atol=0.15)

    def test_separable_data_does_not_blow_up(self):
        # Perfectly separable data would diverge without the ridge penalty.
        x = np.array([[float(i)] for i in range(-10, 11)])
        y = (x[:, 0] > 0).astype(float)
        fit = fit_logistic_irls(x, y, l2=1.0, max_iter=100)
        assert np.isfinite(fit["intercept"])
        assert np.all(np.isfinite(fit["coefficients"]))


class TestMetaModelTraining:
    def test_learns_known_coefficients_approximately(self, tmp_path):
        history = make_synthetic_history(500)
        model = MetaModel(coeff_path=tmp_path / "meta.json")
        status = model.train_from_history(history)

        assert status["state"] == "trained"
        assert status["n_samples"] == 500
        assert model.is_trained
        learned = status["coefficients"]
        # Ridge shrinks toward zero, so demand the right sign and a magnitude
        # in the neighbourhood of truth rather than exact recovery.
        for name, true_val in TRUE_COEFFS.items():
            got = learned[name]
            assert got * true_val > 0, f"{name}: sign flipped ({got} vs {true_val})"
            assert abs(got - true_val) < 0.45, f"{name}: {got} too far from {true_val}"

    def test_persists_coefficients(self, tmp_path):
        path = tmp_path / "meta.json"
        model = MetaModel(coeff_path=path)
        model.train_from_history(make_synthetic_history(300))
        assert path.exists()
        import json

        saved = json.loads(path.read_text())
        assert saved["feature_names"] == list(FEATURE_NAMES)
        assert len(saved["coefficients"]) == len(FEATURE_NAMES)

    def test_trained_predictions_track_outcome_direction(self, tmp_path):
        model = MetaModel(coeff_path=tmp_path / "meta.json")
        model.train_from_history(make_synthetic_history(500))
        strong = {name: RAW_SCALES[name][0] + RAW_SCALES[name][1] * (1.0 if TRUE_COEFFS[name] > 0 else -1.0)
                  for name in FEATURE_NAMES if name != "market_no_vig"}
        weak = {name: RAW_SCALES[name][0] + RAW_SCALES[name][1] * (-1.0 if TRUE_COEFFS[name] > 0 else 1.0)
                for name in FEATURE_NAMES if name != "market_no_vig"}
        p_strong = model.predict_probability(strong, 0.56, fallback_probability=0.5)
        p_weak = model.predict_probability(weak, 0.44, fallback_probability=0.5)
        assert p_strong > 0.5 > p_weak
        assert 0.0 < p_weak < p_strong < 1.0


class TestSmallSampleFallback:
    def test_below_threshold_reports_insufficient_data(self, tmp_path):
        history = make_synthetic_history(22)
        model = MetaModel(coeff_path=tmp_path / "meta.json")
        status = model.train_from_history(history)
        assert status["state"] == "fallback"
        assert status["message"] == f"insufficient data (n=22/{MIN_TRAIN_SAMPLES})"
        assert status["coefficients"] is None
        assert not model.is_trained
        assert not (tmp_path / "meta.json").exists()

    def test_fallback_returns_blend_unchanged(self, tmp_path):
        model = MetaModel(coeff_path=tmp_path / "meta.json")
        model.train_from_history(make_synthetic_history(22))
        for blend in (0.31, 0.5, 0.6789):
            assert model.predict_probability({"pitcher_home": 80.0}, 0.52, blend) == blend

    def test_entries_without_signals_do_not_count(self, tmp_path):
        # 200 graded entries, but none carry module signals -> still fallback.
        history = [
            {"result": "win" if i % 2 else "loss", "no_vig_probability": 0.5}
            for i in range(200)
        ]
        model = MetaModel(coeff_path=tmp_path / "meta.json")
        status = model.train_from_history(history)
        assert status["state"] == "fallback"
        assert status["n_samples"] == 0

    def test_exact_threshold_trains(self, tmp_path):
        model = MetaModel(coeff_path=tmp_path / "meta.json", min_samples=150)
        status = model.train_from_history(make_synthetic_history(150))
        assert status["state"] == "trained"

    def test_current_real_history_falls_back(self):
        """The actual archived pick_history.json must route to the fallback."""
        import json
        from pathlib import Path

        real = json.loads(
            (Path(__file__).resolve().parents[1] / "docs" / "data" / "pick_history.json").read_text()
        )
        model = MetaModel(coeff_path=Path("/nonexistent/never-written.json"))
        status = model.train_from_history(real)
        assert status["state"] == "fallback"
        assert status["n_samples"] < MIN_TRAIN_SAMPLES


class TestFeatureVector:
    def test_missing_signals_use_neutral_defaults(self):
        model = MetaModel()
        vec = model.feature_vector(None, None)
        assert len(vec) == len(FEATURE_NAMES)
        assert vec[list(FEATURE_NAMES).index("market_no_vig")] == pytest.approx(0.5)
        assert vec[list(FEATURE_NAMES).index("bullpen_home")] == pytest.approx(65.0)

    def test_non_numeric_signal_falls_back_to_default(self):
        model = MetaModel()
        vec = model.feature_vector({"pitcher_home": "not-a-number"}, 0.55)
        assert vec[list(FEATURE_NAMES).index("pitcher_home")] == pytest.approx(50.0)
