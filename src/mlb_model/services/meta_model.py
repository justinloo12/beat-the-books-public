"""Stacked meta-model: logistic regression over module signals + market price.

Stacks [module signals..., market no-vig probability] -> P(pick wins) with a
ridge-regularised logistic regression fit by IRLS (numpy only — numpy is
already a transitive dependency via pybaseball, so no new install).

HARD RULE — small-data safety: the model only trains when at least
MIN_TRAIN_SAMPLES graded picks with stored module signals exist. Below that
threshold `predict_probability` returns the config-weighted blend UNCHANGED
and `status()` reports "insufficient data (n=X/150)". With the current
archive (~22 graded leans, none of which carry module signals yet) the model
is purely infrastructure: signals accumulate first, training starts later.

Trained coefficients are persisted to config/meta_model.json so the fitted
state is inspectable and versioned alongside the rest of the model config.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_COEFF_PATH = ROOT / "config" / "meta_model.json"

# Fixed feature ordering. The market no-vig probability is deliberately a
# feature: the stacker learns how much to trust the modules OVER the market,
# which is the only question that matters for beating the close.
FEATURE_NAMES: tuple[str, ...] = (
    "pitcher_home",
    "pitcher_away",
    "bullpen_home",
    "bullpen_away",
    "offense_home",
    "offense_away",
    "weather_stack",
    "market_no_vig",
)

# Neutral defaults used when a stored signal is missing.
_FEATURE_DEFAULTS: dict[str, float] = {
    "pitcher_home": 50.0,
    "pitcher_away": 50.0,
    "bullpen_home": 65.0,
    "bullpen_away": 65.0,
    "offense_home": 50.0,
    "offense_away": 50.0,
    "weather_stack": 0.0,
    "market_no_vig": 0.5,
}

MIN_TRAIN_SAMPLES = 150


def _sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -35.0, 35.0)))


def fit_logistic_irls(
    features: np.ndarray,
    outcomes: np.ndarray,
    l2: float = 1.0,
    max_iter: int = 50,
    tol: float = 1e-8,
) -> dict[str, Any]:
    """Ridge-penalised logistic regression via iteratively reweighted least
    squares (Newton-Raphson). The intercept is not penalised.

    features: (n, k) design matrix WITHOUT an intercept column.
    outcomes: (n,) array of 0/1.
    Returns {"intercept": float, "coefficients": (k,) array, "converged": bool,
             "iterations": int}.
    """
    n, k = features.shape
    design = np.hstack([np.ones((n, 1)), features])
    beta = np.zeros(k + 1)
    penalty = np.full(k + 1, l2)
    penalty[0] = 0.0  # never shrink the intercept

    converged = False
    iterations = 0
    for iterations in range(1, max_iter + 1):
        probs = _sigmoid(design @ beta)
        weights = np.maximum(probs * (1.0 - probs), 1e-10)
        gradient = design.T @ (outcomes - probs) - penalty * beta
        hessian = (design.T * weights) @ design + np.diag(penalty + 1e-12)
        try:
            step = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            step = np.linalg.lstsq(hessian, gradient, rcond=None)[0]
        beta = beta + step
        if float(np.max(np.abs(step))) < tol:
            converged = True
            break
    return {
        "intercept": float(beta[0]),
        "coefficients": beta[1:],
        "converged": converged,
        "iterations": iterations,
    }


class MetaModel:
    """Logistic stacker over module signals with a strict small-sample gate."""

    def __init__(self, coeff_path: Path | None = None, min_samples: int = MIN_TRAIN_SAMPLES) -> None:
        self.coeff_path = coeff_path or DEFAULT_COEFF_PATH
        self.min_samples = int(min_samples)
        self._state: dict[str, Any] | None = None  # populated only when trained
        self._n_available = 0

    # ------------------------------------------------------------------ data

    @staticmethod
    def _trainable(entries: Sequence[dict]) -> list[dict]:
        """Graded win/loss entries carrying module signals and a market prob."""
        usable = []
        for entry in entries:
            if entry.get("result") not in ("win", "loss"):
                continue
            signals = entry.get("module_signals")
            if not isinstance(signals, dict) or not signals:
                continue
            if entry.get("no_vig_probability") is None:
                continue
            usable.append(entry)
        return usable

    def feature_vector(self, signals: dict | None, market_no_vig: float | None) -> np.ndarray:
        signals = signals or {}
        values = []
        for name in FEATURE_NAMES:
            if name == "market_no_vig":
                raw = market_no_vig
            else:
                raw = signals.get(name)
            try:
                value = float(raw) if raw is not None else _FEATURE_DEFAULTS[name]
            except (TypeError, ValueError):
                value = _FEATURE_DEFAULTS[name]
            values.append(value)
        return np.array(values, dtype=float)

    # -------------------------------------------------------------- training

    def train_from_history(self, entries: Sequence[dict]) -> dict[str, Any]:
        """Fit when enough usable samples exist; otherwise arm the fallback.

        Returns the public status dict either way.
        """
        usable = self._trainable(entries)
        self._n_available = len(usable)
        if self._n_available < self.min_samples:
            self._state = None
            return self.status()

        matrix = np.vstack(
            [self.feature_vector(e.get("module_signals"), e.get("no_vig_probability")) for e in usable]
        )
        outcomes = np.array([1.0 if e["result"] == "win" else 0.0 for e in usable])

        # Standardise so the ridge penalty treats every signal equally and the
        # solver stays numerically tame regardless of raw scales.
        means = matrix.mean(axis=0)
        stds = matrix.std(axis=0)
        stds[stds < 1e-9] = 1.0
        standardized = (matrix - means) / stds

        fit = fit_logistic_irls(standardized, outcomes, l2=1.0)
        predictions = _sigmoid(fit["intercept"] + standardized @ fit["coefficients"])
        accuracy = float(np.mean((predictions >= 0.5) == (outcomes == 1.0)))

        self._state = {
            "trained_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "n_samples": self._n_available,
            "feature_names": list(FEATURE_NAMES),
            "feature_means": means.tolist(),
            "feature_stds": stds.tolist(),
            "intercept": fit["intercept"],
            "coefficients": fit["coefficients"].tolist(),
            "converged": fit["converged"],
            "iterations": fit["iterations"],
            "train_accuracy": round(accuracy, 4),
        }
        self._persist()
        return self.status()

    def _persist(self) -> None:
        if self._state is None:
            return
        try:
            self.coeff_path.parent.mkdir(parents=True, exist_ok=True)
            self.coeff_path.write_text(json.dumps(self._state, indent=2), encoding="utf-8")
        except OSError:
            pass  # persistence is best-effort; the in-memory model still works

    # ------------------------------------------------------------- inference

    @property
    def is_trained(self) -> bool:
        return self._state is not None

    def predict_probability(
        self,
        signals: dict | None,
        market_no_vig: float | None,
        fallback_probability: float,
    ) -> float:
        """Stacked probability when trained; the config-weighted blend
        (fallback_probability) UNCHANGED otherwise."""
        if self._state is None:
            return fallback_probability
        vector = self.feature_vector(signals, market_no_vig)
        means = np.array(self._state["feature_means"])
        stds = np.array(self._state["feature_stds"])
        coeffs = np.array(self._state["coefficients"])
        z = self._state["intercept"] + float(((vector - means) / stds) @ coeffs)
        return float(_sigmoid(np.array([z]))[0])

    # ---------------------------------------------------------------- status

    def status(self) -> dict[str, Any]:
        if self._state is None:
            return {
                "state": "fallback",
                "message": f"insufficient data (n={self._n_available}/{self.min_samples})",
                "n_samples": self._n_available,
                "threshold": self.min_samples,
                "coefficients": None,
                "trained_at": None,
            }
        return {
            "state": "trained",
            "message": f"trained on {self._state['n_samples']} graded picks",
            "n_samples": self._state["n_samples"],
            "threshold": self.min_samples,
            "coefficients": dict(
                zip(self._state["feature_names"], [round(c, 4) for c in self._state["coefficients"]])
            ),
            "intercept": round(self._state["intercept"], 4),
            "train_accuracy": self._state["train_accuracy"],
            "converged": self._state["converged"],
            "trained_at": self._state["trained_at"],
        }
