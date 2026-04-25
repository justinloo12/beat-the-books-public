from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlmodel import Session, select

from mlb_model.config import get_settings
from mlb_model.models import ModelCalibration, ModuleSignal, Pick


class CalibrationService:
    def __init__(self) -> None:
        self.settings = get_settings()

    def optimize(self, session: Session) -> dict[str, Any]:
        picks = list(session.exec(select(Pick).where(Pick.clv_value.is_not(None))).all())
        signals = list(session.exec(select(ModuleSignal)).all())
        if not picks or not signals:
            return {
                "updated": False,
                "reason": "Insufficient graded pick and module signal history for recalibration.",
            }

        grouped: dict[str, list[float]] = {}
        clv_by_game = {pick.game_id: pick.clv_value or 0.0 for pick in picks}
        for signal in signals:
            if signal.game_id not in clv_by_game:
                continue
            grouped.setdefault(signal.module_name, []).append(signal.score * clv_by_game[signal.game_id])

        current = self.settings.load_model_settings()
        scores = {name: max(0.01, sum(values) / len(values)) for name, values in grouped.items() if values}
        total = sum(scores.values())
        if not total:
            return {"updated": False, "reason": "Signals had no positive explanatory power."}

        new_weights = current.weights.copy()
        for name, value in scores.items():
            if name in new_weights:
                new_weights[name] = round(value / total, 4)

        config_path = Path(self.settings.model_config_path)
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        payload["weights"] = new_weights
        config_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        session.add(
            ModelCalibration(
                sample_size=len(picks),
                weights=new_weights,
                metrics={"module_scores": scores, "method": "normalized_clv_signal_product"},
            )
        )
        session.commit()
        return {"updated": True, "weights": new_weights, "sample_size": len(picks)}
