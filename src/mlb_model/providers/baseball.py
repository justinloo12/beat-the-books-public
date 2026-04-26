from __future__ import annotations

from datetime import date
from functools import lru_cache
from pathlib import Path
import re
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError
from pybaseball import pitching_stats

from mlb_model.config import get_settings


STATCAST_BATTED_BALL_DESCRIPTIONS = {"hit_into_play", "hit_into_play_no_out", "hit_into_play_score"}
PLATE_ENDING_EVENTS = {
    "single",
    "double",
    "triple",
    "home_run",
    "strikeout",
    "strikeout_double_play",
    "walk",
    "intent_walk",
    "hit_by_pitch",
    "field_out",
    "grounded_into_double_play",
    "force_out",
    "fielders_choice",
    "fielders_choice_out",
    "double_play",
    "triple_play",
    "sac_fly",
    "sac_bunt",
    "sac_fly_double_play",
}


class BaseballSavantProvider:
    def __init__(self) -> None:
        settings = get_settings()
        self.data_dir = settings.data_dir / "baseball_savant"

    async def healthcheck(self) -> dict[str, Any]:
        return {"provider": "pybaseball", "status": "ok"}

    async def fetch_pitching_baseline(self, season: int) -> list[dict[str, Any]]:
        frame = pitching_stats(season, qual=0)
        return frame.to_dict(orient="records")

    async def fetch_pitcher_history(
        self,
        pitcher_name: str,
        season: int,
    ) -> dict[str, Any]:
        rows = await self.fetch_pitching_baseline(season)
        match = next((row for row in rows if row.get("Name") == pitcher_name), {})
        return match

    async def fetch_team_handedness_offense(self, season: int) -> list[dict[str, Any]]:
        return []

    async def fetch_pitch_mix(self, pitcher_id: int, season: int) -> dict[str, float]:
        return {}

    async def fetch_recent_starts(self, pitcher_id: int, season: int) -> list[dict[str, Any]]:
        return []

    def _normalize_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        renamed = frame.rename(columns={'\ufeff"pitch_type"': "pitch_type"})
        if "pitch_type" not in renamed.columns and 'pitch_type"' in renamed.columns:
            renamed = renamed.rename(columns={'pitch_type"': "pitch_type"})
        return renamed

    @lru_cache(maxsize=8)
    def _load_files(self, file_names: tuple[str, ...]) -> pd.DataFrame:
        frames = []
        for file_name in file_names:
            path = self.data_dir / file_name
            if path.suffix.lower() == ".csv":
                try:
                    frames.append(self._normalize_frame(pd.read_csv(path, low_memory=False)))
                except EmptyDataError:
                    continue
        if not frames:
            return pd.DataFrame()
        frame = pd.concat(frames, ignore_index=True)
        return self._normalize_frame(frame)

    def load_statcast(self, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        statcast_range = re.compile(r"^statcast_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.csv$")
        files = sorted(
            path.name
            for path in self.data_dir.glob("statcast_*.csv")
            if statcast_range.match(path.name)
        )
        frame = self._load_files(tuple(files))
        if frame.empty:
            return frame
        if "game_date" in frame.columns:
            frame["game_date"] = pd.to_datetime(frame["game_date"]).dt.date
        else:
            return pd.DataFrame()
        if start_date:
            frame = frame[frame["game_date"] >= start_date]
        if end_date:
            frame = frame[frame["game_date"] <= end_date]
        return frame.copy()

    @lru_cache(maxsize=6)
    def _load_batter_expected_stats(self, season: int) -> pd.DataFrame:
        path = self.data_dir / f"statcast_batter_expected_stats_{season}.csv"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, low_memory=False)

    @lru_cache(maxsize=6)
    def _load_batter_percentiles(self, season: int) -> pd.DataFrame:
        path = self.data_dir / f"statcast_batter_percentile_ranks_{season}.csv"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, low_memory=False)

    @lru_cache(maxsize=6)
    def _load_pitcher_arsenal_stats(self, season: int) -> pd.DataFrame:
        path = self.data_dir / f"statcast_pitcher_arsenal_stats_{season}.csv"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, low_memory=False)

    def _pitch_quality_score(self, rv100: float, whiff_pct: float, est_woba: float) -> int:
        """Compute a Stuff+ proxy (60–170 scale, 100 = average) from outcome metrics."""
        score = (
            100
            + (-rv100) * 7.0           # each -1 run/100 pitches → +7 quality
            + (whiff_pct - 20.0) * 0.7  # each % whiff above avg → +0.7 quality
            - (est_woba - 0.315) * 50   # each 0.01 xwOBA below avg → +0.5 quality
        )
        return round(max(60, min(170, score)))

    def _safe_pct(self, series: pd.Series) -> float:
        if len(series) == 0:
            return 0.0
        return float(series.mean())

    def _barrel_pct(self, frame: pd.DataFrame) -> float:
        if frame.empty:
            return 0.0
        launch_speed = pd.to_numeric(frame.get("launch_speed"), errors="coerce")
        launch_angle = pd.to_numeric(frame.get("launch_angle"), errors="coerce")
        barrels = ((launch_speed >= 98) & launch_angle.between(26, 30, inclusive="both")).fillna(False)
        return float(barrels.mean())

    def _ev50(self, frame: pd.DataFrame) -> float:
        if frame.empty:
            return 0.0
        launch_speed = pd.to_numeric(frame.get("launch_speed"), errors="coerce").dropna()
        if launch_speed.empty:
            return 0.0
        return float(launch_speed.quantile(0.5))

    def _quality_of_contact(self, frame: pd.DataFrame) -> float:
        values = pd.to_numeric(frame.get("estimated_woba_using_speedangle"), errors="coerce").dropna()
        if values.empty:
            return 0.0
        return float(values.mean())

    def _recent_window(self, frame: pd.DataFrame, end_date: date | None, days: int = 30) -> pd.DataFrame:
        if frame.empty or "game_date" not in frame.columns:
            return pd.DataFrame()
        anchor = end_date or frame["game_date"].max()
        if pd.isna(anchor):
            return pd.DataFrame()
        recent_start = anchor - pd.Timedelta(days=days) if isinstance(anchor, pd.Timestamp) else date.fromordinal(anchor.toordinal() - days)
        return frame[frame["game_date"] >= recent_start].copy()

    def _swing_path_score(self, frame: pd.DataFrame) -> float:
        tilt = pd.to_numeric(frame.get("swing_path_tilt"), errors="coerce").dropna()
        attack = pd.to_numeric(frame.get("attack_angle"), errors="coerce").dropna()
        if tilt.empty and attack.empty:
            return 0.0
        tilt_score = float(tilt.mean()) if not tilt.empty else 0.0
        attack_score = float(attack.mean()) if not attack.empty else 0.0
        return (tilt_score * 0.6) + (attack_score * 0.4)

    def _player_handedness(self, frame: pd.DataFrame, field: str) -> str | None:
        if field not in frame.columns or frame.empty:
            return None
        series = frame[field].dropna()
        if series.empty:
            return None
        return str(series.mode().iloc[0])

    def _nan_to_zero(self, value: Any) -> float:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(numeric):
            return 0.0
        return float(numeric)

    def build_pitcher_arsenal_profile(
        self,
        pitcher_id: int,
        batter_hand: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        frame = self.load_statcast(start_date, end_date)
        if frame.empty:
            return {"pitcher_id": pitcher_id, "pitch_arsenal": [], "handedness": None}
        pitcher_frame = frame[pd.to_numeric(frame["pitcher"], errors="coerce") == pitcher_id].copy()
        if batter_hand:
            pitcher_frame = pitcher_frame[pitcher_frame["stand"] == batter_hand]
        if pitcher_frame.empty:
            return {"pitcher_id": pitcher_id, "pitch_arsenal": [], "handedness": None}

        batter_frame = pitcher_frame[pitcher_frame["description"].isin(STATCAST_BATTED_BALL_DESCRIPTIONS)].copy()
        # Only include the last pitch of each PA (events set) — not intermediate pitches (events=NaN)
        terminal_frame = pitcher_frame[
            pitcher_frame["events"].fillna("").isin(PLATE_ENDING_EVENTS)
        ].copy()
        recent_pitcher_frame = self._recent_window(pitcher_frame, end_date)
        recent_batter_frame = recent_pitcher_frame[recent_pitcher_frame["description"].isin(STATCAST_BATTED_BALL_DESCRIPTIONS)].copy()
        recent_terminal_frame = recent_pitcher_frame[
            recent_pitcher_frame["events"].fillna("").isin(PLATE_ENDING_EVENTS)
        ].copy()
        total_pitches = max(len(pitcher_frame), 1)

        # Load arsenal-level leaderboard stats (most recent available season)
        current_year = end_date.year if end_date else date.today().year
        arsenal_stats_df = self._load_pitcher_arsenal_stats(current_year)
        if arsenal_stats_df.empty:
            arsenal_stats_df = self._load_pitcher_arsenal_stats(current_year - 1)
        pitcher_arsenal_stats: dict[str, dict] = {}
        if not arsenal_stats_df.empty:
            rows = arsenal_stats_df[
                pd.to_numeric(arsenal_stats_df.get("player_id"), errors="coerce") == pitcher_id
            ]
            for _, row in rows.iterrows():
                pt = str(row.get("pitch_type", ""))
                pitcher_arsenal_stats[pt] = row.to_dict()

        arsenal: list[dict[str, Any]] = []

        for pitch_type, pitch_frame in pitcher_frame.groupby("pitch_type", dropna=True):
            if pd.isna(pitch_type):
                continue
            pitch_bbe = batter_frame[batter_frame["pitch_type"] == pitch_type]
            pitch_terminal = terminal_frame[terminal_frame["pitch_type"] == pitch_type]
            hard_hit = (pd.to_numeric(pitch_bbe.get("launch_speed"), errors="coerce") >= 95).mean()
            strikeout_rate = pitch_terminal["events"].fillna("").isin({"strikeout", "strikeout_double_play"}).mean()
            walk_rate = pitch_terminal["events"].fillna("").isin({"walk", "intent_walk"}).mean()
            xba = pd.to_numeric(pitch_bbe.get("estimated_ba_using_speedangle"), errors="coerce").mean()
            usage_pct = len(pitch_frame) / total_pitches
            run_value = pd.to_numeric(pitch_frame.get("delta_run_exp"), errors="coerce").mean()

            # Pitch quality from leaderboard (more stable sample than pitch-level alone)
            ls = pitcher_arsenal_stats.get(str(pitch_type), {})
            rv100 = float(ls.get("run_value_per_100", 0.0)) if ls else 0.0
            whiff_pct_lb = float(ls.get("whiff_percent", 20.0)) if ls else 20.0
            est_woba_lb = float(ls.get("est_woba", 0.315)) if ls else 0.315
            has_leaderboard = bool(ls)
            pitch_quality = self._pitch_quality_score(rv100, whiff_pct_lb, est_woba_lb) if has_leaderboard else None

            pitch_profile = {
                "pitch_type": str(pitch_type),
                "pitch_name": str(ls.get("pitch_name", pitch_type)) if ls else str(pitch_type),
                "usage_pct": round(float(usage_pct), 4),
                "xba": round(float(0.0 if pd.isna(xba) else xba), 4),
                "hard_hit_pct": round(float(0.0 if pd.isna(hard_hit) else hard_hit), 4),
                "k_pct": round(float(0.0 if pd.isna(strikeout_rate) else strikeout_rate), 4),
                "bb_pct": round(float(0.0 if pd.isna(walk_rate) else walk_rate), 4),
                "barrel_pct": round(self._barrel_pct(pitch_bbe), 4),
                "extension": round(self._nan_to_zero(pd.to_numeric(pitch_frame.get("release_extension"), errors="coerce").mean()), 3),
                "ev50": round(self._ev50(pitch_bbe), 3),
                "run_value": round(float(0.0 if pd.isna(run_value) else -run_value), 4),
                "run_value_per_100": round(rv100, 2) if has_leaderboard else None,
                "whiff_pct": round(whiff_pct_lb, 1) if has_leaderboard else None,
                "pitch_quality": pitch_quality,
                "quality_of_contact": round(self._quality_of_contact(pitch_bbe), 4),
                "vertical_movement": round(self._nan_to_zero(pd.to_numeric(pitch_frame.get("pfx_z"), errors="coerce").mean()), 4),
                "horizontal_movement": round(self._nan_to_zero(pd.to_numeric(pitch_frame.get("pfx_x"), errors="coerce").mean()), 4),
                "spin_dir": round(self._nan_to_zero(pd.to_numeric(pitch_frame.get("spin_dir"), errors="coerce").mean()), 3),
                "spin_axis": round(self._nan_to_zero(pd.to_numeric(pitch_frame.get("spin_axis"), errors="coerce").mean()), 3),
                "arm_angle": round(self._nan_to_zero(pd.to_numeric(pitch_frame.get("arm_angle"), errors="coerce").mean()), 3),
            }
            arsenal.append(pitch_profile)

        arsenal.sort(key=lambda item: item["usage_pct"], reverse=True)
        pitcher_overall_bbe = pitcher_frame[pitcher_frame["description"].isin(STATCAST_BATTED_BALL_DESCRIPTIONS)]
        weighted_run_value = sum(p["run_value"] * p["usage_pct"] for p in arsenal)
        weighted_k_pct = sum(p["k_pct"] * p["usage_pct"] for p in arsenal)
        weighted_bb_pct = sum(p["bb_pct"] * p["usage_pct"] for p in arsenal)
        weighted_movement = sum((abs(p["vertical_movement"]) + abs(p["horizontal_movement"])) * p["usage_pct"] for p in arsenal)
        # Weighted average pitch quality across arsenal (None-safe)
        quality_pitches = [(p["pitch_quality"], p["usage_pct"]) for p in arsenal if p["pitch_quality"] is not None]
        stuff_plus = round(sum(q * u for q, u in quality_pitches) / max(sum(u for _, u in quality_pitches), 1e-9)) if quality_pitches else None
        return {
            "pitcher_id": pitcher_id,
            "sample_pitches": int(len(pitcher_frame)),
            "sample_bbe": int(len(pitcher_overall_bbe)),
            "handedness": self._player_handedness(pitcher_frame, "p_throws"),
            "arm_angle": round(self._nan_to_zero(pd.to_numeric(pitcher_frame.get("arm_angle"), errors="coerce").mean()), 3),
            "extension": round(self._nan_to_zero(pd.to_numeric(pitcher_frame.get("release_extension"), errors="coerce").mean()), 3),
            "xba": round(self._nan_to_zero(pd.to_numeric(pitcher_overall_bbe.get("estimated_ba_using_speedangle"), errors="coerce").mean()), 4),
            "hard_hit_pct": round(self._nan_to_zero((pd.to_numeric(pitcher_overall_bbe.get("launch_speed"), errors="coerce") >= 95).mean()), 4),
            "barrel_pct": round(self._barrel_pct(pitcher_overall_bbe), 4),
            "ev50": round(self._ev50(pitcher_overall_bbe), 3),
            "weighted_run_value": round(weighted_run_value, 4),
            "weighted_k_pct": round(weighted_k_pct, 4),
            "weighted_bb_pct": round(weighted_bb_pct, 4),
            "movement_score": round(weighted_movement, 4),
            "stuff_plus": stuff_plus,
            "k_pct": round(self._nan_to_zero(terminal_frame["events"].fillna("").isin({"strikeout", "strikeout_double_play"}).mean()), 4),
            "bb_pct": round(self._nan_to_zero(terminal_frame["events"].fillna("").isin({"walk", "intent_walk"}).mean()), 4),
            "recent_xba": round(self._nan_to_zero(pd.to_numeric(recent_batter_frame.get("estimated_ba_using_speedangle"), errors="coerce").mean()), 4),
            "recent_hard_hit_pct": round(self._nan_to_zero((pd.to_numeric(recent_batter_frame.get("launch_speed"), errors="coerce") >= 95).mean()), 4),
            "recent_barrel_pct": round(self._barrel_pct(recent_batter_frame), 4),
            "recent_ev50": round(self._ev50(recent_batter_frame), 3),
            "recent_k_pct": round(self._nan_to_zero(recent_terminal_frame["events"].fillna("").isin({"strikeout", "strikeout_double_play"}).mean()), 4),
            "recent_bb_pct": round(self._nan_to_zero(recent_terminal_frame["events"].fillna("").isin({"walk", "intent_walk"}).mean()), 4),
            "pitch_arsenal": arsenal,
        }

    def build_batter_matchup_profile(
        self,
        batter_id: int,
        pitcher_hand: str | None = None,
        pitch_types: set[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        frame = self.load_statcast(start_date, end_date)
        if frame.empty:
            return {"batter_id": batter_id, "pitch_profiles": [], "handedness": None}
        batter_frame = frame[pd.to_numeric(frame["batter"], errors="coerce") == batter_id].copy()
        if pitcher_hand:
            batter_frame = batter_frame[batter_frame["p_throws"] == pitcher_hand]
        if pitch_types:
            batter_frame = batter_frame[batter_frame["pitch_type"].isin(pitch_types)]
        if batter_frame.empty:
            return {"batter_id": batter_id, "pitch_profiles": [], "handedness": None}

        bbe_frame = batter_frame[batter_frame["description"].isin(STATCAST_BATTED_BALL_DESCRIPTIONS)].copy()
        # Only include the last pitch of each PA (events set) — not intermediate pitches (events=NaN)
        terminal_frame = batter_frame[batter_frame["events"].fillna("").isin(PLATE_ENDING_EVENTS)].copy()
        recent_batter_frame = self._recent_window(batter_frame, end_date)
        recent_bbe_frame = recent_batter_frame[recent_batter_frame["description"].isin(STATCAST_BATTED_BALL_DESCRIPTIONS)].copy()
        recent_terminal_frame = recent_batter_frame[
            recent_batter_frame["events"].fillna("").isin(PLATE_ENDING_EVENTS)
        ].copy()
        profiles: list[dict[str, Any]] = []
        for pitch_type, pitch_frame in batter_frame.groupby("pitch_type", dropna=True):
            if pd.isna(pitch_type):
                continue
            pitch_bbe = bbe_frame[bbe_frame["pitch_type"] == pitch_type]
            pitch_terminal = terminal_frame[terminal_frame["pitch_type"] == pitch_type]
            hard_hit = (pd.to_numeric(pitch_bbe.get("launch_speed"), errors="coerce") >= 95).mean()
            bb_rate = pitch_terminal["events"].fillna("").isin({"walk", "intent_walk"}).mean()
            k_rate = pitch_terminal["events"].fillna("").isin({"strikeout", "strikeout_double_play"}).mean()
            xwoba = pd.to_numeric(pitch_bbe.get("estimated_woba_using_speedangle"), errors="coerce").mean()
            run_value = pd.to_numeric(pitch_frame.get("delta_run_exp"), errors="coerce").mean()
            profiles.append(
                {
                    "pitch_type": str(pitch_type),
                    "xwoba": round(float(0.0 if pd.isna(xwoba) else xwoba), 4),
                    "ev50": round(self._ev50(pitch_bbe), 3),
                    "hard_hit_pct": round(float(0.0 if pd.isna(hard_hit) else hard_hit), 4),
                    "bb_pct": round(float(0.0 if pd.isna(bb_rate) else bb_rate), 4),
                    "k_pct": round(float(0.0 if pd.isna(k_rate) else k_rate), 4),
                    "run_value": round(float(0.0 if pd.isna(run_value) else run_value), 4),
                    "quality_of_contact": round(self._quality_of_contact(pitch_bbe), 4),
                }
            )

        overall_bbe = batter_frame[batter_frame["description"].isin(STATCAST_BATTED_BALL_DESCRIPTIONS)]
        MIN_BBE = 8  # below this, contact stats are too noisy to display
        MIN_PA  = 10

        def _bbe_stat(frame: pd.DataFrame, col: str) -> float | None:
            if len(frame) < MIN_BBE:
                return None
            v = pd.to_numeric(frame.get(col), errors="coerce").mean()
            return None if pd.isna(v) else round(float(v), 4)

        def _hh(frame: pd.DataFrame) -> float | None:
            if len(frame) < MIN_BBE:
                return None
            v = (pd.to_numeric(frame.get("launch_speed"), errors="coerce") >= 95).mean()
            return None if pd.isna(v) else round(float(v), 4)

        def _pa_rate(frame: pd.DataFrame, events: set) -> float | None:
            if len(frame) < MIN_PA:
                return None
            v = frame["events"].fillna("").isin(events).mean()
            return None if pd.isna(v) else round(float(v), 4)

        return {
            "batter_id": batter_id,
            "sample_pa": int(len(terminal_frame)),
            "sample_bbe": int(len(overall_bbe)),
            "handedness": self._player_handedness(batter_frame, "stand"),
            "xwoba": _bbe_stat(overall_bbe, "estimated_woba_using_speedangle"),
            "ev50": round(self._ev50(overall_bbe), 3) if len(overall_bbe) >= MIN_BBE else None,
            "hard_hit_pct": _hh(overall_bbe),
            "bb_pct": _pa_rate(terminal_frame, {"walk", "intent_walk"}),
            "k_pct": _pa_rate(terminal_frame, {"strikeout", "strikeout_double_play"}),
            "quality_of_contact": round(self._quality_of_contact(overall_bbe), 4),
            "recent_xwoba": _bbe_stat(recent_bbe_frame, "estimated_woba_using_speedangle"),
            "recent_ev50": round(self._ev50(recent_bbe_frame), 3) if len(recent_bbe_frame) >= MIN_BBE else None,
            "recent_hard_hit_pct": _hh(recent_bbe_frame),
            "recent_bb_pct": _pa_rate(recent_terminal_frame, {"walk", "intent_walk"}),
            "recent_k_pct": _pa_rate(recent_terminal_frame, {"strikeout", "strikeout_double_play"}),
            "recent_quality_of_contact": round(self._quality_of_contact(recent_bbe_frame), 4),
            "swing_path_score": round(self._swing_path_score(batter_frame), 3),
            "swing_path_tilt": round(self._nan_to_zero(pd.to_numeric(batter_frame.get("swing_path_tilt"), errors="coerce").mean()), 3),
            "attack_angle": round(self._nan_to_zero(pd.to_numeric(batter_frame.get("attack_angle"), errors="coerce").mean()), 3),
            "swing_length": round(self._nan_to_zero(pd.to_numeric(batter_frame.get("swing_length"), errors="coerce").mean()), 3),
            "pitch_profiles": profiles,
        }

    def compute_pitcher_matchup(
        self,
        batter_pitch_profiles: list[dict[str, Any]],
        pitcher_arsenal: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Usage-weighted matchup stats: K/BB/contact are independent of balls in play.
        Returns empty dict if coverage is too thin to be meaningful.
        """
        if not batter_pitch_profiles or not pitcher_arsenal:
            return {}

        usage: dict[str, float] = {}
        for pitch in pitcher_arsenal:
            pt = pitch.get("pitch_type")
            u = pitch.get("pitch_usage")
            if pt and u is not None:
                try:
                    usage[str(pt)] = float(u) / 100.0
                except (ValueError, TypeError):
                    pass
        total_usage = sum(usage.values())
        if total_usage <= 0:
            return {}
        usage = {k: v / total_usage for k, v in usage.items()}

        batter_by_pitch: dict[str, dict] = {
            str(p["pitch_type"]): p for p in batter_pitch_profiles if p.get("pitch_type")
        }

        w_xwoba = w_k = w_bb = w_hh = covered = 0.0
        for pt, weight in usage.items():
            bp = batter_by_pitch.get(pt)
            if bp:
                w_xwoba += weight * float(bp.get("xwoba") or 0.318)
                w_k     += weight * float(bp.get("k_pct") or 0.228)
                w_bb    += weight * float(bp.get("bb_pct") or 0.076)
                w_hh    += weight * float(bp.get("hard_hit_pct") or 0.375)
                covered += weight

        if covered < 0.20:
            return {}

        scale = 1.0 / covered
        k_risk    = min(0.55, max(0.05, w_k  * scale))
        bb_upside = min(0.25, max(0.02, w_bb * scale))
        hh        = min(0.75, max(0.10, w_hh * scale))
        xwoba     = min(0.600, max(0.100, w_xwoba * scale))
        contact_rate = max(0.0, 1.0 - k_risk - bb_upside)

        return {
            "matchup_xwoba":        round(xwoba, 4),
            "matchup_k_risk":       round(k_risk, 4),
            "matchup_bb_upside":    round(bb_upside, 4),
            "matchup_contact_rate": round(contact_rate, 4),
            "matchup_hard_hit_pct": round(hh, 4),
            "covered_pitch_weight": round(covered, 3),
        }

    def build_batter_summary_profile(self, batter_id: int, season: int) -> dict[str, Any]:
        expected = self._load_batter_expected_stats(season)
        percentiles = self._load_batter_percentiles(season)
        expected_row = expected[pd.to_numeric(expected.get("player_id"), errors="coerce") == batter_id]
        percentile_row = percentiles[pd.to_numeric(percentiles.get("player_id"), errors="coerce") == batter_id]
        if expected_row.empty and percentile_row.empty:
            return {
                "batter_id": batter_id,
                "sample_pa": 0,
                "sample_bbe": 0,
                "handedness": None,
                "xwoba": 0.0,
                "ev50": 0.0,
                "hard_hit_pct": 0.0,
                "bb_pct": None,
                "k_pct": None,
                "quality_of_contact": 0.0,
                "recent_xwoba": 0.0,
                "recent_ev50": 0.0,
                "recent_hard_hit_pct": 0.0,
                "recent_bb_pct": None,
                "recent_k_pct": None,
                "recent_quality_of_contact": 0.0,
                "attack_angle": 0.0,
                "swing_path_tilt": 0.0,
                "pitch_profiles": [],
            }
        expected_item = expected_row.iloc[0].to_dict() if not expected_row.empty else {}
        percentile_item = percentile_row.iloc[0].to_dict() if not percentile_row.empty else {}
        est_woba = expected_item.get("est_woba")
        xwoba_val = round(self._nan_to_zero(est_woba), 4) if est_woba is not None and not (isinstance(est_woba, float) and pd.isna(est_woba)) else None
        return {
            "batter_id": batter_id,
            "sample_pa": 0,
            "sample_bbe": 0,
            "handedness": None,
            "xwoba": xwoba_val,
            "ev50": None,
            "hard_hit_pct": None,
            "bb_pct": None,
            "k_pct": None,
            "quality_of_contact": 0.0,
            "recent_xwoba": xwoba_val,
            "recent_ev50": None,
            "recent_hard_hit_pct": None,
            "recent_bb_pct": None,
            "recent_k_pct": None,
            "recent_quality_of_contact": 0.0,
            "attack_angle": 0.0,
            "swing_path_tilt": 0.0,
            "pitch_profiles": [],
        }
