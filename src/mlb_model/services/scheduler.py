from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from mlb_model.config import get_settings
from mlb_model.services.orchestrator import ModelOrchestrator


class SchedulerService:
    def __init__(self, orchestrator: ModelOrchestrator) -> None:
        self.settings = get_settings()
        self.scheduler = AsyncIOScheduler(timezone=ZoneInfo("America/New_York"))
        self.orchestrator = orchestrator

    def start(self) -> None:
        self.scheduler.add_job(
            self._morning_refresh,
            CronTrigger(hour=self.settings.morning_refresh_hour_et, minute=0),
            id="morning_refresh",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._lineup_refresh,
            CronTrigger(hour=self.settings.lineup_poll_hour_et, minute="*/15"),
            id="lineup_refresh",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._night_refresh,
            CronTrigger(hour=1, minute=30),
            id="night_refresh",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.orchestrator.recalibrate_weights,
            CronTrigger(hour=3, minute=0),
            id="recalibration",
            replace_existing=True,
        )
        self.scheduler.start()

    async def _morning_refresh(self) -> None:
        await self.orchestrator.rebuild_slate(date.today())

    async def _lineup_refresh(self) -> None:
        await self.orchestrator.rebuild_slate(date.today())

    async def _night_refresh(self) -> None:
        await self.orchestrator.rebuild_slate(date.today())
