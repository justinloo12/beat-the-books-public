from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from mlb_model.config import get_settings
from mlb_model.db import init_db
from mlb_model.services.daily_model import DailyPredictionService
from mlb_model.services.orchestrator import ModelOrchestrator
from mlb_model.services.scheduler import SchedulerService
from mlb_model.services.site_service import SiteService


settings = get_settings()
orchestrator = ModelOrchestrator()
daily_service = DailyPredictionService()
site_service = SiteService()
scheduler = SchedulerService(orchestrator)
DOCS_DIR = Path(__file__).resolve().parents[2] / "docs"


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    scheduler.start()
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
if DOCS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=DOCS_DIR), name="assets")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "app": settings.app_name}


@app.get("/")
async def site_index() -> FileResponse:
    return FileResponse(DOCS_DIR / "index.html")


@app.get("/styles.css")
async def site_styles() -> FileResponse:
    return FileResponse(DOCS_DIR / "styles.css")


@app.get("/app.js")
async def site_script() -> FileResponse:
    return FileResponse(DOCS_DIR / "app.js")


@app.get("/data/{file_name}")
async def site_data_file(file_name: str) -> FileResponse:
    return FileResponse(DOCS_DIR / "data" / file_name)


@app.post("/slate/{slate_date}")
async def rebuild_slate(slate_date: date) -> dict:
    return await orchestrator.rebuild_slate(slate_date)


@app.get("/dashboard/{slate_date}")
async def dashboard(slate_date: date) -> dict:
    return (await orchestrator.dashboard(slate_date)).model_dump()


@app.post("/evaluate")
async def evaluate(payload: dict) -> dict:
    picks = await orchestrator.evaluate_game(payload["game_context"], payload["market_bundle"], persist=True)
    return {"picks": picks, "count": len(picks)}


@app.post("/recalibrate")
async def recalibrate() -> dict:
    return await orchestrator.recalibrate_weights()


@app.get("/daily-picks/{slate_date}")
async def daily_picks(slate_date: date) -> dict:
    return await daily_service.daily_picks(slate_date)


@app.get("/api/site/{slate_date}")
async def site_payload(slate_date: date) -> dict:
    return await site_service.site_payload(slate_date)


@app.get("/api/site/today")
async def site_payload_today() -> dict:
    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    return await site_service.site_payload(today_et)
