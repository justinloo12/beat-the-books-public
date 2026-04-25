from __future__ import annotations

from typing import Any


class UmpireProvider:
    async def healthcheck(self) -> dict[str, Any]:
        return {"provider": "umpire", "status": "stub"}

    async def fetch_assignments(self) -> list[dict[str, Any]]:
        return []
