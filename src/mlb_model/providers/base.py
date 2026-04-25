from __future__ import annotations

from datetime import date
from typing import Any, Protocol


class Provider(Protocol):
    async def healthcheck(self) -> dict[str, Any]:
        ...


class SlateProvider(Protocol):
    async def fetch_slate(self, slate_date: date) -> list[dict[str, Any]]:
        ...
