"""Приёмник вебхуков для демо (профиль compose demo). Хранит всё в памяти.

POST /webhook -> 200, POST /fail -> всегда 500, GET /received -> что пришло.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

KEEP_LAST = 500

app = FastAPI(title="Webhook sink", docs_url=None, redoc_url=None)
received: list[dict[str, Any]] = []


async def _remember(request: Request, path: str) -> None:
    try:
        body = await request.json()
    except ValueError:
        body = (await request.body()).decode(errors="replace")
    received.insert(
        0,
        {
            "received_at": datetime.now(UTC).isoformat(),
            "path": path,
            "headers": {k: v for k, v in request.headers.items() if k.startswith("x-")},
            "body": body,
        },
    )
    del received[KEEP_LAST:]


@app.post("/webhook")
async def accept(request: Request) -> dict[str, str]:
    await _remember(request, "/webhook")
    return {"status": "received"}


@app.post("/fail")
async def refuse(request: Request) -> JSONResponse:
    await _remember(request, "/fail")
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"status": "boom"})


@app.get("/received")
async def list_received() -> list[dict[str, Any]]:
    return received
