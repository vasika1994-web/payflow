from __future__ import annotations

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api.deps import require_api_key
from app.core.db import session_scope, transaction

# ключ нужен и тут (по ТЗ на всех эндпоинтах), healthcheck в compose его передаёт
router = APIRouter(prefix="/health", tags=["Служебное"], dependencies=[Depends(require_api_key)])


@router.get(
    "",
    summary="Сервис жив и база отвечает",
    responses={503: {"description": "База недоступна"}},
)
async def health() -> JSONResponse:
    try:
        async with session_scope() as session, transaction(session):
            await session.execute(text("SELECT 1"))
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"status": "unavailable"}
        )
    return JSONResponse(content={"status": "ok"})
