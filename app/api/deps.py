from __future__ import annotations

import hmac
import re
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.docs import SECURITY_SCHEME_NAME
from app.api.errors import ApiError
from app.core.config import get_settings
from app.core.db import session_scope

# auto_error=False: ответ в общем формате ошибок, а не 403 от FastAPI
api_key_header = APIKeyHeader(
    name="X-API-Key",
    scheme_name=SECURITY_SCHEME_NAME,
    auto_error=False,
    description="Статический ключ доступа к API.",
)


async def require_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    expected = get_settings().api_key
    if not api_key or not hmac.compare_digest(api_key.encode(), expected.encode()):
        raise ApiError(status.HTTP_401_UNAUTHORIZED, "unauthorized", "неверный или отсутствующий X-API-Key")


IDEMPOTENCY_KEY_PATTERN = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


async def require_idempotency_key(
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description="Уникальный ключ запроса, до 128 символов из A-Za-z0-9_.:-. "
            "Повтор с тем же ключом и телом вернёт тот же платёж.",
        ),
    ] = None,
) -> str:
    if idempotency_key is None:
        raise ApiError(
            status.HTTP_400_BAD_REQUEST, "idempotency_key_missing", "нужен заголовок Idempotency-Key"
        )
    if not IDEMPOTENCY_KEY_PATTERN.fullmatch(idempotency_key):
        raise ApiError(
            status.HTTP_400_BAD_REQUEST,
            "idempotency_key_invalid",
            "Idempotency-Key: до 128 символов из A-Za-z0-9_.:-",
        )
    return idempotency_key


async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_scope() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKeyDep = Annotated[str, Depends(require_idempotency_key)]
