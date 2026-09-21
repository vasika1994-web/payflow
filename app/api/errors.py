"""Единый формат ошибок: {"error": {"code": ..., "message": ..., ...}}"""

from __future__ import annotations

from typing import Any

from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        headers: dict[str, str] | None = None,
        **extra: Any,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.headers = headers
        self.extra = extra


def error_response(
    status_code: int, code: str, message: str, headers: dict[str, str] | None = None, **extra: Any
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, **extra}},
        headers=headers,
    )


async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
    return error_response(exc.status_code, exc.code, exc.message, exc.headers, **exc.extra)


async def handle_http_exception(_request: Request, exc: HTTPException) -> JSONResponse:
    code = {
        status.HTTP_404_NOT_FOUND: "not_found",
        status.HTTP_405_METHOD_NOT_ALLOWED: "method_not_allowed",
    }.get(exc.status_code, "http_error")
    return error_response(exc.status_code, code, str(exc.detail), exc.headers)


async def handle_validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
    # исходные значения не возвращаем: на невалидном UTF-8 это роняло бы ответ
    fields: dict[str, list[str]] = {}
    for error in exc.errors():
        # loc = (body, поле, ...); числа - позиция в JSON или индекс, а не поле
        location = [str(part) for part in error["loc"] if part != "body" and not isinstance(part, int)]
        fields.setdefault(".".join(location) or "body", []).append(error["msg"])
    return error_response(
        status.HTTP_422_UNPROCESSABLE_CONTENT,
        "validation_failed",
        "проверка данных не пройдена",
        fields=fields,
    )
