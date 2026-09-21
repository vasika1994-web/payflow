from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Response, status

from app.api.deps import IdempotencyKeyDep, SessionDep, require_api_key
from app.api.errors import ApiError
from app.core.db import transaction
from app.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentAccepted, PaymentCreate, PaymentDetails
from app.services.create_payment import IdempotencyConflictError, create_payment

router = APIRouter(prefix="/api/v1/payments", tags=["Платежи"], dependencies=[Depends(require_api_key)])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Создать платёж",
    description=(
        "Платёж записывается в статусе `pending` и ставится в очередь на обработку. "
        "Ответ приходит сразу, результат обработки придёт на `webhook_url`.\n\n"
        "Повтор с тем же `Idempotency-Key` и тем же телом возвращает тот же платёж "
        "и заголовок `Idempotent-Replayed: true`, тот же ключ с другим телом: "
        "**409** `idempotency_conflict`."
    ),
    responses={
        400: {"description": "Нет заголовка Idempotency-Key или он не по формату"},
        401: {"description": "Неверный или отсутствующий X-API-Key"},
        409: {"description": "Ключ идемпотентности уже использован с другим телом"},
        422: {"description": "Тело не прошло валидацию"},
    },
)
async def post_payment(
    data: PaymentCreate, idempotency_key: IdempotencyKeyDep, session: SessionDep, response: Response
) -> PaymentAccepted:
    try:
        async with transaction(session):
            result = await create_payment(session, data, idempotency_key)
    except IdempotencyConflictError:
        raise ApiError(
            status.HTTP_409_CONFLICT,
            "idempotency_conflict",
            f"ключ идемпотентности {idempotency_key!r} уже использован для другого запроса",
        ) from None

    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    payment = result.payment
    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@router.get(
    "/{payment_id}",
    response_model=PaymentDetails,
    summary="Получить платёж",
    responses={
        401: {"description": "Неверный или отсутствующий X-API-Key"},
        404: {"description": "Платежа с таким id нет"},
    },
)
async def get_payment(payment_id: str, session: SessionDep) -> PaymentDetails:
    # невалидный uuid = 404, а не 422
    try:
        parsed = uuid.UUID(payment_id)
    except ValueError:
        parsed = None
    payment = None
    if parsed is not None:
        async with transaction(session):
            payment = await PaymentRepository(session).get(parsed)
    if payment is None:
        raise ApiError(status.HTTP_404_NOT_FOUND, "payment_not_found", f"платёж {payment_id} не найден")
    return PaymentDetails.model_validate(payment)
