from __future__ import annotations

import httpx

from app.schemas.payments import WebhookPayload


class WebhookDeliveryError(Exception):
    pass


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def send(self, url: str, payload: WebhookPayload, *, attempt: int) -> int:
        """Доставлено = любой 2xx. X-Attempt - номер попытки обработки, не только доставки."""
        headers = {
            "X-Payment-Id": str(payload.payment_id),
            "X-Attempt": str(attempt),
        }
        try:
            response = await self.client.post(url, content=payload.model_dump_json(), headers=headers)
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if not response.is_success:
            raise WebhookDeliveryError(f"получатель ответил {response.status_code}")
        return response.status_code


def make_http_client(
    timeout_seconds: float, transport: httpx.AsyncBaseTransport | None = None
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=transport,
        timeout=httpx.Timeout(timeout_seconds),
        headers={"Content-Type": "application/json", "User-Agent": "payment-processing/1.0"},
        # шлём ровно на тот адрес, что дал клиент
        follow_redirects=False,
    )
