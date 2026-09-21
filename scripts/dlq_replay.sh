#!/usr/bin/env bash
# Забирает сообщения из payments.dlq и кладёт их обратно в payments.new с attempt=1.
# Использовать после того, как причина (получатель webhook, база, шлюз) починена.
set -euo pipefail

cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; . ./.env; set +a; fi

MGMT="${RABBITMQ_MGMT_URL:-http://guest:guest@localhost:${RABBITMQ_MANAGEMENT_PORT:-15672}}"
command -v jq >/dev/null || { echo "нужен jq"; exit 1; }

count=0
while :; do
    msg=$(curl -sS -X POST "$MGMT/api/queues/%2F/payments.dlq/get" -H 'Content-Type: application/json' \
        -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}' | jq -c '.[0] // empty')
    [ -z "$msg" ] && break

    payment_id=$(echo "$msg" | jq -r '.payload | fromjson | .payment_id')
    body=$(jq -cn --arg id "$payment_id" '{payment_id: $id, attempt: 1}')
    curl -sS -X POST "$MGMT/api/exchanges/%2F/payments/publish" -H 'Content-Type: application/json' \
        -d "$(jq -cn --arg body "$body" '{properties: {content_type: "application/json", delivery_mode: 2}, routing_key: "payments.new", payload: $body, payload_encoding: "string"}')" >/dev/null
    echo "$payment_id"
    count=$((count + 1))
done
echo "переложено: $count"
