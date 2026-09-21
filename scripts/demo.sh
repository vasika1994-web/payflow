#!/usr/bin/env bash
# Сквозная проверка на поднятом compose (профиль demo):
#   1. платёж с рабочим webhook_url -> succeeded|failed, уведомление в webhook-sink
#   2. платёж с webhook_url на /fail -> три попытки, сообщение в DLQ
# При расхождении с ожидаемым выходит с ненулевым кодом, используется в CI.
set -euo pipefail

# порты из того же .env, что читает compose
cd "$(dirname "$0")/.."
if [ -f .env ]; then set -a; . ./.env; set +a; fi

API_URL="${API_URL:-http://localhost:${API_PORT:-8000}}"
API_KEY="${API_KEY:-local-dev-api-key}"
SINK_URL="${SINK_URL:-http://localhost:${WEBHOOK_SINK_PORT:-9000}}"
RABBITMQ_MGMT_URL="${RABBITMQ_MGMT_URL:-http://guest:guest@localhost:${RABBITMQ_MANAGEMENT_PORT:-15672}}"
# адрес приёмника изнутри сети compose
SINK_INTERNAL_URL="${SINK_INTERNAL_URL:-http://webhook-sink:9000}"

command -v jq >/dev/null || { echo "нужен jq"; exit 1; }

api() { curl -sS -H "X-API-Key: $API_KEY" "$@"; }

create_payment() {
    local key="$1" webhook="$2"
    api -X POST "$API_URL/api/v1/payments" \
        -H "Idempotency-Key: $key" -H "Content-Type: application/json" \
        -d "{\"amount\": \"1500.00\", \"currency\": \"RUB\", \"description\": \"Заказ #1042\",
             \"metadata\": {\"order_id\": 1042}, \"webhook_url\": \"$webhook\"}"
}

wait_for() {  # wait_for <секунд> <jq-условие> <payment_id>
    local seconds="$1" condition="$2" id="$3" body
    for _ in $(seq 1 "$seconds"); do
        body=$(api "$API_URL/api/v1/payments/$id")
        if echo "$body" | jq -e "$condition" >/dev/null; then echo "$body"; return 0; fi
        sleep 1
    done
    echo "$body"; return 1
}

expect() {  # expect <описание> <jq-условие> <json>
    if echo "$3" | jq -e "$2" >/dev/null; then echo "  ✓ $1"; else echo "  ✗ $1"; echo "$3" | jq .; exit 1; fi
}

run_id=$(date +%s)

echo "1. Платёж с рабочим webhook"
created=$(create_payment "demo-ok-$run_id" "$SINK_INTERNAL_URL/webhook")
expect "принят со статусом pending" '.status == "pending"' "$created"
id=$(echo "$created" | jq -r .payment_id)

replay=$(create_payment "demo-ok-$run_id" "$SINK_INTERNAL_URL/webhook")
expect "повтор с тем же ключом вернул тот же payment_id" ".payment_id == \"$id\"" "$replay"

final=$(wait_for 20 '.status != "pending" and .webhook_delivered_at != null' "$id") \
    || { echo "  ✗ не дождались обработки"; echo "$final" | jq .; exit 1; }
expect "обработан: $(echo "$final" | jq -r .status)" '.processed_at != null' "$final"
expect "уведомление доставлено" '.webhook_delivered_at != null' "$final"

received=$(curl -sS "$SINK_URL/received")
expect "webhook-sink получил уведомление" "map(select(.body.payment_id == \"$id\")) | length == 1" "$received"

echo
echo "2. Платёж с webhook, который всегда отвечает 500 → retry → DLQ"
before=$(curl -sS "$RABBITMQ_MGMT_URL/api/queues/%2F/payments.dlq" | jq '.messages // 0')
created=$(create_payment "demo-fail-$run_id" "$SINK_INTERNAL_URL/fail")
id=$(echo "$created" | jq -r .payment_id)

final=$(wait_for 20 '.status != "pending"' "$id") || { echo "  ✗ не дождались шлюза"; exit 1; }
expect "шлюз отработал ($(echo "$final" | jq -r .status)), уведомление не доставлено" \
    '.webhook_delivered_at == null' "$final"

echo "  ждём три попытки (задержки 1 с и 2 с)..."
for _ in $(seq 1 20); do
    after=$(curl -sS "$RABBITMQ_MGMT_URL/api/queues/%2F/payments.dlq" | jq '.messages // 0')
    [ "$after" -gt "$before" ] && break
    sleep 1
done
[ "$after" -gt "$before" ] && echo "  ✓ сообщение в payments.dlq (было $before, стало $after)" \
    || { echo "  ✗ в DLQ ничего не пришло"; exit 1; }

attempts=$(curl -sS "$SINK_URL/received" | jq "map(select(.body.payment_id == \"$id\")) | length")
[ "$attempts" -eq 3 ] && echo "  ✓ webhook-sink видел ровно 3 попытки" \
    || { echo "  ✗ попыток доставки: $attempts, ожидалось 3"; exit 1; }

echo
echo "Всё сошлось. DLQ: $RABBITMQ_MGMT_URL/#/queues/%2F/payments.dlq" | sed 's#guest:guest@##'
