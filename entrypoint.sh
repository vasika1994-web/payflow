#!/bin/sh
# Миграции накатывает только api (RUN_MIGRATIONS=true), consumer и relay ждут его healthcheck.
set -e

if [ "${RUN_MIGRATIONS}" = "true" ]; then
    echo "Накатываю миграции..."
    alembic upgrade head
fi

exec "$@"
