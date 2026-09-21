"""structlog + стандартный logging через один обработчик: JSON в проде, консоль локально."""

from __future__ import annotations

import logging
import sys

import structlog
from structlog.typing import FilteringBoundLogger


def configure_logging(level: str = "INFO", *, json_output: bool = True) -> None:
    resolved_level = logging.getLevelNamesMapping()[level.upper()]

    if json_output:
        renderer = structlog.processors.JSONRenderer(ensure_ascii=False)
    else:
        renderer = structlog.dev.ConsoleRenderer()

    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
            # ExtraAdder: поля из extra= (FastStream кладёт туда очередь и message_id) идут в запись
            foreign_pre_chain=[structlog.stdlib.ExtraAdder(), *shared],
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolved_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            *shared,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(resolved_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    return structlog.get_logger(name)
