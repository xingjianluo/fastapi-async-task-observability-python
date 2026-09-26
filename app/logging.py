"""结构化日志：所有日志输出关联标识与判定依据。"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from app.context import current_context

_LOGGER_NAME = "app"


class StructuredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            ctx = current_context()
            correlation_id = ctx.correlation_id
            task_id = ctx.task_id
            span = ctx.span
        except Exception:
            correlation_id = getattr(record, "correlation_id", None)
            task_id = getattr(record, "task_id", None)
            span = getattr(record, "span", None)
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id,
            "task_id": task_id,
            "span": span,
        }
        for key, value in (getattr(record, "fields", None) or {}).items():
            payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """输出一条结构化日志，fields 中应包含判定依据（decision/reason 等）。"""
    logger.log(level, message, extra={"fields": fields})
