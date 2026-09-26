"""结构化 JSON 日志：每条日志携带关联标识与判定依据。"""
from __future__ import annotations

import json
import logging
import sys
import time

from .context import get_correlation_id, get_task_id


class _ContextFilter(logging.Filter):
    """把链路上下文注入每条日志记录。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id() or "-"
        record.task_id = get_task_id() or "-"
        return True


class JsonFormatter(logging.Formatter):
    """输出单行 JSON，便于检索与验证。"""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": round(record.created, 6),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", "-"),
            "task_id": getattr(record, "task_id", "-"),
        }
        for key, value in record.__dict__.items():
            if key.startswith("obs_"):  # 业务侧以 obs_ 前缀传递判定依据
                payload[key[4:]] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: int = logging.INFO) -> None:
    """配置结构化 JSON 日志输出（幂等，可重复调用）。"""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(_ContextFilter())
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
