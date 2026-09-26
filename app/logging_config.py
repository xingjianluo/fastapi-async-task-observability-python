"""结构化 JSON 日志：每条日志携带关联标识与判定依据。"""
from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """配置结构化 JSON 日志输出。"""
    raise NotImplementedError


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
