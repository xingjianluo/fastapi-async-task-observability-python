"""下游依赖：模拟同步调用、失败与超时；异常在边界处被归一化。"""
from __future__ import annotations

import asyncio

from .errors import DownstreamError, DownstreamTimeout


class DownstreamClient:
    """可配置行为的模拟下游，用于生产逻辑与测试。"""

    def __init__(self) -> None:
        self.calls = 0

    async def call(self, payload: dict) -> dict:
        raise NotImplementedError


def raise_downstream_error(message: str, *, reason: str = "") -> None:
    raise DownstreamError(message, reason=reason)
