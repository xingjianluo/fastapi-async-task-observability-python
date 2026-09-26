"""下游依赖客户端：把底层异常/超时归一化为可区分错误，不原样透出。

归一化规则：
- asyncio.TimeoutError / 内置超时  -> DownstreamTimeout (downstream_timeout)
- 其他底层异常                     -> DownstreamError  (downstream_error)，
  底层异常仅作为 cause 记录，消息不外泄。
"""

from __future__ import annotations

import asyncio

from app.context import current_context
from app.errors import DownstreamError, DownstreamTimeout
from app.logging import get_logger, log_event

logger = get_logger()


async def call_downstream(
    payload: str,
    *,
    delay: float = 0.0,
    fail: bool = False,
    timeout: float | None = None,
) -> dict:
    """模拟一次下游调用，并对失败做归一化。

    delay: 下游模拟耗时（秒）；fail: 下游模拟返回内部错误；
    timeout: 本次调用允许的最长等待，超时归一化为 DownstreamTimeout。
    """
    ctx = current_context()
    try:
        return await asyncio.wait_for(_invoke(payload, delay=delay, fail=fail), timeout=timeout)
    except asyncio.TimeoutError as exc:
        log_event(
            logger,
            30,  # WARNING
            "downstream call timed out",
            decision="normalize_error",
            category="downstream_timeout",
            reason=f"exceeded_timeout_seconds={timeout}",
        )
        raise DownstreamTimeout(
            "downstream call timed out",
            reason=f"exceeded_timeout_seconds={timeout}",
            cause=exc,
        ) from exc
    except (DownstreamError, DownstreamTimeout):
        raise
    except Exception as exc:
        log_event(
            logger,
            40,  # ERROR
            "downstream call failed",
            decision="normalize_error",
            category="downstream_error",
            reason=f"underlying={type(exc).__name__}",
        )
        raise DownstreamError(
            "downstream call failed",
            reason=f"underlying={type(exc).__name__}",
            cause=exc,
        ) from exc


async def _invoke(payload: str, *, delay: float, fail: bool) -> dict:
    await asyncio.sleep(delay)
    if fail:
        raise RuntimeError("simulated downstream 500: internal db exploded")
    return {"echo": payload, "ok": True}
