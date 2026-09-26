"""下游依赖：模拟同步调用、失败与超时；异常在边界处被归一化。"""
from __future__ import annotations

import asyncio

from .context import get_correlation_id
from .errors import DownstreamError, DownstreamTimeout
from .logging_config import get_logger


class DownstreamClient:
    """可配置行为的模拟下游，用于生产逻辑与测试。

    fail_mode:
      - None：正常返回
      - "error"：抛出下游业务错误
      - "hang"：永不返回，用于触发超时归一化
    """

    def __init__(
        self,
        latency: float = 0.01,
        timeout: float = 1.0,
        fail_mode: str | None = None,
        fanout: int = 2,
    ) -> None:
        self.latency = latency
        self.timeout = timeout
        self.fail_mode = fail_mode
        self.fanout = fanout
        self.calls = 0
        self._log = get_logger("downstream")

    async def _single_call(self, payload: dict, index: int) -> dict:
        self.calls += 1
        call_no = self.calls
        self._log.info(
            "downstream call start",
            extra={"obs_call_no": call_no, "obs_index": index},
        )
        try:
            async with asyncio.timeout(self.timeout):
                if self.fail_mode == "error":
                    raise DownstreamError(
                        "downstream rejected the request",
                        reason="simulated downstream 5xx",
                    )
                if self.fail_mode == "hang":
                    await asyncio.sleep(3600)
                await asyncio.sleep(self.latency)
                return {"call_no": call_no, "index": index, "echo": payload}
        except TimeoutError as exc:
            # 超时归一化：不透出原始 asyncio 异常
            raise DownstreamTimeout(
                "downstream call timed out",
                reason=f"exceeded timeout={self.timeout}s on call_no={call_no}",
            ) from exc

    async def call(self, payload: dict) -> dict:
        """单次下游调用。"""
        return await self._single_call(payload, 0)

    async def fan_out(self, payload: dict) -> list[dict]:
        """并发子操作：同一请求的多个下游调用共享关联标识。"""
        cid = get_correlation_id()
        self._log.info(
            "fan-out start",
            extra={"obs_fanout": self.fanout, "obs_correlation": cid},
        )
        results = await asyncio.gather(
            *(self._single_call(payload, i) for i in range(self.fanout))
        )
        self._log.info("fan-out done", extra={"obs_results": len(results)})
        return list(results)


def raise_downstream_error(message: str, *, reason: str = "") -> None:
    raise DownstreamError(message, reason=reason)
