"""任务保留与回收：终态任务按保留期自动清理，历史总量设上限，支持手动释放。"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import Callable

from .logging_config import get_logger
from .store import TaskRecord, TaskStore


@dataclasses.dataclass
class RetentionConfig:
    """保留策略配置。"""

    retention_seconds: float = 3600.0
    max_history: int = 10000
    sweep_interval: float = 60.0


@dataclasses.dataclass
class RetentionStats:
    """回收可观测指标（进程内累计）。"""

    total_reclaimed: int = 0
    sweep_count: int = 0
    last_sweep_at: float | None = None
    last_reclaimed: int = 0

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


class RetentionSweeper:
    """后台周期回收器：调用存储的 sweep_terminal，并维护可观测指标。"""

    def __init__(
        self,
        store: TaskStore,
        config: RetentionConfig | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._config = config or RetentionConfig()
        self._clock = clock
        self._stats = RetentionStats()
        self._task: asyncio.Task | None = None
        self._log = get_logger("retention")

    @property
    def stats(self) -> RetentionStats:
        return self._stats

    async def sweep_once(self) -> list[TaskRecord]:
        """执行一次回收：清理到期终态任务并按容量上限淘汰，返回被清理的记录。

        每次回收都更新可观测指标（累计回收数、最近回收时间），
        并输出携带判定依据的日志，可按关联标识与既有链路核对。
        """
        now = self._clock()
        reclaimed = await self._store.sweep_terminal(
            now=now,
            retention_seconds=self._config.retention_seconds,
            max_history=self._config.max_history,
        )
        self._stats.sweep_count += 1
        self._stats.last_sweep_at = now
        self._stats.last_reclaimed = len(reclaimed)
        self._stats.total_reclaimed += len(reclaimed)
        self._log.info(
            "retention sweep done",
            extra={
                "obs_reclaimed": len(reclaimed),
                "obs_total_reclaimed": self._stats.total_reclaimed,
                "obs_sweep_count": self._stats.sweep_count,
                "obs_retention_seconds": self._config.retention_seconds,
                "obs_max_history": self._config.max_history,
                "obs_reclaimed_task_ids": [r.task_id for r in reclaimed],
                "obs_reason": (
                    "terminal tasks past retention or beyond max_history reclaimed"
                    if reclaimed
                    else "no terminal task eligible for reclaim"
                ),
            },
        )
        return reclaimed

    def start(self) -> None:
        """启动后台周期回收（幂等：重复调用不重复启动）。"""
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop(), name="retention-sweeper")
        self._log.info(
            "retention sweeper started",
            extra={"obs_sweep_interval": self._config.sweep_interval},
        )

    async def stop(self) -> None:
        """停止后台周期回收：取消循环并等待退出，不泄漏后台任务。"""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self._log.info("retention sweeper stopped")

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.sweep_interval)
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 回收失败不拖垮服务，下轮重试
                self._log.error(
                    "retention sweep failed",
                    extra={"obs_reason": f"normalized from {type(exc).__name__}: {exc}"},
                )
