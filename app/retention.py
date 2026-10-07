"""任务保留与回收：终态任务按保留期自动清理，历史总量设上限，幂等键可复用。"""
from __future__ import annotations

import asyncio
import dataclasses
import time

from .context import ensure_correlation_id
from .logging_config import get_logger
from .store import TaskRecord, TaskStore


@dataclasses.dataclass
class RetentionConfig:
    """保留策略配置。"""

    retention_seconds: float = 3600.0  # 终态任务保留时长
    max_history: int = 10000           # 终态历史总量上限，超出按最旧优先淘汰
    sweep_interval: float = 60.0       # 后台回收扫描间隔


@dataclasses.dataclass
class RetentionStats:
    """回收可观测指标。"""

    total_reclaimed: int = 0
    last_reclaim_at: float | None = None  # 最近一次发生回收的墙钟时间
    sweep_count: int = 0

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


class TaskReaper:
    """后台回收器：周期扫描存储，清理到期终态任务并淘汰超限历史。

    每轮回收生成独立关联标识，日志携带每条记录的判定依据
    （到期回收 / 超限淘汰、终态、finished_at、留存时长），
    运维可按关联标识核对一轮回收的完整决策链。
    """

    def __init__(
        self,
        store: TaskStore,
        config: RetentionConfig | None = None,
        *,
        wall_clock=time.time,
    ) -> None:
        self._store = store
        self._config = config or RetentionConfig()
        self._wall_clock = wall_clock
        self._stats = RetentionStats()
        self._task: asyncio.Task | None = None
        self._log = get_logger("retention")

    async def sweep_once(self) -> list[TaskRecord]:
        """执行一轮回收：先按保留期清理到期终态，再按总量上限淘汰最旧终态。"""
        ensure_correlation_id()
        cfg = self._config
        expired = await self._store.reap_expired_terminal(cfg.retention_seconds)
        evicted = await self._store.evict_oldest_terminal(cfg.max_history)
        reclaimed = expired + evicted

        self._stats.sweep_count += 1
        now_wall = self._wall_clock()
        for record in expired:
            self._log.info(
                "task reclaimed",
                extra={
                    "obs_task_id": record.task_id,
                    "obs_idempotency_key": record.idempotency_key,
                    "obs_status": record.status.value,
                    "obs_finished_at": record.finished_at,
                    "obs_reason": (
                        f"terminal task exceeded retention_seconds={cfg.retention_seconds}"
                    ),
                },
            )
        for record in evicted:
            self._log.info(
                "task reclaimed",
                extra={
                    "obs_task_id": record.task_id,
                    "obs_idempotency_key": record.idempotency_key,
                    "obs_status": record.status.value,
                    "obs_finished_at": record.finished_at,
                    "obs_reason": f"history exceeded max_history={cfg.max_history}, oldest first",
                },
            )
        if reclaimed:
            self._stats.total_reclaimed += len(reclaimed)
            self._stats.last_reclaim_at = now_wall
        self._log.info(
            "sweep done",
            extra={
                "obs_expired": len(expired),
                "obs_evicted": len(evicted),
                "obs_total_reclaimed": self._stats.total_reclaimed,
            },
        )
        return reclaimed

    async def start(self) -> None:
        """启动后台周期回收（幂等：重复调用不重复建任务）。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop(), name="task-reaper")
        self._log.info(
            "reaper started",
            extra={
                "obs_retention_seconds": self._config.retention_seconds,
                "obs_max_history": self._config.max_history,
                "obs_sweep_interval": self._config.sweep_interval,
            },
        )

    async def stop(self) -> None:
        """停止后台回收并等待退出，不泄漏后台任务。"""
        if self._task is None:
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self._log.info("reaper stopped")

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.sweep_interval)
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 回收失败不拖垮服务，下轮重试
                self._log.error(
                    "sweep failed",
                    extra={"obs_reason": f"normalized from {type(exc).__name__}: {exc}"},
                )

    def stats(self) -> RetentionStats:
        return self._stats
