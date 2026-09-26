"""任务执行器：容量背压、并发子操作、超时与优雅停机。"""
from __future__ import annotations

import asyncio

from .downstream import DownstreamClient
from .logging_config import get_logger
from .store import TaskStore


class ExecutorConfig:
    def __init__(
        self,
        max_workers: int = 4,
        max_pending: int = 8,
        task_timeout: float = 5.0,
        shutdown_grace: float = 5.0,
    ) -> None:
        self.max_workers = max_workers
        self.max_pending = max_pending
        self.task_timeout = task_timeout
        self.shutdown_grace = shutdown_grace


class TaskExecutor:
    def __init__(
        self,
        store: TaskStore,
        downstream: DownstreamClient,
        config: ExecutorConfig | None = None,
    ) -> None:
        self._store = store
        self._downstream = downstream
        self._config = config or ExecutorConfig()
        self._semaphore = asyncio.Semaphore(self._config.max_workers)
        self._inflight: dict[str, asyncio.Task] = {}
        self._shutdown = asyncio.Event()
        self._log = get_logger("executor")

    async def enqueue(self, task_id: str) -> None:
        """排队执行；积压超过上限时立即拒绝（不无限等待）。"""
        raise NotImplementedError

    async def shutdown(self) -> None:
        """优雅停机：在限定时间内收尾在途任务，超时进入明确终态。"""
        raise NotImplementedError

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)
