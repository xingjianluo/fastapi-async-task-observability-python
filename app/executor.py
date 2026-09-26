"""任务执行器：容量背压、并发子操作、超时与优雅停机。"""
from __future__ import annotations

import asyncio

from .context import set_task_id, reset_task_id
from .downstream import DownstreamClient
from .errors import (
    AppError,
    CapacityExceeded,
    InvalidStateTransition,
    TaskCancelled,
    TaskTimeout,
    normalize_exception,
)
from .logging_config import get_logger
from .statemachine import TaskStatus
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
        self._queued = 0
        self._accepting = True
        self._lock = asyncio.Lock()
        self._log = get_logger("executor")

    async def enqueue(self, task_id: str) -> None:
        """排队执行；积压超过上限时立即拒绝（不无限等待）。"""
        async with self._lock:
            if not self._accepting:
                raise CapacityExceeded(
                    "executor is shutting down",
                    reason="reject: not accepting new tasks",
                )
            if self._queued >= self._config.max_pending:
                raise CapacityExceeded(
                    "pending queue is full",
                    reason=(
                        f"reject: queued={self._queued} "
                        f">= max_pending={self._config.max_pending}"
                    ),
                )
            self._queued += 1
        task = asyncio.create_task(self._run(task_id), name=f"task-{task_id}")
        self._inflight[task_id] = task
        self._log.info(
            "task enqueued",
            extra={"obs_queued": self._queued, "obs_inflight": len(self._inflight)},
        )

    async def shutdown(self) -> None:
        """优雅停机：在限定时间内收尾在途任务，超时进入明确终态。"""
        async with self._lock:
            self._accepting = False
        tasks = list(self._inflight.values())
        self._log.info(
            "shutdown start",
            extra={"obs_inflight": len(tasks), "obs_grace": self._config.shutdown_grace},
        )
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=self._config.shutdown_grace)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._log.info(
            "shutdown done",
            extra={"obs_finished": len(done), "obs_cancelled": len(pending)},
        )

    @property
    def inflight_count(self) -> int:
        return len(self._inflight)

    async def cancel(self, task_id: str) -> bool:
        """取消在途任务（排队或运行中）。返回是否命中了在途任务。"""
        task = self._inflight.get(task_id)
        if task is None:
            return False
        task.cancel()
        self._log.info("cancel requested", extra={"obs_task_id": task_id})
        return True

    async def _run(self, task_id: str) -> None:
        token = set_task_id(task_id)
        try:
            try:
                await self._semaphore.acquire()
            except asyncio.CancelledError:
                # 停机时仍在排队的任务：进入明确终态，不执行
                self._queued -= 1
                await self._safe_transition(
                    task_id,
                    TaskStatus.CANCELLED,
                    error=TaskCancelled(
                        "cancelled while queued",
                        reason="shutdown before slot acquired",
                    ),
                )
                return
            self._queued -= 1
            try:
                record = await self._store.get(task_id)
                try:
                    record = await self._store.transition(task_id, TaskStatus.RUNNING)
                except InvalidStateTransition as exc:
                    # 排队期间已被取消/迁移：不执行，避免半完成结果与重复副作用
                    self._log.info(
                        "skip execution",
                        extra={"obs_reason": exc.reason},
                    )
                    return
                self._log.info("task running", extra={"obs_attempt": record.attempt})
                try:
                    async with asyncio.timeout(self._config.task_timeout):
                        result = await self._downstream.fan_out(record.payload)
                    await self._store.transition(
                        task_id, TaskStatus.SUCCEEDED, result={"items": result}
                    )
                    self._log.info("task succeeded")
                except TimeoutError:
                    await self._safe_transition(
                        task_id,
                        TaskStatus.TIMED_OUT,
                        error=TaskTimeout(
                            "task exceeded its deadline",
                            reason=f"task_timeout={self._config.task_timeout}s exceeded",
                        ),
                    )
                    self._log.warning("task timed out")
                except asyncio.CancelledError:
                    # 停机取消：进入明确终态，不重复执行
                    await self._safe_transition(
                        task_id,
                        TaskStatus.CANCELLED,
                        error=TaskCancelled(
                            "cancelled while running",
                            reason="shutdown grace exceeded",
                        ),
                    )
                    self._log.warning("task cancelled by shutdown")
                except AppError as exc:
                    await self._safe_transition(task_id, TaskStatus.FAILED, error=exc)
                    self._log.warning(
                        "task failed",
                        extra={"obs_category": exc.category.value, "obs_reason": exc.reason},
                    )
                except Exception as exc:  # noqa: BLE001 - 边界处统一归一化
                    normalized = normalize_exception(exc)
                    await self._safe_transition(task_id, TaskStatus.FAILED, error=normalized)
                    self._log.warning(
                        "task failed",
                        extra={
                            "obs_category": normalized.category.value,
                            "obs_reason": normalized.reason,
                        },
                    )
            finally:
                self._semaphore.release()
        finally:
            reset_task_id(token)
            self._inflight.pop(task_id, None)

    async def _safe_transition(
        self, task_id: str, target: TaskStatus, *, error: AppError
    ) -> None:
        """终态迁移的兜底：已处终态时记录日志而非抛出。"""
        try:
            await self._store.transition(task_id, target, error=error)
        except InvalidStateTransition as exc:
            self._log.info(
                "transition skipped",
                extra={"obs_reason": exc.reason, "obs_target": target.value},
            )
