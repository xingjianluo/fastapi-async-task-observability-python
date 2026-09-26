"""任务执行器：有界容量背压、worker 调度、取消/超时、优雅停机。

并发模型：
- submit/cancel 等状态变更在 manager 锁内串行化，worker 只在迁移状态时持锁；
- 任务在独立协程中执行，超时用 asyncio.wait_for，取消用 asyncio.Task.cancel；
- 幂等判定与容量检查在同一把 manager 锁内完成，避免并发下重复副作用；
- contextvars 随 asyncio 任务传播，子操作通过 child 上下文携带同一 correlation_id。
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable

from app.config import Settings
from app.context import TraceContext, current_context, use_context
from app.downstream import call_downstream
from app.errors import (
    AppError,
    InvalidStateTransitionError,
    QueueFullError,
    ShuttingDownError,
    TaskNotFoundError,
    TaskTimeoutError,
)
from app.idempotency import IdempotencyStore, SubmissionOutcome
from app.logging import get_logger, log_event
from app.statemachine import TERMINAL_STATUSES, TaskStatus, ensure_transition

logger = get_logger()

TaskHandler = Callable[[Any], Awaitable[Any]]


@dataclass
class TaskRecord:
    id: str
    idempotency_key: str | None
    status: TaskStatus
    payload: Any = None
    result: Any = None
    error: dict | None = None
    correlation_id: str = ""
    created_at: float = 0.0
    finished_at: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


async def default_handler(payload: Any, default_downstream_timeout: float | None = None) -> Any:
    """默认任务：一次下游调用，或对 children 并发执行子操作。

    子操作共享请求的 correlation_id，并各自携带独立的 child span。
    """
    if not isinstance(payload, dict):
        payload = {"text": str(payload)}

    children = payload.get("children")
    if children:
        ctx = current_context()

        async def run_child(index: int, child: Any) -> Any:
            child_ctx = ctx.child(span=f"child-{index}")
            with use_context(child_ctx):
                log_event(logger, 20, "child operation started",
                          decision="dispatch_child", child_index=index)
                result = await default_handler(child)
                log_event(logger, 20, "child operation finished",
                          decision="child_done", child_index=index)
                return result

        return await asyncio.gather(
            *(run_child(i, child) for i, child in enumerate(children))
        )

    text = str(payload.get("text", ""))
    delay = float(payload.get("delay", 0.0))
    fail = bool(payload.get("fail", False))
    timeout = payload.get("downstream_timeout")
    effective_timeout = float(timeout) if timeout is not None else default_downstream_timeout
    return await call_downstream(text, delay=delay, fail=fail, timeout=effective_timeout)


class TaskManager:
    def __init__(self, settings: Settings, handler: TaskHandler | None = None) -> None:
        self._settings = settings
        if handler is not None:
            self._handler: TaskHandler = handler
        else:
            async def bound_handler(payload: Any) -> Any:
                return await default_handler(
                    payload,
                    default_downstream_timeout=settings.downstream_timeout_seconds,
                )
            self._handler = bound_handler
        self._tasks: dict[str, TaskRecord] = {}
        self._inflight: dict[str, asyncio.Task] = {}
        self._done: dict[str, asyncio.Event] = {}
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._idempotency = IdempotencyStore(ttl_seconds=settings.idempotency_ttl_seconds)
        self._workers: list[asyncio.Task] = []
        self._cleaner: asyncio.Task | None = None
        self._accepting = False
        self._started = False

    # ------------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        if self._started:
            return
        self._accepting = True
        self._started = True
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"task-worker-{i}")
            for i in range(self._settings.worker_count)
        ]
        self._cleaner = asyncio.create_task(self._cleanup_loop(), name="idempotency-cleaner")
        log_event(logger, 20, "task manager started", decision="start",
                  worker_count=self._settings.worker_count)

    async def shutdown(self) -> None:
        """优雅停机：在 grace 时间内等待在途任务收尾，超时后强制进入终态。"""
        if not self._started:
            return
        self._accepting = False
        log_event(logger, 20, "shutdown initiated", decision="stop_accepting",
                  grace_seconds=self._settings.shutdown_grace_seconds)

        await self._drain_or_abort(self._settings.shutdown_grace_seconds)

        for worker in self._workers:
            worker.cancel()
        for worker in self._workers:
            await _await_quietly(worker)
        if self._cleaner is not None:
            self._cleaner.cancel()
            await _await_quietly(self._cleaner)
        self._started = False
        log_event(logger, 20, "task manager stopped", decision="shutdown_complete")

    async def _drain_or_abort(self, grace_seconds: float) -> None:
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline:
            if self._queue.empty() and not self._inflight:
                return
            await asyncio.sleep(0.01)
        # 超时仍有在途任务：统一强制进入终态，不得留下半完成结果
        while not self._queue.empty():
            task_id = self._queue.get_nowait()
            record = self._tasks.get(task_id)
            if record is not None and record.status == TaskStatus.PENDING:
                async with self._lock:
                    self._transition_locked(record, TaskStatus.ABORTED)
                    record.finished_at = time.monotonic()
                    self._mark_done_locked(record)
                log_event(logger, 30, "queued task aborted on shutdown",
                          decision="shutdown_abort", task_id=task_id)
        async with self._lock:
            inflight = dict(self._inflight)
        for task_id, job in inflight.items():
            job.cancel()
            await _await_quietly(job)

    # ------------------------------------------------------------------ 提交

    async def submit(
        self,
        payload: Any,
        *,
        idempotency_key: str | None = None,
        correlation_id: str | None = None,
    ) -> tuple[TaskRecord, SubmissionOutcome]:
        correlation_id = correlation_id or current_context().correlation_id
        task_id = uuid.uuid4().hex
        async with self._lock:
            if not self._accepting:
                log_event(logger, 40, "submit rejected: shutting down",
                          decision="reject_submit", category="shutting_down",
                          reason="manager_not_accepting")
                raise ShuttingDownError(
                    "task manager is shutting down", reason="manager_not_accepting"
                )

            if idempotency_key is not None:
                outcome, idem = await self._idempotency.acquire(
                    idempotency_key, task_id, correlation_id
                )
                if outcome != SubmissionOutcome.CREATED:
                    existing = self._tasks[idem.task_id]
                    log_event(logger, 20, "duplicate submission reused",
                              decision="reuse_idempotent_result",
                              idempotency_key=idempotency_key,
                              reused_task_id=existing.id, outcome=str(outcome))
                    return existing, outcome

            # 容量背压：排队积压（含运行中）达到上限立即拒绝，不无限等待
            active = sum(
                1
                for r in self._tasks.values()
                if r.status in (TaskStatus.PENDING, TaskStatus.RUNNING)
            )
            if active >= self._settings.max_pending_tasks:
                log_event(logger, 30, "submit rejected: queue full",
                          decision="reject_submit", category="queue_full",
                          reason=f"active={active},capacity={self._settings.max_pending_tasks}")
                raise QueueFullError(
                    "task queue is full",
                    reason=f"active={active},capacity={self._settings.max_pending_tasks}",
                )

            record = TaskRecord(
                id=task_id,
                idempotency_key=idempotency_key,
                status=TaskStatus.PENDING,
                payload=payload,
                correlation_id=correlation_id,
                created_at=time.monotonic(),
            )
            self._tasks[task_id] = record
            self._done[task_id] = asyncio.Event()
            self._queue.put_nowait(task_id)
            log_event(logger, 20, "task submitted", decision="accept_submit",
                      task_id=task_id, idempotency_key=idempotency_key, active=active)
            return record, SubmissionOutcome.CREATED

    # ------------------------------------------------------------------ 查询/取消

    async def get(self, task_id: str) -> TaskRecord:
        record = self._tasks.get(task_id)
        if record is None:
            raise TaskNotFoundError(f"task not found: {task_id}", reason="unknown_task_id")
        return record

    async def wait(self, task_id: str, timeout: float | None = None) -> TaskRecord:
        record = await self.get(task_id)
        if record.status in TERMINAL_STATUSES:
            return record
        await asyncio.wait_for(self._done[task_id].wait(), timeout=timeout)
        return await self.get(task_id)

    async def cancel(self, task_id: str) -> TaskRecord:
        async with self._lock:
            record = await self.get(task_id)
            if record.status == TaskStatus.PENDING:
                self._transition_locked(record, TaskStatus.CANCELLED)
                record.finished_at = time.monotonic()
                self._mark_done_locked(record)
                log_event(logger, 20, "task cancelled while queued",
                          decision="cancel", task_id=task_id)
            elif record.status == TaskStatus.RUNNING:
                self._transition_locked(record, TaskStatus.CANCELLED)
                job = self._inflight.get(task_id)
                if job is not None:
                    job.cancel()
                log_event(logger, 20, "task cancelled while running",
                          decision="cancel", task_id=task_id)
            else:
                raise InvalidStateTransitionError(
                    f"cannot cancel task in terminal state: {record.status}",
                    current=str(record.status), target="cancelled",
                    reason="current_status_is_terminal",
                )
        return record

    # ------------------------------------------------------------------ worker

    async def _worker(self, index: int) -> None:
        while True:
            task_id = await self._queue.get()
            try:
                record = self._tasks.get(task_id)
                if record is None or record.status != TaskStatus.PENDING:
                    # 排队期间已被取消/中止：不执行，无副作用
                    continue
                await self._run_task(index, task_id, record)
            finally:
                self._queue.task_done()

    async def _run_task(self, worker_index: int, task_id: str, record: TaskRecord) -> None:
        ctx = TraceContext(
            correlation_id=record.correlation_id, task_id=task_id, span="task"
        )
        with use_context(ctx):
            async with self._lock:
                self._transition_locked(record, TaskStatus.RUNNING)
            log_event(logger, 20, "task started", decision="dispatch",
                      task_id=task_id, worker=worker_index)

            job = asyncio.create_task(self._handler(record.payload))
            async with self._lock:
                self._inflight[task_id] = job

            try:
                result = await asyncio.wait_for(
                    asyncio.shield(job), timeout=self._settings.task_timeout_seconds
                )
            except asyncio.TimeoutError:
                job.cancel()
                await _await_quietly(job)
                await self._finish_failed(
                    record,
                    TaskTimeoutError(
                        "task execution timed out",
                        reason=f"exceeded_task_timeout_seconds={self._settings.task_timeout_seconds}",
                    ),
                    target=TaskStatus.TIMED_OUT,
                )
                return
            except asyncio.CancelledError:
                # 区分：job 被用户取消（worker 本身未被取消）vs worker 被停机取消
                job.cancel()
                await _await_quietly(job)
                await self._cancel_or_abort(record)
                if asyncio.current_task().cancelling() > 0:
                    raise
                return
            except AppError as exc:
                await self._finish_failed(record, exc, target=TaskStatus.FAILED)
                return
            except Exception as exc:  # 内部错误归一化，不原样透出
                await self._finish_failed(
                    record,
                    AppError("internal task error",
                             reason=f"underlying={type(exc).__name__}", cause=exc),
                    target=TaskStatus.FAILED,
                )
                return
            finally:
                async with self._lock:
                    self._inflight.pop(task_id, None)

            async with self._lock:
                if record.status == TaskStatus.RUNNING:
                    self._transition_locked(record, TaskStatus.SUCCEEDED)
                    record.result = result
                    record.finished_at = time.monotonic()
                # 无论是否在与取消竞争，都保证 done 事件被设置
                self._mark_done_locked(record)
            log_event(logger, 20, "task succeeded", decision="complete", task_id=task_id)

    async def _finish_failed(
        self, record: TaskRecord, exc: AppError, *, target: TaskStatus
    ) -> None:
        async with self._lock:
            if record.status == TaskStatus.CANCELLED:
                # 取消与失败竞争：取消优先，保证取消后无半完成结果
                self._mark_done_locked(record)
                return
            self._transition_locked(record, target)
            record.error = exc.to_dict()
            record.finished_at = time.monotonic()
            self._mark_done_locked(record)
        log_event(logger, 40, "task finished with failure", decision="finish_failed",
                  task_id=record.id, final_status=str(target),
                  error_category=str(exc.category), reason=exc.reason)

    async def _cancel_or_abort(self, record: TaskRecord) -> None:
        async with self._lock:
            if record.status == TaskStatus.RUNNING:
                # worker 被停机取消且用户未取消：进入 ABORTED 终态
                self._transition_locked(record, TaskStatus.ABORTED)
            record.finished_at = time.monotonic()
            self._mark_done_locked(record)
        log_event(logger, 30, "task execution interrupted", decision="interrupt",
                  task_id=record.id, final_status=str(record.status))

    # ------------------------------------------------------------------ 清理

    async def _cleanup_loop(self) -> None:
        interval = self._settings.cleanup_interval_seconds
        while True:
            await asyncio.sleep(interval)
            removed = await self._idempotency.cleanup_expired()
            if removed:
                log_event(logger, 20, "expired idempotency records cleaned",
                          decision="cleanup", removed=removed)

    # ------------------------------------------------------------------ 内部

    def _transition_locked(self, record: TaskRecord, target: TaskStatus) -> None:
        ensure_transition(record.status, target)
        log_event(logger, 20, "task state transition", decision="transition",
                  task_id=record.id, from_status=str(record.status), to_status=str(target))
        record.status = target

    def _mark_done_locked(self, record: TaskRecord) -> None:
        event = self._done.get(record.id)
        if event is not None:
            event.set()
        if record.idempotency_key is not None and record.status in TERMINAL_STATUSES:
            self._idempotency.mark_completed(record.idempotency_key)


async def _await_quietly(task: asyncio.Task) -> None:
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass
