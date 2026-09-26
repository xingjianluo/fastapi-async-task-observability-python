"""任务服务：提交/查询/取消的门面，串联存储、执行器与链路上下文。"""
from __future__ import annotations

import uuid

from .context import ensure_correlation_id
from .errors import CapacityExceeded, TaskNotFound
from .executor import TaskExecutor
from .logging_config import get_logger
from .statemachine import TaskStatus
from .store import DuplicateSubmitOutcome, TaskRecord, TaskStore


class TaskService:
    def __init__(self, store: TaskStore, executor: TaskExecutor) -> None:
        self._store = store
        self._executor = executor
        self._log = get_logger("service")

    async def submit(self, idempotency_key: str, payload: dict) -> tuple[TaskRecord, bool]:
        """幂等提交，返回 (记录, 是否本次新建)。

        - 相同幂等键重复提交返回既有记录，不产生重复副作用；
        - 背压拒绝时回滚占位记录，任务不留下半完成状态。
        """
        correlation_id = ensure_correlation_id()
        record = TaskRecord(
            task_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
            status=TaskStatus.PENDING,
            payload=payload,
        )
        record, outcome = await self._store.submit(record)
        if outcome is DuplicateSubmitOutcome.DUPLICATE:
            self._log.info(
                "duplicate submit",
                extra={
                    "obs_idempotency_key": idempotency_key,
                    "obs_task_id": record.task_id,
                    "obs_reason": "idempotency key already exists, returning existing record",
                },
            )
            return record, False
        try:
            await self._executor.enqueue(record.task_id)
        except CapacityExceeded:
            # 回滚占位：幂等键释放，任务不进入任何执行路径
            await self._store.discard_pending(record.task_id)
            self._log.warning(
                "submit rejected by backpressure",
                extra={"obs_idempotency_key": idempotency_key},
            )
            raise
        self._log.info(
            "task submitted",
            extra={"obs_idempotency_key": idempotency_key, "obs_task_id": record.task_id},
        )
        return record, True

    async def get(self, task_id: str) -> TaskRecord:
        record = await self._store.get(task_id)
        if record is None:
            raise TaskNotFound(
                f"task {task_id} not found",
                reason="lookup by task_id missed",
            )
        return record

    async def get_by_key(self, idempotency_key: str) -> TaskRecord:
        record = await self._store.get_by_key(idempotency_key)
        if record is None:
            raise TaskNotFound(
                f"task with key {idempotency_key} not found",
                reason="lookup by idempotency_key missed",
            )
        return record

    async def cancel(self, task_id: str) -> TaskRecord:
        """取消任务：状态迁移由存储裁定，非法迁移（如已完成）明确拒绝。"""
        record = await self._store.transition(
            task_id,
            TaskStatus.CANCELLED,
            error=None,
        )
        await self._executor.cancel(task_id)
        self._log.info(
            "task cancelled",
            extra={"obs_task_id": task_id, "obs_from": "running_or_pending"},
        )
        return record
