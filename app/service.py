"""任务服务：提交/查询/取消的门面，串联存储、执行器与链路上下文。"""
from __future__ import annotations

import uuid

from .context import ensure_correlation_id
from .errors import CapacityExceeded, TaskNotFound
from .executor import TaskExecutor
from .logging_config import get_logger
from .retention import RetentionStats, RetentionSweeper
from .statemachine import TaskStatus
from .store import DuplicateSubmitOutcome, TaskRecord, TaskStore


class TaskService:
    def __init__(
        self,
        store: TaskStore,
        executor: TaskExecutor,
        sweeper: RetentionSweeper | None = None,
    ) -> None:
        self._store = store
        self._executor = executor
        self._sweeper = sweeper
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

    async def list_tasks(
        self,
        *,
        status: TaskStatus | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> tuple[list[TaskRecord], int]:
        """按状态与时间范围筛选并分页查询，返回 (页内记录, 总命中数)。"""
        records, total = await self._store.list_records(
            status=status,
            created_from=created_from,
            created_to=created_to,
            offset=offset,
            limit=limit,
        )
        self._log.info(
            "tasks listed",
            extra={
                "obs_status": status.value if status else "-",
                "obs_total": total,
                "obs_returned": len(records),
                "obs_offset": offset,
                "obs_limit": limit,
            },
        )
        return records, total

    async def release(self, task_id: str) -> TaskRecord:
        """手动释放终态任务占用的幂等键。

        只允许终态任务；不存在或非终态时由存储抛出可区分的归一化错误，
        判定依据在此留痕，不静默丢弃。
        """
        record = await self._store.release_terminal(task_id)
        self._log.info(
            "task released",
            extra={
                "obs_task_id": task_id,
                "obs_idempotency_key": record.idempotency_key,
                "obs_status": record.status.value,
                "obs_reason": "terminal task released manually, idempotency key freed",
            },
        )
        return record

    def retention_stats(self) -> RetentionStats | None:
        """回收指标（未配置回收器时为 None）。"""
        return self._sweeper.stats if self._sweeper else None
