"""任务存储：幂等提交与合法状态迁移的唯一裁定者（进程内实现）。"""
from __future__ import annotations

import asyncio
import dataclasses
import enum
import time

from .errors import AppError, InvalidStateTransition
from .statemachine import TERMINAL_STATES, TaskStatus, is_legal_transition


@dataclasses.dataclass
class TaskRecord:
    task_id: str
    idempotency_key: str
    correlation_id: str
    status: TaskStatus
    payload: dict
    result: dict | None = None
    error: dict | None = None
    created_at: float = 0.0
    updated_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    attempt: int = 0

    def to_dict(self) -> dict:
        return dataclasses.asdict(self) | {"status": self.status.value}


class DuplicateSubmitOutcome(enum.Enum):
    CREATED = "created"
    DUPLICATE = "duplicate"


class TaskStore:
    """以幂等键去重；状态迁移必须合法；并发安全。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._by_id: dict[str, TaskRecord] = {}
        self._by_key: dict[str, str] = {}

    async def submit(self, record: TaskRecord) -> tuple[TaskRecord, DuplicateSubmitOutcome]:
        """幂等提交：同一幂等键只创建一次记录，重复提交返回既有记录。"""
        async with self._lock:
            existing_id = self._by_key.get(record.idempotency_key)
            if existing_id is not None:
                return self._by_id[existing_id], DuplicateSubmitOutcome.DUPLICATE
            now = time.monotonic()
            record.created_at = record.updated_at = now
            self._by_id[record.task_id] = record
            self._by_key[record.idempotency_key] = record.task_id
            return record, DuplicateSubmitOutcome.CREATED

    async def transition(
        self,
        task_id: str,
        target: TaskStatus,
        *,
        result: dict | None = None,
        error: AppError | None = None,
    ) -> TaskRecord:
        """状态迁移：非法迁移明确拒绝并携带可区分的原因。"""
        async with self._lock:
            record = self._by_id.get(task_id)
            if record is None:
                raise InvalidStateTransition(
                    f"task {task_id} not found",
                    current="<missing>",
                    target=target.value,
                    reason="transition rejected: task does not exist",
                )
            if not is_legal_transition(record.status, target):
                raise InvalidStateTransition(
                    f"illegal transition {record.status.value} -> {target.value}",
                    current=record.status.value,
                    target=target.value,
                    reason=(
                        "transition rejected: terminal state"
                        if record.status in TERMINAL_STATES
                        else "transition rejected: not in allowed set"
                    ),
                )
            record.status = target
            record.updated_at = time.monotonic()
            if target is TaskStatus.RUNNING:
                record.started_at = record.updated_at
                record.attempt += 1
            if target in TERMINAL_STATES:
                record.finished_at = record.updated_at
                record.result = result
                record.error = error.to_dict() if error else None
            return record

    async def get(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            return self._by_id.get(task_id)

    async def discard_pending(self, task_id: str) -> bool:
        """回滚：仅允许删除仍处于 PENDING 的记录（如背压拒绝后清理占位）。"""
        async with self._lock:
            record = self._by_id.get(task_id)
            if record is None or record.status is not TaskStatus.PENDING:
                return False
            del self._by_id[task_id]
            self._by_key.pop(record.idempotency_key, None)
            return True

    async def get_by_key(self, idempotency_key: str) -> TaskRecord | None:
        async with self._lock:
            task_id = self._by_key.get(idempotency_key)
            return self._by_id.get(task_id) if task_id else None

    async def all_records(self) -> list[TaskRecord]:
        async with self._lock:
            return list(self._by_id.values())
