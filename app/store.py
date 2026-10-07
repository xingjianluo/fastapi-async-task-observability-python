"""任务存储：幂等提交与合法状态迁移的唯一裁定者（进程内实现）。"""
from __future__ import annotations

import asyncio
import dataclasses
import enum
import time
from collections.abc import Callable

from .errors import AppError, InvalidStateTransition, TaskNotFound
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

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = asyncio.Lock()
        self._by_id: dict[str, TaskRecord] = {}
        self._by_key: dict[str, str] = {}
        # 时钟可注入：默认墙钟（Unix 时间戳），便于按时间范围查询与保留期判定
        self._clock = clock

    async def submit(self, record: TaskRecord) -> tuple[TaskRecord, DuplicateSubmitOutcome]:
        """幂等提交：同一幂等键只创建一次记录，重复提交返回既有记录。"""
        async with self._lock:
            existing_id = self._by_key.get(record.idempotency_key)
            if existing_id is not None:
                return self._by_id[existing_id], DuplicateSubmitOutcome.DUPLICATE
            now = self._clock()
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
            record.updated_at = self._clock()
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

    async def sweep_terminal(
        self, *, now: float, retention_seconds: float, max_history: int
    ) -> list[TaskRecord]:
        """回收终态任务，返回被清理的记录（含幂等键释放）。

        判定规则（在同一锁内完成，与并发提交互斥）：
        - 到期回收：终态且 ``now - finished_at >= retention_seconds``（达到保留期即回收）；
        - 容量淘汰：到期回收后若终态记录仍超过 ``max_history``，按 finished_at 最旧优先清理；
        - 在途任务（PENDING/RUNNING）与未到期终态任务一律不动。
        """
        async with self._lock:
            reclaimed: list[TaskRecord] = []
            cutoff = now - retention_seconds
            for task_id, record in list(self._by_id.items()):
                if (
                    record.status in TERMINAL_STATES
                    and record.finished_at is not None
                    and record.finished_at <= cutoff
                ):
                    reclaimed.append(record)
                    del self._by_id[task_id]
                    self._by_key.pop(record.idempotency_key, None)
            if max_history >= 0:
                terminal = sorted(
                    (r for r in self._by_id.values() if r.status in TERMINAL_STATES),
                    key=lambda r: (r.finished_at if r.finished_at is not None else 0.0),
                )
                excess = len(terminal) - max_history
                for record in terminal[: max(excess, 0)]:
                    reclaimed.append(record)
                    del self._by_id[record.task_id]
                    self._by_key.pop(record.idempotency_key, None)
            return reclaimed

    async def release_terminal(self, task_id: str) -> TaskRecord:
        """手动释放终态任务：删除记录并释放幂等键，返回被释放的记录。

        只允许作用于终态任务；不存在或非终态时明确拒绝并携带可区分的原因。
        """
        async with self._lock:
            record = self._by_id.get(task_id)
            if record is None:
                raise TaskNotFound(
                    f"task {task_id} not found",
                    reason="release rejected: task does not exist",
                )
            if record.status not in TERMINAL_STATES:
                raise InvalidStateTransition(
                    f"cannot release task in {record.status.value}",
                    current=record.status.value,
                    target="<released>",
                    reason="release rejected: task not in terminal state",
                )
            del self._by_id[task_id]
            self._by_key.pop(record.idempotency_key, None)
            return record

    async def list_records(
        self,
        *,
        status: TaskStatus | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> tuple[list[TaskRecord], int]:
        """按状态与创建时间范围筛选，按创建时间升序分页，返回 (页内记录, 总命中数)。"""
        async with self._lock:
            records = sorted(self._by_id.values(), key=lambda r: r.created_at)
            if status is not None:
                records = [r for r in records if r.status is status]
            if created_from is not None:
                records = [r for r in records if r.created_at >= created_from]
            if created_to is not None:
                records = [r for r in records if r.created_at <= created_to]
            total = len(records)
            return records[offset : offset + limit], total
