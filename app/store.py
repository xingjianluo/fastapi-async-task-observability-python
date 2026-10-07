"""任务存储：幂等提交与合法状态迁移的唯一裁定者（进程内实现）。"""
from __future__ import annotations

import asyncio
import dataclasses
import enum
import time
from collections.abc import Callable

from .errors import AppError, InvalidStateTransition, TaskNotFound, TaskNotReleasable
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
    """以幂等键去重；状态迁移必须合法；并发安全。

    clock 可注入（默认墙钟 time.time）：记录时间戳面向运维查询，
    测试可注入假时钟以确定性覆盖保留期边界。
    """

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = asyncio.Lock()
        self._by_id: dict[str, TaskRecord] = {}
        self._by_key: dict[str, str] = {}
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

    def _remove_locked(self, record: TaskRecord) -> None:
        """在锁内移除记录并释放其幂等键（调用方必须已持有锁）。"""
        self._by_id.pop(record.task_id, None)
        self._by_key.pop(record.idempotency_key, None)

    async def reap_expired_terminal(self, retention_seconds: float) -> list[TaskRecord]:
        """清除超过保留期的终态任务，返回被清除的记录。

        判定依据：status 属于终态 且 finished_at 距当前时钟 >= retention_seconds。
        在途（PENDING/RUNNING）与未到期终态任务一律保留。
        """
        async with self._lock:
            now = self._clock()
            victims = [
                r
                for r in self._by_id.values()
                if r.status in TERMINAL_STATES
                and r.finished_at is not None
                and now - r.finished_at >= retention_seconds
            ]
            for record in victims:
                self._remove_locked(record)
            return victims

    async def evict_oldest_terminal(self, max_history: int) -> list[TaskRecord]:
        """终态历史超过 max_history 时，按 finished_at 最旧优先淘汰。"""
        async with self._lock:
            terminal = sorted(
                (r for r in self._by_id.values() if r.status in TERMINAL_STATES),
                key=lambda r: (r.finished_at if r.finished_at is not None else 0.0),
            )
            excess = len(terminal) - max_history
            if excess <= 0:
                return []
            victims = terminal[:excess]
            for record in victims:
                self._remove_locked(record)
            return victims

    async def release(self, task_id: str) -> TaskRecord:
        """手动释放终态任务占用的幂等键并移除记录。

        - 目标不存在：TaskNotFound（404 语义）；
        - 目标未进终态：TaskNotReleasable（409 语义），不释放；
        - 终态：移除记录并释放幂等键，同键再次提交将创建全新任务。
        """
        async with self._lock:
            record = self._by_id.get(task_id)
            if record is None:
                raise TaskNotFound(
                    f"task {task_id} not found",
                    reason="release rejected: task does not exist",
                )
            if record.status not in TERMINAL_STATES:
                raise TaskNotReleasable(
                    f"task {task_id} is not in a terminal state",
                    current=record.status.value,
                    reason="release rejected: only terminal tasks can be released",
                )
            self._remove_locked(record)
            return record

    async def query(
        self,
        *,
        status: TaskStatus | None = None,
        created_from: float | None = None,
        created_to: float | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[TaskRecord], int]:
        """按状态与创建时间范围筛选并分页，返回 (本页记录, 符合条件的总数)。"""
        async with self._lock:
            matched = [
                r
                for r in self._by_id.values()
                if (status is None or r.status is status)
                and (created_from is None or r.created_at >= created_from)
                and (created_to is None or r.created_at <= created_to)
            ]
            matched.sort(key=lambda r: r.created_at)
            return matched[offset : offset + limit], len(matched)
