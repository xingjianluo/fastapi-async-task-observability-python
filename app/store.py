"""任务存储：幂等提交与合法状态迁移的唯一裁定者（进程内实现）。"""
from __future__ import annotations

import asyncio
import dataclasses
import enum
import time

from .errors import AppError, InvalidStateTransition
from .statemachine import TaskStatus, is_legal_transition


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
        raise NotImplementedError

    async def transition(
        self,
        task_id: str,
        target: TaskStatus,
        *,
        result: dict | None = None,
        error: AppError | None = None,
    ) -> TaskRecord:
        raise NotImplementedError

    async def get(self, task_id: str) -> TaskRecord | None:
        raise NotImplementedError

    async def get_by_key(self, idempotency_key: str) -> TaskRecord | None:
        raise NotImplementedError

    async def all_records(self) -> list[TaskRecord]:
        raise NotImplementedError
