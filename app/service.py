"""任务服务：提交/查询/取消的门面，串联存储、执行器与链路上下文。"""
from __future__ import annotations

import uuid

from .context import get_correlation_id
from .executor import TaskExecutor
from .logging_config import get_logger
from .statemachine import TaskStatus
from .store import TaskRecord, TaskStore


class TaskService:
    def __init__(self, store: TaskStore, executor: TaskExecutor) -> None:
        self._store = store
        self._executor = executor
        self._log = get_logger("service")

    async def submit(self, idempotency_key: str, payload: dict) -> tuple[TaskRecord, bool]:
        """幂等提交，返回 (记录, 是否本次新建)。"""
        raise NotImplementedError

    async def get(self, task_id: str) -> TaskRecord:
        raise NotImplementedError

    async def get_by_key(self, idempotency_key: str) -> TaskRecord:
        raise NotImplementedError

    async def cancel(self, task_id: str) -> TaskRecord:
        raise NotImplementedError
