"""幂等提交存储：相同幂等键的并发/重复提交只产生一次副作用。

语义：
- acquire 在锁内完成"检查-插入"，并发提交只有一个胜出者真正创建任务；
- 后续提交命中时返回胜出任务的 task_id 与命中类型（进行中/已完成），不产生副作用；
- 只有已完成记录可在 TTL 到期后被清理；进行中记录不会过期，避免重复副作用。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import StrEnum


class SubmissionOutcome(StrEnum):
    # 本次提交实际创建了任务
    CREATED = "created"
    # 命中已有进行中任务（并发/重复提交）
    REUSED_INFLIGHT = "reused_inflight"
    # 命中已完成任务，返回一致结果
    REUSED_COMPLETED = "reused_completed"


@dataclass
class IdempotencyRecord:
    key: str
    task_id: str
    correlation_id: str
    created_at: float
    completed: bool = False
    completed_at: float | None = None


class IdempotencyStore:
    def __init__(self, *, ttl_seconds: float = 0.0) -> None:
        self._records: dict[str, IdempotencyRecord] = {}
        self._lock = asyncio.Lock()
        self._ttl = ttl_seconds

    async def acquire(
        self, key: str, task_id: str, correlation_id: str
    ) -> tuple[SubmissionOutcome, IdempotencyRecord]:
        """尝试占用幂等键。

        返回 (结果, 记录)。结果为 CREATED 表示本次提交应真正创建任务；
        其余结果表示命中已有记录，调用方必须复用其 task_id，不得重复执行。
        """
        async with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                outcome = (
                    SubmissionOutcome.REUSED_COMPLETED
                    if existing.completed
                    else SubmissionOutcome.REUSED_INFLIGHT
                )
                return outcome, existing
            record = IdempotencyRecord(
                key=key,
                task_id=task_id,
                correlation_id=correlation_id,
                created_at=time.monotonic(),
            )
            self._records[key] = record
            return SubmissionOutcome.CREATED, record

    def mark_completed(self, key: str) -> None:
        record = self._records.get(key)
        if record is not None:
            record.completed = True
            record.completed_at = time.monotonic()

    async def get(self, key: str) -> IdempotencyRecord | None:
        async with self._lock:
            return self._records.get(key)

    async def cleanup_expired(self, now: float | None = None) -> int:
        """清理已完成且超过 TTL 的记录；进行中记录不清理。返回清理数量。"""
        if self._ttl <= 0:
            return 0
        now = time.monotonic() if now is None else now
        expired: list[str] = []
        async with self._lock:
            for key, record in self._records.items():
                if record.completed and record.completed_at is not None:
                    if now - record.completed_at >= self._ttl:
                        expired.append(key)
            for key in expired:
                self._records.pop(key, None)
        return len(expired)
