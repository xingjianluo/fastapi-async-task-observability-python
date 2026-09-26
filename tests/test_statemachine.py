"""非法状态迁移被明确拒绝，且原因可区分。"""
from __future__ import annotations

import pytest

from app.errors import InvalidStateTransition
from app.logging_config import get_logger
from app.statemachine import TaskStatus
from app.store import TaskRecord, TaskStore

log = get_logger("tests.statemachine")


def _record(task_id: str, key: str) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        idempotency_key=key,
        correlation_id="cid-sm",
        status=TaskStatus.PENDING,
        payload={},
    )


async def test_legal_path_pending_to_terminal():
    store = TaskStore()
    await store.submit(_record("t1", "k1"))
    await store.transition("t1", TaskStatus.RUNNING)
    record = await store.transition("t1", TaskStatus.SUCCEEDED, result={"ok": True})
    assert record.status is TaskStatus.SUCCEEDED
    assert record.result == {"ok": True}
    assert record.finished_at is not None


async def test_terminal_state_rejects_any_transition():
    store = TaskStore()
    await store.submit(_record("t2", "k2"))
    await store.transition("t2", TaskStatus.RUNNING)
    await store.transition("t2", TaskStatus.FAILED)
    with pytest.raises(InvalidStateTransition) as exc_info:
        await store.transition("t2", TaskStatus.RUNNING)
    err = exc_info.value
    log.info(
        "判定依据：终态迁移拒绝原因应标记 terminal",
        extra={"obs_reason": err.reason, "obs_current": err.current},
    )
    assert err.current == "failed" and err.target == "running"
    assert "terminal" in err.reason


async def test_skip_state_rejected_with_distinct_reason():
    store = TaskStore()
    await store.submit(_record("t3", "k3"))
    with pytest.raises(InvalidStateTransition) as exc_info:
        await store.transition("t3", TaskStatus.SUCCEEDED)  # PENDING 不能直接成功
    err = exc_info.value
    log.info(
        "判定依据：跳态拒绝原因应标记 not in allowed set",
        extra={"obs_reason": err.reason},
    )
    assert "not in allowed set" in err.reason
    assert err.current == "pending" and err.target == "succeeded"


async def test_missing_task_rejected_with_distinct_reason():
    store = TaskStore()
    with pytest.raises(InvalidStateTransition) as exc_info:
        await store.transition("ghost", TaskStatus.RUNNING)
    err = exc_info.value
    log.info(
        "判定依据：不存在任务的拒绝原因应标记 does not exist",
        extra={"obs_reason": err.reason},
    )
    assert err.current == "<missing>" and "does not exist" in err.reason


async def test_cancelled_task_not_reexecuted(make_service):
    """排队期间取消的任务不会被 worker 再执行（无半完成结果）。"""
    service = make_service(latency=0.2, max_workers=1, max_pending=4)
    first, _ = await service.submit("kA", {})
    second, _ = await service.submit("kB", {})
    await service.cancel(second.task_id)  # 第一个占住 worker，第二个还在排队
    await service._executor.shutdown()

    record = await service.get(second.task_id)
    log.info(
        "判定依据：排队中取消的任务终态为 cancelled 且未产生下游副作用",
        extra={"obs_status": record.status.value, "obs_attempt": record.attempt},
    )
    assert record.status is TaskStatus.CANCELLED
    assert record.attempt == 0  # 从未进入 RUNNING
    assert service._executor._downstream.calls == 2  # 只有第一个任务的 fanout=2
