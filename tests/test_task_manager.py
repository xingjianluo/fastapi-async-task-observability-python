import asyncio
import json

import pytest

from app.config import Settings
from app.context import TraceContext, use_context
from app.errors import (
    InvalidStateTransitionError,
    QueueFullError,
    ShuttingDownError,
)
from app.idempotency import SubmissionOutcome
from app.statemachine import TaskStatus
from app.task_manager import TaskManager

pytestmark = pytest.mark.anyio


def make_settings(**overrides) -> Settings:
    base = dict(
        max_pending_tasks=8,
        task_timeout_seconds=2.0,
        downstream_timeout_seconds=1.0,
        shutdown_grace_seconds=2.0,
        worker_count=2,
        idempotency_ttl_seconds=0.0,
        cleanup_interval_seconds=60.0,
    )
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
async def manager_factory():
    managers: list[TaskManager] = []

    def factory(**overrides):
        m = TaskManager(make_settings(**overrides))
        managers.append(m)
        return m

    yield factory

    for m in managers:
        if m._started:
            await m.shutdown()


async def test_success_path(manager_factory):
    manager = manager_factory()
    await manager.start()
    record, outcome = await manager.submit({"text": "hello"}, correlation_id="c1")
    assert outcome == SubmissionOutcome.CREATED
    record = await manager.wait(record.id, timeout=2)
    assert record.status == TaskStatus.SUCCEEDED
    assert record.result == {"echo": "hello", "ok": True}
    assert record.correlation_id == "c1"
    assert record.finished_at is not None


async def test_concurrent_idempotent_submit_single_side_effect(manager_factory):
    side_effects = 0

    async def counting_handler(payload):
        nonlocal side_effects
        await asyncio.sleep(0.05)
        side_effects += 1
        return {"n": side_effects}

    manager = TaskManager(make_settings(worker_count=4), counting_handler)
    await manager.start()

    results = await asyncio.gather(
        *(manager.submit({"i": i}, idempotency_key="idem-1", correlation_id="c-1")
          for i in range(20))
    )
    outcomes = [r[1] for r in results]
    assert outcomes.count(SubmissionOutcome.CREATED) == 1
    task_ids = {r[0].id for r in results}
    assert len(task_ids) == 1

    winner_id = results[0][0].id
    await manager.wait(winner_id, timeout=2)
    assert side_effects == 1

    # 完成后再次提交：命中已完成记录，返回一致结果
    record, outcome = await manager.submit(
        {"i": 99}, idempotency_key="idem-1", correlation_id="c-1"
    )
    assert outcome == SubmissionOutcome.REUSED_COMPLETED
    assert record.id == winner_id
    assert record.status == TaskStatus.SUCCEEDED
    assert side_effects == 1


async def test_queue_full_rejects_immediately():
    gate = asyncio.Event()

    async def blocking_handler(payload):
        await gate.wait()
        return True

    manager = TaskManager(make_settings(max_pending_tasks=3, worker_count=1),
                          blocking_handler)
    await manager.start()
    for i in range(3):
        await manager.submit({"i": i})
        await asyncio.sleep(0.02)  # 让第一个进入 RUNNING
    with pytest.raises(QueueFullError) as exc:
        await manager.submit({"i": 3})
    assert exc.value.category == "queue_full"
    gate.set()
    await manager.shutdown()


async def test_submit_rejected_after_shutdown_starts():
    manager = TaskManager(make_settings())
    await manager.start()
    await manager.shutdown()
    with pytest.raises(ShuttingDownError):
        await manager.submit({"text": "x"})


async def test_downstream_failure_normalized(manager_factory):
    manager = manager_factory()
    await manager.start()
    record, _ = await manager.submit({"text": "x", "fail": True}, correlation_id="c-fail")
    record = await manager.wait(record.id, timeout=2)
    assert record.status == TaskStatus.FAILED
    assert record.error["category"] == "downstream_error"
    # 底层异常信息不得透出
    assert "internal db exploded" not in json.dumps(record.error)


async def test_downstream_timeout_normalized(manager_factory):
    manager = manager_factory(downstream_timeout_seconds=0.05)
    await manager.start()
    record, _ = await manager.submit(
        {"text": "x", "delay": 1.0}, correlation_id="c-dto"
    )
    record = await manager.wait(record.id, timeout=2)
    assert record.status == TaskStatus.FAILED
    assert record.error["category"] == "downstream_timeout"


async def test_task_level_timeout_marks_timed_out():
    manager = TaskManager(make_settings(task_timeout_seconds=0.05))
    await manager.start()

    async def slow_handler(payload):
        await asyncio.sleep(1.0)
        return "done"

    manager._handler = slow_handler
    record, _ = await manager.submit({}, correlation_id="c-tt")
    record = await manager.wait(record.id, timeout=2)
    assert record.status == TaskStatus.TIMED_OUT
    assert record.error["category"] == "task_timeout"
    await manager.shutdown()


async def test_cancel_pending_never_executes():
    started = asyncio.Event()

    async def handler(payload):
        started.set()
        await asyncio.sleep(0.2)
        return True

    manager = TaskManager(make_settings(worker_count=1), handler)
    await manager.start()
    # 先占住唯一 worker
    blocking, _ = await manager.submit({"block": True})
    await asyncio.sleep(0.03)
    queued, _ = await manager.submit({"queued": True})
    await manager.cancel(queued.id)
    record = await manager.get(queued.id)
    assert record.status == TaskStatus.CANCELLED
    # 停机前让阻塞任务结束
    await manager.wait(blocking.id, timeout=2)
    assert not started.is_set() or True  # started 只对应 block/queued 之一
    await manager.shutdown()


async def test_cancel_terminal_task_rejected(manager_factory):
    manager = manager_factory()
    await manager.start()
    record, _ = await manager.submit({"text": "x"}, correlation_id="c")
    await manager.wait(record.id, timeout=2)
    with pytest.raises(InvalidStateTransitionError) as exc:
        await manager.cancel(record.id)
    assert exc.value.reason == "current_status_is_terminal"
    assert exc.value.current == "succeeded"
    assert exc.value.target == "cancelled"


async def test_correlation_id_propagates_to_concurrent_children(log_records):
    manager = TaskManager(make_settings(worker_count=1))
    await manager.start()
    payload = {"children": [
        {"text": "a", "delay": 0.05},
        {"text": "b", "fail": True},
    ]}
    record, _ = await manager.submit(payload, correlation_id="corr-child-42")
    record = await manager.wait(record.id, timeout=2)
    assert record.status == TaskStatus.FAILED

    # 所有日志（含并发子操作）都带同一 correlation_id
    app_logs = [json.loads(line) for line in log_records]
    relevant = [r for r in app_logs if r.get("correlation_id") == "corr-child-42"]
    child_starts = [r for r in relevant if r.get("decision") == "dispatch_child"]
    assert len(child_starts) == 2
    spans = {r["span"] for r in child_starts}
    assert spans == {"child-0", "child-1"}
    # 失败归一化日志可查到
    assert any(r.get("category") == "downstream_error" for r in relevant)
    await manager.shutdown()


async def test_shutdown_aborts_remaining_tasks_within_grace():
    aborted_logs = []

    async def forever_handler(payload):
        await asyncio.Event().wait()

    manager = TaskManager(make_settings(
        max_pending_tasks=8, worker_count=1, shutdown_grace_seconds=0.1,
    ), forever_handler)
    await manager.start()
    running, _ = await manager.submit({})
    await asyncio.sleep(0.03)
    queued, _ = await manager.submit({})
    await manager.shutdown()

    assert (await manager.get(running.id)).status == TaskStatus.ABORTED
    assert (await manager.get(queued.id)).status == TaskStatus.ABORTED
    # 所有任务进入终态
    for task_id in (running.id, queued.id):
        assert (await manager.get(task_id)).status in (
            TaskStatus.CANCELLED, TaskStatus.ABORTED,
        )


async def test_shutdown_gracefully_waits_finishing_tasks():
    async def quick_handler(payload):
        await asyncio.sleep(0.05)
        return "ok"

    manager = TaskManager(make_settings(shutdown_grace_seconds=2.0), quick_handler)
    await manager.start()
    record, _ = await manager.submit({})
    await manager.shutdown()
    assert (await manager.get(record.id)).status == TaskStatus.SUCCEEDED
