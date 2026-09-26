"""容量背压与优雅停机：超限明确拒绝，在途任务进入明确终态，无重复执行与泄漏。"""
from __future__ import annotations

import asyncio

import pytest

from app.errors import CapacityExceeded
from app.logging_config import get_logger
from app.statemachine import TaskStatus

log = get_logger("tests.shutdown")


async def test_backpressure_rejects_when_queue_full(make_service):
    service = make_service(latency=0.3, max_workers=1, max_pending=1, task_timeout=10)
    r0, _ = await service.submit("k0", {})
    await asyncio.sleep(0.05)  # 让 worker 领取 k0
    r1, _ = await service.submit("k1", {})  # 占满唯一排队位

    log.info(
        "判定依据：queued=1 达到 max_pending=1，新提交应立即被拒绝",
        extra={"obs_queued": 1, "obs_max_pending": 1},
    )
    with pytest.raises(CapacityExceeded) as exc_info:
        await service.submit("k2", {})
    assert "max_pending" in exc_info.value.reason

    # 被拒绝的提交不留下占位记录（幂等键已释放，可重试）
    with pytest.raises(Exception):
        await service.get_by_key("k2")

    await service._executor.shutdown()
    assert (await service.get(r0.task_id)).status is TaskStatus.SUCCEEDED
    assert (await service.get(r1.task_id)).status is TaskStatus.SUCCEEDED


async def test_shutdown_cancels_inflight_to_terminal_state(make_service):
    service = make_service(latency=30.0, task_timeout=60.0, shutdown_grace=0.1)
    record, _ = await service.submit("k-run", {})
    await asyncio.sleep(0.05)  # 任务进入 RUNNING

    await service._executor.shutdown()

    record = await service.get(record.task_id)
    log.info(
        "判定依据：停机宽限超时后，在途任务进入 cancelled 终态",
        extra={"obs_status": record.status.value, "obs_reason": record.error["reason"]},
    )
    assert record.status is TaskStatus.CANCELLED
    assert record.error["category"] == "task_cancelled"
    assert record.finished_at is not None
    assert service._executor.inflight_count == 0  # 无资源泄漏


async def test_shutdown_drains_queued_tasks(make_service):
    service = make_service(latency=30.0, max_workers=1, max_pending=4,
                           task_timeout=60.0, shutdown_grace=0.1)
    running, _ = await service.submit("kR", {})
    queued, _ = await service.submit("kQ", {})
    await asyncio.sleep(0.05)

    await service._executor.shutdown()

    q = await service.get(queued.task_id)
    log.info(
        "判定依据：停机时排队任务同样进入明确终态，不被执行",
        extra={"obs_status": q.status.value, "obs_attempt": q.attempt},
    )
    assert q.status is TaskStatus.CANCELLED
    assert q.attempt == 0
    assert service._executor.inflight_count == 0


async def test_no_duplicate_execution_after_shutdown(make_service):
    service = make_service(latency=30.0, task_timeout=60.0, shutdown_grace=0.05, fanout=1)
    record, _ = await service.submit("k-once", {})
    await asyncio.sleep(0.05)
    await service._executor.shutdown()

    downstream = service._executor._downstream
    calls_after_shutdown = downstream.calls
    await asyncio.sleep(0.1)  # 观察是否有残留任务再次执行
    log.info(
        "判定依据：停机后下游调用数不再增长，无重复执行",
        extra={"obs_calls": downstream.calls},
    )
    assert downstream.calls == calls_after_shutdown
    assert (await service.get(record.task_id)).attempt == 1


async def test_enqueue_rejected_after_shutdown(make_service):
    service = make_service()
    await service._executor.shutdown()
    log.info("判定依据：停机后不再接受新任务", extra={"obs_reason": "not accepting"})
    with pytest.raises(CapacityExceeded) as exc_info:
        await service.submit("k-late", {})
    assert "shutting down" in exc_info.value.message


async def test_graceful_shutdown_lets_short_tasks_finish(make_service):
    service = make_service(latency=0.05, task_timeout=5.0, shutdown_grace=2.0)
    record, _ = await service.submit("k-quick", {})
    await service._executor.shutdown()
    record = await service.get(record.task_id)
    log.info(
        "判定依据：宽限期内可完成的任务正常收尾为 succeeded",
        extra={"obs_status": record.status.value},
    )
    assert record.status is TaskStatus.SUCCEEDED
