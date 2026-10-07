"""保留与回收：到期边界、在途保护、幂等键复用、容量淘汰、并发回收、手动释放与查询。"""
from __future__ import annotations

import asyncio

import pytest

from app.errors import TaskNotFound, TaskNotReleasable
from app.logging_config import get_logger
from app.retention import RetentionConfig, TaskReaper

from app.statemachine import TERMINAL_STATES, TaskStatus


async def wait_terminal(service, task_id: str, timeout: float = 2.0):
    """轮询等待任务进入终态（不触发执行器停机，后续提交不受影响）。"""
    for _ in range(int(timeout / 0.01)):
        record = await service._store.get(task_id)
        if record is not None and record.status in TERMINAL_STATES:
            return record
        await asyncio.sleep(0.01)
    raise AssertionError(f"task {task_id} did not reach terminal state")


log = get_logger("tests.retention")


class FakeClock:
    """可手动推进的时钟，让保留期边界可确定性验证。"""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_reaper(service, clock, **cfg) -> TaskReaper:
    return TaskReaper(service._store, RetentionConfig(**cfg), wall_clock=clock)


async def test_retention_expired_vs_not_expired_boundary(make_service):
    clock = FakeClock()
    service = make_service(clock=clock, fanout=1)
    reaper = make_reaper(service, clock, retention_seconds=100.0, max_history=1000)

    old, _ = await service.submit("k-old", {})
    await wait_terminal(service, old.task_id)
    clock.advance(50.0)  # old 已终态 50s
    fresh, _ = await service.submit("k-fresh", {})
    await wait_terminal(service, fresh.task_id)
    clock.advance(60.0)  # old 终态已 110s(>=100)，fresh 终态约 60s(<100)

    reclaimed = await reaper.sweep_once()
    reclaimed_ids = {r.task_id for r in reclaimed}
    log.info(
        "判定依据：retention=100s，old 终态 110s 应被回收，fresh 终态 60s 应保留",
        extra={"obs_reclaimed": len(reclaimed), "obs_stats": reaper.stats().to_dict()},
    )
    assert reclaimed_ids == {old.task_id}
    assert await service._store.get(old.task_id) is None
    assert (await service._store.get(fresh.task_id)).status is TaskStatus.SUCCEEDED
    assert reaper.stats().total_reclaimed == 1
    assert reaper.stats().last_reclaim_at == clock.now
    await service._executor.shutdown()


async def test_reaper_does_not_touch_inflight_tasks(make_service):
    clock = FakeClock()
    service = make_service(clock=clock, latency=0.3, fanout=1)
    reaper = make_reaper(service, clock, retention_seconds=10.0, max_history=1000)

    running, _ = await service.submit("k-run", {})
    await asyncio.sleep(0.05)  # 进入 RUNNING
    clock.advance(10_000.0)  # 时钟远超保留期

    reclaimed = await reaper.sweep_once()
    log.info(
        "判定依据：在途任务无 finished_at，即使时钟远超保留期也不得被回收",
        extra={"obs_reclaimed": len(reclaimed), "obs_status": "running"},
    )
    assert reclaimed == []
    record = await service._store.get(running.task_id)
    assert record is not None and record.status is TaskStatus.RUNNING
    await service._executor.shutdown()


async def test_idempotency_key_semantics_before_and_after_reclaim(make_service):
    clock = FakeClock()
    service = make_service(clock=clock, fanout=1)
    reaper = make_reaper(service, clock, retention_seconds=100.0, max_history=1000)
    downstream = service._executor._downstream

    first, created1 = await service.submit("k-reuse", {"v": 1})
    await wait_terminal(service, first.task_id)
    clock.advance(50.0)  # 保留期内

    dup, created2 = await service.submit("k-reuse", {"v": 2})
    log.info(
        "判定依据：保留期内同键提交必须命中同一任务，不重复执行",
        extra={"obs_created": created2, "obs_same_task": dup.task_id == first.task_id},
    )
    assert created2 is False and dup.task_id == first.task_id
    assert downstream.calls == 1

    clock.advance(60.0)  # 终态已 110s，超过保留期
    await reaper.sweep_once()
    renewed, created3 = await service.submit("k-reuse", {"v": 3})
    log.info(
        "判定依据：回收后同键提交视为全新任务，重新执行并产生新业务结果",
        extra={"obs_created": created3, "obs_new_task": renewed.task_id != first.task_id},
    )
    assert created3 is True and renewed.task_id != first.task_id
    final = await wait_terminal(service, renewed.task_id)
    assert downstream.calls == 2
    assert final.status is TaskStatus.SUCCEEDED
    assert final.payload == {"v": 3}
    await service._executor.shutdown()


async def test_max_history_evicts_oldest_terminal_first(make_service):
    clock = FakeClock()
    service = make_service(clock=clock, fanout=1)
    reaper = make_reaper(service, clock, retention_seconds=1e9, max_history=2)

    ids = []
    for i in range(3):
        record, _ = await service.submit(f"k-cap-{i}", {})
        await wait_terminal(service, record.task_id)
        ids.append(record.task_id)
        clock.advance(10.0)  # finished_at 依次变新

    evicted = await reaper.sweep_once()
    log.info(
        "判定依据：max_history=2，3 条终态中 finished_at 最旧的 k-cap-0 应被淘汰",
        extra={"obs_evicted": [r.task_id for r in evicted]},
    )
    assert [r.task_id for r in evicted] == [ids[0]]
    assert await service._store.get(ids[0]) is None
    assert await service._store.get(ids[1]) is not None
    assert await service._store.get(ids[2]) is not None
    await service._executor.shutdown()


async def test_reclaim_and_submit_same_key_concurrently(make_service):
    clock = FakeClock()
    service = make_service(clock=clock, fanout=1)
    reaper = make_reaper(service, clock, retention_seconds=100.0, max_history=1000)

    first, _ = await service.submit("k-race", {})
    await wait_terminal(service, first.task_id)
    clock.advance(200.0)  # 已过期，回收与提交竞争

    (reclaimed, (record, created)) = await asyncio.gather(
        reaper.sweep_once(), service.submit("k-race", {"v": 9})
    )
    log.info(
        "判定依据：回收与提交在锁下串行化，两种合法结局都必须保持键与记录一致",
        extra={
            "obs_reclaimed": len(reclaimed),
            "obs_created": created,
            "obs_same_task": record.task_id == first.task_id,
        },
    )
    if created:
        # 回收先完成：同键视为全新任务，新任务在途不得被本轮回收
        assert record.task_id != first.task_id
        assert [r.task_id for r in reclaimed] == [first.task_id]
        assert (await service.get_by_key("k-race")).task_id == record.task_id
    else:
        # 提交先命中旧记录：随后回收将其清掉，键随之释放
        assert record.task_id == first.task_id
        assert await service._store.get(first.task_id) is None
        with pytest.raises(TaskNotFound):
            await service.get_by_key("k-race")
    await service._executor.shutdown()


async def test_manual_release_terminal_task_frees_key(make_service):
    service = make_service(fanout=1)
    first, _ = await service.submit("k-rel", {})
    await wait_terminal(service, first.task_id)

    released = await service.release(first.task_id)
    log.info(
        "判定依据：终态任务手动释放后记录移除、幂等键可复用",
        extra={"obs_status": released.status.value, "obs_key": released.idempotency_key},
    )
    assert released.status is TaskStatus.SUCCEEDED
    assert await service._store.get(first.task_id) is None

    renewed, created = await service.submit("k-rel", {"v": 2})
    assert created is True and renewed.task_id != first.task_id
    await service._executor.shutdown()


async def test_manual_release_rejects_running_and_missing(make_service):
    service = make_service(latency=0.3, fanout=1)
    running, _ = await service.submit("k-busy", {})
    await asyncio.sleep(0.05)  # 进入 RUNNING

    with pytest.raises(TaskNotReleasable) as exc_info:
        await service.release(running.task_id)
    log.info(
        "判定依据：在途任务释放被拒，类别 task_not_releasable，键仍被占用",
        extra={"obs_category": exc_info.value.category.value, "obs_current": exc_info.value.current},
    )
    assert exc_info.value.category.value == "task_not_releasable"
    assert exc_info.value.current == "running"
    dup, created = await service.submit("k-busy", {})
    assert created is False and dup.task_id == running.task_id  # 键未被释放

    with pytest.raises(TaskNotFound) as exc_info2:
        await service.release("ghost-task")
    log.info(
        "判定依据：不存在的目标给出可区分的 task_not_found",
        extra={"obs_category": exc_info2.value.category.value},
    )
    assert exc_info2.value.category.value == "task_not_found"
    await service._executor.shutdown()


async def test_query_filters_and_pagination(make_service):
    clock = FakeClock()
    service = make_service(clock=clock, fanout=1, fail_mode=None)
    fail_service = service  # 同一服务内制造不同状态

    ok, _ = await service.submit("q-ok", {})
    await wait_terminal(service, ok.task_id)
    clock.advance(10.0)
    bad, _ = await fail_service.submit("q-bad", {})
    await service._store.transition(bad.task_id, TaskStatus.RUNNING)
    await service._store.transition(
        bad.task_id, TaskStatus.FAILED, error=TaskNotFound("x", reason="simulated")
    )
    clock.advance(10.0)
    # 直接在存储中占位一条 PENDING 记录（不经执行器，状态稳定在排队中）
    from app.store import TaskRecord

    pending = TaskRecord(
        task_id="q-pending-id",
        idempotency_key="q-pending",
        correlation_id="test",
        status=TaskStatus.PENDING,
        payload={},
    )
    await service._store.submit(pending)

    all_tasks, total = await service.list_tasks()
    log.info(
        "判定依据：无过滤应返回全部 3 条并按创建时间升序",
        extra={"obs_total": total, "obs_statuses": [r.status.value for r in all_tasks]},
    )
    assert total == 3
    assert [r.idempotency_key for r in all_tasks] == ["q-ok", "q-bad", "q-pending"]

    failed, total_failed = await service.list_tasks(status=TaskStatus.FAILED)
    assert total_failed == 1 and failed[0].idempotency_key == "q-bad"

    t_mid = clock.now - 15.0  # q-ok 创建于 now-20，q-bad 创建于 now-10
    ranged, total_ranged = await service.list_tasks(created_from=t_mid)
    log.info(
        "判定依据：created_from 过滤掉更早创建的任务",
        extra={"obs_from": t_mid, "obs_total": total_ranged},
    )
    assert {r.idempotency_key for r in ranged} == {"q-bad", "q-pending"}

    page1, _ = await service.list_tasks(limit=2, offset=0)
    page2, _ = await service.list_tasks(limit=2, offset=2)
    assert [r.idempotency_key for r in page1] == ["q-ok", "q-bad"]
    assert [r.idempotency_key for r in page2] == ["q-pending"]
    await service._executor.shutdown()
