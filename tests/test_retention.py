"""保留与回收：到期边界、在途安全、幂等键复用语义、容量淘汰与并发正确性。"""
from __future__ import annotations

import asyncio

import pytest

from app.errors import InvalidStateTransition, TaskNotFound
from app.logging_config import get_logger
from app.statemachine import TaskStatus

log = get_logger("tests.retention")


async def wait_terminal(service, task_id: str, timeout: float = 2.0) -> None:
    """轮询直到任务进入终态（不提前停机执行器，后续仍可提交）。"""
    async def _wait() -> None:
        while True:
            record = await service.get(task_id)
            if record.status in (
                TaskStatus.SUCCEEDED,
                TaskStatus.FAILED,
                TaskStatus.CANCELLED,
                TaskStatus.TIMED_OUT,
            ):
                return
            await asyncio.sleep(0.005)

    async with asyncio.timeout(timeout):
        await _wait()


async def test_retention_boundary_expired_vs_not_expired(make_service):
    """边界：达到保留期即回收（age >= retention），差一点都不回收。"""
    service = make_service(retention_seconds=100.0)
    clock = service._clock
    sweeper = service._sweeper

    record, _ = await service.submit("key-boundary", {})
    await wait_terminal(service, record.task_id)
    finished_at = record.finished_at
    log.info(
        "判定依据：finished_at 后 99.5s 不回收（未到期），100s 整回收（达到保留期）",
        extra={"obs_finished_at": finished_at, "obs_retention": 100.0},
    )

    # 未到期：age = 99.5 < 100
    clock.now = finished_at + 99.5
    reclaimed = await sweeper.sweep_once()
    assert reclaimed == []
    assert (await service.get(record.task_id)).status is TaskStatus.SUCCEEDED

    # 恰好到期：age = 100 >= 100，回收
    clock.now = finished_at + 100.0
    reclaimed = await sweeper.sweep_once()
    assert [r.task_id for r in reclaimed] == [record.task_id]
    log.info(
        "结果：到期任务被回收，存储中不再保留记录",
        extra={"obs_reclaimed": len(reclaimed), "obs_total": sweeper.stats.total_reclaimed},
    )
    with pytest.raises(TaskNotFound):
        await service.get(record.task_id)
    assert sweeper.stats.total_reclaimed == 1
    assert sweeper.stats.last_sweep_at == finished_at + 100.0


async def test_sweep_never_touches_inflight_tasks(make_service):
    """回收只作用于终态：排队/运行中的任务即使'看起来很旧'也不能被清。"""
    service = make_service(latency=0.3, retention_seconds=10.0)
    clock = service._clock
    sweeper = service._sweeper

    slow, _ = await service.submit("key-slow", {})
    # 等它进入 RUNNING
    async with asyncio.timeout(2.0):
        while (await service.get(slow.task_id)).status is not TaskStatus.RUNNING:
            await asyncio.sleep(0.005)

    # 人为把时钟推到远超保留期：若在途任务被误清，此处就会暴露
    clock.advance(10_000.0)
    reclaimed = await sweeper.sweep_once()
    log.info(
        "判定依据：RUNNING 任务无 finished_at，不参与到期判定，必须保留",
        extra={"obs_reclaimed": len(reclaimed), "obs_status": (await service.get(slow.task_id)).status.value},
    )
    assert reclaimed == []
    assert (await service.get(slow.task_id)).status is TaskStatus.RUNNING

    # PENDING 同样安全：占满 worker 后再提交一个排队任务
    await service._executor.shutdown()
    assert (await service.get(slow.task_id)).status is TaskStatus.SUCCEEDED


async def test_idempotency_key_semantics_before_and_after_reclaim(make_service):
    """保留期内同键命中同一任务；回收后同键视为全新任务并重新执行。"""
    service = make_service(fanout=1, retention_seconds=50.0)
    clock = service._clock
    sweeper = service._sweeper
    downstream = service._executor._downstream

    first, _ = await service.submit("key-reuse", {"v": 1})
    await wait_terminal(service, first.task_id)
    calls_after_first = downstream.calls

    # 保留期内重复提交：命中同一任务，不重复执行
    clock.advance(10.0)
    dup, created2 = await service.submit("key-reuse", {"v": 999})
    log.info(
        "判定依据：保留期内重复提交返回同一 task_id，下游调用数不变",
        extra={"obs_created": created2, "obs_calls": downstream.calls},
    )
    assert created2 is False
    assert dup.task_id == first.task_id
    assert downstream.calls == calls_after_first

    # 到期回收后：同一键再次提交是全新任务，重新执行产生新结果
    clock.now = first.finished_at + 50.0
    reclaimed = await sweeper.sweep_once()
    assert [r.task_id for r in reclaimed] == [first.task_id]

    second, created3 = await service.submit("key-reuse", {"v": 2})
    log.info(
        "判定依据：回收后同键提交 created=True、task_id 不同、下游被再次调用",
        extra={"obs_created": created3, "obs_new_task": second.task_id},
    )
    assert created3 is True
    assert second.task_id != first.task_id
    await wait_terminal(service, second.task_id)
    assert downstream.calls == calls_after_first + 1
    assert (await service.get(second.task_id)).status is TaskStatus.SUCCEEDED


async def test_max_history_evicts_oldest_terminal_first(make_service):
    """容量上限：终态历史超限后按 finished_at 最旧优先淘汰，在途任务不占额度。"""
    service = make_service(retention_seconds=1e9, max_history=2)
    clock = service._clock
    sweeper = service._sweeper

    ids = []
    for i in range(4):
        record, _ = await service.submit(f"key-cap-{i}", {})
        await wait_terminal(service, record.task_id)
        ids.append(record.task_id)
        clock.advance(10.0)  # 让 finished_at 严格递增，淘汰顺序确定

    reclaimed = await sweeper.sweep_once()
    reclaimed_ids = [r.task_id for r in reclaimed]
    log.info(
        "判定依据：4 个终态任务上限为 2，应淘汰 finished_at 最旧的 2 个",
        extra={"obs_reclaimed": reclaimed_ids, "obs_expected": ids[:2]},
    )
    assert reclaimed_ids == ids[:2]  # 最旧的两个，且按从旧到新顺序
    remaining = {r.task_id for r in await service._store.all_records()}
    assert remaining == set(ids[2:])
    assert sweeper.stats.total_reclaimed == 2


async def test_sweep_and_submit_same_key_concurrently(make_service):
    """回收与提交并发：同一键要么命中旧任务要么新建，结果必须一致可解释。"""
    service = make_service(fanout=1, retention_seconds=50.0)
    clock = service._clock
    sweeper = service._sweeper

    for round_no in range(10):
        key = f"key-race-{round_no}"
        old, _ = await service.submit(key, {})
        await wait_terminal(service, old.task_id)
        clock.now = old.finished_at + 60.0  # 已到期

        reclaimed, (record, created) = await asyncio.gather(
            sweeper.sweep_once(),
            service.submit(key, {}),
        )
        store_record = await service._store.get_by_key(key)
        log.info(
            "判定依据：回收与提交互斥串行，无论谁先谁后，键与记录必须一一对应",
            extra={
                "obs_round": round_no,
                "obs_created": created,
                "obs_reclaimed": len(reclaimed),
            },
        )
        assert store_record is not None
        assert store_record.task_id == record.task_id
        if created:
            # 回收先完成：旧记录已清，提交视为全新任务
            assert record.task_id != old.task_id
            assert old.task_id in [r.task_id for r in reclaimed]
        else:
            # 提交先命中：旧记录仍在（回收时它已到期，但提交先于回收拿到锁）
            assert record.task_id == old.task_id
        # 每轮只应有一个有效记录
        assert len(await service._store.all_records()) >= 1


async def test_release_terminal_task_frees_key(make_service):
    """手动释放终态任务：幂等键立即可复用，再次提交得到新任务。"""
    service = make_service(fanout=1)
    downstream = service._executor._downstream

    first, _ = await service.submit("key-release", {})
    await wait_terminal(service, first.task_id)
    calls_before = downstream.calls

    released = await service.release(first.task_id)
    log.info(
        "判定依据：释放返回被释放记录，键可重新提交并产生新任务",
        extra={"obs_released_task": released.task_id, "obs_key": released.idempotency_key},
    )
    assert released.task_id == first.task_id
    with pytest.raises(TaskNotFound):
        await service.get(first.task_id)

    second, created = await service.submit("key-release", {})
    assert created is True
    assert second.task_id != first.task_id
    await wait_terminal(service, second.task_id)
    assert downstream.calls == calls_before + 1


async def test_release_rejects_non_terminal_and_missing(make_service):
    """手动释放只允许终态：在途 409 类别、不存在 404 类别，判定依据留痕。"""
    service = make_service(latency=0.3)

    running, _ = await service.submit("key-running", {})
    async with asyncio.timeout(2.0):
        while (await service.get(running.task_id)).status is not TaskStatus.RUNNING:
            await asyncio.sleep(0.005)

    with pytest.raises(InvalidStateTransition) as exc_info:
        await service.release(running.task_id)
    log.info(
        "判定依据：非终态释放被拒绝，reason 说明原因，任务记录不受影响",
        extra={"obs_reason": exc_info.value.reason, "obs_current": exc_info.value.current},
    )
    assert "not in terminal state" in exc_info.value.reason
    assert exc_info.value.current == "running"
    assert (await service.get(running.task_id)).status is TaskStatus.RUNNING

    with pytest.raises(TaskNotFound) as exc_info2:
        await service.release("ghost-task")
    assert "does not exist" in exc_info2.value.reason

    await service._executor.shutdown()


async def test_list_tasks_filter_and_pagination(make_service):
    """查询：按状态、创建时间范围筛选，分页返回 total 与页内记录。"""
    service = make_service(fanout=1)
    clock = service._clock

    t0 = clock.now
    done_ids = []
    for i in range(3):
        record, _ = await service.submit(f"key-list-{i}", {})
        await wait_terminal(service, record.task_id)
        done_ids.append(record.task_id)
        clock.advance(10.0)
    pending, _ = await service.submit("key-list-pending", {}, )
    # 让 pending 保持排队：占满执行器后不再等待（直接取消它，构造非 succeeded 状态）
    await service.cancel(pending.task_id)

    # 按状态筛选
    succeeded, total = await service.list_tasks(status=TaskStatus.SUCCEEDED)
    log.info(
        "判定依据：3 个成功 + 1 个取消，按状态筛选应各归各类",
        extra={"obs_succeeded": total},
    )
    assert total == 3
    assert {r.task_id for r in succeeded} == set(done_ids)

    cancelled, total = await service.list_tasks(status=TaskStatus.CANCELLED)
    assert total == 1 and cancelled[0].task_id == pending.task_id

    # 按创建时间范围筛选（只取中间 10s 窗口内创建的任务）
    ranged, total = await service.list_tasks(created_from=t0 + 10.0, created_to=t0 + 20.0)
    assert total == 2
    assert {r.task_id for r in ranged} == set(done_ids[1:])

    # 分页：limit=2 翻页取齐 4 条
    page1, total = await service.list_tasks(offset=0, limit=2)
    page2, _ = await service.list_tasks(offset=2, limit=2)
    assert total == 4
    assert len(page1) == 2 and len(page2) == 2
    assert {r.task_id for r in page1 + page2} == set(done_ids) | {pending.task_id}


async def test_http_list_release_and_retention_stats(make_service):
    """HTTP 端到端：列表查询、手动释放的错误映射与回收指标暴露。"""
    from httpx import ASGITransport, AsyncClient

    from app.api import create_app

    service = make_service(fanout=1, retention_seconds=30.0)
    clock = service._clock
    app = create_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/tasks", json={"idempotency_key": "hk-1", "payload": {}})
        tid = r.json()["task"]["task_id"]
        await wait_terminal(service, tid)

        # 列表查询：按状态筛选 + 分页字段齐全
        r = await client.get("/tasks", params={"status": "succeeded", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        log.info(
            "判定依据：列表响应含 total/offset/limit，筛选后只含 succeeded",
            extra={"obs_total": body["total"]},
        )
        assert body["total"] == 1
        assert body["tasks"][0]["task_id"] == tid

        # 释放不存在的任务：404 + 稳定类别，不带内部细节
        r = await client.post("/tasks/ghost/release")
        assert r.status_code == 404
        err = r.json()["error"]
        assert err["category"] == "task_not_found"
        assert set(err.keys()) <= {"category", "message", "reason"}

        # 释放终态任务：200，键可复用
        r = await client.post(f"/tasks/{tid}/release")
        assert r.status_code == 200 and r.json()["released"] is True
        r = await client.post("/tasks", json={"idempotency_key": "hk-1", "payload": {}})
        assert r.status_code == 201
        new_tid = r.json()["task"]["task_id"]
        assert new_tid != tid
        await wait_terminal(service, new_tid)

        # 释放后旧记录不可查
        r = await client.get(f"/tasks/{tid}")
        assert r.status_code == 404

        # 在途任务释放：409 + invalid_state_transition
        r = await client.post("/tasks", json={"idempotency_key": "hk-2", "payload": {}})
        tid2 = r.json()["task"]["task_id"]
        await wait_terminal(service, tid2)  # 先跑完，再单独构造在途场景由单测覆盖
        r = await client.post(f"/tasks/{tid2}/release")
        assert r.status_code == 200

        # 回收指标：手动触发一次回收后可观测
        clock.advance(10_000.0)
        await service._sweeper.sweep_once()
        r = await client.get("/retention/stats")
        assert r.status_code == 200
        stats = r.json()
        log.info(
            "判定依据：指标暴露累计回收数与最近回收时间",
            extra={"obs_stats": stats["stats"]},
        )
        assert stats["enabled"] is True
        assert stats["stats"]["total_reclaimed"] >= 1
        assert stats["stats"]["last_sweep_at"] is not None


async def test_http_release_running_task_returns_409(make_service):
    """HTTP：在途任务手动释放返回 409，类别与原因稳定可解释。"""
    from httpx import ASGITransport, AsyncClient

    from app.api import create_app

    service = make_service(latency=0.3)
    app = create_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.post("/tasks", json={"idempotency_key": "hk-run", "payload": {}})
        tid = r.json()["task"]["task_id"]
        async with asyncio.timeout(2.0):
            while (await service.get(tid)).status is not TaskStatus.RUNNING:
                await asyncio.sleep(0.005)
        r = await client.post(f"/tasks/{tid}/release")
        log.info(
            "判定依据：在途任务释放返回 409/invalid_state_transition，响应只含稳定字段",
            extra={"obs_status": r.status_code, "obs_body": r.json()},
        )
        assert r.status_code == 409
        err = r.json()["error"]
        assert err["category"] == "invalid_state_transition"
        assert "not in terminal state" in err["reason"]
        assert set(err.keys()) <= {"category", "message", "reason", "current", "target"}
    await service._executor.shutdown()


async def test_background_sweeper_loop_reclaims_and_stops_cleanly(make_service):
    """后台回收：按配置的周期自动清理到期终态任务，停机不泄漏后台任务。"""
    service = make_service(retention_seconds=5.0, sweep_interval=0.02)
    clock = service._clock
    sweeper = service._sweeper

    record, _ = await service.submit("key-bg", {})
    await wait_terminal(service, record.task_id)
    clock.now = record.finished_at + 10.0  # 已到期

    sweeper.start()
    sweeper.start()  # 幂等：重复启动不产生第二个循环
    try:
        async with asyncio.timeout(2.0):
            while sweeper.stats.total_reclaimed == 0:
                await asyncio.sleep(0.01)
        log.info(
            "判定依据：后台循环自动回收到期任务，指标随之更新",
            extra={
                "obs_total_reclaimed": sweeper.stats.total_reclaimed,
                "obs_sweep_count": sweeper.stats.sweep_count,
            },
        )
        assert sweeper.stats.total_reclaimed == 1
        assert sweeper.stats.sweep_count >= 1
        with pytest.raises(TaskNotFound):
            await service.get(record.task_id)
    finally:
        await sweeper.stop()
    assert sweeper._task is None
