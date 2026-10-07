"""HTTP 端到端：关联标识透传、幂等语义与错误归一化响应。"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import create_app
from app.logging_config import get_logger
from app.retention import RetentionConfig, TaskReaper

log = get_logger("tests.api")


@pytest.fixture
async def client(make_service):
    service = make_service(latency=0.001)
    reaper = TaskReaper(service._store, RetentionConfig())
    app = create_app(service, reaper)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        c.service = service
        c.reaper = reaper
        yield c


async def test_correlation_id_propagated_and_generated(client):
    r = await client.post(
        "/tasks",
        json={"idempotency_key": "h1", "payload": {}},
        headers={"x-correlation-id": "cid-client-1"},
    )
    assert r.status_code == 201
    assert r.headers["x-correlation-id"] == "cid-client-1"
    log.info(
        "判定依据：请求头关联标识被透传到响应与任务记录",
        extra={"obs_correlation": "cid-client-1"},
    )
    assert r.json()["task"]["correlation_id"] == "cid-client-1"

    r2 = await client.post("/tasks", json={"idempotency_key": "h2", "payload": {}})
    generated = r2.headers["x-correlation-id"]
    assert generated and generated != "cid-client-1"
    assert r2.json()["task"]["correlation_id"] == generated


async def test_http_idempotent_submit(client):
    r1 = await client.post("/tasks", json={"idempotency_key": "h3", "payload": {"v": 1}})
    r2 = await client.post("/tasks", json={"idempotency_key": "h3", "payload": {"v": 1}})
    log.info(
        "判定依据：重复提交返回 200 与同一 task_id，首次为 201",
        extra={"obs_s1": r1.status_code, "obs_s2": r2.status_code},
    )
    assert r1.status_code == 201 and r2.status_code == 200
    assert r1.json()["task"]["task_id"] == r2.json()["task"]["task_id"]
    assert r2.json()["created"] is False


async def test_error_mapping(client):
    # 404：任务不存在
    r = await client.get("/tasks/ghost")
    assert r.status_code == 404
    assert r.json()["error"]["category"] == "task_not_found"

    # 409：对终态任务取消
    r = await client.post("/tasks", json={"idempotency_key": "h4", "payload": {}})
    tid = r.json()["task"]["task_id"]
    await client.service._executor.shutdown()
    r = await client.post(f"/tasks/{tid}/cancel")
    log.info(
        "判定依据：终态任务取消返回 409，类别为 invalid_state_transition",
        extra={"obs_category": r.json()["error"]["category"]},
    )
    assert r.status_code == 409
    assert r.json()["error"]["category"] == "invalid_state_transition"
    assert "terminal" in r.json()["error"]["reason"]


async def test_concurrent_http_same_key_single_task(make_service):
    service = make_service(latency=0.001, fanout=1)
    app = create_app(service)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        responses = await asyncio.gather(
            *(c.post("/tasks", json={"idempotency_key": "h5", "payload": {}})
              for _ in range(10))
        )
    task_ids = {r.json()["task"]["task_id"] for r in responses}
    created = sum(1 for r in responses if r.json()["created"])
    log.info(
        "判定依据：10 个并发 HTTP 提交同一幂等键，仅 1 个任务、1 次 created",
        extra={"obs_distinct": len(task_ids), "obs_created": created},
    )
    assert len(task_ids) == 1 and created == 1
    await service._executor.shutdown()
    assert service._executor._downstream.calls == 1


async def test_http_list_tasks_with_filters(client):
    for i in range(3):
        await client.post("/tasks", json={"idempotency_key": f"l{i}", "payload": {}})
    await client.service._executor.shutdown()

    r = await client.get("/tasks", params={"status": "succeeded", "limit": 2})
    body = r.json()
    log.info(
        "判定依据：按状态过滤并分页，total 为符合条件的总量，与 limit 无关",
        extra={"obs_total": body["total"], "obs_page": len(body["tasks"])},
    )
    assert r.status_code == 200
    assert body["total"] == 3 and len(body["tasks"]) == 2
    assert all(t["status"] == "succeeded" for t in body["tasks"])

    r2 = await client.get("/tasks", params={"limit": 2, "offset": 2})
    assert len(r2.json()["tasks"]) == 1

    r3 = await client.get("/tasks", params={"status": "not-a-status"})
    assert r3.status_code == 422  # 非法状态值由参数校验拒绝


async def test_http_release_terminal_task_then_key_reusable(client):
    r = await client.post("/tasks", json={"idempotency_key": "r1", "payload": {}})
    tid = r.json()["task"]["task_id"]
    for _ in range(200):  # 轮询等待终态，不停机
        if (await client.get(f"/tasks/{tid}")).json()["task"]["status"] == "succeeded":
            break
        await asyncio.sleep(0.01)

    rel = await client.post(f"/tasks/{tid}/release")
    assert rel.status_code == 200
    assert rel.json()["released"] is True

    again = await client.post("/tasks", json={"idempotency_key": "r1", "payload": {}})
    log.info(
        "判定依据：终态任务释放后，同键提交得到 201 与全新 task_id",
        extra={"obs_status": again.status_code, "obs_created": again.json()["created"]},
    )
    assert again.status_code == 201
    assert again.json()["created"] is True
    assert again.json()["task"]["task_id"] != tid
    await client.service._executor.shutdown()


async def test_http_release_rejects_running_and_missing(make_service):
    # 慢下游保证任务在释放时仍处于在途状态
    service = make_service(latency=0.3, fanout=1)
    app = create_app(service)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        r = await client.post("/tasks", json={"idempotency_key": "r2", "payload": {}})
        tid = r.json()["task"]["task_id"]

        missing = await client.post("/tasks/ghost/release")
        assert missing.status_code == 404
        assert missing.json()["error"]["category"] == "task_not_found"

        not_terminal = await client.post(f"/tasks/{tid}/release")
        log.info(
            "判定依据：不存在 404 / 未终态 409，类别可区分且不带内部细节",
            extra={
                "obs_missing": missing.json()["error"]["category"],
                "obs_not_terminal": not_terminal.json()["error"]["category"],
            },
        )
        assert not_terminal.status_code == 409
        assert not_terminal.json()["error"]["category"] == "task_not_releasable"
        assert not_terminal.json()["error"]["current"] in ("pending", "running")
    await service._executor.shutdown()


async def test_http_retention_stats(client):
    r = await client.get("/retention/stats")
    body = r.json()
    log.info(
        "判定依据：回收指标对外暴露累计回收数、最近回收时间与扫描轮次",
        extra={"obs_body": body},
    )
    assert body["enabled"] is True
    assert body["total_reclaimed"] == 0
    assert body["last_reclaim_at"] is None
    await client.service._executor.shutdown()
