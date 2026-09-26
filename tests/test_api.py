"""HTTP 端到端：关联标识透传、幂等语义与错误归一化响应。"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.api import create_app
from app.logging_config import get_logger

log = get_logger("tests.api")


@pytest.fixture
async def client(make_service):
    service = make_service(latency=0.001)
    app = create_app(service)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        c.service = service
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
