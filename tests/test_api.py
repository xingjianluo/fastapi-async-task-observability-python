import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from contextlib import asynccontextmanager

from app.config import Settings
from app.main import create_app

pytestmark = pytest.mark.anyio


def make_settings(**overrides) -> Settings:
    base = dict(
        max_pending_tasks=16,
        task_timeout_seconds=2.0,
        downstream_timeout_seconds=1.0,
        shutdown_grace_seconds=2.0,
        worker_count=2,
        idempotency_ttl_seconds=0.0,
        cleanup_interval_seconds=60.0,
    )
    base.update(overrides)
    return Settings(**base)


@asynccontextmanager
async def make_client(**overrides):
    app = create_app(make_settings(**overrides))
    manager = app.state.task_manager
    await manager.start()
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        await manager.shutdown()


async def test_submit_and_get_task():
    async with make_client() as client:
        resp = await client.post(
            "/tasks",
            json={"payload": {"text": "hi"}},
            headers={"x-correlation-id": "corr-api-1"},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["outcome"] == "created"
        assert body["correlation_id"] == "corr-api-1"
        # 响应头回传关联标识
        assert resp.headers["x-correlation-id"] == "corr-api-1"

        task_id = body["task_id"]
        # 轮询直到完成
        for _ in range(100):
            got = await client.get(f"/tasks/{task_id}")
            if got.json()["status"] in ("succeeded", "failed"):
                break
            await asyncio.sleep(0.02)
        record = got.json()
        assert record["status"] == "succeeded"
        assert record["correlation_id"] == "corr-api-1"


async def test_idempotent_submit_via_api():
    async with make_client() as client:
        first = await client.post(
            "/tasks", json={"payload": {"text": "x"}, "idempotency_key": "k-1"}
        )
        second = await client.post(
            "/tasks", json={"payload": {"text": "x"}, "idempotency_key": "k-1"}
        )
        assert first.json()["task_id"] == second.json()["task_id"]
        assert second.json()["outcome"] in ("reused_inflight", "reused_completed")


async def test_get_unknown_task_404():
    async with make_client() as client:
        resp = await client.get("/tasks/nope")
        assert resp.status_code == 404
        assert resp.json()["error"]["category"] == "task_not_found"


async def test_cancel_terminal_task_409():
    async with make_client() as client:
        resp = await client.post("/tasks", json={"payload": {"text": "x"}})
        task_id = resp.json()["task_id"]
        for _ in range(100):
            got = await client.get(f"/tasks/{task_id}")
            if got.json()["status"] == "succeeded":
                break
            await asyncio.sleep(0.02)
        cancel = await client.post(f"/tasks/{task_id}/cancel")
        assert cancel.status_code == 409
        error = cancel.json()["error"]
        assert error["category"] == "invalid_state_transition"
        assert error["reason"] == "current_status_is_terminal"


async def test_downstream_failure_mapped_502():
    async with make_client() as client:
        resp = await client.post("/tasks", json={"payload": {"text": "x", "fail": True}})
        task_id = resp.json()["task_id"]
        for _ in range(100):
            got = await client.get(f"/tasks/{task_id}")
            if got.json()["status"] == "failed":
                break
            await asyncio.sleep(0.02)
        error = got.json()["error"]
        assert error["category"] == "downstream_error"
        assert "internal db exploded" not in str(error)


async def test_queue_full_returns_429():
    import asyncio as _asyncio

    gate = _asyncio.Event()

    async def blocking_handler(payload):
        await gate.wait()
        return True

    @asynccontextmanager
    async def make_blocking_client():
        app = create_app(make_settings(max_pending_tasks=2, worker_count=1),
                         handler=blocking_handler)
        manager = app.state.task_manager
        await manager.start()
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                yield client
        finally:
            gate.set()
            await manager.shutdown()

    async with make_blocking_client() as client:
        for _ in range(2):
            r = await client.post("/tasks", json={"payload": {}})
            assert r.status_code == 201
            await asyncio.sleep(0.02)
        r = await client.post("/tasks", json={"payload": {}})
        assert r.status_code == 429
        assert r.json()["error"]["category"] == "queue_full"
        assert "capacity=2" in r.json()["error"]["reason"]
