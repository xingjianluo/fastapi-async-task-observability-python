import asyncio

import pytest

from app.idempotency import IdempotencyStore, SubmissionOutcome

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def test_first_acquire_creates():
    store = IdempotencyStore()
    outcome, record = await store.acquire("k1", "task-1", "corr-1")
    assert outcome == SubmissionOutcome.CREATED
    assert record.task_id == "task-1"


async def test_concurrent_acquire_single_winner():
    store = IdempotencyStore()
    results = await asyncio.gather(
        *(store.acquire("k", f"task-{i}", f"corr-{i}") for i in range(50))
    )
    created = [r for r in results if r[0] == SubmissionOutcome.CREATED]
    reused = [r for r in results if r[0] == SubmissionOutcome.REUSED_INFLIGHT]
    assert len(created) == 1
    assert len(reused) == 49
    # 所有命中者复用同一个 task_id
    winner_task_id = created[0][1].task_id
    assert all(r[1].task_id == winner_task_id for r in reused)


async def test_completed_record_reused_as_completed():
    store = IdempotencyStore()
    await store.acquire("k", "task-1", "corr-1")
    store.mark_completed("k")
    outcome, record = await store.acquire("k", "task-2", "corr-2")
    assert outcome == SubmissionOutcome.REUSED_COMPLETED
    assert record.task_id == "task-1"


async def test_ttl_cleanup_only_completed():
    store = IdempotencyStore(ttl_seconds=10.0)
    await store.acquire("inflight", "task-1", "c1")
    await store.acquire("done", "task-2", "c2")
    store.mark_completed("done")
    # 时间推进超过 TTL
    now = (await store.get("done")).completed_at + 11.0
    removed = await store.cleanup_expired(now=now)
    assert removed == 1
    assert await store.get("done") is None
    # 进行中记录不会被清理
    assert await store.get("inflight") is not None
