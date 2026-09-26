"""并发幂等提交：同一幂等键只产生一次副作用，结果一致可解释。"""
from __future__ import annotations

import asyncio

import pytest

from app.context import get_correlation_id
from app.errors import CapacityExceeded
from app.logging_config import get_logger
from app.statemachine import TaskStatus

log = get_logger("tests.idempotency")


async def test_concurrent_same_idempotency_key_single_side_effect(make_service):
    service = make_service(fanout=3)
    downstream = service._executor._downstream

    cid = get_correlation_id()
    log.info(
        "判定依据：20 个并发提交同一幂等键，期望仅 1 次 created、1 个任务、fanout 次副作用",
        extra={"obs_correlation": cid, "obs_concurrency": 20},
    )

    results = await asyncio.gather(
        *(service.submit("key-X", {"n": i}) for i in range(20))
    )

    created_flags = [is_new for _, is_new in results]
    task_ids = {record.task_id for record, _ in results}
    log.info(
        f"结果：created 次数={sum(created_flags)}，任务数={len(task_ids)}",
        extra={"obs_created": sum(created_flags), "obs_distinct_tasks": len(task_ids)},
    )

    assert sum(created_flags) == 1
    assert len(task_ids) == 1

    await service._executor.shutdown()
    log.info(
        f"判定依据：下游调用总数={downstream.calls}，应恰好等于 fanout=3",
        extra={"obs_downstream_calls": downstream.calls},
    )
    assert downstream.calls == 3

    record = await service.get_by_key("key-X")
    assert record.status is TaskStatus.SUCCEEDED
    assert record.correlation_id == cid
    assert len(record.result["items"]) == 3


async def test_sequential_duplicate_submit_returns_same_record(make_service):
    service = make_service(fanout=1)
    record1, created1 = await service.submit("key-Y", {"a": 1})
    record2, created2 = await service.submit("key-Y", {"a": 1})  # 不同 payload 也不重放
    log.info(
        "判定依据：重复提交返回既有任务，不覆盖 payload",
        extra={"obs_reason": "same idempotency_key reuses first record"},
    )
    assert created1 is True and created2 is False
    assert record1.task_id == record2.task_id
    assert record2.payload == {"a": 1}


async def test_different_keys_execute_independently(make_service):
    service = make_service(fanout=1, max_workers=4)
    results = await asyncio.gather(
        *(service.submit(f"key-{i}", {}) for i in range(4))
    )
    assert len({r.task_id for r, _ in results}) == 4
    await service._executor.shutdown()
    records = await service._store.all_records()
    assert all(r.status is TaskStatus.SUCCEEDED for r in records)
    assert service._executor._downstream.calls == 4
