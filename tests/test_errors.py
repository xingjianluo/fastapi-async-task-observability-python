"""下游异常、超时与内部错误的归一化：类别可区分，不静默吞掉，不透出底层异常。"""
from __future__ import annotations

import asyncio

import pytest

from app.downstream import DownstreamClient
from app.errors import (
    DownstreamError,
    DownstreamTimeout,
    ErrorCategory,
    normalize_exception,
)
from app.logging_config import get_logger
from app.statemachine import TaskStatus

log = get_logger("tests.errors")


async def test_downstream_error_normalized_to_failed(make_service):
    service = make_service(fail_mode="error")
    record, _ = await service.submit("k-err", {})
    await service._executor.shutdown()

    record = await service.get(record.task_id)
    log.info(
        "判定依据：下游 5xx 归一化为 downstream_error，任务进入 failed 终态",
        extra={"obs_category": record.error["category"], "obs_reason": record.error["reason"]},
    )
    assert record.status is TaskStatus.FAILED
    assert record.error["category"] == ErrorCategory.DOWNSTREAM_ERROR.value
    assert "simulated downstream 5xx" in record.error["reason"]


async def test_downstream_timeout_normalized_to_task_timeout(make_service):
    # 下游挂起 + 任务级超时 -> 任务进入 timed_out，类别可区分
    service = make_service(fail_mode="hang", timeout=60.0, task_timeout=0.05)
    record, _ = await service.submit("k-timeout", {})
    await service._executor.shutdown()

    record = await service.get(record.task_id)
    log.info(
        "判定依据：任务超时归一化为 task_timeout，与下游错误类别可区分",
        extra={"obs_category": record.error["category"], "obs_reason": record.error["reason"]},
    )
    assert record.status is TaskStatus.TIMED_OUT
    assert record.error["category"] == ErrorCategory.TASK_TIMEOUT.value
    assert "task_timeout=0.05" in record.error["reason"]


async def test_downstream_client_timeout_normalized():
    client = DownstreamClient(fail_mode="hang", timeout=0.02)
    with pytest.raises(DownstreamTimeout) as exc_info:
        await client.call({})
    log.info(
        "判定依据：下游调用超时归一化为 DownstreamTimeout，不透出 asyncio.TimeoutError",
        extra={"obs_reason": exc_info.value.reason},
    )
    assert exc_info.value.category is ErrorCategory.DOWNSTREAM_TIMEOUT


async def test_raw_exceptions_normalized_not_leaked():
    cases = [
        (asyncio.TimeoutError(), ErrorCategory.DOWNSTREAM_TIMEOUT),
        (asyncio.CancelledError(), ErrorCategory.TASK_CANCELLED),
        (ValueError("boom"), ErrorCategory.INTERNAL_ERROR),
        (DownstreamError("x"), ErrorCategory.DOWNSTREAM_ERROR),
    ]
    for raw, expected in cases:
        normalized = normalize_exception(raw)
        log.info(
            f"判定依据：{type(raw).__name__} -> {normalized.category.value}",
            extra={"obs_reason": normalized.reason},
        )
        assert normalized.category is expected
        # 归一化结果不携带原始堆栈文本
        assert "Traceback" not in normalized.reason


async def test_failure_categories_are_distinguishable(make_service):
    """同一服务内，下游错误与超时失败的原因字段可区分。"""
    svc_err = make_service(fail_mode="error")
    r1, _ = await svc_err.submit("k-c1", {})
    await svc_err._executor.shutdown()

    svc_to = make_service(fail_mode="hang", timeout=60.0, task_timeout=0.05)
    r2, _ = await svc_to.submit("k-c2", {})
    await svc_to._executor.shutdown()

    e1 = (await svc_err.get(r1.task_id)).error
    e2 = (await svc_to.get(r2.task_id)).error
    log.info(
        "判定依据：两类失败的 category 与 reason 均不同",
        extra={"obs_c1": e1["category"], "obs_c2": e2["category"]},
    )
    assert e1["category"] != e2["category"]
    assert e1["reason"] != e2["reason"]
