"""测试公共装置：结构化日志、关联标识与可配置的下游/服务工厂。"""
from __future__ import annotations

import logging

import pytest

from app.context import new_correlation_id, set_correlation_id
from app.downstream import DownstreamClient
from app.executor import ExecutorConfig, TaskExecutor
from app.logging_config import configure_logging, get_logger
from app.service import TaskService
from app.store import TaskStore

configure_logging(logging.INFO)


@pytest.fixture(autouse=True)
def test_correlation_context(request):
    """每个用例拥有独立关联标识，日志可据此串联判定依据。"""
    cid = f"test-{new_correlation_id()[:12]}"
    set_correlation_id(cid)
    get_logger("tests").info(
        "case start",
        extra={"obs_case": request.node.name, "obs_correlation": cid},
    )
    yield cid


@pytest.fixture
async def make_service():
    """构造可独立配置下游行为与执行器参数的服务实例。"""
    services: list[TaskService] = []

    def factory(
        *,
        latency: float = 0.001,
        timeout: float = 1.0,
        fail_mode: str | None = None,
        fanout: int = 2,
        max_workers: int = 4,
        max_pending: int = 8,
        task_timeout: float = 2.0,
        shutdown_grace: float = 1.0,
    ) -> TaskService:
        store = TaskStore()
        downstream = DownstreamClient(
            latency=latency, timeout=timeout, fail_mode=fail_mode, fanout=fanout
        )
        executor = TaskExecutor(
            store,
            downstream,
            ExecutorConfig(
                max_workers=max_workers,
                max_pending=max_pending,
                task_timeout=task_timeout,
                shutdown_grace=shutdown_grace,
            ),
        )
        service = TaskService(store, executor)
        services.append(service)
        return service

    yield factory

    # 用例结束后统一停机，避免泄漏后台任务
    import asyncio

    await asyncio.gather(*(s._executor.shutdown() for s in services))
