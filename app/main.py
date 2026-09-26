"""FastAPI 入口：关联标识中间件 + 任务提交/查询/取消接口 + 错误映射 + 优雅停机。"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import Settings, load_settings
from app.context import TraceContext, new_correlation_id, use_context
from app.errors import (
    AppError,
    ErrorCategory,
    InvalidStateTransitionError,
    QueueFullError,
    ShuttingDownError,
    TaskNotFoundError,
)
from app.idempotency import SubmissionOutcome
from app.logging import get_logger, log_event
from app.task_manager import TaskHandler, TaskManager

logger = get_logger()

CORRELATION_HEADER = "x-correlation-id"

# 错误类别 -> HTTP 状态码
_STATUS_BY_CATEGORY = {
    ErrorCategory.QUEUE_FULL: 429,
    ErrorCategory.SHUTTING_DOWN: 503,
    ErrorCategory.TASK_NOT_FOUND: 404,
    ErrorCategory.INVALID_STATE_TRANSITION: 409,
    ErrorCategory.DOWNSTREAM_TIMEOUT: 502,
    ErrorCategory.DOWNSTREAM_ERROR: 502,
    ErrorCategory.INTERNAL_ERROR: 500,
}


class SubmitRequest(BaseModel):
    payload: Any = Field(default=None)
    idempotency_key: str | None = None


class SubmitResponse(BaseModel):
    task_id: str
    status: str
    outcome: str
    correlation_id: str


def create_app(settings: Settings | None = None, handler: TaskHandler | None = None) -> FastAPI:
    settings = settings or load_settings()
    manager = TaskManager(settings, handler=handler)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await manager.start()
        try:
            yield
        finally:
            await manager.shutdown()

    app = FastAPI(title="async-task-observability", lifespan=lifespan)
    app.state.task_manager = manager

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        correlation_id = request.headers.get(CORRELATION_HEADER) or new_correlation_id()
        ctx = TraceContext(correlation_id=correlation_id, span="request")
        with use_context(ctx):
            log_event(logger, 20, "request received", decision="accept_request",
                      method=request.method, path=request.url.path)
            response = await call_next(request)
            response.headers[CORRELATION_HEADER] = correlation_id
            return response

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError):
        status = _STATUS_BY_CATEGORY.get(exc.category, 500)
        log_event(logger, 30, "request failed", decision="map_error",
                  category=str(exc.category), reason=exc.reason)
        return JSONResponse(status_code=status, content={"error": exc.to_dict()})

    @app.post("/tasks", status_code=201)
    async def submit_task(body: SubmitRequest, request: Request) -> SubmitResponse:
        correlation_id = request.headers.get(CORRELATION_HEADER) or new_correlation_id()
        record, outcome = await manager.submit(
            body.payload,
            idempotency_key=body.idempotency_key,
            correlation_id=correlation_id,
        )
        return SubmitResponse(
            task_id=record.id,
            status=str(record.status),
            outcome=str(outcome),
            correlation_id=correlation_id,
        )

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict:
        record = await manager.get(task_id)
        return record.to_dict()

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict:
        record = await manager.cancel(task_id)
        return record.to_dict()

    return app


app = create_app()
