"""HTTP API：关联标识中间件、错误归一化响应、任务路由。"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .context import new_correlation_id, reset_correlation_id, set_correlation_id
from .errors import (
    AppError,
    CapacityExceeded,
    ErrorCategory,
    InvalidStateTransition,
    TaskNotFound,
    normalize_exception,
)
from .logging_config import get_logger
from .service import TaskService

_STATUS_BY_CATEGORY = {
    ErrorCategory.CAPACITY_EXCEEDED: 429,
    ErrorCategory.INVALID_STATE_TRANSITION: 409,
    ErrorCategory.TASK_NOT_FOUND: 404,
    ErrorCategory.DOWNSTREAM_ERROR: 502,
    ErrorCategory.DOWNSTREAM_TIMEOUT: 504,
}


class SubmitRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=128)
    payload: dict = Field(default_factory=dict)


def create_app(service: TaskService) -> FastAPI:
    app = FastAPI()
    log = get_logger("api")

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        correlation_id = request.headers.get("x-correlation-id") or new_correlation_id()
        token = set_correlation_id(correlation_id)
        try:
            response = await call_next(request)
            response.headers["x-correlation-id"] = correlation_id
            return response
        finally:
            reset_correlation_id(token)

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
        status = _STATUS_BY_CATEGORY.get(exc.category, 500)
        log.warning(
            "request failed",
            extra={"obs_category": exc.category.value, "obs_reason": exc.reason},
        )
        return JSONResponse(status_code=status, content={"error": exc.to_dict()})

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        normalized = normalize_exception(exc)
        log.error(
            "unhandled error normalized",
            extra={"obs_reason": normalized.reason},
        )
        return JSONResponse(status_code=500, content={"error": normalized.to_dict()})

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/tasks", status_code=201)
    async def submit_task(body: SubmitRequest) -> JSONResponse:
        record, created = await service.submit(body.idempotency_key, body.payload)
        return JSONResponse(
            status_code=201 if created else 200,
            content={"task": record.to_dict(), "created": created},
        )

    @app.get("/tasks/{task_id}")
    async def get_task(task_id: str) -> dict:
        return {"task": (await service.get(task_id)).to_dict()}

    @app.get("/tasks/by-key/{idempotency_key}")
    async def get_task_by_key(idempotency_key: str) -> dict:
        return {"task": (await service.get_by_key(idempotency_key)).to_dict()}

    @app.post("/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str) -> dict:
        return {"task": (await service.cancel(task_id)).to_dict()}

    return app
