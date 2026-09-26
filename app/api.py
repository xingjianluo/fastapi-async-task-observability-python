"""HTTP API：关联标识中间件、错误归一化响应、任务路由。"""
from __future__ import annotations

from fastapi import FastAPI

from .service import TaskService


def create_app(service: TaskService) -> FastAPI:
    app = FastAPI()

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    return app
