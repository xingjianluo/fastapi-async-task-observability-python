"""服务入口：装配存储、执行器、服务与 API，并接入优雅停机。"""
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api import create_app
from app.downstream import DownstreamClient
from app.executor import ExecutorConfig, TaskExecutor
from app.logging_config import configure_logging, get_logger
from app.retention import RetentionConfig, TaskReaper
from app.service import TaskService
from app.store import TaskStore

configure_logging()
log = get_logger("main")

store = TaskStore()
downstream = DownstreamClient(latency=0.05, timeout=2.0, fanout=2)
executor = TaskExecutor(store, downstream, ExecutorConfig())
service = TaskService(store, executor)
reaper = TaskReaper(store, RetentionConfig())


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("service started")
    await reaper.start()
    yield
    await reaper.stop()
    await executor.shutdown()
    log.info("service stopped")


app = create_app(service, reaper)
app.router.lifespan_context = lifespan

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
