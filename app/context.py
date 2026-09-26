"""请求级链路上下文：同一请求的同步调用、异步任务与并发子操作共享关联标识。"""
from __future__ import annotations

import contextvars
import uuid

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="")
_task_id: contextvars.ContextVar[str] = contextvars.ContextVar("task_id", default="")


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def get_correlation_id() -> str:
    return _correlation_id.get()


def set_correlation_id(value: str) -> contextvars.Token:
    return _correlation_id.set(value)


def reset_correlation_id(token: contextvars.Token) -> None:
    _correlation_id.reset(token)


def get_task_id() -> str:
    return _task_id.get()


def set_task_id(value: str) -> contextvars.Token:
    return _task_id.set(value)


def reset_task_id(token: contextvars.Token) -> None:
    _task_id.reset(token)


def ensure_correlation_id() -> str:
    """若当前无关联标识则生成一个，返回当前标识。"""
    raise NotImplementedError
