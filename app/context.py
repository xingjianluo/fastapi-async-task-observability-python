"""请求级链路上下文：基于 contextvars 在同步调用、异步任务与并发子操作间传递关联标识。"""

from __future__ import annotations

import contextvars
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass(frozen=True)
class TraceContext:
    """一次请求的链路上下文。"""

    correlation_id: str
    task_id: str | None = None
    span: str = "request"
    parent_span: str | None = None

    def child(self, *, span: str, task_id: str | None = None) -> "TraceContext":
        return TraceContext(
            correlation_id=self.correlation_id,
            task_id=task_id if task_id is not None else self.task_id,
            span=span,
            parent_span=self.span,
        )


_current: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar(
    "trace_context", default=None
)


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def current_context() -> TraceContext:
    ctx = _current.get()
    if ctx is None:
        ctx = TraceContext(correlation_id=new_correlation_id())
        _current.set(ctx)
    return ctx


def set_context(ctx: TraceContext) -> contextvars.Token:
    return _current.set(ctx)


def reset_context(token: contextvars.Token) -> None:
    _current.reset(token)


@contextmanager
def use_context(ctx: TraceContext):
    token = set_context(ctx)
    try:
        yield ctx
    finally:
        reset_context(token)
