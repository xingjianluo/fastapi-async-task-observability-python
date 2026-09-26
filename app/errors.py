"""统一错误分类：下游异常、超时与内部错误归一化为可区分的类别。"""
from __future__ import annotations

import enum


class ErrorCategory(str, enum.Enum):
    """可归一化区分的错误类别。"""

    DOWNSTREAM_ERROR = "downstream_error"
    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    INVALID_STATE_TRANSITION = "invalid_state_transition"
    TASK_NOT_FOUND = "task_not_found"
    TASK_CANCELLED = "task_cancelled"
    TASK_TIMEOUT = "task_timeout"
    INTERNAL_ERROR = "internal_error"


class AppError(Exception):
    """所有归一化错误的基类，携带类别与可解释的判定依据。"""

    category: ErrorCategory = ErrorCategory.INTERNAL_ERROR

    def __init__(self, message: str, *, reason: str = "") -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason

    def to_dict(self) -> dict:
        return {
            "category": self.category.value,
            "message": self.message,
            "reason": self.reason,
        }


class DownstreamError(AppError):
    category = ErrorCategory.DOWNSTREAM_ERROR


class DownstreamTimeout(AppError):
    category = ErrorCategory.DOWNSTREAM_TIMEOUT


class CapacityExceeded(AppError):
    category = ErrorCategory.CAPACITY_EXCEEDED


class InvalidStateTransition(AppError):
    category = ErrorCategory.INVALID_STATE_TRANSITION

    def __init__(self, message: str, *, current: str, target: str, reason: str = "") -> None:
        super().__init__(message, reason=reason)
        self.current = current
        self.target = target

    def to_dict(self) -> dict:
        d = super().to_dict()
        d.update({"current": self.current, "target": self.target})
        return d


class TaskNotFound(AppError):
    category = ErrorCategory.TASK_NOT_FOUND


class TaskCancelled(AppError):
    category = ErrorCategory.TASK_CANCELLED


class TaskTimeout(AppError):
    category = ErrorCategory.TASK_TIMEOUT


def normalize_exception(exc: BaseException) -> AppError:
    """把底层异常归一化为 AppError；不透出原始异常，不静默吞掉。"""
    raise NotImplementedError
