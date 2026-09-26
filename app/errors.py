"""错误分类体系：所有内部/下游异常归一化为可区分的错误类别。"""

from __future__ import annotations

from enum import StrEnum


class ErrorCategory(StrEnum):
    """归一化后的错误类别。"""

    DOWNSTREAM_ERROR = "downstream_error"
    DOWNSTREAM_TIMEOUT = "downstream_timeout"
    INTERNAL_ERROR = "internal_error"
    QUEUE_FULL = "queue_full"
    INVALID_STATE_TRANSITION = "invalid_state_transition"
    TASK_NOT_FOUND = "task_not_found"
    TASK_CANCELLED = "task_cancelled"
    TASK_TIMEOUT = "task_timeout"
    SHUTTING_DOWN = "shutting_down"
    DUPLICATE_SUBMISSION = "duplicate_submission"


class AppError(Exception):
    """应用级错误基类：携带类别与判定依据，不直接透出底层异常。"""

    category: ErrorCategory = ErrorCategory.INTERNAL_ERROR

    def __init__(self, message: str, *, reason: str = "", cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.reason = reason
        self.cause = cause

    def to_dict(self) -> dict:
        return {
            "category": str(self.category),
            "message": self.message,
            "reason": self.reason,
        }


class DownstreamError(AppError):
    category = ErrorCategory.DOWNSTREAM_ERROR


class DownstreamTimeout(AppError):
    category = ErrorCategory.DOWNSTREAM_TIMEOUT


class QueueFullError(AppError):
    category = ErrorCategory.QUEUE_FULL


class InvalidStateTransitionError(AppError):
    category = ErrorCategory.INVALID_STATE_TRANSITION

    def __init__(self, message: str, *, current: str, target: str, reason: str = "") -> None:
        super().__init__(message, reason=reason)
        self.current = current
        self.target = target

    def to_dict(self) -> dict:
        data = super().to_dict()
        data["current"] = self.current
        data["target"] = self.target
        return data


class TaskNotFoundError(AppError):
    category = ErrorCategory.TASK_NOT_FOUND


class TaskCancelledError(AppError):
    category = ErrorCategory.TASK_CANCELLED


class TaskTimeoutError(AppError):
    category = ErrorCategory.TASK_TIMEOUT


class ShuttingDownError(AppError):
    category = ErrorCategory.SHUTTING_DOWN
