"""任务状态机：定义合法状态迁移路径，非法迁移被明确拒绝。"""

from __future__ import annotations

from enum import StrEnum

from app.errors import InvalidStateTransitionError


class TaskStatus(StrEnum):
    # 已提交、在队列中等待调度
    PENDING = "pending"
    # 已被某个 worker 领取、正在执行
    RUNNING = "running"
    # 执行成功（终态）
    SUCCEEDED = "succeeded"
    # 执行失败（终态，失败原因已归一化）
    FAILED = "failed"
    # 被用户取消（终态）
    CANCELLED = "cancelled"
    # 执行超时（终态）
    TIMED_OUT = "timed_out"
    # 停机时仍未收尾、被强制标记（终态）
    ABORTED = "aborted"


# 合法迁移表
_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.PENDING: frozenset(
        {TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.ABORTED}
    ),
    TaskStatus.RUNNING: frozenset(
        {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED,
         TaskStatus.TIMED_OUT, TaskStatus.ABORTED}
    ),
    TaskStatus.SUCCEEDED: frozenset(),
    TaskStatus.FAILED: frozenset(),
    TaskStatus.CANCELLED: frozenset(),
    TaskStatus.TIMED_OUT: frozenset(),
    TaskStatus.ABORTED: frozenset(),
}


TERMINAL_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMED_OUT,
        TaskStatus.ABORTED,
    }
)


def ensure_transition(current: TaskStatus, target: TaskStatus) -> None:
    """校验迁移合法性，非法迁移抛出可区分原因的 InvalidStateTransitionError。"""
    allowed = _TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        if current in TERMINAL_STATUSES:
            reason = "current_status_is_terminal"
        else:
            reason = "transition_not_allowed"
        raise InvalidStateTransitionError(
            f"illegal task state transition: {current} -> {target}",
            current=str(current),
            target=str(target),
            reason=reason,
        )
