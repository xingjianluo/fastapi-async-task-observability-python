"""关键配置项（可通过环境变量覆盖）。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    # 排队积压上限（含运行中任务），达到上限时新提交被明确拒绝
    max_pending_tasks: int
    # 单个任务执行超时（秒）
    task_timeout_seconds: float
    # 下游单次调用超时（秒）
    downstream_timeout_seconds: float
    # 优雅停机最长等待（秒），在途任务须在此时间内收尾或进入终态
    shutdown_grace_seconds: float
    # 后台工作协程数
    worker_count: int
    # 幂等结果保留时长（秒）；0 表示永久保留
    idempotency_ttl_seconds: float
    # 超时任务清理扫描间隔（秒）
    cleanup_interval_seconds: float


def load_settings() -> Settings:
    return Settings(
        max_pending_tasks=_env_int("APP_MAX_PENDING_TASKS", 128),
        task_timeout_seconds=_env_float("APP_TASK_TIMEOUT_SECONDS", 10.0),
        downstream_timeout_seconds=_env_float("APP_DOWNSTREAM_TIMEOUT_SECONDS", 2.0),
        shutdown_grace_seconds=_env_float("APP_SHUTDOWN_GRACE_SECONDS", 5.0),
        worker_count=_env_int("APP_WORKER_COUNT", 4),
        idempotency_ttl_seconds=_env_float("APP_IDEMPOTENCY_TTL_SECONDS", 3600.0),
        cleanup_interval_seconds=_env_float("APP_CLEANUP_INTERVAL_SECONDS", 1.0),
    )
