# 可观测异步任务执行链路

基于 FastAPI + asyncio 的异步任务系统，使 **请求 → 异步任务 → 并发子操作** 的关联关系、状态变化与失败原因可一致地追踪与验证。

## 1. 任务状态机

任务记录（`app/store.py`）是状态与幂等键的唯一裁定者，迁移规则由 `app/statemachine.py` 执行：

```
PENDING ──> RUNNING ──> SUCCEEDED        (终态)
   │          ├──> FAILED                (终态)
   │          ├──> CANCELLED             (终态)
   │          └──> TIMED_OUT             (终态)
   └────────> CANCELLED                  (排队中取消)
```

- 所有终态（`SUCCEEDED/FAILED/CANCELLED/TIMED_OUT`）不可再迁移，违反时抛出 `InvalidStateTransition`，`reason` 区分：
  - `transition rejected: terminal state`（终态再迁移）
  - `transition rejected: not in allowed set`（跳态，如 PENDING→SUCCEEDED）
  - `transition rejected: task does not exist`（任务不存在）
- 取消或超时后任务直接进入终态并写入 `finished_at`，不会留下半完成结果；执行器对终态迁移做兜底（已终态则只记录日志，不覆盖）。
- `attempt` 只在真正进入 RUNNING 时 +1，可验证"排队中被取消的任务从未执行"。

仅允许取消 PENDING/RUNNING 中的任务；RUNNING 取消后执行器取消底层 asyncio 任务，下游子操作随之取消，不重复执行。

## 2. 幂等与并发语义

- 提交必须带 `idempotency_key`（`POST /tasks`）。store 在单个 `asyncio.Lock` 内完成"查键 → 占位 → 建记录"，因此 **20 个并发同键提交只会创建 1 个任务、执行 1 次（fanout 个）下游调用**，其余请求返回同一记录。
- 重复提交（包括不同 payload）只返回既有记录，不重放、不覆盖，响应中 `created=false`（首次 201，重复 200）。
- 背压拒绝时自动回滚 PENDING 占位记录，幂等键立即释放，可安全重试；被拒任务不进入任何执行路径。
- 下游 `fan_out` 用 `asyncio.gather` 发起并发子操作，全部继承请求的 `correlation_id`（基于 `contextvars`，asyncio 任务创建时自动复制上下文）。

## 3. 错误分类规则

所有失败在边界处归一化为 `AppError`（`app/errors.py`），携带 `category` 与人类可核查的 `reason`（判定依据），不静默吞掉、不透出原始异常与堆栈：

| category | 触发场景 | HTTP 状态码 |
|---|---|---|
| `downstream_error` | 下游返回错误 | 502 |
| `downstream_timeout` | 下游调用超过自身 timeout | 504 |
| `task_timeout` | 任务整体超过 task_timeout | 500*（可按需扩展） |
| `task_cancelled` | 主动取消 / 停机中断 | — |
| `capacity_exceeded` | 排队满 / 停机中提交 | 429 |
| `invalid_state_transition` | 非法状态迁移 | 409 |
| `task_not_found` | 任务不存在（含手动释放目标缺失） | 404 |
| `task_not_releasable` | 手动释放未终态任务 | 409 |
| `internal_error` | 未预期异常（含类型名，不含堆栈） | 500 |

## 4. 容量背压与优雅停机

执行器（`app/executor.py`）参数见 `ExecutorConfig`：

- 执行槽位由 `asyncio.Semaphore(max_workers)` 限制；排队中的任务计数达到 `max_pending` 时 **立即拒绝**（`CapacityExceeded`，429），不无限等待。
- 停机 `shutdown()`：先标记不再接收新任务 → `asyncio.wait` 宽限 `shutdown_grace` 秒 → 未收尾任务统一 `cancel()` 并等待它们进入 `CANCELLED` 终态 → 清空在途表。在途任务不重复执行、不泄漏后台任务。
- 任务执行受 `task_timeout` 约束（`asyncio.timeout`），超时进入 `TIMED_OUT`，失败原因可一致地清理和观测。

## 5. 保留、回收与幂等键复用

终态任务不会无限堆积。`TaskReaper`（`app/retention.py`）在后台按 `sweep_interval` 周期扫描 `TaskStore`，每轮做两件事：

1. **到期回收**：`finished_at` 距当前时间 ≥ `retention_seconds` 的终态任务被移除（记录与幂等键映射一起删除）；
2. **容量淘汰**：终态历史总量超过 `max_history` 时，按 `finished_at` **最旧优先**淘汰，直到回到上限以内。

规则与边界：

- 在途任务（`PENDING/RUNNING`）没有 `finished_at`，任何情况下都不会被回收；未到期的终态任务同样保留。
- **幂等键语义以回收为界**：保留期内同键重复提交仍命中同一任务（`created=false`，不重复执行）；任务被回收（或手动释放）后，同一键再次提交视为全新任务，重新执行并产生新的业务结果。
- 回收与提交并发时，两者在存储的同一把锁下串行化：要么提交先命中旧记录（随后被回收、键释放），要么回收先完成（随后提交创建全新任务），不会出现键与记录不一致的中间态。
- 可观测性：`GET /retention/stats` 暴露 `total_reclaimed`（累计回收数）、`last_reclaim_at`（最近一次回收的墙钟时间）、`sweep_count`（扫描轮次）。每轮回收生成独立关联标识，每条被回收记录输出 `task reclaimed` 日志，携带 `task_id`、`idempotency_key`、`status`、`finished_at` 与判定依据（`exceeded retention_seconds` / `history exceeded max_history, oldest first`），可按关联标识核对一轮回收的完整决策链。

## 6. 查询与手动释放

- `GET /tasks?status=&created_from=&created_to=&limit=&offset=`：按状态与创建时间范围（墙钟秒）筛选并分页，返回 `tasks`（按创建时间升序）、`total`（符合条件的总量，与分页无关）、`limit`、`offset`。`status` 非法返回 422；`limit` 范围 1–200。
- `POST /tasks/{task_id}/release`：手动释放终态任务占用的幂等键并移除记录，之后同键提交得到全新任务。处理结果可区分：
  - 终态：200，`{"released": true, ...}`；
  - 未终态（在途）：409，`category=task_not_releasable`，`current` 给出当前状态，键仍被占用；
  - 不存在：404，`category=task_not_found`。
  
  对外响应只含稳定类别与原因，不带内部实现细节；判定依据以 `obs_*` 日志留痕（如 `task released` 日志携带键与状态）。

## 7. 可观测性

- 每条日志为单行 JSON，字段包含 `correlation_id`、`task_id`、`level`、`msg`，业务判定依据以 `obs_*` 前缀的 extra 传入（输出时去掉前缀），如 `obs_reason`、`obs_category`。
- HTTP 中间件优先复用 `X-Correlation-Id` 请求头，没有则生成，并在响应头回写；该 ID 写入任务记录，贯穿异步任务与并发子操作。
- 任务状态与错误保存在进程内 `TaskStore`，可通过 `GET /tasks/{id}`、`GET /tasks/by-key/{key}` 验证终态与归一化错误。

## 8. 关键配置项

`ExecutorConfig`（`app/executor.py`）与 `DownstreamClient`（`app/downstream.py`）：

| 配置 | 默认 | 含义 |
|---|---|---|
| `max_workers` | 4 | 并发执行槽位 |
| `max_pending` | 8 | 排队上限，超过立即拒绝 |
| `task_timeout` | 5.0s | 单任务整体超时 → `timed_out` |
| `shutdown_grace` | 5.0s | 停机宽限期，超时取消在途任务 |
| `DownstreamClient.timeout` | 1.0s | 单次下游调用超时 → `downstream_timeout` |
| `DownstreamClient.fail_mode` | None | `error` / `hang` 模拟下游失败与挂起 |
| `DownstreamClient.fanout` | 2 | 每任务的并发子操作数 |

`RetentionConfig`（`app/retention.py`）：

| 配置 | 默认 | 含义 |
|---|---|---|
| `retention_seconds` | 3600.0s | 终态任务保留时长，超过即被回收 |
| `max_history` | 10000 | 终态历史总量上限，超出按最旧优先淘汰 |
| `sweep_interval` | 60.0s | 后台回收扫描间隔 |

## 9. 本地验证方法

```bash
# 安装依赖（uv 已在 pyproject 中配置镜像）
uv sync

# 运行全部单测（日志为 JSON，带 correlation_id 与判定依据）
uv run pytest -q

# 只看某类场景
uv run pytest tests/test_idempotency.py -q   # 并发幂等
uv run pytest tests/test_statemachine.py -q  # 非法状态迁移
uv run pytest tests/test_errors.py -q        # 下游异常与超时
uv run pytest tests/test_shutdown.py -q      # 背压与停机中断
uv run pytest tests/test_api.py -q           # HTTP 端到端

# 启动服务
uv run python main.py
```

手动验证：

```bash
# 提交（首次 201；带相同键再发 -> 200，同一 task_id）
curl -s -X POST localhost:PORT/tasks \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: demo-cid-1' \
  -d '{"idempotency_key":"k1","payload":{"x":1}}'

# 查询终态与归一化错误
curl -s localhost:PORT/tasks/<task_id>

# 分页查询（按状态过滤）
curl -s 'localhost:PORT/tasks?status=succeeded&limit=10'

# 手动释放终态任务的幂等键（之后同键可重新提交为新任务）
curl -s -X POST localhost:PORT/tasks/<task_id>/release

# 回收指标：累计回收数 / 最近回收时间 / 扫描轮次
curl -s localhost:PORT/retention/stats
```

本地复现回收行为：把 `main.py` 中 `RetentionConfig` 调小（如
`retention_seconds=5, sweep_interval=2`），提交任务待其进入终态，
5 秒后观察日志中的 `task reclaimed`（含判定依据）与
`GET /retention/stats` 的 `total_reclaimed` 增长；随后用同一幂等键
重新提交，应返回 201 与全新 `task_id`。

## 10. 模块结构

| 模块 | 职责 |
|---|---|
| `app/context.py` | 请求级关联标识（contextvars） |
| `app/logging_config.py` | 结构化 JSON 日志与上下文注入 |
| `app/errors.py` | 错误类别定义与异常归一化 |
| `app/statemachine.py` | 合法迁移规则 |
| `app/store.py` | 幂等提交、状态迁移、占位回滚、回收/释放/查询 |
| `app/downstream.py` | 模拟下游与并发子操作 |
| `app/executor.py` | 背压、超时、优雅停机 |
| `app/retention.py` | 终态保留期回收、历史容量淘汰、回收指标 |
| `app/service.py` | 提交/查询/取消门面 |
| `app/api.py` | 中间件、错误响应映射、路由 |
