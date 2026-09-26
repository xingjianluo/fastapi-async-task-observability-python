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
| `task_not_found` | 任务不存在 | 404 |
| `internal_error` | 未预期异常（含类型名，不含堆栈） | 500 |

## 4. 容量背压与优雅停机

执行器（`app/executor.py`）参数见 `ExecutorConfig`：

- 执行槽位由 `asyncio.Semaphore(max_workers)` 限制；排队中的任务计数达到 `max_pending` 时 **立即拒绝**（`CapacityExceeded`，429），不无限等待。
- 停机 `shutdown()`：先标记不再接收新任务 → `asyncio.wait` 宽限 `shutdown_grace` 秒 → 未收尾任务统一 `cancel()` 并等待它们进入 `CANCELLED` 终态 → 清空在途表。在途任务不重复执行、不泄漏后台任务。
- 任务执行受 `task_timeout` 约束（`asyncio.timeout`），超时进入 `TIMED_OUT`，失败原因可一致地清理和观测。

## 5. 可观测性

- 每条日志为单行 JSON，字段包含 `correlation_id`、`task_id`、`level`、`msg`，业务判定依据以 `obs_*` 前缀的 extra 传入（输出时去掉前缀），如 `obs_reason`、`obs_category`。
- HTTP 中间件优先复用 `X-Correlation-Id` 请求头，没有则生成，并在响应头回写；该 ID 写入任务记录，贯穿异步任务与并发子操作。
- 任务状态与错误保存在进程内 `TaskStore`，可通过 `GET /tasks/{id}`、`GET /tasks/by-key/{key}` 验证终态与归一化错误。

## 6. 关键配置项

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

## 7. 本地验证方法

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
```

## 8. 模块结构

| 模块 | 职责 |
|---|---|
| `app/context.py` | 请求级关联标识（contextvars） |
| `app/logging_config.py` | 结构化 JSON 日志与上下文注入 |
| `app/errors.py` | 错误类别定义与异常归一化 |
| `app/statemachine.py` | 合法迁移规则 |
| `app/store.py` | 幂等提交、状态迁移、占位回滚 |
| `app/downstream.py` | 模拟下游与并发子操作 |
| `app/executor.py` | 背压、超时、优雅停机 |
| `app/service.py` | 提交/查询/取消门面 |
| `app/api.py` | 中间件、错误响应映射、路由 |
