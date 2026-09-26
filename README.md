# 可观测异步任务执行链路

基于 FastAPI + asyncio 的异步任务服务，提供请求级链路追踪、幂等任务提交、
有界容量背压、任务超时/取消与优雅停机，所有状态变化与失败原因均结构化记录、
归一化分类并可测试验证。

## 目录结构

```
app/
  config.py         # 配置项（环境变量覆盖）
  context.py        # 基于 contextvars 的请求链路上下文
  logging.py        # JSON 结构化日志（关联标识 + 判定依据）
  errors.py         # 归一化错误分类体系
  statemachine.py   # 任务状态机与合法迁移校验
  idempotency.py    # 幂等键存储（并发去重 + TTL 清理）
  downstream.py     # 下游调用封装（异常/超时归一化）
  task_manager.py   # 任务执行器（有界队列、worker、取消、超时、停机）
  main.py           # FastAPI 路由、关联标识中间件、错误映射
tests/              # 并发幂等、非法迁移、下游异常/超时、停机中断等测试
```

## 任务状态机

```
              ┌─────────────────── 合法迁移 ───────────────────┐
              ▼                                                  │
 PENDING ──► RUNNING ──► SUCCEEDED                              │
    │           ├──► FAILED                                     │
    │           ├──► TIMED_OUT                                  │
    │           └──► CANCELLED                                  │
    ├──► CANCELLED（排队中取消，不执行、无副作用）               │
    └──► ABORTED（停机宽限期结束后强制收尾）                     │
 RUNNING ──► ABORTED（停机宽限期结束后强制收尾）                 │
```

- 终态：`succeeded` / `failed` / `timed_out` / `cancelled` / `aborted`，终态不可再迁移。
- 任何非法迁移（如 `pending -> succeeded`、对终态任务取消）抛出
  `invalid_state_transition`，并携带 `current`、`target` 与可区分原因
  （`terminal_status` / `transition_not_allowed`），API 返回 409。
- 取消与失败发生竞争时**取消优先**：取消后不会留下半完成结果。

## 幂等与并发语义

- 提交时携带 `idempotency_key`；`IdempotencyStore` 在锁内完成“检查—插入”，
  并发或重复提交只有一个胜出者真正创建并执行任务（副作用恰好一次）。
- 重复提交返回同一 `task_id` 与可解释结果：
  - `outcome=created`：本次新建；
  - `outcome=reused_inflight`：命中进行中任务；
  - `outcome=reused_completed`：命中已完成任务，返回与首次一致的结果。
- 仅已完成记录参与 TTL 清理；进行中记录不会过期，避免清理后重复副作用。

## 错误分类规则

底层异常**不会原样透出**，统一归一化为以下类别（`error.category`）：

| 类别 | 触发场景 | HTTP |
| --- | --- | --- |
| `downstream_error` | 下游依赖返回内部错误 | 502 |
| `downstream_timeout` | 下游调用超过 `APP_DOWNSTREAM_TIMEOUT_SECONDS` | 502 |
| `task_timeout` | 任务执行超过 `APP_TASK_TIMEOUT_SECONDS`（任务终态 `timed_out`） | 200(任务结果) |
| `queue_full` | 积压（排队+运行中）达到 `APP_MAX_PENDING_TASKS`，立即拒绝 | 429 |
| `invalid_state_transition` | 非法状态迁移（如取消终态任务） | 409 |
| `task_not_found` | 未知 task_id | 404 |
| `shutting_down` | 停机期间收到新提交 | 503 |
| `task_cancelled` / `internal_error` | 用户取消 / 未预期内部错误 | — / 500 |

归一化后的错误只暴露类别与判定依据（`reason`），底层异常仅保存在内部
`cause` 中供排查。

## 链路追踪与结构化日志

- 入口中间件从请求头 `x-correlation-id` 读取关联标识（缺省时生成），
  通过 `contextvars` 传递到同步调用、异步任务与并发子操作（`asyncio.gather`）；
  响应头原样回传。
- 每条日志为单行 JSON，至少包含：`correlation_id`、`task_id`、`span`、
  `message`，以及判定依据字段（如 `decision=reject_submit`、`category`、`reason`）。
- 并发子操作使用同一 `correlation_id` 与独立的 child span（`child-0`、`child-1`）。

## 容量背压与优雅停机

- 容量统计 = `pending + running`，达到 `APP_MAX_PENDING_TASKS` 时提交立即以
  `queue_full`（429）失败，**不无限排队等待**。
- 收到停机信号后停止接收新提交（503）；在 `APP_SHUTDOWN_GRACE_SECONDS` 内
  等待在途任务自然收尾。
- 宽限期结束后仍未完成：排队任务置为 `aborted`，运行中任务取消并置为
  `aborted`；所有任务进入终态、不重复执行、不残留协程。
- 任务执行超时后 job 被取消，任务进入 `timed_out`，幂等记录由清理循环按 TTL 扫描。

## 关键配置项（环境变量）

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `APP_MAX_PENDING_TASKS` | 128 | 排队+运行中任务上限 |
| `APP_TASK_TIMEOUT_SECONDS` | 10.0 | 单任务执行超时 |
| `APP_DOWNSTREAM_TIMEOUT_SECONDS` | 2.0 | 下游单次调用超时 |
| `APP_SHUTDOWN_GRACE_SECONDS` | 5.0 | 停机在途任务宽限时间 |
| `APP_WORKER_COUNT` | 4 | worker 协程数 |
| `APP_IDEMPOTENCY_TTL_SECONDS` | 3600 | 已完成幂等结果保留时长（0=永久） |
| `APP_CLEANUP_INTERVAL_SECONDS` | 1.0 | 超时/过期记录清理扫描间隔 |

## HTTP 接口

```
POST /tasks                     {"payload": {...}, "idempotency_key": "k1"}
GET  /tasks/{task_id}           查询任务（状态、result、error）
POST /tasks/{task_id}/cancel    取消排队中/运行中任务
```

`payload` 支持：
- `{"text": "hi", "delay": 0, "fail": false, "downstream_timeout": 1.0}`：单次下游调用；
- `{"children": [ {...}, {...} ]}`：并发执行子操作，共享关联标识。

## 本地验证

```bash
# 安装依赖并启动
uv run python main.py

# 提交任务（可显式传入关联标识）
curl -s -X POST localhost:8000/tasks \
  -H 'content-type: application/json' -H 'x-correlation-id: demo-1' \
  -d '{"payload": {"text": "hello"}, "idempotency_key": "demo-key"}'

curl -s localhost:8000/tasks/<task_id>
curl -s -X POST localhost:8000/tasks/<task_id>/cancel

# 全量测试（并发幂等、非法迁移、下游异常/超时、停机中断、结构化日志断言）
uv run pytest -q
```
