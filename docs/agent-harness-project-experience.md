# Agent Harness 项目经历说明

## 1. 项目定位

**项目名**：Provider-neutral Agent Harness 与长任务 Canonical DAG Runtime

**一句话**：用 Go 设计一个确定性 Agent 控制面，把模型 proposal、Context Compiler、Tool/MCP、
Memory、Skills、Subagent、DAG scheduler、lease/retry、人工 gate、checkpoint/replay 和
评测反馈串成可恢复闭环，并用离线 artifact 纵向切片验证副作用边界。

## 2. 要解决的问题

长任务 Agent 的主要失败不是单轮回答，而是：上下文膨胀、模型把 proposal 当成已提交副作用、
超时后的重复支付、worker 崩溃丢状态、人工验收无法恢复，以及“模型说完成”却没有 artifact
或 verifier evidence。

设计原则是：

```text
Model proposal -> deterministic control plane -> environment effect -> evidence
```

模型负责提出下一步；runtime 负责身份、权限、状态、幂等、预算、恢复和完成判定。

## 3. 简历版项目描述

> 使用 Go 设计并实现 provider-neutral Agent Harness 与 Canonical DAG Runtime：构建统一 Model API
> （complete/stream/structured output/fallback）、Context Compiler（stable/dynamic context、
> prefix cache、budget/compaction）、Tool/MCP 权限与幂等、Skills/Memory、Subagent fan-out/fan-in、
> lease/retry、人工 gate、checkpoint/event hash replay 和 trajectory evaluation；将模型输出限定为
> proposal，由确定性 control plane 提交状态与副作用。以 SQLite deterministic fixture 实现 artifact
> 交付纵向切片，覆盖 tool schema/scope、不可逆副作用幂等、UNKNOWN 结果、
> 并发幂等 replay、Harness session SSE 事件重放和
> feedback funnel，并用 Go 单测、race detector、vet、离线 demo 和 DAG 崩溃恢复验证契约。

这段描述只陈述代码和可复现测试能够证明的内容，不写线上用户量、成功率或模型训练成果。

## 4. 面试讲解顺序

1. 先画四层：Model、Control、Environment、Evidence；说明模型不能直接提交状态。
2. 走 Harness demo：context -> memory/tool -> observation -> structured output/verifier -> checkpoint/replay。
3. 走 DAG happy path：schema/semantic validation -> ready set -> lease -> artifact/evidence -> UAT。
4. 走 DAG rework：V1 verification failure -> diagnosis -> fresh V2 attempt -> UAT；强调失败 attempt 保留。
5. 走工具异常：副作用超时进入 UNKNOWN，不重复执行，使用相同幂等键重放并进入对账。
6. 展示 hash-chain replay、optimistic conflict、loop/budget、scope deny 和 user reject。
7. 解释评测：固定 fixture、Wilson CI、p95、ablation 和 promotion guardrail；不把样例数字说成线上指标。
8. 最后讲生产替换：PostgreSQL/queue/distributed lease/sandbox/OIDC/CAS/OpenTelemetry/provider cache。

## 5. 技术取舍

### 为什么用 Go

核心是长期运行的 scheduler/worker/control plane，需要 goroutine 并发、`context` 取消、静态
契约、标准 HTTP/SSE、`go test -race` 和单二进制交付。模型通过 provider-neutral HTTP/MCP
接口接入，因此不需要把控制面绑定到某个 Python agent framework。

### 为什么保留 SQLite fixture

SQLite 让事件链、version conflict、幂等、lease 和恢复在本地可重复；抽象通过 repository/store
接口保留了替换 PostgreSQL 的边界。它是实验和面试演示的证据，不是分布式容量结论。

### 为什么使用 scripted model

固定响应可以逐轮重放 tool call、user gate、verifier failure 和 fallback，便于测试 Harness
逻辑。真实模型只通过 `OpenAICompatibleAdapter` 接入，当前没有把 scripted model 当作模型能力
评测结果。

## 6. 代码和证据索引

| 主题 | 代码 | 可复现入口 |
|---|---|---|
| Harness contract/store | `harness/types.go`、`store.go` | event replay/tamper/conflict tests |
| Agent loop/context | `runtime.go`、`context.go` | `go run ./cmd/agent-harness -mode demo` |
| Model API | `model.go` | router/structured/stream tests |
| Tool/MCP/policy | `tools.go`、`policy.go` | schema/scope/UNKNOWN/MCP tests |
| Skills/Memory | `skills.go`、`memory.go` | lifecycle/version/forget tests |
| DAG | `harness/dag/`、`configs/` | `go run ./cmd/agent-dag`、DAG tests |
| Harness session API | `httpapi/server.go` | `POST /api/harness/runs`、`GET /api/harness/runs/{id}`、SSE events |
| HTTP/SSE/feedback | `httpapi/server.go` | HTTP end-to-end test |
| Evaluation/evolution | `evaluation.go`、`experiment.go` | CI/ablation/promotion tests |

## 7. 诚实边界

当前项目可以证明：

- 控制面协议和状态不变量在本地可运行；
- 失败、UNKNOWN、幂等、lease、fresh retry、UAT 和 replay 有测试证据；
- 知识点有对应实现入口和可复现命令；
- 研究和产品迭代有数据结构与实验骨架。

当前项目不能由代码自动证明：

- 线上吞吐、SLO、真实支付成功率或大规模用户满意度；
- 真实 benchmark 上的模型智能提升；
- 学历、论文、开源影响力、比赛成绩或团队协作经历；
- 已完成 PostgreSQL 集群、容器 sandbox、OIDC/KMS、真实 provider KV 或合规认证。

## 8. 运行命令

```bash
go test ./...
go test -race ./...
go vet ./...
make build
go run ./cmd/agent-harness -mode demo
go run ./cmd/agent-dag
```

本地服务入口：`http://127.0.0.1:8080`。主要 API 和生产替换项见
`README.md`、`DESIGN.md` 和 `docs/agent-harness-runtime-design.md`。
