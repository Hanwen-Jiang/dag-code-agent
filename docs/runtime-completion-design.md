# 运行时能力补齐设计文档

> 目标：把控制面从「架构像框架」推进到「能力也是框架」——补上 6 项实测缺口。
> 前置：`docs/embeddability-design.md` 已交付（C1–C4 达成，G1–G8 护栏机器强制）。
> 状态：设计完成；S5 协作数据面第一阶段已实施，其余交付物按阶段实施（本文明确区分已运行与待接线能力）。
> 不改动：可嵌入性四条验收标准 C1–C4 必须保持成立。

---

## 0. 范围与验收标准

本文覆盖 7 项工作，每项都有**可机械验证**的验收标准：

| # | 工作 | 验收标准 | 验证方式 |
|---|---|---|---|
| 1 | 流式暴露给嵌入者 | 嵌入者能收到模型增量文本；**journal 仍是唯一事实来源**（replay 结果不变） | 新增 `StreamConformance` 测试：有流/无流两条路径产出**完全相同**的 `MODEL_PROPOSED` |
| 2 | 子代理真实执行 | 子代理走**完整 agent loop**（工具、预算、journal），且崩溃后可复用已完成子任务 | `TestSubagentDurability`：kill 后重启，已完成的子代理不重跑 |
| 3 | orchestrator → agent-core | 执行类步骤经 gRPC 真实调用；不可达时降级并**在结果中标注实际执行后端** | smoke 增加「断言 agent-core 收到请求」检查项 |
| 4 | 真实 MCP 客户端 | 能连**外部** MCP server（stdio），其工具出现在 `ToolRegistry` 并受既有策略约束 | 一个真实 stdio MCP server 的端到端测试 |
| 5 | v0.1.0 + 版本策略 | tag 可被**无 `replace`** 的外部 module `go get` 并构建 | CI release job |
| 6 | 7 个悬空包接入 | 每个包在运行时路径上**真的被执行**；核心依赖预算不退化 | 每个包一个集成测试 + G2/G7 仍绿 |
| 7 | 协作数据面 | `previous_results`、Workspace/Topic CAS、Mailbox protocol 在两个后端行为一致；DAG → Harness 输入 handoff 使用 `previous_results`，Mailbox 留待 Handoff 策略接线 | `TestPreviousResultsAreExplicitlyCompiled`、`TestHarnessNodeRunnerInjectsInputArtifactsAsPreviousResults`、`TestCollaborationStoreInMemory`、`TestCollaborationStoreSQLite` |

**完成定义**：以上 7 项全部达成，且 `go build/vet/test -race`、`embed-guards.sh --self-test`、`smoke.sh` 全绿。当前第 7 项已完成“两个后端 + DAG 输入注入”部分，Mailbox 尚未接入 Handoff；因此本轮状态为 🟡，不能宣称 7 项全部完成。

**本文的完成定义**：接口边界、事件语义、降级行为、依赖边界、迁移顺序和验收用例已经确定；实现阶段不得把“先接上再补语义”当成完成。代码交付后，必须把本节 7 项逐项改成带提交/测试链接的事实记录。

### 0.1 非目标与术语

- 本轮不把 `agent-core` 改造成控制面 agent loop；它仍然是 WASI 执行与 RAG 服务。
- 本轮不实现 Computer Use 工具族，只为截图/输入结果预留上下文与工具面接口。
- 本轮不引入 OpenTelemetry、全局队列或宿主级认证层；这些属于独立的适配层工作。
- 文中“核心”专指 `harness` 及其不带重依赖的子包；`cmd/*`、`packages/*`、`mcp`、`agentcore` 均是宿主或扩展层。
- “必须”是合入门槛，“应”是默认实现要求，“可以”是扩展点；任何偏离必须写入 ADR 并补充对应测试。

---

## 1. 现状证据

所有缺口均为实测，非推测。

### 1.1 流式

```go
// harness/model.go —— 模型层已有流式能力
type ModelAdapter interface {
    Name() string
    Complete(context.Context, ModelRequest) (ModelResponse, error)
    Stream(context.Context, ModelRequest) (<-chan StreamEvent, error)
}
type StreamEvent struct {
    Type string         `json:"event_type"`
    Data map[string]any `json:"data,omitempty"`
}
```

但运行时**从不调用它**：

```
$ grep -n "\.Complete(\|\.Stream(" harness/runtime.go
harness/runtime.go:347:  response, err := r.model.Complete(ctx, modelRequest)
```

HTTP 层的 SSE 推的是**日志事件**（每 150ms 轮询 `EventsSince`），不是模型 token：

```go
// httpapi/server.go:440
func writeHarnessSSE(ctx, w, store harness.CoreStore, taskID string, after int, follow bool) {
    ticker := time.NewTicker(150 * time.Millisecond)
    // ... 轮询 store.EventsSince，写 id/event/data 帧
}
```

### 1.2 子代理

协调器**存在**，但只做一次模型调用——没有工具、没有持久化、没有步进：

```go
// harness/multi_agent.go:61  —— FanOut 的全部执行逻辑
response, err := adapter.Complete(ctx, ModelRequest{...})
results[index] = SubagentResult{AgentID: spec.AgentID, Status: "SUCCEEDED", Summary: response.Text, ...}
```

而且**它从未被运行时调用**。运行时的全部处置只有记录：

```go
// harness/runtime.go:422
if len(response.SubagentRequests) > 0 {
    state.Observations = append(state.Observations, map[string]any{"type": "subagents_requested", ...})
    r.commitEvent(&state, "SUBAGENTS_REQUESTED", ...)   // 然后就没有然后了
}
```

### 1.3 orchestrator → agent-core

配置字段存在但**从未被读取**：

```
$ grep -c "ExecuteTask\|NewAgentServiceClient\|grpc.Dial" cmd/orchestrator/main.go
0
$ grep -n "AgentCoreAddr" cmd/orchestrator/main.go
main.go:46:   AgentCoreAddr string `yaml:"agent_core_addr"`  // host:port (gRPC health at /health)
main.go:184:  AgentCoreAddr: getEnv("AGENT_CORE_ADDR", "127.0.0.1:50051"),
```

实际执行走的是 llm-service + 本地降级：

```go
// cmd/orchestrator/main.go:573
func (a *activities) ExecuteStep(ctx, in StepInput) (string, error) {
    if out, err := a.callLLM(ctx, in.Params, in.Step); err == nil && out != "" {
        return out, nil
    }
    return localStepOutput(in), nil   // 确定性降级
}
```

**必须正视的边界**：agent-core 的 `ExecuteTask` 是「WASI 工具执行 **或** RAG 问答」，**不是**控制面 agent loop：

```go
// cmd/agent-core/main.go:326
func (c *agentCore) ExecuteTask(ctx, in *pb.ExecuteTaskRequest) (*pb.ExecuteTaskResponse, error) {
    if spec, ok := parseToolCall(in.GetQuery()); ok {
        return c.executeTool(ctx, spec, in.GetConfig())   // 沙箱执行
    }
    return c.executeRAG(ctx, in.GetQuery())               // 向量问答
}
```

所以「打通 orchestrator → agent-core」= 打通**工作流层 → 执行层**，而非「→ 控制面」。这一点在设计中必须写清，否则又会变成一句含糊的"三层已连通"。

### 1.4 MCP

`MCPClient` 是**进程内自环**，没有任何传输层：

```go
// harness/tools.go:222
type MCPClient struct {
    Server          *MCPServer   // ← 直接持有本地 server
    Auth            AuthContext
    ProtocolVersion string
    initialized     bool
}
func (c *MCPClient) Call(ctx, call ToolCall) ToolResult {
    return c.Server.CallTool(ctx, call, c.Auth)   // ← 自环
}
```

```
$ grep -rn "jsonrpc\|stdio\|http.Post\|exec.Command" harness/tools.go
(无输出)
```

即"MCP 支持"目前是**协议形状的本地适配器**，不具备连接外部 server 的能力。

### 1.5 版本化

```
git tag | wc -l   → 0
go.mod            → module github.com/Hanwen-Jiang/dag-code-agent, go 1.26.0
```

且 `examples/embed_external` 用 `replace => ../..`，**没有验证过 tag 可安装性**。

### 1.6 7 个悬空包

被引用数实测（排除包内自引用）：

| 包 | 被引用 | 第三方依赖足迹 | 现有薄替代 |
|---|---|---|---|
| `skills` | **0** | **0** | `harness/skills.go`（96 行，词法匹配） |
| `permissions` | **0** | 3（zap, yaml, multierr） | `harness/policy.go`（58 行） |
| `loopsdetect` | **0** | **0** | `r.loopSeen` map + 一行 `LOOP_DETECTED` |
| `hooks` | **0** | 1（yaml） | 无 |
| `prompt-engine` | **0** | 11（sprig 等） | 无 |
| `vector` | **0** | **0** | `MemoryManager` 词法重叠 |
| `multiagent` | **0** | **0** | `MultiAgentCoordinator`（半成品） |

### 1.7 协作数据面（新增审计）

```text
previous_results: TaskRequest/TaskState -> ContextBuilder -> provider message  已接入并有测试
Workspace + Topic: harness/collaboration.go + SQLite/InMemory CAS             已接入并有测试
Mailbox + protocol: harness/collaboration.go + SQLite/InMemory sequence/ack   已接入并有测试
```

这三项目前属于“地基已运行、上层能力未接线”：它们还没有驱动 `MultiAgentCoordinator`、DAG 节点间 handoff 或 Swarm 策略，因此不能把 Handoff/Swarm 的判定从 ❌ 直接改成 ✅。

现有循环检测的全部实现（`harness/runtime.go:294` + `:400`）：

```go
seen := r.loopSeen[taskID]                      // map[string]int
if seen == nil { seen = map[string]int{}; r.loopSeen[taskID] = seen }
// ...
return r.failAndCommit(state, "LOOP", errors.New("LOOP_DETECTED"))
```

对比 `loopsdetect` 提供的能力：11 种检测器、三级决策（`Continue` / `Nudge` / `ForceStop`）、连续 Nudge 自动升级为 ForceStop。**现有实现只会直接杀死任务，不会先警告模型调整策略。**

---

## 2. 设计原则

在可嵌入性文档 P1–P4 之上，本文新增三条硬约束：

**P5 · 流式是观察通道，不是事实来源。**
增量文本**永不入 journal**。权威输出仍由 `MODEL_PROPOSED` 事件承载。任何让 replay 结果依赖流式路径的设计都是错的。

**P6 · 能力包分级接入，不搞一刀切。**
按「是否为正确性所需」×「依赖足迹」两个维度决定是**直接 import**、**seam + 适配器**，还是**合并进核心**。详见 §7.1。

**P7 · 核心依赖闭包是不可回退的资产。**
`harness` 当前由护栏测得为 2 个直接第三方依赖；仓库整体 `go list -deps ./harness/...` 会包含标准库和若干 SQLite/JSON Schema 传递包，文档中的“12 个模块”只表示设计基线，不作为不透明的运行时计数。新增任何重依赖（zap / sprig / yaml / pgx / qdrant / wasmtime / temporal）都视为设计错误，由护栏拦截。

---

## 3. 流式暴露给嵌入者

### 3.1 目标形态

```go
// harness/stream.go 【新】
type ChunkKind string

const (
    ChunkText     ChunkKind = "text"      // 模型增量文本（瞬时，不入库）
    ChunkToolCall ChunkKind = "tool_call" // 工具调用已解析
    ChunkEvent    ChunkKind = "event"     // 一条已提交的 journal 事件（权威）
    ChunkStep     ChunkKind = "step"      // 步进边界
    ChunkSubagent ChunkKind = "subagent"  // 子代理生命周期（§4）
    ChunkDone     ChunkKind = "done"      // 本轮结束
)

// Chunk 是流式通道上的一条消息。Kind=ChunkEvent 时 Event 非空且已持久化；
// 其余 Kind 均为瞬时观察，重放时不会重现。
type Chunk struct {
    Kind     ChunkKind
    TaskID   string
    Sequence int            // 仅 ChunkEvent：journal 序号
    Text     string         // 仅 ChunkText
    Event    *Event         // 仅 ChunkEvent
    Data     map[string]any // 其余负载
}

// Emitter 接收流式观察。实现必须是并发安全的，且不得阻塞调用方（见 3.3）。
type Emitter interface{ Emit(Chunk) }

// EmitFunc 让普通函数满足 Emitter。
type EmitFunc func(Chunk)

func (f EmitFunc) Emit(c Chunk) { f(c) }

// WithEmitter 注入流式观察通道。默认 nil（完全关闭流式，行为与今天一致）。
func WithEmitter(e Emitter) Option

// RunStream 是嵌入者最常用的入口：返回一个带缓冲的通道，运行结束时关闭。
// 取消 ctx 会终止运行并关闭通道。
func (r *Runtime) RunStream(ctx context.Context, req TaskRequest, auth AuthContext) (<-chan Chunk, error)
```

`RunStream` 为这一次运行创建带缓冲的 `asyncEmitter`；`WithEmitter` 表示 Runtime 级别的并发安全观察者，必须通过 `TaskID` 区分任务，不能把某个任务的通道存入 Runtime 全局状态。每条已提交事件先写入 store，再通过本次运行的 emitter 和可选的默认观察者转发；运行返回前发送一个 `ChunkDone`，随后关闭本次通道。订阅者若需要断线恢复，只能使用既有 `EventsSince`，不能依赖瞬时 chunk。

### 3.2 模型调用改为「可流式装配」

新增内部路径，替换 `runtime.go:347` 的单次调用：

```go
// harness/stream.go
// completeModel 在注入 Emitter 且适配器支持流式时走 Stream，并把增量装配回
// 一个完整的 ModelResponse；否则退回 Complete。
//
// 装配出的 ModelResponse 必须与 Complete 的返回值语义等价，因为它才是写入
// MODEL_PROPOSED 的内容。流式路径不改变任何持久化语义。
func (r *Runtime) completeModel(ctx context.Context, req ModelRequest) (ModelResponse, error)
```

装配规则（写入实现前必须逐条落实）：

| 情况 | 处理 |
|---|---|
| 未注入 Emitter | 直接 `Complete`（零行为变化） |
| Emitter 存在，适配器流式成功 | 累积 text / tool_calls / usage / finish_reason → `ModelResponse` |
| 流式在**任何增量产出前**失败 | 退回 `Complete`，并提交 `MODEL_STREAM_DEGRADED` 事件（可审计） |
| 流式**中途**失败 | 该步失败，类别 `MODEL`。**不得**退回 Complete——否则用户已看到文本又被重新生成，出现重复输出 |
| 适配器 `Stream` 返回 `ErrNotSupported` | 静默走 `Complete`，不提交降级事件 |

装配器必须抽取为一个共享的增量解析组件：OpenAI 兼容适配器和 Runtime 都使用同一个组件处理 `text_delta`、`tool_call_delta`、`usage`、`finish_reason` 与错误事件。当前 `model.go` 只有 provider 层解析，尚没有可复用的 Runtime 装配器；实现时不得复制两套增量拼接逻辑。

**确定性要求**：`MODEL_PROPOSED` 的权威 payload 不能包含墙上时间、随机 ID 或本地延迟测量。`Usage.LatencyMS` 改为单独的观测字段（或单独的 `MODEL_LATENCY` 非状态事件）；`TURN`/子任务 ID 通过可注入的 `IDSource` 生成。`StreamConformance` 使用固定 `Clock`、固定 `IDSource` 和同一任务身份，才可以断言 `MODEL_PROPOSED` payload、`_state_digest` 与 `event_hash` 逐字节相同。生产运行仍可记录延迟，但该观测不能改变 replay 结果。

### 3.3 背压策略：宁可丢帧，不可阻塞

```go
// harness/stream.go
// asyncEmitter 把 Emit 变成非阻塞投递：缓冲区满时丢弃该增量并计数。
// 阻塞 agent loop 去等一个慢消费者是不可接受的——权威文本仍会由
// MODEL_PROPOSED 事件送达，丢的只是"过程动画"。
type asyncEmitter struct {
    out    chan Chunk
    dropped atomic.Int64
}
```

丢弃计数随 `TURN_COMPLETED` 事件一起提交（字段 `stream_dropped`），这样"用户看到的文本不全"是可观测的，而不是静默的。

### 3.4 HTTP 层

在既有 SSE 端点上增加 `?include=text`（默认关闭，保持向后兼容）：

```go
// httpapi/stream.go 【新】
type streamHub struct {
    mu   sync.RWMutex
    subs map[string]map[*subscriber]struct{}  // taskID → 订阅者
}
func (h *streamHub) Publish(taskID string, c harness.Chunk)
func (h *streamHub) Subscribe(taskID string, buf int) (<-chan harness.Chunk, func())
```

帧格式（与既有 `id/event/data` 约定一致）：

```
id: 12
event: MODEL_PROPOSED      ← 权威事件，与今天完全相同
data: {...}

event: token               ← 新增：瞬时增量
data: {"text":"控制面"}

event: stream_dropped
data: {"count":3}
```

设计要点：

1. **权威性不变**。订阅者看到的 `event:` 帧仍来自 journal 轮询，token 帧是叠加的。断线重连只会丢 token 帧，不会丢事件——与今天行为一致。
2. `include=text` 关闭时，hub 完全不订阅，零开销。
3. 慢客户端：hub 每订阅者一个带缓冲通道，满则丢帧 + 计数，**绝不阻塞运行**。
4. 运行在 session goroutine，SSE 在请求 goroutine，因此 hub 是唯一的跨 goroutine 通道，必须 `-race` 覆盖。

### 3.5 验收

`harness/stream_conformance_test.go`：

```
TestStreamingAssemblesIdenticalResponse
  ├─ 用 ScriptedModel 跑一遍（无 Emitter）
  ├─ 用实现了 Stream 的同语义适配器跑一遍（有 Emitter）
  └─ 断言：两次的 MODEL_PROPOSED 事件 payload **逐字节相同**
            （即 _state_digest 与 event_hash 都相同）
TestStreamMidFlightFailureDoesNotFallBack
TestStreamPrefersCompleteWhenNoEmitter
TestStreamDroppedFramesAreCounted
TestStreamEventOrderingAndClose
```

第一条是最重要的：它把「流式不改变持久化语义」变成断言，而不是承诺。

---

## 4. 子代理真实执行

### 4.1 目标形态：子代理 = 持久的子任务

核心改动：子代理不再是一次模型调用，而是**同一条 `run()` 循环跑出来的独立任务**——有自己的 journal、工具循环、预算与步进上限。

```go
// harness/subagent.go 【新】
type SubagentPolicy struct {
    MaxParallel     int           // 并发上限，默认 4
    MaxDepth        int           // 嵌套深度，默认 1（子代理不得再分子代理）
    DefaultMaxSteps int           // 单个子代理步进上限，默认 6
    BudgetShare     float64       // 从父任务预算中切给每个子代理的比例，默认 0.25
    ConflictPolicy  ConflictPolicy // 结果冲突时：Retry / Gate / Fail
}

func WithSubagentPolicy(p SubagentPolicy) Option
// 子代理默认复用父 runtime 的模型；需要异构模型时用这个覆盖。
func WithSubagentModel(factory func(SubagentSpec) ModelAdapter) Option
```

执行入口（在 `run()` 的 `MODEL_PROPOSED` 之后插入）：

```go
// harness/subagent.go
// runSubagents 把模型提出的子代理请求变成真实执行。
//
// 每个子代理是一个独立任务：taskID = parent + ":" + agentID，
// 复用同一个 store 与 journal，因此它的每一步都可重放、可审计。
func (r *Runtime) runSubagents(ctx context.Context, parent *TaskState, specs []SubagentSpec, auth AuthContext) ([]SubagentResult, error)
```

父任务不能在持有全局 Runtime 锁时递归调用自己的 `Run`。实现必须使用 S3 的 per-task coordinator：父任务只提交 spawn 事件，子任务由独立的 task worker 执行；它们可以共享同一个 store、模型工厂和工具注册表，但每个任务拥有独立的运行锁、预算视图和 journal 轨迹。

### 4.2 事件序列（父任务 journal）

```
SUBAGENTS_REQUESTED   {requests:[...]}          ← 已存在
SUBAGENT_SPAWNED      {agent_id, child_task_id, role}
SUBAGENT_COMPLETED    {agent_id, status, summary_digest, steps, usage}   ← 每个子代理一条
  或 SUBAGENT_FAILED  {agent_id, category, error}
SUBAGENTS_MERGED      {status, count, conflicts}
```

`SUBAGENTS_MERGED` 之后，合并摘要写入 `state.Observations`，**循环继续**——父模型在下一步就能看到子代理结论并据此作答。这修掉了当前 `isFinal` 语义里"提出子代理请求就不算最终答案，但没有后续"的逻辑空洞。

### 4.3 三个必须做对的语义

**① 幂等复用（崩溃安全）**

fan-out 之前，对每个 `childTaskID` 先查 store：

```go
if state, _, err := r.store.GetTask(childTaskID); err == nil && state.Status == TaskCompleted {
    // 复用已完成结果，不重跑
}
```

这是「崩溃后重启不重复烧钱」的关键，也是把子代理做成持久任务的**唯一理由**——如果只是并发调用模型，用 `errgroup` 就够了。

**② 深度与扇出爆炸防护**

`MaxDepth` 默认 1；子代理运行时把深度写入 `TaskState.Metadata["subagent_depth"]`，超限直接拒绝并提交 `SUBAGENT_REJECTED`。扇出总量受 `MaxParallel` × `MaxSteps` × 预算三重约束。

**③ 预算隔离**

子代理共享父任务的 `BudgetLedger`（因此总预算真实收敛），但每个子代理有独立上限（`BudgetShare`）。超限的子代理以 `BUDGET` 类别失败，**不影响其他子代理**——单个子代理失控不能拖垮整棵树。

### 4.4 与 `multiagent` 包的职责切分（必须明确）
`packages/multiagent` 提供 `TeamOrchestrator`、`FormTeam`/`RunTeam`、`SelectBestAgents`、`ComputeAgentFitScore`、冲突检测。但它的 `LLM` 接口是**单次补全**，它自己的 `RunTeam` 驱动的是一个没有工具、没有 journal 的循环。

**因此采取如下切分，并作为对 `multiagent` 的刻意偏差记录下来：**

| 职责 | 归属 |
|---|---|
| 团队组建、agent 选择（fit score）、协调策略、冲突检测 | `packages/multiagent` |
| 执行（工具循环、步进、持久化、预算、审批） | `harness` 的 `run()` |

即：把 `multiagent` 当作**规划器**用，不用它的执行器。`multiagent.LLM` 用一个桥接实现（内部调 `ModelAdapter.Complete`），只为满足其接口，不用于驱动子代理执行。

> 这条切分如果不写清，实现者很容易直接调 `TeamOrchestrator.RunTeam`，那样子代理又会退回"无持久化的并发补全"，白做一遍。

**Handoff 语义（已决策，与 `framework-foundation-design.md` §4.2/§9.2 一致）**：子代理**返回结果给父**，不做控制权转移。

- 父任务**始终**持有 task 所有权与 journal 写入权；
- 子代理是子任务（`parent + ":" + agentID`），结果以 `SUBAGENT_COMPLETED` 回到父任务，父模型在下一步看到合并摘要（§4.3）；
- 因此**不需要**跨任务状态迁移，也**不需要**"当前 agent"这类可变全局；
- Swarm 模式（`framework-foundation-design.md` §4.2）与本节的子代理执行**是同一套机制**，区别只在"派给谁"（按 `multiagent` 的 fit score 选人）。

若将来需要「另一个 agent 接管整个任务」，应在父任务显式 `Finish` 后新建任务承接未完成目标，而不是改变 Handoff 的语义——这样今天的实现不会被将来的需求推翻。

### 4.5 验收

```
TestSubagentRunsFullLoop         子代理调用工具、步进、被 journal 记录
TestSubagentCrashReuse           第一次 fan-out 中途 kill；重启后已完成的子代理不重跑
TestSubagentDepthLimit           子代理请求再分子代理 → SUBAGENT_REJECTED
TestSubagentBudgetIsolation      一个子代理超预算失败，其余成功
TestSubagentConflictPolicy       三策略各自行为
TestSubagentParentSeesSummary    父模型下一步的上下文包含合并摘要
```

---

## 5. orchestrator → agent-core

### 5.1 目标形态

新增公开客户端包 `agentcore/`（**不得**放进 `harness`，否则把 gRPC 拉进核心闭包）：

```go
// agentcore/client.go 【新】
type Client struct {
    conn    *grpc.ClientConn
    svc     pb.AgentServiceClient
    timeout time.Duration
    breaker *harness.CircuitBreaker   // 复用核心已有的熔断器
    caps    Capabilities
}

func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error)
func WithTimeout(d time.Duration) Option
func WithBreaker(b *harness.CircuitBreaker) Option

func (c *Client) Health(ctx context.Context) (Health, error)
func (c *Client) DiscoverTools(ctx context.Context) ([]ToolCapability, error)
func (c *Client) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResult, error)
// Stream 把服务端的 TaskUpdate 逐条交给 onUpdate，用于 Temporal 心跳（见 5.3）。
func (c *Client) Stream(ctx context.Context, req ExecuteRequest, onUpdate func(TaskUpdate) error) (ExecuteResult, error)
func (c *Client) Close() error
```

`ExecuteRequest` 是 proto 消息的 Go 友好投影（避免让调用方直接依赖 `pb`）：

```go
type ExecuteRequest struct {
    SessionID string
    Query     string            // 工具调用 JSON 或自然语言查询
    Context   map[string]any
    Tools     []string          // 白名单，来自 DiscoverTools 结果
    Timeout   time.Duration
    Budget    int
}
type ExecuteResult struct {
    Status   string
    Output   string
    ToolUsed string
    Backend  string   // "agent-core" | "llm-service" | "local"
    Latency  time.Duration
}
```

### 5.2 路由与降级

**前置改动**：当前 `PlanStep` 只有 `Name` 与 `Objective`，没有可路由的类型信息：

```go
// cmd/orchestrator/main.go:77  —— 现状
type PlanStep struct {
    Name      string `json:"name"`
    Objective string `json:"objective"`
}
```

因此必须先加字段（否则"按步骤类型路由"无从谈起）：

```go
type PlanStep struct {
    Name      string `json:"name"`
    Objective string `json:"objective"`
    // Kind 决定执行后端：llm | sandbox | retrieval。
    // 缺省/未知一律按 llm 处理，即保持今天的行为——新增字段不得改变既有 manifest 的语义。
    Kind string `json:"kind,omitempty"`
}
```

`Kind` 由 `PlanTask` activity 产出。首版用确定性规则（objective 中出现“执行/运行/沙箱/检索”等关键词）；规则必须是纯函数，并在 `PLAN_COMPILED` 事件中记录。后续可以交给规划模型，但未知值仍按 `llm` 处理。**默认 `llm`** 是关键：既有的 workflow 与请求不带 `Kind`，必须走原路径。

`ExecuteStep` 改造后：

```go
// cmd/orchestrator/main.go:573 改造后
func (a *activities) ExecuteStep(ctx context.Context, in StepInput) (StepOutput, error) {
    switch in.Step.Kind {
    case "sandbox", "retrieval":
        if out, err := a.callAgentCore(ctx, in); err == nil {
            return out, nil                     // Backend = "agent-core"
        }                                       // 失败继续往下降级
    }
    if out, err := a.callLLM(ctx, in.Params, in.Step); err == nil && out != "" {
        return StepOutput{Output: out, Backend: "llm-service"}, nil
    }
    return StepOutput{Output: localStepOutput(in), Backend: "local"}, nil
}
```

```go
type StepOutput struct {
    Output        string `json:"output"`
    Backend       string `json:"backend"` // agent-core | llm-service | local
    RequestDigest string `json:"request_digest"`
    Attempt       int    `json:"attempt"`
    Degraded      bool   `json:"degraded,omitempty"`
    DegradedFrom  string `json:"degraded_from,omitempty"`
    ErrorCode     string `json:"error_code,omitempty"`
}

type WorkflowResult struct {
    Output      string       `json:"output"`
    Steps       []string     `json:"steps"`
    StepOutputs []StepOutput `json:"step_outputs"`
}
```

**关键要求**：返回值从 `string` 改为 `StepOutput`，携带 `Backend`、`RequestDigest`、`Attempt` 和可选的降级原因。工作流把每一步的 `StepOutput` 写入 `WorkflowResult.StepOutputs`，HTTP 查询和最终结果都暴露该字段；`/health` 只报告服务能力，不伪装成任务结果。理由——上次“三层没连通”的根因不是缺代码，而是缺可观测性：没有任何地方能看出某一步到底跑了哪个后端。

`callAgentCore` 必须使用 `proto/agent_core.AgentServiceClient` 的 `ExecuteTask`/`StreamExecuteTask`，不能通过 agent-core 的管理 HTTP `/api/execute` 代替。管理 HTTP 仍供运维和现有 WASI smoke 使用；工作流接线的验收必须在 gRPC 服务端观察到真实 RPC。

### 5.3 流式 → Temporal 心跳

先澄清现状，避免夸大收益：`activityOpts()` **没有设置 `HeartbeatTimeout`**：

```go
// cmd/orchestrator/main.go:418  —— 现状
func activityOpts() workflow.ActivityOptions {
    return workflow.ActivityOptions{
        StartToCloseTimeout: 60 * time.Second,   // ← 唯一的硬约束
        RetryPolicy: &sdktypes.RetryPolicy{...},
    }
}
```

没有 `HeartbeatTimeout` 时 Temporal **不要求**心跳，所以今天不存在"长步骤被误判重试"的 bug。真正的约束是 `StartToCloseTimeout: 60s`——沙箱类步骤很容易超过它，一旦超时就被判定失败并按 `MaximumAttempts: 3` 重试，**重试会重复执行副作用**。

因此心跳的收益是**解锁长步骤**，而不是修 bug：

```go
opts := workflow.ActivityOptions{
    StartToCloseTimeout: 30 * time.Minute,   // 沙箱步骤可以合法地跑很久
    HeartbeatTimeout:    10 * time.Minute,   // = StartToClose / 3，写死此比例
    RetryPolicy:         &sdktypes.RetryPolicy{...},
}
```

```go
result, err := a.agentCore.Stream(ctx, req, func(u agentcore.TaskUpdate) error {
    activity.RecordHeartbeat(ctx, map[string]any{"iteration": u.Iteration, "status": u.Status})
    return nil
})
```

- 有了心跳，`StartToCloseTimeout` 才能安全地放大——因为 worker 崩溃会被心跳超时**更快**发现，而不是干等 30 分钟。
- agent-core 的 `StreamExecuteTask` 已经存在（server streaming），此前无人消费；接上后 `handleTaskStatus` 也能报告中间进度。
- 如果服务端流创建失败且尚未收到任何更新，客户端可以退回一次 `ExecuteTask`；收到首个更新后再失败必须把该步标为 `agent-core` 的不确定结果，不得静默改走另一后端。
- **必须同时做**：沙箱步骤的幂等性。超时重试仍可能重复执行，因此 `ExecuteStep` 必须把 `StepInput` 的确定性摘要作为幂等键传给 agent-core，由后端去重（复用核心既有的 `IdempotencyStore` 语义）。只加心跳不加幂等，等于把重复副作用的风险从"60 秒后"推迟到"10 分钟后"。

### 5.4 健康门与能力握手

启动时（`main.go` 中目前只有 `tcpReachable`）：

1. `HealthCheck` → 决定 `agent-core` 是否进入可用后端列表；
2. `DiscoverTools` → 缓存能力集，路由时只把 agent-core 真正支持的工具/步骤类型交给它；
3. 结果进入 orchestrator 的 `/health` 输出：`{"agent_core": {"reachable": true, "tools": [...], "degraded": false}}`。该字段表示最近一次握手，不表示每个任务一定会选用 agent-core。

熔断由 `harness.CircuitBreaker`（核心已有，`model.go` 中使用）承担：连续失败后短路，避免每个步骤都白等一个 `StartToCloseTimeout`。

### 5.5 边界声明（写进文档，避免误导）

本项打通的是 **工作流层 → 执行层**。agent-core 不运行控制面 agent loop，它做的是 WASI 沙箱执行与 RAG 问答。

如果目标是「orchestrator 驱动**可持久化的 agent 任务**」，那需要的是一个**控制面客户端**（orchestrator → `harness`），而不是复用 agent-core。这两件事不在本轮范围内；触发条件和处理方式见 §10.2。

### 5.6 验收

```
集成测试与 smoke 新增检查：
  ok: fake/real agent-core 的 gRPC 服务端收到 ExecuteTask 或 StreamExecuteTask
  ok: 步骤结果标注 backend=agent-core
  ok: agent-core 不可达时 backend=local 且任务仍成功
Test 心跳：注入一个慢 StreamExecuteTask，断言 activity 未因 HeartbeatTimeout 重试
```

`smoke.sh` 可以继续通过 agent-core 的 HTTP 管理面验证 WASI；“orchestrator 真的调用 agent-core”由可记录 RPC 次数的 gRPC fixture 集成测试验证，避免把管理面请求误当成工作流接线。

---

## 6. 真实 MCP 客户端

### 6.1 包位置与依赖卫生

新增公开包 `mcp/`，**独立于 `harness`**。原因：stdio 传输会 `exec` 外部进程，这类能力不能进可嵌入核心的闭包（否则每个嵌入者都被迫接受"库会起子进程"）。

`harness` 只定义接缝：

```go
// harness/toolsource.go 【新】
// ToolSource 是一个外部工具来源（MCP server、插件、远程注册表）。
// 核心只依赖这个接口，不认识任何具体传输。
type ToolSource interface {
    Name() string
    Tools(ctx context.Context) ([]ToolSpec, error)
    Call(ctx context.Context, call ToolCall, auth AuthContext) ToolResult
    Close() error
}

// RegisterSource 拉取来源的工具并注册。返回注册成功（通过 schema 校验）的数量。
// 工具名统一加前缀 "<source>__" 以避免跨来源碰撞。
func (r *ToolRegistry) RegisterSource(ctx context.Context, src ToolSource) (int, error)
```

`RegisterSource` 不把外部 handler 直接暴露给模型：它为每个 `<source>__<tool>` 注册一个受核心管线控制的 wrapper，wrapper 再调用 `ToolSource.Call`。这样 MCP 工具**直接进入既有工具管线**：schema 校验（`jsonschema/v5`，核心已有）、scope、超时、重试、幂等、审计——全部复用，无需新写。

### 6.2 客户端

```go
// mcp/client.go 【新】
type Transport interface {
    Send(ctx context.Context, payload []byte) error
    Receive(ctx context.Context) ([]byte, error)
    Close() error
}

// StdioTransport：exec 一个 MCP server，按行分隔的 JSON-RPC 走 stdin/stdout。
type StdioTransport struct{ Command string; Args []string; Env []string; AllowedRoots []string }

// HTTPTransport：streamable HTTP（POST JSON-RPC，响应可为 JSON 或 SSE）。
type HTTPTransport struct{ Endpoint string; Headers map[string]string; Client *http.Client }

type Client struct { /* ... */ }

type ServerConfig struct {
    Name      string
    Transport Transport
    Timeout   time.Duration
}

func Dial(ctx context.Context, cfg ServerConfig) (*Client, error)
func (c *Client) Initialize(ctx context.Context) (ServerInfo, error)
func (c *Client) Tools(ctx context.Context) ([]harness.ToolSpec, error) // 满足 harness.ToolSource
func (c *Client) Call(ctx context.Context, call harness.ToolCall) harness.ToolResult
func (c *Client) Close() error
```

`Client` 内部只有一个 reader，按 JSON-RPC `id` 把响应投递给等待者；写入由带锁的 writer 完成。`Tools` 必须在 `Initialize` 成功后才能调用，`Call` 必须拒绝未知工具名。`Close` 对尚未完成的请求统一返回 `MCP_CLOSED`，并等待子进程组退出或在超时后强制回收。

### 6.3 必须实现的协议行为

| 项 | 要求 |
|---|---|
| 握手 | `initialize`（protocolVersion / capabilities / clientInfo）→ `notifications/initialized` |
| 版本协商 | 客户端提议 `2025-06-18`；服务端返回更低受支持版本时**接受**（规范行为），不报错 |
| 请求关联 | JSON-RPC 2.0 `id` 关联；并发请求各自等待，单 reader goroutine 分发 |
| 错误 | JSON-RPC `error` → `ToolResult{Status: FAILED, ErrorCode: 服务端 code}`；`isError:true` 同样映射 FAILED |
| 取消 | ctx 取消 → `notifications/cancelled` + 该请求立即返回 |
| 超时 | 每次调用独立超时；握手超时单独可配 |
| 服务端反向请求 | `sampling/*`、`roots/list`：**首版统一回 `MethodNotFound`**，并在日志标注能力未实现（诚实优于假装支持） |
| 进度通知 | `notifications/progress` → 转成 `Chunk{Kind: ChunkToolCall, Data:{"progress":...}}`（复用 §3 的流式通道） |

### 6.4 工具描述映射

```go
// mcp/toolspec.go
// MCP tool → harness.ToolSpec
//   name        → "<server>__<tool>"
//   description → Description
//   inputSchema → InputSchema（已是 JSON Schema，直接复用核心校验）
//   annotations.readOnlyHint    → SideEffect = "NONE"
//   annotations.destructiveHint → SideEffect = "IRREVERSIBLE"
//   annotations.idempotentHint  → IdempotencyRequired = false
//   无 annotations              → SideEffect = "UNKNOWN"，IdempotencyRequired = true
```

**安全默认**：MCP 服务端可以声称任何事。只有 `readOnlyHint=true` 才能自动获得无副作用并发资格；未标注的工具按 `UNKNOWN` 处理，并要求幂等键。`destructiveHint=true` 与 `UNKNOWN` 都不能绕过 `Authorizer` 或 `HookBus`。

### 6.5 配置在核心之外

MCP 配置（server 列表、命令、环境变量）属于宿主关注点，**不得**进核心：

```
modelenv/ 或新增 mcpconfig/  ← 读 .mcp.json / env
cmd/*/main.go                ← 组装并注入 harness
```

`mcp` 包只接受显式 `[]ServerConfig`。

### 6.6 stdio 的安全面

stdio 传输本质是**执行任意本地二进制**，与 `packages/hooks` 面对的是同一类风险。直接复用其已验证的做法：

- 可执行文件**必须**位于显式允许的根目录内（canonical path 校验，禁止裸 PATH 查找，禁止 symlink 逃逸）；
- 子进程环境变量白名单，不透传整个 `os.Environ()`；
- 进程组 kill，确保 `Close()` 不留孤儿。

### 6.7 验收

```
TestMCPStdioEndToEnd        起一个真实的最小 stdio MCP server（testdata 下的 Go 程序），
                            完成 initialize → tools/list → tools/call
TestMCPToolEntersRegistry   MCP 工具出现在 ToolRegistry，且被 Authorizer 拦截时会拒绝
TestMCPVersionNegotiation   服务端回更低版本时接受
TestMCPCancellation         ctx 取消发出 notifications/cancelled
TestMCPAllowedRoots         允许根之外的二进制被拒绝
```

`testdata/mcp_echo_server`：一个约 60 行的 Go MCP server，供测试与示例复用（同时也是最好的文档）。

---

## 7. 7 个悬空包接入

### 7.1 决策规则（依赖足迹驱动）

| 条件 | 决策 |
|---|---|
| **正确性所需** 且 **零第三方依赖** | `harness` **直接 import** —— 能力必须默认生效，不能是可选增强 |
| 带第三方依赖，或属于宿主策略 | `harness` 只留 **seam 接口**，适配器放 `integrations/<name>/`，使用者按需 import |
| 与核心已有实现重复且语义重叠 | 先**合并或废弃**，不引入第二套 |

按此规则逐包裁定：

| 包 | 依赖 | 性质裁定 | 决策 |
|---|---|---|---|
| `loopsdetect` | 0 | 正确性：替换 `loopSeen` 的粗糙实现，且 `Nudge` 是真实能力增益 | **直接 import** |
| `skills` | 0 | 正确性：技能解析直接影响上下文编译结果 | **直接 import**（解析路径）；学习/版本管理走 seam 可选 |
| `multiagent` | 0 | 正确性：§4 的团队组建与冲突检测是子代理执行的一部分 | **直接 import**（仅规划器职责，见 4.4） |
| `vector` | 0 | 增强：检索质量；且需要 EmbeddingService（宿主 provider 关注点） | **seam**（`Retriever`）+ `integrations/vector` |
| `permissions` | 3（zap/yaml/multierr） | 宿主策略：YAML 配置 + 审批回调 | **seam**（`Authorizer`）+ `integrations/permissions` |
| `hooks` | 1（yaml） | 宿主策略：会 exec 任意二进制 | **seam**（`HookBus`）+ `integrations/hooks` |
| `prompt-engine` | 11（sprig 等） | 宿主关注点：模板与版本管理 | **seam**（`PromptRenderer`）+ `integrations/promptengine` |

> 直接 import 的三个包**不增加第三方依赖**，因此 C4 与 G2/G7 仍然成立。代价是 `harness` 的编译依赖了 `packages/` 中的三个包——这是单 module 内的分层让步，可接受；若将来拆多 module，这三个包必须随核心一起搬（记入 §10.2）。

### 7.2 接缝接口定义

```go
// harness/loopguard.go 【新】
type LoopDecision int
const (
    LoopContinue LoopDecision = iota
    LoopNudge
    LoopForceStop
)

type LoopObservation struct {
    TaskID      string
    Step        int
    ToolCalls   []ToolCall
    ToolResults []ToolResult
    Text        string
}

// LoopGuard 判定是否陷入循环。默认实现由 loopsdetect 提供（§7.1）。
type LoopGuard interface {
    Check(LoopObservation) LoopDecision
    Reset(taskID string)
    // Directive 返回 Nudge 时应注入模型的消息，使"警告后调整策略"可定制。
    Directive(LoopObservation) string
}
func WithLoopGuard(g LoopGuard) Option
```

行为映射（替换 `runtime.go:400` 的一行硬失败）：

| 决策 | 运行时行为 |
|---|---|
| `LoopContinue` | 正常继续 |
| `LoopNudge` | 提交 `LOOP_NUDGE` 事件，把 `Directive` 作为一条 observation 注入下一步上下文，**继续**（有次数上限） |
| `LoopForceStop` | 提交 `LOOP_DETECTED` 事件，以类别 `LOOP` 失败（保持今天的终态语义） |

`loopsdetect` 的「连续 3 次 Nudge 自动升级 ForceStop」直接对应上表，无需另写升级逻辑。

```go
// harness/authorizer.go 【新】
// Authorizer 是工具与路径的统一授权入口。它取代 policy.go 中写死的判定，
// 并保留 ExecutionPolicy 作为默认实现（向后兼容）。
type Authorizer interface {
    AuthorizeTool(name string, args map[string]any) error
    AuthorizePath(path string) error
}
func WithAuthorizer(a Authorizer) Option

// harness/hookbus.go 【新】
type HookEvent struct {
    Point     string         // PreToolUse | PostToolUse | SessionStart | Stop
    TaskID    string
    ToolName  string
    Arguments map[string]any
    Result    map[string]any
    Auth      AuthContext
}
type HookDecision struct {
    Allow  bool
    Reason string
}
type HookBus interface {
    PreToolUse(ctx context.Context, e HookEvent) (HookDecision, error)
    PostToolUse(ctx context.Context, e HookEvent)
    SessionStart(ctx context.Context, e HookEvent)
    Stop(ctx context.Context, e HookEvent)
}
func WithHookBus(h HookBus) Option

// harness/prompt.go 【新】
// PromptRenderer 渲染系统提示/模板。核心只在上下文编译时调用它，
// 不认识 text/template 或 sprig。
type PromptRenderer interface {
    Render(name string, data map[string]any) (string, error)
}
func WithPromptRenderer(p PromptRenderer) Option
// ContextBuilder 增加字段：Prompt PromptRenderer、PromptName string
// CompilerVersion 必须把 PromptName+模板版本纳入（否则前缀缓存会复用错上下文）

// harness/retriever.go 【新】
// Retriever 取代 MemoryManager 内的词法打分，使语义检索可插拔。
// 默认实现保持今天的词法重叠（行为不变）。
type Retriever interface {
    Retrieve(ctx context.Context, auth AuthContext, query string, limit int) ([]MemoryRecord, error)
}
func WithRetriever(r Retriever) Option

// harness/skills.go 【扩展】
// SkillResolver 取代薄 SkillRegistry 的 Resolve，使 skills 包的能力默认生效。
type SkillResolver interface {
    Resolve(goal string, auth AuthContext) []SkillManifest
}
```

### 7.3 默认接线矩阵

`New()` 的默认值必须让**正确性类**能力默认生效，**策略类**能力默认关闭：

| 接缝 | 默认值 | 理由 |
|---|---|---|
| `LoopGuard` | loopsdetect 支撑的实现 | 正确性；今天的行为是明显退化的 |
| `SkillResolver` | skills 包支撑的实现 | 正确性 |
| `Retriever` | 词法重叠（现状） | 语义检索需要 provider，不能默认开启 |
| `Authorizer` | 现有 `ExecutionPolicy` | 向后兼容；重实现需 opt-in |
| `HookBus` | nil（空操作） | 会执行外部进程，必须显式启用 |
| `PromptRenderer` | nil（使用内置的确定性拼装） | 引入 sprig 属宿主选择 |

### 7.4 需要先解决的重复

| 重复 | 说明 | 本轮处理 |
|---|---|---|
| `packages/vector` vs `packages/vectordb` | 两套向量抽象；`vectordb` 已接入 agent-core，`vector` 悬空 | **合并到 `vectordb`**，`vector` 的 `QueryClassifier`/`ChunkDocument` 迁过去；否则使用者在两个入口间无法选择 |
| `harness/skills.go` vs `packages/skills` | 薄实现与富实现并存 | 薄实现降级为默认 `SkillResolver` 的 fallback，富实现成为默认 |
| `harness/policy.go` vs `packages/permissions` | 同上 | `ExecutionPolicy` 成为 `Authorizer` 的默认实现，保留 |

> 这三条不处理，接入就只是"多了一条路径"，而不是"消除了一份重复"。**重复本身才是上一轮"19,340 行是展示品"的病根。**

### 7.5 验收

每个包一个集成测试，且必须断言**能力真的生效**（而非仅仅被 import）：

```
TestLoopGuardNudgesBeforeStopping   loopsdetect：重复工具调用先 Nudge，3 次后 ForceStop
TestSkillResolverFeedsContext      skills：上下文里出现被选中技能的 instructions
TestSubagentsUseTeamFormation      multiagent：FormTeam 的 fit score 影响子代理选择
TestVectorRetrieverSeam            integrations/vector：注入后检索结果与词法不同且更相关
TestPermissionsDeniesForbiddenPath integrations/permissions：越界路径被拒且审计留痕
TestHookBusDeniesToolCall          integrations/hooks：exit 2 的 hook 拦住工具调用
TestPromptRendererVersioned        integrations/promptengine：模板版本变化使 prefix cache 失效
```

---

## 8. v0.1.0 与 Go module 版本策略

### 8.1 module 布局：维持单 module（附条件）

**决策**：保持单 module，不拆。当前仓库没有 Git tag；`v0.1.0` 是待发布的第一个版本，不把未发布版本写成既成事实。

理由：

1. 拆 module 会引入多份 `go.mod`/`go.sum` 与 `replace` 维护成本，而当前 C4 已由护栏保证核心闭包干净（2 个直接依赖）。
2. 消费者 `import` 核心时，Go 的模块图会读到本 module 的 `require` 列表（含 wasmtime/temporal/qdrant），但**不会被编译进产物**——因为核心的包闭包不含它们。这是可接受的代价。
3. 拆 module 的真实触发条件只有一个：**核心需要与扩展层解耦版本**。当前不存在这个需求。

**记入文档的代价**（诚实披露）：消费者的 `go.sum` 会因模块图而略大；`go get` 拉取的是整个仓库。若将来出现抱怨，按 §10.2 的方案 B 拆分。

### 8.2 打 tag

```
v0.1.0   ← 可嵌入性改造完成并通过 G1–G8（待发布）
v0.2.0   ← 本文档 6 项工作完成
v1.0.0   ← 公开 API 冻结（harness 面不再破坏性变更）
```

**v0.x 语义**（Go module 规范）：`v0.x` 下 minor 版本**允许**破坏性变更。因此 `v0.1.0` 的承诺是「可用、可安装、有测试」，不是「API 稳定」。

### 8.3 API 稳定分层

| 层 | 稳定性承诺 |
|---|---|
| `harness`、`harness/dag`、`harness/storetest` | **稳定**（v1.0 后破坏性变更需 major） |
| `httpapi`、`backends/postgres`、`mcp`、`agentcore` | **实验**：minor 可改，改动写入 CHANGELOG |
| `modelenv`、`integrations/*`、`packages/*` | **实验**：不承诺 |

每个实验包的包注释必须写明该分级（否则使用者无从判断）。

### 8.4 发布流程

1. `CHANGELOG.md`（Keep a Changelog 格式），每个 minor 一节；
2. `git tag -a v0.1.0 -m ...` 并推送；
3. CI `release` job：
   ```bash
   # 关键：无 replace，验证 tag 真的可安装
   cd $(mktemp -d) && go mod init consumer
   go get github.com/Hanwen-Jiang/dag-code-agent/harness@v0.1.0
   go build ./...
   ```
4. `gorelease`（`golang.org/x/exp/cmd/gorelease`）检查 API 兼容性与 module 规范——**作为工具运行，不作为依赖**；
5. 版本可见性：`printVersion` 让 7 个二进制的 `/health` 输出实际 tag（`-ldflags -X main.version=`），使"跑的是哪个版本"可查。

> 第 3 条是本节的核心：今天的 `examples/embed_external` 用了 `replace`，所以**从未验证过 tag 可安装性**。没有这条，打 tag 只是本地动作。

### 8.5 Go 版本策略

`go.mod` 声明 `go 1.26.0` → 消费者需 Go ≥ 1.26。

**版本决策**：本仓库与 CI 统一使用 Go 1.26（`go.mod` 为 `go 1.26.0`；当前验证工具链为 `go1.26.1`）。在没有最低版本 CI 矩阵之前不下调声明；发布前必须用 Go 1.26.x 重新执行全套护栏。若要下调，必须先增加对应版本的构建矩阵并更新 README 与 release job。

### 8.6 弃用流程

沿用可嵌入性文档 §12：`Deprecated:` 注释 → 至少保留一个 minor → 下个 minor 删除。
**待清理项**：`NewRuntime`（已标 Deprecated）应在 `v0.2.0` 删除。

---

## 9. 阶段计划

按依赖关系排序；每阶段独立可交付、可验证。

| 阶段 | 内容 | 依赖 | 估算 |
|---|---|---|---|
| **S1** | 打 `v0.1.0` + 发布流程 + 无 replace 的安装验证 | 无（纯流程） | 0.5 天 |
| **S2** | §3 流式（harness + httpapi + 一致性测试） | 无 | 3–4 天 |
| **S3** | §7 接缝接口 + 三个直接接入包（loopsdetect/skills/multiagent 规划器） | 无 | 4–5 天 |
| **S4** | §4 子代理真实执行（依赖 S3 的 multiagent 接入与 S2 的流式观察） | S2、S3 | 4–5 天 |
| **S5** | §6 MCP 客户端 + testdata server | 无（与 S2–S4 并行） | 4–5 天 |
| **S6** | §5 orchestrator → agent-core + 心跳 + smoke 检查 | 无 | 3 天 |
| **S7** | §7.4 重复清理（vector/vectordb 合并等） | S3 | 2–3 天 |
| **S8** | 四个适配器包（permissions/hooks/promptengine/vector） | S3 | 3–4 天 |
| **S9** | 打 `v0.2.0` + 文档 + CHANGELOG | 全部 | 0.5 天 |
| | **合计** | | **25–32 天** |

**并行建议**：S2 / S5 / S6 三条互不依赖，可并行推进（S5 与 S6 甚至可由不同人负责）。

**风险最高的两处**：S4（子代理持久化语义）与 S6（跨层超时/心跳）。其余都是机械性工作。

---

## 10. 风险与决策边界

### 10.1 风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 流式路径改变持久化语义 | **高**：破坏 replay 这一核心卖点 | §3.5 的逐字节一致性测试；deltas 永不入库 |
| 子代理持久化引入任务树爆炸 | 中：store 膨胀、成本失控 | `MaxDepth` + `MaxParallel` + 预算三重约束；深度写入 metadata |
| 子代理幂等复用写错 → 重复执行 | 中：重复副作用 | 复用判定必须在 `SUBAGENT_SPAWNED` **之前**，且只认 `TaskCompleted` |
| Temporal 心跳比例设置不当 | 中：放大 `StartToCloseTimeout` 后 worker 崩溃发现变慢 | 写死 `HeartbeatTimeout = StartToCloseTimeout / 3` 并加测试 |
| 只加心跳不加幂等 → 超时重试重复副作用 | **高** | `ExecuteStep` 必须传确定性幂等键，见 §5.3 末段 |
| MCP stdio 逃逸 | **高**：任意代码执行 | 复用 hooks 的 canonical-path 允许根 + 环境白名单 + 进程组 kill |
| 直接 import 三个包造成核心与扩展层耦合 | 低 | 记录为单 module 内的分层让步；拆 module 时随核心搬迁 |
| 重复未清理就接入 | 中：变成"多一条路径" | §7.4 作为 S7 显式排期，不视为可选 |
| tag 打早导致 API 冻结压力 | 低 | `v0.x` 明确允许 minor 破坏 |

### 10.2 已定决策与触发条件

1. **是否需要「orchestrator → 控制面」客户端？**
   §5 打通的是执行层。若目标是让 orchestrator 驱动可持久化 agent 任务，需要一个 `harnessclient`（HTTP 或新增 gRPC 契约）。
   **本轮决定**：不做。只有出现“orchestrator 需要创建/恢复 harness 任务”的明确调用方时，才新增独立 `harnessclient` 设计；不得把控制面协议偷偷塞进 agent-core 客户端。

2. **单 module vs 拆多 module**（§8.1）
   **本轮决定**：维持单 module。只有核心包闭包需要与扩展层独立发版，或外部消费者因模块图/下载体积提出可复现问题时，才启动拆分评估；评估必须包含迁移期 `go get` 与无 `replace` 构建结果。

3. **`packages/vector` 与 `packages/vectordb` 合并方向**
   **本轮决定**：以 `vectordb` 为主干，先做 API 对照表和适配器，再迁移 `QueryClassifier`/`ChunkDocument`/`AdaptiveRetriever`；旧 `vector` 只保留一个带 `Deprecated:` 的兼容周期，未完成对照测试前不删除。

4. **MCP 服务端反向请求（sampling/roots）**
   **本轮决定**：首版回 `MethodNotFound` 并记录；只有真实 server fixture 覆盖请求/响应语义后才增加 sampling/roots。

5. **Go 最低版本**（§8.5）
   **本轮决定**：保持 Go 1.26；任何下调都必须先增加对应版本的构建矩阵，并同步更新 README、CI 和 release job。

6. **`MaxDepth` 默认值**
   **本轮决定**：默认 1（禁止嵌套）。需要嵌套时显式开启，并同时提高预算/扇出上限；配置不能只改 `MaxDepth`。

---

## 11. 与验收标准的对应

| 工作 | 阶段 | 交付物 | 验证 |
|---|---|---|---|
| 流式 | S2 | `harness/stream.go`、`httpapi/stream.go`、`stream_conformance_test.go` | §3.5 五项测试 |
| 子代理 | S4 | `harness/subagent.go`、事件序列（§4.2） | §4.5 六项测试 |
| orchestrator→agent-core | S6 | `agentcore/`、`StepOutput.Backend`、smoke 检查 | §5.6 |
| MCP | S5 | `mcp/`、`harness/toolsource.go`、`testdata/mcp_echo_server` | §6.7 五项测试 |
| 版本化 | S1、S9 | tag、`CHANGELOG.md`、release job、版本注入 `/health` | §8.4 第 3 条 |
| 悬空包接入 | S3、S7、S8 | 6 个接缝 + 4 个 `integrations/*` + 重复清理 | §7.5 七项测试 |

**保持不变的约束**（回归即视为本次工作失败）：

- `CGO_ENABLED=0 go build ./harness/...` 通过（G1）
- 核心闭包不含 wasmtime/temporal/qdrant/pgx（G2）
- 库包零 `os.Getenv`、零相对路径探测（G3/G4）
- 外部 module 可 import（G5）
- 一致性套件对每个后端通过（G6）
- 核心直接第三方依赖 ≤ 4（G7）
- 一进程多实例隔离（G8）

> 后续护栏（实现阶段纳入 CI）：
> **G9** —— 核心闭包不得出现 `zap`/`sprig`/`yaml`（防止 §7 的适配器包被误接进核心）；
> **G10** —— `MODEL_PROPOSED` 的流式/非流式一致性测试必须在 CI 中运行。

---

## 附录 · 一句话总结

这 6 项工作的共同点：**它们都不是"再加一个功能"，而是把已经写好、已经测试、但没有接进运行时路径的能力真正接上**。

其中 §4（子代理）和 §7（悬空包）是同一件事的两面——`multiagent`、`skills`、`loopsdetect` 正是子代理执行所需要的零件。做对这两项，`packages/` 那 19,340 行才会从"展示品"变成"零件"。

§3（流式）与 §5（心跳）则是**可观测性**投资：上一轮"三层没连通"的根因不是缺代码，而是没有任何地方能看出某一步跑了什么。§5.2 的 `Backend` 字段与 §3 的 token 帧就是防复发的机制。
