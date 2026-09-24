# Three-Layer Architecture Migration Guide

## Overview

This document describes the implementation of production-grade **Three-Layer Architecture** based on Shannon's reference implementation and Chapter 20 of *AI Agent Architecture*.

```
┌─────────────────────────────────────────────────────────────┐
│                    User Request                              │
└──────────────────────────┬──────────────────────────────────┘
                           ↓ HTTP/gRPC
┌──────────────────────────▼──────────────────────────────────┐
│  Orchestrator Layer (Go)                                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Temporal Workflow Engine                             │  │
│  │ • Task Routing (Research/Coding/Analysis)           │  │
│  │ • Budget Control (Token/Cost tracking)              │  │
│  │ • Priority Queues (Critical/High/Normal/Low)        │  │
│  │ • Durable Execution (Crash Recovery)                │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           ↓ gRPC
┌──────────────────────────▼──────────────────────────────────┐
│  Agent Core Layer (Rust-style Go)                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ WASI Sandbox                                         │  │
│  │ • Fuel Metering (CPU quota)                         │  │
│  │ • Epoch Timeout (30s limit)                         │  │
│  │ • Memory Protection (256MB cap)                     │  │
│  │ • Zero Network Access (WASI preview1)               │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Permission Engine (5-Layer)                          │  │
│  │ • Hard-blocked → Config deny → Composite decompose   │  │
│  │ • Auto-allow → User approval                         │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Loop Detectors (9 types)                             │  │
│  │ • SearchEscalation, Sleep, Duplicate patterns        │  │
│  │ • Nudge → ForceStop escalation                      │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────┘
                           ↓ HTTP
┌──────────────────────────▼──────────────────────────────────┐
│  LLM Service Layer (Python-style Go)                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Multi-Provider Abstraction                           │  │
│  │ • OpenAI / Anthropic / DeepSeek                     │  │
│  │ • Model Tier Routing (production/experimental)       │  │
│  │ • Cost Tracking & Optimization                      │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Vector DB + RAG Pipeline                             │  │
│  │ • Qdrant Integration                                 │  │
│  │ • Working/Episodic/Semantic Memory                   │  │
│  │ • Context Compaction at 85% threshold                │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

---

## Implementation Status (as of this build)

The design above is **fully implemented and running** as seven Go binaries under a single
`github.com/Hanwen-Jiang/dag-code-agent` module. Each layer degrades gracefully to an in-process local implementation
when its external dependency is unreachable, so the whole stack runs offline
(`./scripts/smoke.sh` → **PASS: 18 / FAIL: 0**).

| Design component | Implemented in | Notes |
|---|---|---|
| Harness Control Plane (event hash-chain, DAG, gates) | `harness`, `harness/dag`, `cmd/agent-harness` | SQLite-backed, `ReplayState` verifies the chain |
| Temporal Workflow Engine + routing + priority queues | `cmd/orchestrator` | Real Temporal SDK; **local inline executor** when Temporal is down |
| WASI Sandbox (fuel / epoch / no-network / preopens) | `packages/sandbox/wasi` (`WasiExecutor`) | Real `bytecodealliance/wasmtime-go` v15; fuel + epoch-interruption + per-run preopens + stdout/stderr capture |
| Permission Engine / Loop Detectors | `packages/permissions`, `packages/loopsdetect` | Deterministic policy + pattern detectors |
| LLM Service (multi-provider, tier routing, cost) | `cmd/llm-service` | OpenAI-compatible HTTP; rate limiter, LRU+TTL cache, metrics, **local provider fallback** |
| Vector DB + RAG | `packages/vectordb` (+ `packages/embedding`) | Qdrant / pgvector / **in-memory** via `NewAutoVectorDB`; local hashed embeddings fallback |
| Agent Core gRPC service | `cmd/agent-core` + `proto/agent_core` | `ExecuteTask` / `StreamExecuteTask` / `HealthCheck` / `DiscoverTools` + HTTP mgmt (`/api/execute` WASI round-trip, `/api/index` RAG ingest) |
| XAI transparency (trace/attribution/explanation) | `cmd/xai-dashboard` + `packages/xai` | SSE live traces, attribution, reasoning narrative, visualization UI |
| Self-evolution (eval/ablation/A-B/failure catalog) | `cmd/self-evolution` + `packages/selfevolution` | Fixed dataset `data/evaluation/tasks.yaml`; deterministic, `SELF_EVOLUTION_ONESHOT=1` |

Port map used by `scripts/smoke.sh` (all on `127.0.0.1`): harness `18080`, llm-service
`18081`, agent-core gRPC `18051` / HTTP `18052`, xai-dashboard `18030`, orchestrator
`18083`. Real infrastructure (Temporal `7233/8233`, Qdrant `6333/6334`) is provided by
`docker compose up -d`.

---

## Component Breakdown

### 1. Orchestrator Layer (cmd/orchestrator/)

**Responsibility**: System "brain" - decides who executes in what order

#### Key Features

| Feature | Implementation | Benefit |
|---------|----------------|---------|
| **Durable Execution** | Temporal Workflows | Automatic crash recovery |
| **Budget Control** | Token/Cost tracking per workflow | Prevent runaway costs |
| **Route Selection** | Heuristic classifier (ReAct/DAG) | Optimal execution strategy |
| **Priority Queues** | Critical/High/Normal/Low | SLA guarantees |
| **Workflow ID Propagation** | `X-Workflow-ID` header | Distributed tracing |

#### Priority Queue Strategy

```go
if priorityQueues {
    startWorker("shannon-tasks-critical", 12, 12)  // Real-time requests
    startWorker("shannon-tasks-high", 10, 10)       // Important tasks
    startWorker("shannon-tasks", 8, 8)              // Normal background
    startWorker("shannon-tasks-low", 4, 4)          // Reports/cleanup
} else {
    startWorker("shannon-tasks", 10, 10)            // Simplified mode
}
```

#### Route Classification

```go
func selectRoute(query string, tools []string) string {
    queryLower := strings.ToLower(query)
    
    if contains(tools, "code_execution") || contains(tools, "file_edit") {
        return "coding"      // Code generation/refactoring
    }
    if contains(queryLower, "analyze") || contains(queryLower, "compare") {
        return "analysis"    // Data/knowledge analysis
    }
    if contains(queryLower, "search") || contains(queryLower, "research") {
        return "research"    // Web search + synthesis
    }
    
    return "default"         // General conversation
}
```

---

### 2. Agent Core Layer (cmd/agent-core/)

**Responsibility**: Security guard - executes tools safely in isolated environment

#### WASI Sandbox Integration

```go
wasiConfig := wasi.WasiSandboxConfig{
    MemoryLimitMB:   256,
    FuelLimit:       1_000_000_000,     // 1B instructions
    Timeout:         30 * time.Second,
    AllowedPaths:    []string{"/tmp/wasi-sandbox"},
    MaxTableElements: 10000,             // Python WASM requirement
}

wasiSandbox, err := wasi.NewWasiExecutor(wasiConfig)
```

#### Five-Layer Permission Engine

```go
permEngine := permissions.NewPermissionEngine(permissions.DefaultConfig())

// For every bash/tool call:
if err := permEngine.Check("bash", map[string]interface{}{"command": cmd}); err != nil {
    // Layers checked sequentially:
    // 1. Hard-blocked (rm -rf /, dd if=/dev/...)
    // 2. Config deny list
    // 3. Composite command decomposition (cmd1 && cmd2)
    // 4. Auto-allow safe commands (ls, pwd, git status)
    // 5. User approval workflow
    
    return fmt.Errorf("permission denied: %w", err)
}
```

#### Loop Detection Integration

```go
loopDetector := loopsdetect.NewLoopDetector(loopsdetect.Config{
    WindowSize: 20,
})

// In execution loop:
decision := loopDetector.Analyze(slidingWindow)
switch decision {
case loopsdetect.Continue:
    continue
case loopsdetect.Nudge:
    injectPromptToLLM("You're repeating actions. Try a different approach.")
case loopsdetect.ForceStop:
    finalCall := llm.CallWithoutTools("Summarize current state")
    return finalCall
}
```

---

### 3. LLM Service Layer (cmd/llm-service/)

**Responsibility**: Voice provider - converses with AI models and manages knowledge base

#### Multi-Provider Abstraction

```go
providers := map[string]Provider{
    "openai":      NewOpenAIProvider(cfg.OpenAIKey),
    "anthropic":   NewAnthropicProvider(cfg.AnthropicKey),
    "deepseek":    NewDeepSeekProvider(cfg.DeepSeekKey),
}

// Select model by tier
func selectModelForTier(tier string) string {
    switch tier {
    case "production":
        return "gpt-4o"  // Most reliable
    case "experimental":
        return "deepseek-coder"  // Cutting edge
    default:
        return "claude-3-sonnet"  // Balance
    }
}
```

#### Vector DB + RAG Pipeline

```go
vdb, err := vectordb.NewQdrantClient("qdrant", 6333, false)

ragPipeline := vectordb.NewRAGPipeline(vdb, embeddingService, config)

// Store memories across multiple types:
ragPipeline.Store(ctx, vectordb.EpisodicMemory, message, metadata)
ragPipeline.Store(ctx, vectordb.SemanticMemory, document, docMetadata)
ragPipeline.AddToWorkingMemory(ctx, sessionContext)

// Retrieve relevant context:
results, err := ragPipeline.Retrieve(ctx, query, topK=5, filters)
for _, r := range results {
    ctx += fmt.Sprintf("Memory[%d]: %v\n", r.Score, r.Metadata)
}

// Auto-compact context when near token limit:
summaryPrompt, shouldCompact, err := ragPipeline.CompactContext(ctx, tokensUsed)
if shouldCompact {
    summary := llm.Summarize(context, summaryPrompt)
    replaceHistoryWithSummary(summary)
}
```

---

## Configuration Setup

### Docker Compose Deployment

```yaml
version: '3.8'

services:
  # Vector Database
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
      - "6334:6334"
    volumes:
      - qdrant_data:/qdrant/storage

  # Temporal Workflow Engine
  temporal:
    image: temporalio/auto-setup:latest
    ports:
      - "7233:7233"
    depends_on:
      - cassandra

  cassandra:
    image: cassandra:4
  
  # Orchestrator Layer (Go)
  orchestrator:
    build: ./cmd/orchestrator
    environment:
      - TEMPORAL_HOST=temporal:7233
      - AGENT_CORE_ADDR=agent-core:50051
      - LLM_SERVICE_ADDR=http://llm-service:8081
    depends_on:
      - temporal
      - agent-core
      - llm-service

  # Agent Core Layer (Rust-style)
  agent-core:
    build: ./cmd/agent-core
    environment:
      - VECTOR_DB_HOST=qdrant
      - VECTOR_DB_PORT=6333
      - WASI_MEMORY_LIMIT_MB=256
      - WASI_FUEL_LIMIT=1000000000
    deploy:
      resources:
        limits:
          memory: 512M

  # LLM Service Layer (Multi-provider)
  llm-service:
    build: ./cmd/llm-service
    environment:
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
      - DEEPSEEK_API_KEY=${DEEPSEEK_API_KEY}
    deploy:
      resources:
        limits:
          memory: 1G

volumes:
  qdrant_data:
```

---

## API Usage Examples

### Orchestrator API

```bash
curl -X POST http://localhost:8080/agent/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Analyze quarterly financial reports and identify risks",
    "context": {"company_id": "ABC123"},
    "allowed_tools": ["web_search", "file_read", "data_analysis"],
    "model_tier": "production",
    "max_tokens": 4096,
    "budget": {
      "token_limit": 10000,
      "cost_limit": 50.00
    }
  }'
```

### Agent Core Direct API

```bash
curl -X POST http://localhost:50051/AgentService/ExecuteTask \
  -H "Content-Type: application/grpc-web+proto" \
  --data-binary @task_request.bin
```

### LLM Service Chat API (OpenAI-compatible)

```bash
curl -X POST http://localhost:8081/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o",
    "messages": [
      {"role": "system", "content": "You are a helpful coding assistant"},
      {"role": "user", "content": "Explain this code snippet"}
    ],
    "tools": [{"type": "code_executor"}],
    "max_tokens": 2048
  }'
```

---

## Observability

### Distributed Tracing

All layers inject `X-Workflow-ID` header:

```go
// Orchestrator
req.Header.Set("X-Workflow-ID", taskID)
req.Header.Set("X-Run-ID", runID)

// Agent Core
ctx = context.WithValue(ctx, "workflow_id", taskID)
zap.With(zap.String("workflow_id", taskID)).Info(...)

// LLM Service
trace.SpanFromContext(ctx).SetTag("workflow_id", taskID)
```

### Metrics Collection

```go
metrics := metricsCollector{
    requests_total: prometheus.NewCounterVec(
        prometheus.CounterOpts{Name: "dag_requests_total"},
        []string{"layer", "status"},
    ),
    tokens_used: prometheus.Histogram{
        Buckets: []float64{100, 1000, 10000, 100000},
    },
    latency_ms: prometheus.Histogram{
        Buckets: prometheus.DefBuckets,
    },
}
```

---

## Performance Benchmarks

Based on Shannon's production experience:

| Metric | Monolithic | Three-Layer | Improvement |
|--------|-----------|-------------|-------------|
| Startup Time | 10s+ | 3-5s per layer | 2x faster |
| Concurrent Users | ~100 | ~1,000 | 10x scale |
| Crash Recovery | Manual | Automatic (<1min) | Better UX |
| Resource Isolation | None | Strong (WASI) | Security |
| Cost Tracking | Per-session | Per-workflow | Accurate billing |

---

## Security Checklist

- [ ] WASI sandbox enabled for all code execution
- [ ] Five-layer permission engine active
- [ ] Loop detectors configured (window size ≥ 20)
- [ ] Hooks configured for audit logging
- [ ] Rate limiting on all endpoints
- [ ] Token budget enforcement per request
- [ ] Network isolation in WASI (disabled by default)
- [ ] Memory guard pages enabled (64MB minimum)
- [ ] Fuel limit set (≥1B instructions recommended)
- [ ] Epoch timeout every 100ms for 30s max runtime

---

## Migration Path

### Phase 1: Parallel Run (Week 1-2)
1. Deploy three-layer alongside existing system
2. Run traffic split 90/10 for comparison
3. Monitor cost/performance differences

### Phase 2: Gradual Cutover (Week 3-4)
1. Increase three-layer ratio to 50%
2. Enable WASI sandbox gradually
3. Configure hook system for auditing

### Phase 3: Full Migration (Week 5-6)
1. Switch all traffic to three-layer
2. Decommission monolithic version
3. Establish baselines for future optimization

---

## Troubleshooting

### Issue: Temporary Worker Crashes
```
Cause: Connection pool exhaustion
Fix: Increase worker concurrency
    tempWorkerOptions := client.Options{
        MaxConcurrentActivityExecutionSize: 50,  // Was 10
    }
```

### Issue: WASI Execution Timeout
```
Symptom: Epoch deadline exceeded errors
Fix: Adjust timeout or fuel limit
    wasiConfig.Timeout = 60 * time.Second
    wasiConfig.FuelLimit = 2_000_000_000
```

### Issue: Vector DB Connection Failures
```
Symptom: Qdrant connection refused
Fix: Check network and replicas
    curl -s http://qdrant:6333/queues
    docker-compose restart qdrant
```

---

## References

1. **Shannon Reference**: https://github.com/Kocoro-lab/Shannon
2. **Temporal SDK**: https://docs.temporal.io
3. **Qdrant Documentation**: https://qdrant.tech/documentation
4. **WASI Specification**: https://wasi.dev
5. **Chapter 20 AI Agent Architecture**: Production Three-Layer Design
