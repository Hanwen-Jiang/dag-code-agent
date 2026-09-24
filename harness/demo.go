package harness

import (
	"context"
	"encoding/json"
	"errors"
)

type DemoEnvironment struct {
	Store    Store
	Journal  *Journal
	Runtime  *Runtime
	Request  TaskRequest
	Auth     AuthContext
	Model    *ScriptedModel
	Prefix   *PrefixCache
	Registry *ToolRegistry
	Memory   *MemoryManager
	Skills   *SkillRegistry
	Context  *ContextBuilder
	MCP      *MCPClient
	Adapter  ModelAdapter
	Provider ModelProviderStatus
}

func BuildDemoEnvironment() (*DemoEnvironment, error) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		return nil, err
	}
	auth := AuthContext{TenantID: "demo-tenant", SubjectID: "demo-user", Scopes: []string{"memory:read", "artifact:write", "artifact:read"}}
	request := TaskRequest{TaskID: "TASK-HARNESS-DEMO", Goal: "use Harness memory and the verification skill, write and read an artifact, then return verified JSON", TenantID: auth.TenantID, SubjectID: auth.SubjectID, Constraints: []string{"return structured evidence"}, ContextWindow: 2048, ResponseSchema: demoResponseSchema()}
	env, err := BuildDemoEnvironmentWithStore(store, request, auth)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return env, nil
}

// BuildDemoEnvironmentWithStore creates the deterministic offline adapter used
// by both the CLI fixture and the HTTP session API. The store is supplied by
// the caller so a server can keep one durable control-plane database while
// each run still owns its model, tool registry and artifact namespace.
func BuildDemoEnvironmentWithStore(store Store, request TaskRequest, auth AuthContext) (*DemoEnvironment, error) {
	if store == nil {
		return nil, errors.New("HARNESS_STORE_REQUIRED")
	}
	if auth.TenantID == "" {
		auth.TenantID = "demo-tenant"
	}
	if auth.SubjectID == "" {
		auth.SubjectID = "demo-user"
	}
	if len(auth.Scopes) == 0 {
		auth.Scopes = []string{"memory:read", "artifact:write", "artifact:read"}
	}
	request = request.WithDefaults()
	if request.TaskID == "" {
		request.TaskID = NewID("TASK")
	}
	if request.Goal == "" {
		request.Goal = "inspect the Harness runtime and return verified JSON"
	}
	if request.TenantID == "" {
		request.TenantID = auth.TenantID
	}
	if request.SubjectID == "" {
		request.SubjectID = auth.SubjectID
	}
	if request.ResponseSchema == nil {
		request.ResponseSchema = demoResponseSchema()
	}

	registry := NewToolRegistry()
	_ = registry.Register(ToolSpec{Name: "search_memory", Description: "search task memory", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}}, "required": []string{"query"}, "additionalProperties": false}, RequiredScopes: []string{"memory:read"}, Handler: func(args map[string]any, _ AuthContext) (any, error) {
		return map[string]any{"matches": []any{map[string]any{"content": "Harness verifies side effects with evidence"}}}, nil
	}})
	_ = registry.Register(ToolSpec{Name: "write_artifact", Description: "write a demo artifact", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"artifact_id": map[string]any{"type": "string"}, "content": map[string]any{"type": "string"}, "idempotency_key": map[string]any{"type": "string"}}, "required": []string{"artifact_id", "content", "idempotency_key"}, "additionalProperties": false}, RequiredScopes: []string{"artifact:write"}, SideEffect: "IRREVERSIBLE", IdempotencyRequired: true, Handler: func(args map[string]any, toolAuth AuthContext) (any, error) {
		id, _ := args["artifact_id"].(string)
		content, _ := args["content"].(string)
		record, err := store.PutArtifact(toolAuth, id, map[string]any{"content": content})
		if err != nil {
			return nil, err
		}
		return map[string]any{"artifact_id": record.ArtifactID, "content": record.Content["content"], "sha256": record.SHA256}, nil
	}})
	_ = registry.Register(ToolSpec{Name: "read_artifact", Description: "read a demo artifact", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"artifact_id": map[string]any{"type": "string"}}, "required": []string{"artifact_id"}, "additionalProperties": false}, RequiredScopes: []string{"artifact:read"}, Handler: func(args map[string]any, toolAuth AuthContext) (any, error) {
		id, _ := args["artifact_id"].(string)
		record, ok, err := store.GetArtifact(toolAuth, id)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("ARTIFACT_NOT_FOUND")
		}
		return map[string]any{"artifact_id": record.ArtifactID, "content": record.Content["content"], "sha256": record.SHA256}, nil
	}})
	memory := NewMemoryManager(store)
	_, _ = memory.Write(auth, "semantic", "demo", "harness", "Models propose; the control plane commits", []string{"harness"}, .95, "", 0)
	skills := NewSkillRegistry()
	skills.Register(SkillManifest{SkillID: "verification", Version: "1.0.0", Title: "verification", Instructions: "bind final output to evidence", Tags: []string{"verify"}, Status: "ACTIVE"})
	prefix := NewPrefixCache(8)
	model := NewScriptedModel([]ModelResponse{{ToolCalls: []ToolCall{{CallID: "m1", Name: "search_memory", Arguments: map[string]any{"query": "Harness"}}}}, {ToolCalls: []ToolCall{{CallID: "m2", Name: "write_artifact", Arguments: map[string]any{"artifact_id": "ART-HARNESS-DEMO", "content": "verified artifact", "idempotency_key": "artifact-1"}}}}, {ToolCalls: []ToolCall{{CallID: "m3", Name: "read_artifact", Arguments: map[string]any{"artifact_id": "ART-HARNESS-DEMO"}}}}, {Text: `{"status":"completed","artifact_id":"ART-HARNESS-DEMO","verified":true}`, FinishReason: "stop"}})
	model.ModelName = "scripted-harness"
	// The demo is the reference composition for embedders: every collaborator
	// is supplied through an option instead of a positional constructor.
	runtime, err := New(
		WithStore(store),
		WithModel(model),
		WithToolRegistry(registry),
		WithSkills(skills),
		WithPrefixCache(prefix),
		WithMemoryManager(memory),
		WithVerifier(func(_ TaskState, text string) error {
			var output map[string]any
			if err := json.Unmarshal([]byte(text), &output); err != nil {
				return err
			}
			if output["verified"] != true {
				return errors.New("evidence not verified")
			}
			if output["artifact_id"] != "ART-HARNESS-DEMO" {
				return errors.New("wrong artifact")
			}
			return nil
		}),
	)
	if err != nil {
		return nil, err
	}
	mcp := NewMCPClient(NewMCPServer(runtime.Tools()), auth)
	return &DemoEnvironment{Store: store, Journal: runtime.Journal(), Runtime: runtime, Request: request, Auth: auth, Model: model, Adapter: model, Provider: ModelProviderStatus{Provider: "scripted", Mode: "offline", Model: model.Name(), Configured: true, Detail: "离线 Harness fixture，不调用外部模型"}, Prefix: prefix, Registry: registry, Memory: memory, Skills: skills, Context: runtime.ContextBuilder(), MCP: mcp}, nil
}

// The conversational (chat) environment used to live here. It read
// HARNESS_MODEL_* and HARNESS_WORKSPACE_ROOT directly, which made the core
// impossible to embed predictably. It now lives in internal/modelenv, which
// translates the environment into explicit harness options.

func demoResponseSchema() map[string]any {
	return map[string]any{"type": "object", "required": []string{"status", "artifact_id", "verified"}, "properties": map[string]any{"status": map[string]any{"type": "string"}, "artifact_id": map[string]any{"type": "string"}, "verified": map[string]any{"type": "boolean"}}, "additionalProperties": false}
}
func (e *DemoEnvironment) Run(ctx context.Context) (RunResult, error) {
	return e.Runtime.Run(ctx, e.Request, e.Auth)
}

// Snapshot exposes the same durable/runtime boundaries exercised by the
// offline fixture. It is intentionally a diagnostic projection: handlers and
// private model state stay inside the Go process, while contracts and replay
// evidence remain inspectable by the control-room UI.
func (e *DemoEnvironment) Snapshot(result RunResult) map[string]any {
	state := result.State
	packet, packetErr := e.Context.Build(e.Request, e.Auth, state, nil)
	mcpInfo, mcpErr := e.MCP.Initialize()
	mcpTools, toolsErr := e.MCP.ListTools()
	memoryRecords, memoryErr := e.Memory.Retrieve(e.Auth, e.Request.Goal, "")
	resolvedSkills := e.Skills.Resolve(e.Request.Goal, e.Auth)
	checkpoint, checkpointErr := e.Journal.Store().LatestCheckpoint(result.TaskID)
	replayed, replayErr := e.Journal.Replay(result.TaskID)
	if _, stateReplayErr := e.Journal.ReplayState(result.TaskID); replayErr == nil && stateReplayErr != nil {
		replayErr = stateReplayErr
	}

	stableMessages := 0
	for _, message := range packet.Messages {
		if stable, ok := message["stable"].(bool); ok && stable {
			stableMessages++
		}
	}
	tools := make([]map[string]any, 0)
	for _, spec := range e.Registry.List() {
		tools = append(tools, map[string]any{
			"name": spec.Name, "description": spec.Description, "input_schema": spec.InputSchema,
			"required_scopes": spec.RequiredScopes, "side_effect": spec.SideEffect,
			"idempotency_required": spec.IdempotencyRequired, "timeout_seconds": spec.TimeoutSeconds,
			"max_retries": spec.MaxRetries, "tags": spec.Tags,
		})
	}
	skills := make([]map[string]any, 0, len(resolvedSkills))
	for _, skill := range resolvedSkills {
		skills = append(skills, map[string]any{"skill_id": skill.SkillID, "version": skill.Version, "title": skill.Title, "status": skill.Status, "tags": skill.Tags, "instructions": skill.Instructions})
	}
	memories := make([]map[string]any, 0, len(memoryRecords))
	for _, memory := range memoryRecords {
		memories = append(memories, map[string]any{"memory_id": memory.MemoryID, "type": memory.MemoryType, "namespace": memory.Namespace, "key": memory.Key, "content": memory.Content, "confidence": memory.Confidence, "version": memory.Version, "source": memory.Source})
	}

	headHash := "GENESIS"
	eventTypes := make([]string, 0, len(result.Events))
	modelRequestEvents := 0
	if len(result.Events) > 0 {
		headHash = result.Events[len(result.Events)-1].EventHash
		for _, event := range result.Events {
			eventTypes = append(eventTypes, event.Type)
			if event.Type == "MODEL_REQUESTED" {
				modelRequestEvents++
			}
		}
	}
	contextSnapshot := map[string]any{
		"compiler_version": e.Context.CompilerVersion,
		"compile_error":    errorText(packetErr),
		"estimated_tokens": packet.EstimatedTokens,
		"budget_tokens":    packet.BudgetTokens,
		"stable_messages":  stableMessages,
		"dynamic_messages": len(packet.Messages) - stableMessages,
		"memory_items":     len(packet.Memories),
		"skill_items":      len(packet.Skills),
		"visible_tools":    len(packet.Tools),
		"prefix_cache_key": packet.PrefixCacheKey,
		"prefix_cache_hit": packet.PrefixCacheHit,
		"exact_invariants": packet.ExactInvariants,
	}

	adapterName := e.Model.Name()
	if e.Adapter != nil {
		adapterName = e.Adapter.Name()
	}
	fixtureMode := e.Provider.Mode
	if fixtureMode == "" {
		fixtureMode = "offline"
	}
	snapshot := map[string]any{
		"fixture":      map[string]any{"name": "dag-code-agent-session", "mode": fixtureMode, "provider_boundary": "OpenAI-compatible adapter contract"},
		"request":      e.Request,
		"auth":         e.Auth,
		"model":        map[string]any{"adapter": adapterName, "provider_status": e.Provider, "rounds_recorded": state.ModelRound, "in_memory_requests": len(e.Model.Requests), "streaming_contract": true, "structured_output": e.Request.ResponseSchema != nil, "tool_calls": true, "fallback_boundary": "ModelAdapter", "request_events": modelRequestEvents, "visible_input_reconstructable": modelRequestEvents == state.ModelRound},
		"context":      contextSnapshot,
		"tools":        tools,
		"mcp":          map[string]any{"initialized": mcpErr == nil, "initialize": mcpInfo, "initialize_error": errorText(mcpErr), "tools": mcpTools, "tools_error": errorText(toolsErr)},
		"skills":       skills,
		"memory":       memories,
		"memory_error": errorText(memoryErr),
		"cache":        map[string]any{"max_entries": e.Prefix.MaxEntries, "stats": e.Prefix.Stats(), "provider_kv_boundary": "provider/model service"},
		"durability":   map[string]any{"checkpoint_sequence": checkpoint.Sequence, "checkpoint_created_at": checkpoint.CreatedAt, "checkpoint_available": checkpointErr == nil, "replay_verified": replayErr == nil, "replay_events": len(replayed), "event_head_hash": headHash, "hash_chain": "sha256(previous_hash, sequence, event_type, canonical_payload)", "event_types": eventTypes, "model_visible_input": modelRequestEvents > 0},
		"contract":     map[string]any{"agent_loop": []string{"context_compile", "model_proposal", "tool_or_subagent_or_gate", "observation_commit", "verifier", "final_commit"}, "policy": []string{"identity", "scope", "schema", "timeout", "retry", "idempotency", "UNKNOWN"}, "long_horizon": []string{"checkpoint", "lease", "fresh_attempt", "DAG", "UAT"}, "evaluation": []string{"success_rate", "Wilson_CI", "p95_latency", "ablation", "promotion_guardrail"}},
	}
	return snapshot
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
