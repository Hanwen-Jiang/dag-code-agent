package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOfflineDemoClosesLoopAndReplays(t *testing.T) {
	env, err := BuildDemoEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer env.Store.Close()
	result := env.Run(context.Background())
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s error=%v", result.Status, result.State.LastError)
	}
	var output map[string]any
	if err := json.Unmarshal([]byte(result.FinalAnswer), &output); err != nil {
		t.Fatal(err)
	}
	if output["artifact_id"] != "ART-HARNESS-DEMO" {
		t.Fatalf("answer=%v", output)
	}
	events, err := env.Runtime.Replay(result.TaskID)
	if err != nil || len(events) != len(result.Events) {
		t.Fatalf("replay=%d err=%v", len(events), err)
	}
	eventTypes := map[string]bool{}
	for _, event := range events {
		eventTypes[event.Type] = true
	}
	for _, eventType := range []string{"TURN_STARTED", "STEP_STARTED", "CONTEXT_COMPILED", "MODEL_REQUESTED", "MODEL_PROPOSED", "TOOL_INVOKED", "TOOL_OBSERVED", "STEP_COMPLETED", "TURN_COMPLETED", "TASK_COMPLETED"} {
		if !eventTypes[eventType] {
			t.Fatalf("missing durable lifecycle event %s", eventType)
		}
	}
	var requestEvent Event
	for _, event := range events {
		if event.Type == "MODEL_REQUESTED" {
			requestEvent = event
			break
		}
	}
	if _, ok := requestEvent.Payload["model_request"].(map[string]any); !ok {
		t.Fatalf("model request was not persisted: %+v", requestEvent.Payload)
	}
	state, err := env.Runtime.Recover(result.TaskID)
	if err != nil || state.Status != TaskCompleted {
		t.Fatalf("recover=%s err=%v", state.Status, err)
	}
	if env.Prefix.Stats()["hits"] == 0 {
		t.Fatal("expected stable-prefix cache hit")
	}
	if len(env.Model.Requests) == 0 {
		t.Fatal("expected model requests")
	}
	contexts := map[string]bool{}
	for _, message := range env.Model.Requests[0].Messages {
		role := stringValue(message["role"])
		if role != "system" && role != "developer" && role != "user" && role != "assistant" && role != "tool" {
			t.Fatalf("provider received unsupported role %q", role)
		}
		if _, ok := message["content"].(string); !ok {
			t.Fatalf("provider received non-string content: %+v", message)
		}
		content := stringValue(message["content"])
		for _, kind := range []string{"state_context", "memory_context", "skill_context"} {
			if strings.HasPrefix(content, "["+kind+"]") {
				contexts[kind] = true
			}
		}
	}
	for _, kind := range []string{"state_context", "memory_context", "skill_context"} {
		if !contexts[kind] {
			t.Fatalf("provider context missing %s: %+v", kind, env.Model.Requests[0].Messages)
		}
	}
	for _, request := range env.Model.Requests {
		for _, message := range request.Messages {
			if _, ok := message["stable"]; ok {
				t.Fatal("private cache metadata leaked to provider")
			}
		}
	}
}

func newRuntimeForTest(t *testing.T, responses []ModelResponse) (*Runtime, Store, AuthContext, TaskRequest) {
	t.Helper()
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	registry := NewToolRegistry()
	tools := NewToolRuntime(registry, store)
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel(responses)), WithTools(tools), WithContextBuilder(NewContextBuilder(nil, NewSkillRegistry(), registry, NewPrefixCache(4))))
	if err != nil {
		t.Fatal(err)
	}
	request := TaskRequest{TaskID: "task", Goal: "finish the task", TenantID: auth.TenantID, SubjectID: auth.SubjectID, ContextWindow: 2048, MaxSteps: 8}
	return runtime, store, auth, request
}

func TestUserGateResumeAndImmutableTaskIdentity(t *testing.T) {
	runtime, store, auth, request := newRuntimeForTest(t, []ModelResponse{{NeedUser: map[string]any{"kind": "approval", "question": "continue?"}}, {Text: "done"}})
	defer store.Close()
	waiting := runtime.Run(context.Background(), request, auth)
	if waiting.Status != TaskWaitingUser {
		t.Fatalf("status=%s", waiting.Status)
	}
	resumed := runtime.Resume(context.Background(), request.TaskID, "continue", auth)
	if resumed.Status != TaskCompleted || resumed.FinalAnswer != "done" {
		t.Fatalf("resumed=%+v", resumed)
	}
	changed := request
	changed.Goal = "different goal"
	conflict := runtime.Run(context.Background(), changed, auth)
	if conflict.Status != TaskFailed || !strings.Contains(conflict.State.LastErrorString(), "TASK_IDENTITY_MISMATCH") {
		t.Fatalf("conflict=%+v", conflict.State.LastError)
	}
}

func TestTerminalTaskStatesAreMonotonic(t *testing.T) {
	env, err := BuildDemoEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer env.Store.Close()
	completed := env.Run(context.Background())
	resumed := env.Runtime.Resume(context.Background(), completed.TaskID, "late input", env.Auth)
	if resumed.Status != TaskCompleted || resumed.FailureCategory != "STATE" {
		t.Fatalf("resume terminal=%+v", resumed)
	}
	state, err := env.Runtime.Recover(completed.TaskID)
	if err != nil || state.Status != TaskCompleted {
		t.Fatalf("recovered terminal=%s err=%v", state.Status, err)
	}
	for _, event := range resumed.Events {
		if event.Type == "TASK_FAILED" {
			t.Fatal("terminal resume appended TASK_FAILED")
		}
	}

	failedEnv, err := BuildDemoEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer failedEnv.Store.Close()
	failedEnv.Model.responses = nil
	failed := failedEnv.Run(context.Background())
	if failed.Status != TaskFailed {
		t.Fatalf("failed status=%s", failed.Status)
	}
	cancelled := failedEnv.Runtime.Cancel(failed.TaskID, failedEnv.Auth)
	if cancelled.Status != TaskFailed {
		t.Fatalf("cancel rewrote failed task=%+v", cancelled)
	}
}

func TestLoopAndContextBudgetTerminateDeterministically(t *testing.T) {
	call := ToolCall{CallID: "same", Name: "echo", Arguments: map[string]any{"value": "x"}}
	runtime, store, auth, request := newRuntimeForTest(t, []ModelResponse{{ToolCalls: []ToolCall{call}}, {ToolCalls: []ToolCall{call}}, {ToolCalls: []ToolCall{call}}, {ToolCalls: []ToolCall{call}}})
	defer store.Close()
	_ = runtime.Tools().Registry.Register(ToolSpec{Name: "echo", Description: "echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}}, Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil }})
	looped := runtime.Run(context.Background(), request, auth)
	if looped.Status != TaskFailed || looped.FailureCategory != "LOOP" {
		t.Fatalf("loop=%+v", looped)
	}

	tinyRuntime, tinyStore, tinyAuth, tiny := newRuntimeForTest(t, []ModelResponse{{Text: "unused"}})
	defer tinyStore.Close()
	tiny.TaskID = "tiny"
	tiny.ContextWindow = 8
	budgeted := tinyRuntime.Run(context.Background(), tiny, tinyAuth)
	if budgeted.Status != TaskFailed || budgeted.FailureCategory != "CONTEXT" {
		t.Fatalf("budget=%+v", budgeted)
	}
}

func TestEventTamperingAndOptimisticConflict(t *testing.T) {
	env, err := BuildDemoEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	defer env.Store.Close()
	result := env.Run(context.Background())
	if _, err := env.Store.(*SQLiteStore).db.Exec(`UPDATE harness_events SET payload_json='{"tampered":true}' WHERE task_id=? AND sequence=2`, result.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Runtime.Replay(result.TaskID); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("tamper err=%v", err)
	}
	stateStore, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer stateStore.Close()
	stateEnv, err := BuildDemoEnvironmentWithStore(stateStore, TaskRequest{TaskID: "state-tamper", Goal: "verify state binding"}, AuthContext{TenantID: "demo-tenant", SubjectID: "demo-user", Scopes: []string{"memory:read", "artifact:write", "artifact:read"}})
	if err != nil {
		t.Fatal(err)
	}
	stateResult := stateEnv.Run(context.Background())
	if _, err := stateStore.db.Exec(`UPDATE harness_tasks SET state_json='{"task_id":"state-tamper","goal":"rewritten","status":"COMPLETED"}' WHERE task_id=?`, stateResult.TaskID); err != nil {
		t.Fatal(err)
	}
	if _, err := NewJournal(stateStore, SystemClock()).ReplayState(stateResult.TaskID); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("state tamper err=%v", err)
	}

	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()
	state := TaskState{TaskID: "optimistic", Goal: "g", TenantID: "t", SubjectID: "s"}
	state.Normalize()
	journal := NewJournal(store, SystemClock())
	if _, err := journal.Create(state, "CREATED", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	// The optimistic-concurrency check now lives in the store primitive that
	// the journal drives: a stale writer must be refused, not fork the chain.
	tail, ok, err := store.LatestEvent(state.TaskID)
	if err != nil || !ok {
		t.Fatalf("tail ok=%v err=%v", ok, err)
	}
	next, err := nextEvent(&tail, state, "BAD", map[string]any{}, SystemClock().Now())
	if err != nil {
		t.Fatal(err)
	}
	next.TaskVersion = tail.TaskVersion + 1
	wrong := 9
	if err := store.AppendEvent(state, &wrong, &tail.Sequence, next); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	// A non-contiguous event must be refused even with no version hint.
	forked := next
	forked.Sequence = tail.Sequence + 5
	if err := store.AppendEvent(state, nil, nil, forked); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("fork err=%v", err)
	}
}

func TestToolSchemaScopeIdempotencyAndUnknown(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()
	registry := NewToolRegistry()
	runtime := NewToolRuntime(registry, store)
	auth := AuthContext{TenantID: "t", SubjectID: "s", Scopes: []string{"write"}}
	var writes atomic.Int32
	_ = registry.Register(ToolSpec{Name: "write", Description: "write", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}, "idempotency_key": map[string]any{"type": "string"}}, "required": []string{"value", "idempotency_key"}, "additionalProperties": false}, RequiredScopes: []string{"write"}, SideEffect: "IRREVERSIBLE", IdempotencyRequired: true, Handler: func(args map[string]any, _ AuthContext) (any, error) {
		writes.Add(1)
		return map[string]any{"saved": args["value"]}, nil
	}})
	first := runtime.Execute(context.Background(), ToolCall{CallID: "1", Name: "write", Arguments: map[string]any{"value": "x", "idempotency_key": "key"}}, auth)
	replay := runtime.Execute(context.Background(), ToolCall{CallID: "2", Name: "write", Arguments: map[string]any{"value": "other", "idempotency_key": "key"}}, auth)
	if !first.OK() || !replay.IdempotentReplay || writes.Load() != 1 {
		t.Fatalf("first=%+v replay=%+v calls=%d", first, replay, writes.Load())
	}
	invalid := runtime.Execute(context.Background(), ToolCall{CallID: "3", Name: "write", Arguments: map[string]any{"value": 2, "idempotency_key": "bad"}}, auth)
	if invalid.ErrorCode != "INVALID_ARGUMENTS" {
		t.Fatalf("invalid=%+v", invalid)
	}
	denied := runtime.Execute(context.Background(), ToolCall{CallID: "4", Name: "write", Arguments: map[string]any{"value": "x", "idempotency_key": "denied"}}, AuthContext{TenantID: "t", SubjectID: "s"})
	if denied.ErrorCode != "PERMISSION_DENIED" {
		t.Fatalf("denied=%+v", denied)
	}
	policyRuntime := NewToolRuntimeWithPolicy(registry, store, ExecutionPolicy{AllowedTools: []string{"read-only"}})
	policyDenied := policyRuntime.Execute(context.Background(), ToolCall{CallID: "4b", Name: "write", Arguments: map[string]any{"value": "x", "idempotency_key": "policy"}}, auth)
	if policyDenied.ErrorCode != "POLICY_DENIED" {
		t.Fatalf("policy denied=%+v", policyDenied)
	}
	if visible := registry.SchemasForAuth(nil, AuthContext{TenantID: "t", SubjectID: "s"}); len(visible) != 0 {
		t.Fatalf("unauthorized tool schema leaked: %+v", visible)
	}
	_ = registry.Register(ToolSpec{Name: "unsafe_write", Description: "unsafe", InputSchema: map[string]any{"type": "object"}, SideEffect: "IRREVERSIBLE", Handler: func(map[string]any, AuthContext) (any, error) { return "ok", nil }})
	missingKey := runtime.Execute(context.Background(), ToolCall{CallID: "4c", Name: "unsafe_write", Arguments: map[string]any{}}, auth)
	if missingKey.ErrorCode != "IDEMPOTENCY_REQUIRED" {
		t.Fatalf("irreversible tool without key=%+v", missingKey)
	}

	var slowCalls atomic.Int32
	_ = registry.Register(ToolSpec{Name: "slow", Description: "slow", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"idempotency_key": map[string]any{"type": "string"}}, "required": []string{"idempotency_key"}}, RequiredScopes: []string{"write"}, SideEffect: "IRREVERSIBLE", IdempotencyRequired: true, TimeoutSeconds: .01, Handler: func(_ map[string]any, _ AuthContext) (any, error) {
		slowCalls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "late", nil
	}})
	unknown := runtime.Execute(context.Background(), ToolCall{CallID: "5", Name: "slow", Arguments: map[string]any{"idempotency_key": "slow-key"}}, auth)
	unknownReplay := runtime.Execute(context.Background(), ToolCall{CallID: "6", Name: "slow", Arguments: map[string]any{"idempotency_key": "slow-key"}}, auth)
	if unknown.Status != ToolUnknown || !unknownReplay.IdempotentReplay || slowCalls.Load() != 1 {
		t.Fatalf("unknown=%+v replay=%+v calls=%d", unknown, unknownReplay, slowCalls.Load())
	}
}

func TestMCPMemoryAndSkillLifecycle(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()
	registry := NewToolRegistry()
	tools := NewToolRuntime(registry, store)
	_ = registry.Register(ToolSpec{Name: "echo", Description: "echo", InputSchema: map[string]any{"type": "object"}, Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil }})
	auth := AuthContext{TenantID: "t", SubjectID: "s", Scopes: []string{"read"}}
	client := NewMCPClient(NewMCPServer(tools), auth)
	init, err := client.Initialize()
	if err != nil || init["protocolVersion"] == "" {
		t.Fatalf("init=%v err=%v", init, err)
	}
	if result := client.Call(context.Background(), ToolCall{CallID: "mcp", Name: "echo", Arguments: map[string]any{"x": "y"}}); !result.OK() {
		t.Fatalf("mcp=%+v", result)
	}
	bad := NewMCPClient(NewMCPServer(tools), auth)
	bad.ProtocolVersion = "old"
	if _, err := bad.Initialize(); err == nil {
		t.Fatal("expected protocol mismatch")
	}

	memory := NewMemoryManager(store)
	old, err := memory.Write(auth, "semantic", "prefs", "language", "Chinese", nil, .8, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	newer, err := memory.Write(auth, "semantic", "prefs", "language", "Simplified Chinese", nil, .9, "", old.Version)
	if err != nil {
		t.Fatal(err)
	}
	if newer.Supersedes != old.MemoryID || newer.Version != 2 {
		t.Fatalf("newer=%+v", newer)
	}
	if _, err := memory.Write(auth, "semantic", "prefs", "language", "stale", nil, .5, "", 1); err == nil {
		t.Fatal("expected memory version conflict")
	}
	if ok, _ := memory.Forget(auth, newer.MemoryID); !ok {
		t.Fatal("forget failed")
	}
	if records, _ := memory.Retrieve(auth, "Chinese", "prefs"); len(records) != 0 {
		t.Fatalf("forgotten memory leaked old version: %+v", records)
	}

	skills := NewSkillRegistry()
	skills.Register(SkillManifest{SkillID: "research", Version: "1.10", Title: "research", Status: "ACTIVE"})
	skills.Register(SkillManifest{SkillID: "research", Version: "2.0", Title: "research", Status: "ACTIVE"})
	active, _ := skills.Activate("research")
	if active.Version != "2.0" {
		t.Fatalf("version=%s", active.Version)
	}
	skills.Deprecate("research", "2.0")
	active, _ = skills.Activate("research")
	if active.Version != "1.10" {
		t.Fatalf("fallback=%s", active.Version)
	}
}

type failingModel struct{}

func (failingModel) Name() string { return "broken" }
func (failingModel) Complete(context.Context, ModelRequest) (ModelResponse, error) {
	return ModelResponse{}, &ModelError{Message: "down", Retryable: true}
}
func (failingModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, &ModelError{Message: "down", Retryable: true}
}

func TestModelAdapterRouterStructuredOutputAndStreaming(t *testing.T) {
	var sawModel atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] == "remote" {
			sawModel.Store(true)
		}
		if streaming, _ := body["stream"].(bool); streaming {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n")
			return
		}
		write := map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": `{"ok":true}`}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 4, "completion_tokens": 2, "prompt_tokens_details": map[string]any{"cached_tokens": 2}}}
		_ = json.NewEncoder(w).Encode(write)
	}))
	defer server.Close()
	adapter := &OpenAICompatibleAdapter{BaseURL: server.URL, ModelName: "remote", Client: server.Client()}
	schema := map[string]any{"type": "object", "required": []string{"ok"}, "properties": map[string]any{"ok": map[string]any{"type": "boolean"}}}
	response, err := adapter.Complete(context.Background(), ModelRequest{TaskID: "m", ResponseSchema: schema})
	if err != nil || !sawModel.Load() || response.Usage.InputTokens != 4 || response.Usage.CachedInputTokens != 2 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if err := ValidateStructured(response.Text, schema); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStructured(`{}`, schema); err == nil {
		t.Fatal("missing required field accepted")
	}
	strict := map[string]any{"type": "object", "properties": map[string]any{"mode": map[string]any{"enum": []string{"safe"}}}, "required": []string{"mode"}, "additionalProperties": false}
	if err := ValidateStructured(`{"mode":"unsafe","extra":1}`, strict); err == nil {
		t.Fatal("enum/additionalProperties violation accepted")
	}
	router := ModelRouter{Primary: failingModel{}, Fallback: NewScriptedModel([]ModelResponse{{Text: "fallback"}})}
	if got, err := router.Complete(context.Background(), ModelRequest{}); err != nil || got.Text != "fallback" {
		t.Fatalf("router=%+v err=%v", got, err)
	}
	stream, err := adapter.Stream(context.Background(), ModelRequest{})
	if err != nil {
		t.Fatal(err)
	}
	text := ""
	for event := range stream {
		if event.Type == "text_delta" {
			text += stringValue(event.Data["delta"])
		}
	}
	if text != "hello" {
		t.Fatalf("stream=%q", text)
	}
}

func TestMultiAgentConflictEvaluationAndPrefixCache(t *testing.T) {
	runtime, store, auth, request := newRuntimeForTest(t, nil)
	defer store.Close()
	coordinator := MultiAgentCoordinator{Factory: func(spec SubagentSpec) ModelAdapter {
		return NewScriptedModel([]ModelResponse{{Text: "result-" + spec.AgentID}})
	}, Context: runtime.ContextBuilder(), Tools: runtime.Tools().Registry, Runtime: runtime.Tools(), MaxParallel: 2}
	results, err := coordinator.FanOut(context.Background(), []SubagentSpec{{AgentID: "a", Role: "researcher", Objective: "research"}, {AgentID: "b", Role: "verifier", Objective: "verify"}}, request, auth)
	if err != nil || len(results) != 2 {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	merged := coordinator.Merge(results, true)
	if merged["status"] != "CONFLICT" {
		t.Fatalf("merged=%+v", merged)
	}
	cases := []EvaluationCase{{CaseID: "a"}, {CaseID: "b"}}
	variants := RunAblation(cases, map[string]Evaluator{"base": func(c EvaluationCase) (map[string]any, error) {
		return map[string]any{"ok": c.CaseID == "a", "steps": 2, "latency_ms": 10}, nil
	}, "new": func(EvaluationCase) (map[string]any, error) {
		return map[string]any{"ok": true, "steps": 1, "latency_ms": 5}, nil
	}})
	if variants["new"].Metrics.TaskSuccessRate != 1 || variants["base"].Metrics.TaskSuccessRateCILow == 0 {
		t.Fatalf("variants=%+v", variants)
	}
	if CompareEvaluation(variants["base"], variants["new"])["delta_task_success_rate"] <= 0 {
		t.Fatal("expected positive delta")
	}
	cache := NewPrefixCache(1)
	key := cache.Key([]map[string]any{{"role": "system", "content": "stable", "stable": true}}, nil)
	if cache.Observe(key, 3) || !cache.Observe(key, 3) {
		t.Fatal("cache miss/hit sequence wrong")
	}
	identity := PrefixCacheIdentity{Model: "model-a", ModelRevision: "r1", TenantID: "tenant-a", PolicyVersion: "policy-v1"}
	otherTenant := identity
	otherTenant.TenantID = "tenant-b"
	if cache.KeyWithIdentity(nil, nil, identity) == cache.KeyWithIdentity(nil, nil, otherTenant) {
		t.Fatal("prefix key crossed tenant boundary")
	}
}

func TestFeedbackPromotionGateAndExecutionPolicy(t *testing.T) {
	store, _ := NewSQLiteStore(":memory:")
	defer store.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "user"}
	collector := FeedbackCollector{Store: store}
	for _, signal := range []FeedbackSignal{{TaskID: "a", Stage: "complete", Outcome: "completed", Rating: 5, Category: "success"}, {TaskID: "b", Stage: "tool", Outcome: "human_handoff", Rating: 3, Category: "tool_timeout"}, {TaskID: "c", Stage: "start", Outcome: "drop_off", Category: "context"}} {
		if err := collector.Record(auth, signal); err != nil {
			t.Fatal(err)
		}
	}
	metrics, err := collector.Metrics("tenant")
	if err != nil || metrics.Signals != 3 || metrics.Completed != 1 || metrics.HumanHandoff != 1 || metrics.DropOff != 1 || metrics.MeanRating != 4 {
		t.Fatalf("metrics=%+v err=%v", metrics, err)
	}
	cases := make([]EvaluationCase, 20)
	for i := range cases {
		cases[i] = EvaluationCase{CaseID: fmt.Sprint(i)}
	}
	baseline := func(c EvaluationCase) (map[string]any, error) {
		return map[string]any{"ok": c.CaseID != "0" && c.CaseID != "1", "steps": 3, "latency_ms": 20}, nil
	}
	candidate := func(EvaluationCase) (map[string]any, error) {
		return map[string]any{"ok": true, "steps": 2, "latency_ms": 15}, nil
	}
	decision := EvaluatePromotion(cases, baseline, candidate, PromotionPolicy{MinSamples: 20, MinSuccessDelta: .05, MaxP95LatencyDelta: 0, MaxMeanStepsDelta: 0})
	if !decision.Promote {
		t.Fatalf("decision=%+v", decision)
	}
	blocked := EvaluatePromotion(cases[:3], baseline, candidate, PromotionPolicy{MinSamples: 20})
	if blocked.Promote || !ContainsString(blocked.Reasons, "INSUFFICIENT_SAMPLES") {
		t.Fatalf("blocked=%+v", blocked)
	}
	root := t.TempDir()
	policy := ExecutionPolicy{WorkspaceRoot: root, AllowedPaths: []string{"src/**"}, ForbiddenPaths: []string{"src/.env"}, AllowedTools: []string{"read"}}
	if err := policy.AuthorizePath(root + "/src/main.go"); err != nil {
		t.Fatal(err)
	}
	if err := policy.AuthorizePath(root + "/src/.env"); err == nil {
		t.Fatal("forbidden path accepted")
	}
	if err := policy.AuthorizePath(root + "/../outside"); err == nil {
		t.Fatal("path traversal accepted")
	}
	if err := policy.AuthorizeTool("write"); err == nil {
		t.Fatal("tool policy bypassed")
	}
}

func TestArtifactStoreSurvivesRestartAndPreservesIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harness.sqlite")
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject"}
	store, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	written, err := store.PutArtifact(auth, "artifact-1", map[string]any{"content": "verified"})
	if err != nil || written.SHA256 == "" {
		t.Fatalf("written=%+v err=%v", written, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, ok, err := reopened.GetArtifact(auth, "artifact-1")
	if err != nil || !ok || loaded.Content["content"] != "verified" || loaded.SHA256 != written.SHA256 {
		t.Fatalf("loaded=%+v ok=%v err=%v", loaded, ok, err)
	}
	if _, ok, err := reopened.GetArtifact(AuthContext{TenantID: "tenant", SubjectID: "other"}, "artifact-1"); err != nil || ok {
		t.Fatalf("artifact crossed subject boundary: ok=%v err=%v", ok, err)
	}
	if _, err := reopened.PutArtifact(auth, "artifact-1", map[string]any{"content": "rewritten"}); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("content replacement err=%v", err)
	}
}
