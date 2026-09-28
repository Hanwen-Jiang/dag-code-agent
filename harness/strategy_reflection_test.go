package harness

import (
	"context"
	"testing"
)

// scriptedCalls counts model invocations. ScriptedModel records requests rather
// than a counter, and the lock is held because a run may call from more than one
// goroutine once fan-out is involved.
func scriptedCalls(model *ScriptedModel) int {
	model.mu.Lock()
	defer model.mu.Unlock()
	return len(model.Requests)
}

// The claim in the design was that a genuinely different topology is a short
// decorator over the existing seam. Reflection is the test of that claim: it
// adds a verification pass without changing the kernel, the vocabulary or the
// store.

func reflectionScript() []ModelResponse {
	return []ModelResponse{
		{Text: "the answer is 42", FinishReason: "stop", Usage: ModelUsage{InputTokens: 10, OutputTokens: 5}},
		{Text: "the answer is 42, checked against the goal", FinishReason: "stop", Usage: ModelUsage{InputTokens: 30, OutputTokens: 8}},
	}
}

func TestReflectionAddsAVerificationPass(t *testing.T) {
	model := NewScriptedModel(reflectionScript())
	runtime, err := New(
		WithStore(NewInMemoryStore()),
		WithModel(model),
		WithClock(FixedClock(fixedClockTimeForTests())),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(ReflectionStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "reflect", Goal: "what is the answer?", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	// Two model calls: the draft and the verification pass. Without the
	// decorator this task would finish after one.
	if calls := scriptedCalls(model); calls != 2 {
		t.Fatalf("model calls=%d, want 2 (draft plus one verification pass)", calls)
	}
	if result.FinalAnswer != "the answer is 42, checked against the goal" {
		t.Fatalf("final answer=%q, want the corrected answer", result.FinalAnswer)
	}
	// The strategy identity is journaled, so a replay cannot silently switch
	// topology.
	events, err := runtime.Journal().Replay("reflect")
	if err != nil {
		t.Fatal(err)
	}
	recorded := false
	for _, event := range events {
		if event.Type != "TURN_STARTED" {
			continue
		}
		// The payload carries the identity as a structure (name and version),
		// not as a bare string.
		switch identity := event.Payload["strategy"].(type) {
		case map[string]any:
			recorded = stringValue(identity["name"]) == reflectionStrategyName
		case string:
			recorded = identity == reflectionStrategyName
		}
		if recorded {
			break
		}
	}
	if !recorded {
		t.Fatal("the reflection topology was not journaled with the run")
	}
}

// The verification budget is bounded: a decorator that kept reflecting would
// never finish.
func TestReflectionIsBounded(t *testing.T) {
	responses := make([]ModelResponse, 0, 8)
	for i := 0; i < 8; i++ {
		responses = append(responses, ModelResponse{Text: "answer", FinishReason: "stop", Usage: ModelUsage{InputTokens: 5, OutputTokens: 2}})
	}
	model := NewScriptedModel(responses)
	runtime, err := New(
		WithStore(NewInMemoryStore()),
		WithModel(model),
		WithClock(FixedClock(fixedClockTimeForTests())),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(ReflectionStrategy{MaxRounds: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "bounded", Goal: "answer", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 8,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	// One draft plus MaxRounds verification passes, then it must stop.
	if calls := scriptedCalls(model); calls != 3 {
		t.Fatalf("model calls=%d, want 3 (one draft plus two bounded rounds)", calls)
	}
}

// A decorator must not interfere with a topology that does not reach a final
// answer, or it would break tool use.
func TestReflectionDelegatesWhileWorkRemains(t *testing.T) {
	registry := NewToolRegistry()
	if err := registry.Register(ToolSpec{
		Name: "echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}
	model := NewScriptedModel([]ModelResponse{
		{ToolCalls: []ToolCall{{CallID: "c1", Name: "echo", Arguments: map[string]any{}}}},
		{Text: "done", FinishReason: "stop"},
		{Text: "done, verified", FinishReason: "stop"},
	})
	runtime, err := New(
		WithStore(NewInMemoryStore()),
		WithModel(model),
		WithToolRegistry(registry),
		WithClock(FixedClock(fixedClockTimeForTests())),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(ReflectionStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "tools", Goal: "echo something", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	events, err := runtime.Journal().Replay("tools")
	if err != nil {
		t.Fatal(err)
	}
	toolRan := false
	for _, event := range events {
		if event.Type == "TOOLS_EXECUTED" {
			toolRan = true
		}
	}
	if !toolRan {
		t.Fatal("the decorator prevented the tool from running")
	}
}
