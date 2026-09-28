package harness

import (
	"context"
	"testing"
	"time"
)

// The guidance channel is what makes this strategy expressible. Before it, a
// strategy could choose which action to take but not what the model saw while
// taking it.

func cotRuntime(t *testing.T, model ModelAdapter, options ...Option) (*Runtime, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	base := []Option{
		WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(CoTStrategy{}),
	}
	base = append(base, options...)
	runtime, err := New(base...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

func requestContains(model *ScriptedModel, index int, needle string) bool {
	model.mu.Lock()
	defer model.mu.Unlock()
	if index >= len(model.Requests) {
		return false
	}
	for _, message := range model.Requests[index].Messages {
		if content, ok := message["content"].(string); ok && contains(content, needle) {
			return true
		}
	}
	return false
}

// The directive must reach the model, or the strategy is decorative.
func TestCoTGuidanceReachesTheModel(t *testing.T) {
	model := NewScriptedModel([]ModelResponse{{Text: "the answer", FinishReason: "stop"}})
	runtime, auth := cotRuntime(t, model)
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "cot", Goal: "explain something", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !requestContains(model, 0, "reason through the task step by step") {
		t.Fatal("the reasoning directive never reached the model request")
	}
}

// Guidance augments the compiled context; it must not replace it. Replacing is
// the bug the channel exists to prevent.
func TestCoTGuidanceDoesNotReplaceTheContext(t *testing.T) {
	model := NewScriptedModel([]ModelResponse{{Text: "the answer", FinishReason: "stop"}})
	runtime, auth := cotRuntime(t, model)
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "keeps", Goal: "a distinctive goal marker", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !requestContains(model, 0, "a distinctive goal marker") {
		t.Fatal("the compiled context was discarded when guidance was added")
	}
	if !requestContains(model, 0, "reason through the task step by step") {
		t.Fatal("the guidance was not added")
	}
}

// The preamble belongs to the opening of a turn, not to every step: a repeated
// preamble is noise the model learns to skip and costs budget per step.
func TestCoTPreambleIsNotRepeatedWithinATurn(t *testing.T) {
	registry := NewToolRegistry()
	if err := registry.Register(ToolSpec{
		Name: "echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		ConcurrencySafe: true, SideEffect: "NONE",
		Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}
	model := NewScriptedModel([]ModelResponse{
		{ToolCalls: []ToolCall{{CallID: "c1", Name: "echo", Arguments: map[string]any{}}}},
		{Text: "done", FinishReason: "stop"},
	})
	runtime, auth := cotRuntime(t, model, WithToolRegistry(registry))
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "once", Goal: "use a tool", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !requestContains(model, 0, "reason through the task step by step") {
		t.Fatal("the opening call did not carry the preamble")
	}
	if requestContains(model, 1, "reason through the task step by step") {
		t.Fatal("the preamble was repeated after the model had already acted")
	}
}

// The guidance is part of the request digest, so a replayed run sends what the
// original sent.
func TestGuidanceAffectsTheRequestDigest(t *testing.T) {
	plain := ModelRequest{TaskID: "t", Messages: []map[string]any{{"role": "user", "content": "goal"}}}
	withGuidance := ModelRequest{TaskID: "t", Messages: []map[string]any{
		{"role": "user", "content": "goal"},
		{"role": "developer", "content": cotPreamble},
	}}
	first, _ := Digest(plain)
	second, _ := Digest(withGuidance)
	if first == second {
		t.Fatal("guidance does not affect the request digest, so a replay would not notice it")
	}
}

// The channel is general, not CoT-specific: any strategy can use it.
type guidedStrategy struct{}

func (guidedStrategy) Name() string    { return "guided-test" }
func (guidedStrategy) Version() string { return "1.0.0" }
func (guidedStrategy) Next(context.Context, LoopState) (Action, error) {
	return Action{Kind: ActionCallModel, Guidance: []string{"a custom instruction"}}, nil
}
func (guidedStrategy) OnResult(st LoopState, _ StepResult) LoopState { return st }

func TestGuidanceIsAvailableToAnyStrategy(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	model := NewScriptedModel([]ModelResponse{{Text: "ok", FinishReason: "stop"}})
	runtime, err := New(WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()), WithStrategyInstance(guidedStrategy{}))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "custom", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 2,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !requestContains(model, 0, "a custom instruction") {
		t.Fatal("a custom strategy's guidance did not reach the model")
	}
}
