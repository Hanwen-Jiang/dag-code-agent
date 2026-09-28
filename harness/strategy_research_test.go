package harness

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Research is the only iterative topology here, and its hard part is stopping:
// left alone a model either answers after one search or keeps looking forever.
// These tests pin the bound from both sides.

func researchRuntime(t *testing.T, model ModelAdapter, options ...Option) (*Runtime, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	registry := NewToolRegistry()
	if err := registry.Register(ToolSpec{
		Name: "lookup", Description: "look something up",
		InputSchema:     map[string]any{"type": "object", "properties": map[string]any{}},
		ConcurrencySafe: true, SideEffect: "NONE",
		Handler: func(args map[string]any, _ AuthContext) (any, error) { return map[string]any{"found": true}, nil },
	}); err != nil {
		t.Fatal(err)
	}
	base := []Option{
		WithStore(store), WithModel(model), WithToolRegistry(registry),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(ResearchStrategy{}),
	}
	base = append(base, options...)
	runtime, err := New(base...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// researchingModel searches twice, then answers.
type researchingModel struct {
	searches int
	requests []ModelRequest
}

// recordedContains checks what the model was actually sent. The helper in the
// CoT test is typed to ScriptedModel, and this model is not one.
func (m *researchingModel) recordedContains(index int, needle string) bool {
	if index >= len(m.requests) {
		return false
	}
	for _, message := range m.requests[index].Messages {
		if content, ok := message["content"].(string); ok && contains(content, needle) {
			return true
		}
	}
	return false
}

func (m *researchingModel) Name() string { return "researching" }

func (m *researchingModel) Complete(_ context.Context, request ModelRequest) (ModelResponse, error) {
	m.requests = append(m.requests, request)
	text := requestText(request)
	if m.searches < 3 {
		m.searches++
		// Distinct arguments per round. Repeating an identical call is what the
		// loop guard exists to catch, and it correctly suppressed the repeat —
		// which made an earlier version of this test measure the guard rather
		// than the round budget.
		return ModelResponse{ToolCalls: []ToolCall{{
			CallID: fmt.Sprintf("c%d", m.searches), Name: "lookup",
			Arguments: map[string]any{"topic": fmt.Sprintf("topic-%d", m.searches)},
		}}, FinishReason: "tool_calls"}, nil
	}
	_ = text
	return ModelResponse{Text: "the synthesized answer", FinishReason: "stop"}, nil
}

func (m *researchingModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

// The gather instruction must reach the model while rounds remain, and the
// closing instruction must arrive once they are spent. Without the second, a
// model that keeps searching is never told to stop.
func TestResearchGathersThenCloses(t *testing.T) {
	model := &researchingModel{}
	runtime, auth := researchRuntime(t, model, WithStrategyInstance(ResearchStrategy{Rounds: 1}))
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "research", Goal: "find out the facts", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 8,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	if !model.recordedContains(0, "gather evidence") {
		t.Fatal("the gather instruction never reached the model")
	}
	// After the round budget is spent the closing instruction must appear, or
	// the model is never told to stop searching.
	closed := false
	for i := 0; i < 4; i++ {
		if model.recordedContains(i, "Stop gathering") {
			closed = true
		}
	}
	if !closed {
		t.Fatal("the closing instruction never reached the model, so nothing bounds the search")
	}
}

// A round is a step that ran tools, not a step that happened: counting model
// calls would close the phase before any evidence existed.
func TestResearchCountsGatheringRoundsNotModelCalls(t *testing.T) {
	model := &researchingModel{}
	runtime, auth := researchRuntime(t, model, WithStrategyInstance(ResearchStrategy{Rounds: 2}))
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "counted", Goal: "find out", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 8,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	events, err := runtime.Journal().Replay("counted")
	if err != nil {
		t.Fatal(err)
	}
	toolSteps := 0
	for _, event := range events {
		if event.Type == "TOOLS_EXECUTED" {
			toolSteps++
		}
	}
	if toolSteps < 2 {
		t.Fatalf("only %d tool rounds ran; the round budget closed the phase before evidence existed", toolSteps)
	}
}

// Without tools available the strategy must still terminate: it cannot gather,
// so it answers.
func TestResearchWithoutToolsStillAnswers(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store), WithModel(NewScriptedModel([]ModelResponse{{Text: "answered from what I have", FinishReason: "stop"}})),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(ResearchStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "notools", Goal: "answer directly", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
}
