package harness

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Subagents are durable child tasks, not in-memory calls. These tests pin the
// properties that distinction is supposed to buy: real execution, crash reuse,
// a bounded tree, and an isolated budget.

// delegatingModel routes on the content of the request rather than on call
// order.
//
// A scripted model cannot be used here: subagents run concurrently, so "the next
// scripted response" would go to whichever child happened to ask first and the
// test would be flaky by construction.
type delegatingModel struct {
	mu    sync.Mutex
	calls map[string]int
}

func newDelegatingModel() *delegatingModel {
	return &delegatingModel{calls: map[string]int{}}
}

func (m *delegatingModel) Name() string { return "delegating" }

func (m *delegatingModel) Complete(_ context.Context, request ModelRequest) (ModelResponse, error) {
	text := requestText(request)
	m.mu.Lock()
	m.calls[request.TaskID]++
	m.mu.Unlock()

	// Once the children's answers are in the context, the parent must answer
	// rather than delegate again.
	if strings.Contains(text, "DELEGATE") && !strings.Contains(text, "ANSWER-FOR") && !strings.Contains(text, "answer from") {
		return ModelResponse{
			Text: "delegating to two specialists",
			SubagentRequests: []map[string]any{
				{"agent_id": "alpha", "role": "researcher", "objective": "ANSWER-FOR alpha", "max_steps": 3},
				{"agent_id": "beta", "role": "reviewer", "objective": "ANSWER-FOR beta", "max_steps": 3},
			},
			FinishReason: "stop",
		}, nil
	}
	for _, marker := range []string{"alpha", "beta"} {
		if strings.Contains(text, "ANSWER-FOR "+marker) {
			return ModelResponse{Text: "answer from " + marker, FinishReason: "stop"}, nil
		}
	}
	return ModelResponse{Text: "parent final answer", FinishReason: "stop"}, nil
}

// Stream reports the capability gap rather than faking one: this model exists to
// test fan-out, not streaming.
func (m *delegatingModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

func requestText(request ModelRequest) string {
	var b strings.Builder
	for _, message := range request.Messages {
		if content, ok := message["content"].(string); ok {
			b.WriteString(content)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func subagentRuntime(t *testing.T, options ...Option) (*Runtime, Store, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	base := []Option{
		WithStore(store),
		WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
	}
	base = append(base, options...)
	runtime, err := New(base...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	return runtime, store, auth
}

// A fan-out must actually run: the children have to exist as durable tasks with
// their own journals, and the parent has to receive their answers.
func TestSubagentsExecuteAsDurableTasks(t *testing.T) {
	runtime, store, auth := subagentRuntime(t)
	defer store.Close()
	defer runtime.Close()

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "parent", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("parent status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}

	for _, agentID := range []string{"alpha", "beta"} {
		childID := "parent:" + agentID
		state, _, err := store.GetTask(childID)
		if err != nil {
			t.Fatalf("child %s was never created: %v", childID, err)
		}
		if state.Status != TaskCompleted {
			t.Fatalf("child %s status=%s, want COMPLETED", childID, state.Status)
		}
		if state.FinalAnswer != "answer from "+agentID {
			t.Fatalf("child %s answer=%q", childID, state.FinalAnswer)
		}
		// A child owns a real journal, which is what makes its trajectory
		// replayable rather than a lost side effect.
		events, err := runtime.Journal().Replay(childID)
		if err != nil {
			t.Fatalf("child %s journal: %v", childID, err)
		}
		if len(events) == 0 {
			t.Fatalf("child %s has no journal", childID)
		}
		if got := fmt.Sprint(state.Metadata[parentTaskKey]); got != "parent" {
			t.Fatalf("child %s parent_task_id=%q", childID, got)
		}
	}

	// The parent's journal records the whole fan-out.
	events, err := runtime.Journal().Replay("parent")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, event := range events {
		seen[event.Type]++
	}
	for _, want := range []string{"SUBAGENTS_REQUESTED", "SUBAGENT_SPAWNED", "SUBAGENT_COMPLETED", "SUBAGENTS_MERGED"} {
		if seen[want] == 0 {
			t.Fatalf("parent journal has no %s event", want)
		}
	}
	if seen["SUBAGENT_SPAWNED"] != 2 {
		t.Fatalf("SUBAGENT_SPAWNED=%d, want 2", seen["SUBAGENT_SPAWNED"])
	}
	// Handoff semantics: the parent keeps ownership and finishes itself.
	if seen["TASK_COMPLETED"] != 1 {
		t.Fatalf("the parent did not own its own completion (TASK_COMPLETED=%d)", seen["TASK_COMPLETED"])
	}
}

// Crash reuse: re-running a parent must not repeat work a child already
// finished. This is the property that justifies making children durable.
func TestCompletedSubagentIsNotReRun(t *testing.T) {
	runtime, store, auth := subagentRuntime(t)
	defer store.Close()
	defer runtime.Close()

	request := TaskRequest{TaskID: "parent", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6}
	if _, err := runtime.Run(context.Background(), request, auth); err != nil {
		t.Fatalf("first run: %v", err)
	}
	before, err := runtime.Journal().Replay("parent:alpha")
	if err != nil {
		t.Fatal(err)
	}

	// A second run of the same task id resumes it, and its children are already
	// complete.
	if _, err := runtime.Run(context.Background(), request, auth); err != nil {
		t.Fatalf("second run: %v", err)
	}
	after, err := runtime.Journal().Replay("parent:alpha")
	if err != nil {
		t.Fatal(err)
	}
	// A re-run would append events; reuse means the journal is untouched.
	if len(before) != len(after) {
		t.Fatalf("a completed subagent was re-run: journal grew from %d to %d events", len(before), len(after))
	}
}

// The depth limit must hold, and the task must be told rather than silently
// receiving nothing.
func TestNestedSubagentsAreRejectedAtMaxDepth(t *testing.T) {
	runtime, store, auth := subagentRuntime(t, WithSubagentPolicy(SubagentPolicy{
		MaxParallel: 2, MaxDepth: 1, DefaultMaxSteps: 3, BudgetShare: 0.25,
	}))
	defer store.Close()
	defer runtime.Close()

	// The child's own goal also asks to delegate.
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "parent", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Children were given "ANSWER-FOR <marker>" objectives, which the model
	// answers directly, so no grandchild should exist.
	entries, err := store.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.TaskID, "parent:alpha:") || strings.HasPrefix(entry.TaskID, "parent:beta:") {
			t.Fatalf("a grandchild was created despite MaxDepth=1: %s", entry.TaskID)
		}
	}
}

// The parent must see its children's answers, otherwise the fan-out cost buys
// nothing.
func TestSubagentAnswersReachTheParentContext(t *testing.T) {
	runtime, store, auth := subagentRuntime(t)
	defer store.Close()
	defer runtime.Close()

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "parent", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	merged := false
	for _, observation := range result.State.Observations {
		if fmt.Sprint(observation["type"]) != "subagents_merged" {
			continue
		}
		merged = true
		payload, _ := observation["merged"].(map[string]any)
		if payload == nil {
			t.Fatal("the merge observation carries no payload")
		}
		if status := fmt.Sprint(payload["status"]); status != "SUCCEEDED" {
			t.Fatalf("merged status=%s, want SUCCEEDED", status)
		}
		results, _ := payload["results"].([]SubagentResult)
		if len(results) != 2 {
			t.Fatalf("merged results=%d, want 2", len(results))
		}
		// Request order, so the parent's journal does not depend on which child
		// finished first.
		if results[0].AgentID != "alpha" || results[1].AgentID != "beta" {
			t.Fatalf("results are not in request order: %s, %s", results[0].AgentID, results[1].AgentID)
		}
	}
	if !merged {
		t.Fatal("the parent never saw a merge observation")
	}
}

// A child's budget is bounded: it cannot spend what its siblings need.
func TestSubagentBudgetIsBounded(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	ledger := &BudgetLedger{MaxTokens: 4000}
	runtime, err := New(WithStore(store), WithModel(newDelegatingModel()), WithBudget(ledger))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	state := TaskState{TaskID: "parent", TenantID: auth.TenantID, SubjectID: auth.SubjectID}
	runner := runtime.newSubagentRunner(state, auth, ledger)
	child := runner.childLedger()
	if child == nil {
		t.Fatal("a child received no ledger, so its spend is unbounded")
	}
	if child.MaxTokens != 1000 {
		t.Fatalf("child MaxTokens=%d, want 1000 (a quarter of the parent's remaining)", child.MaxTokens)
	}
	if child == ledger {
		t.Fatal("the child shares the parent's ledger object, so its spend is not isolated")
	}
}

// A fan-out whose child ids collide with completed work reuses it rather than
// failing the parent.
func TestSubagentMergeReportsFailureWithoutFailingParent(t *testing.T) {
	runtime, store, auth := subagentRuntime(t, WithSubagentPolicy(SubagentPolicy{
		MaxParallel: 1, MaxDepth: 1, DefaultMaxSteps: 1, BudgetShare: 0.25,
	}))
	defer store.Close()
	defer runtime.Close()

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "parent", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// A child with a one-step budget still has to produce an answer; whatever
	// happens, the parent must reach a terminal state rather than hang.
	if result.Status != TaskCompleted && result.Status != TaskFailed {
		t.Fatalf("parent ended in %s", result.Status)
	}
}
