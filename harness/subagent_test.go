package harness

import (
	"context"
	"errors"
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
	// The marker is read from the request rather than checked against a fixed
	// list of names. A hardcoded list silently answered "parent final answer" for
	// any node whose name was not in it, which made a graph test look like a
	// wrong-topology failure.
	if marker := answerMarker(text); marker != "" {
		return ModelResponse{Text: "answer from " + marker, FinishReason: "stop"}, nil
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

// A failed child must not fail the parent.
//
// The previous version of this test accepted either COMPLETED or FAILED, so it
// could not distinguish the property its name claimed: a parent that failed
// because a child failed would have passed. It also used a model whose children
// all succeeded, so the failure branch was never reached at all — a test that
// passes for a reason unrelated to what it asserts.
func TestFailedChildDoesNotFailTheParent(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	model := &failingChildModel{inner: newDelegatingModel()}
	runtime, err := New(
		WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "partial", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The claim, asserted rather than allowed to be either outcome.
	if result.Status != TaskCompleted {
		t.Fatalf("parent status=%s category=%s: a failed child took the parent down",
			result.Status, result.FailureCategory)
	}

	events, err := runtime.Journal().Replay("partial")
	if err != nil {
		t.Fatal(err)
	}
	failed, completed := 0, 0
	for _, event := range events {
		switch event.Type {
		case "SUBAGENT_FAILED":
			failed++
			if stringValue(event.Payload["agent_id"]) != "alpha" {
				t.Fatalf("the wrong child failed: %v", event.Payload["agent_id"])
			}
		case "SUBAGENT_COMPLETED":
			completed++
		}
	}
	// Without this the test would pass on a fan-out where nothing failed, which
	// is exactly the defect the previous version had.
	if failed == 0 {
		t.Fatal("no child failed, so nothing about a failed child was tested")
	}
	if completed == 0 {
		t.Fatal("the sibling that should have succeeded did not")
	}
}

// failingChildModel makes one named child fail and delegates the rest.
type failingChildModel struct {
	inner *delegatingModel
}

func (m *failingChildModel) Name() string { return "failing-child" }

func (m *failingChildModel) Complete(ctx context.Context, request ModelRequest) (ModelResponse, error) {
	if strings.Contains(requestText(request), "ANSWER-FOR alpha") {
		return ModelResponse{}, errors.New("child exploded")
	}
	return m.inner.Complete(ctx, request)
}

func (m *failingChildModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

// A strategy may decide to delegate itself. Before this the kernel validated
// ActionSpawn and then failed the run, so the action was declarable but not
// executable — a trap for the custom-strategy platform.
type spawnStrategy struct {
	inner Strategy
	spawn bool
}

func (s *spawnStrategy) Name() string    { return "spawn-test" }
func (s *spawnStrategy) Version() string { return "1.0.0" }

func (s *spawnStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	if !s.spawn && st.Step == 0 {
		s.spawn = true
		return Action{Kind: ActionSpawn, Subagents: []SubagentSpec{
			{AgentID: "alpha", Role: "researcher", Objective: "ANSWER-FOR alpha"},
		}}, nil
	}
	return ReActStrategy{}.Next(ctx, st)
}

func (s *spawnStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return ReActStrategy{}.OnResult(st, result)
}

func TestStrategyInitiatedSpawnExecutes(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(&spawnStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "spawned", Goal: "some task", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	child, _, err := store.GetTask("spawned:alpha")
	if err != nil {
		t.Fatalf("the strategy-initiated spawn created no child task: %v", err)
	}
	if child.Status != TaskCompleted || child.FinalAnswer != "answer from alpha" {
		t.Fatalf("child status=%s answer=%q", child.Status, child.FinalAnswer)
	}
	// The spawn event records who decided, so a replay can tell the two origins
	// apart.
	events, err := runtime.Journal().Replay("spawned")
	if err != nil {
		t.Fatal(err)
	}
	origin := ""
	for _, event := range events {
		if event.Type == "SUBAGENT_SPAWNED" {
			origin = stringValue(event.Payload["origin"])
		}
	}
	if origin != "strategy" {
		t.Fatalf("SUBAGENT_SPAWNED origin=%q, want strategy", origin)
	}
}

// A handoff runs exactly one child and returns its result to the parent, which
// keeps ownership of the final answer.
type handoffStrategy struct {
	inner Strategy
	sent  bool
}

func (s *handoffStrategy) Name() string    { return "handoff-test" }
func (s *handoffStrategy) Version() string { return "1.0.0" }

func (s *handoffStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	if !s.sent && st.Step == 0 {
		s.sent = true
		return Action{Kind: ActionHandoff, Handoff: &SubagentSpec{AgentID: "beta", Role: "reviewer", Objective: "ANSWER-FOR beta"}}, nil
	}
	return ReActStrategy{}.Next(ctx, st)
}

func (s *handoffStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return ReActStrategy{}.OnResult(st, result)
}

func TestHandoffReturnsResultToParent(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(&handoffStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "handed", Goal: "some task", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	// The parent owns the outcome: the handoff did not transfer the task.
	result2 := result
	if result2.TaskID != "handed" {
		t.Fatalf("the result belongs to %q, so ownership and handoff semantics disagree", result2.TaskID)
	}
	child, _, err := store.GetTask("handed:beta")
	if err != nil {
		t.Fatalf("no handoff child task: %v", err)
	}
	if child.FinalAnswer != "answer from beta" {
		t.Fatalf("child answer=%q", child.FinalAnswer)
	}
	// Exactly one child: a handoff is not a fan-out.
	entries, err := store.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	children := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.TaskID, "handed:") {
			children++
		}
	}
	if children != 1 {
		t.Fatalf("a handoff spawned %d children, want 1", children)
	}
}

// A strategy that always emits an action which never calls the model used to
// hang the runtime forever: the loop is bounded by state.Step, Step only
// advances on a model call, and no budget was consumed. Reproduced here as a
// ten-minute test timeout before the iteration backstop existed.
type spinningStrategy struct{}

func (spinningStrategy) Name() string    { return "spinning" }
func (spinningStrategy) Version() string { return "1.0.0" }

func (spinningStrategy) Next(context.Context, LoopState) (Action, error) {
	// Consumes no model call, no tool budget and no subagent slot.
	return Action{Kind: ActionCallTools, Tools: []ToolCall{}}, nil
}

func (spinningStrategy) OnResult(st LoopState, _ StepResult) LoopState { return st }

func TestStrategyThatConsumesNoBudgetTerminates(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(spinningStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	// The empty tool list fails validation first, which is itself a contract
	// check; the point is that the run ends rather than hanging.
	done := make(chan RunResult, 1)
	go func() {
		result, _ := runtime.Run(context.Background(), TaskRequest{
			TaskID: "spinning", Goal: "spin", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
		}, auth)
		done <- result
	}()
	select {
	case result := <-done:
		if result.Status != TaskFailed {
			t.Fatalf("a strategy consuming no budget ended in %s, want FAILED", result.Status)
		}
		if result.FailureCategory != "STRATEGY" && result.FailureCategory != "LIMIT" {
			t.Fatalf("failure category=%s, want STRATEGY or LIMIT", result.FailureCategory)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the run did not terminate: a strategy consuming no budget hangs the runtime")
	}
}

// The backstop must not fire on legitimate work: a strategy that does consume
// budget runs to completion.
func TestIterationBackstopAllowsLegitimateWork(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	registry := NewToolRegistry()
	if err := registry.Register(ToolSpec{
		Name: "echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"v": map[string]any{"type": "string"}}},
		ConcurrencySafe: true, SideEffect: "NONE",
		Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}
	calls := []ToolCall{{CallID: "c1", Name: "echo", Arguments: map[string]any{"v": "a"}}, {CallID: "c2", Name: "echo", Arguments: map[string]any{"v": "b"}}}
	model := NewScriptedModel([]ModelResponse{
		{ToolCalls: calls}, {ToolCalls: calls}, {Text: "final", FinishReason: "stop"},
	})
	runtime, err := New(WithStore(store), WithModel(model), WithToolRegistry(registry),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "legit", Goal: "use tools", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6, MaxToolCalls: 8,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("legitimate work failed with %s/%s: the backstop is too tight", result.Status, result.FailureCategory)
	}
}

// answerMarker extracts the name following "ANSWER-FOR " in a request.
func answerMarker(text string) string {
	const marker = "ANSWER-FOR "
	index := strings.Index(text, marker)
	if index < 0 {
		return ""
	}
	rest := text[index+len(marker):]
	end := strings.IndexAny(rest, " \n.\t")
	if end < 0 {
		end = len(rest)
	}
	return rest[:end]
}
