package harness

import (
	"context"
	"testing"
	"time"
)

func totRuntime(t *testing.T, model ModelAdapter, options ...Option) (*Runtime, Store, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	base := []Option{
		WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(TreeOfThoughtsStrategy{}),
	}
	base = append(base, options...)
	runtime, err := New(base...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, store, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// exploringModel attempts when asked to try an approach, and decides otherwise.
type exploringModel struct{}

func (exploringModel) Name() string { return "exploring" }

func (exploringModel) Complete(_ context.Context, request ModelRequest) (ModelResponse, error) {
	text := requestText(request)
	if contains(text, "independent attempts at this task are in the observations") {
		return ModelResponse{Text: "the chosen answer", FinishReason: "stop"}, nil
	}
	if contains(text, "solve the task") || contains(text, "Task:") {
		return ModelResponse{Text: "an attempted approach", FinishReason: "stop"}, nil
	}
	return ModelResponse{Text: "unexpected", FinishReason: "stop"}, nil
}

func (exploringModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

// Branches are real durable tasks and the judging step produces the answer.
func TestTreeOfThoughtsExploresThenChooses(t *testing.T) {
	runtime, store, auth := totRuntime(t, exploringModel{})
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "tot", Goal: "achieve the objective", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	for _, id := range []string{"tot:branch-1", "tot:branch-2"} {
		child, _, err := store.GetTask(id)
		if err != nil {
			t.Fatalf("branch %s was not a durable task: %v", id, err)
		}
		if child.Status != TaskCompleted {
			t.Fatalf("branch %s status=%s", id, child.Status)
		}
	}
	if result.FinalAnswer != "the chosen answer" {
		t.Fatalf("final answer=%q, want the judged choice", result.FinalAnswer)
	}
}

// The branch count is bounded, so a topology cannot silently fan out wider than
// configured.
func TestTreeOfThoughtsRespectsBranchCount(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store), WithModel(exploringModel{}),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(TreeOfThoughtsStrategy{Approaches: []string{"a", "b", "c", "d"}, Branches: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "narrow", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	entries, err := store.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	children := 0
	for _, entry := range entries {
		if len(entry.TaskID) > 7 && entry.TaskID[:7] == "narrow:" {
			children++
		}
	}
	if children != 1 {
		t.Fatalf("spawned %d branches, want 1", children)
	}
}

// A refused gather must fall through to answering, the same way Debate does.
// The skeleton is shared, so the behaviour is too — this asserts it rather than
// assuming it.
func TestTreeOfThoughtsAtMaxDepthStillAnswers(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store), WithModel(exploringModel{}),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithSubagentPolicy(SubagentPolicy{MaxParallel: 2, MaxDepth: 0, DefaultMaxSteps: 3, BudgetShare: 0.25}),
		WithStrategyInstance(TreeOfThoughtsStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "refused", Goal: "achieve something", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s: a refused gather did not fall through", result.Status, result.FailureCategory)
	}
}

// The two topologies share a skeleton but not a phase counter: running one must
// not corrupt the other's progress.
func TestFanoutJudgePhasesAreNamespaced(t *testing.T) {
	if totPhaseKey == debatePhaseKey {
		t.Fatal("the two topologies share a phase key, so their counters would collide")
	}
}
