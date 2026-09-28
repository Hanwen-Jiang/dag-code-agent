package harness

import (
	"context"
	"testing"
	"time"
)

// Debate spends a fan-out before answering. The tests pin the two things that
// make it a topology rather than a prompt: the debaters are real tasks, and the
// judging step actually happens and is the answer the task returns.

func debateRuntime(t *testing.T, model ModelAdapter, options ...Option) (*Runtime, Store, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	base := []Option{
		WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(DebateStrategy{}),
	}
	base = append(base, options...)
	runtime, err := New(base...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, store, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// debatingModel argues a position when asked to advocate, and judges otherwise.
type debatingModel struct{}

func (debatingModel) Name() string { return "debating" }

func (debatingModel) Complete(_ context.Context, request ModelRequest) (ModelResponse, error) {
	text := requestText(request)
	if contains(text, "You are the judge") {
		return ModelResponse{Text: "the judged answer", FinishReason: "stop"}, nil
	}
	if contains(text, "advocate") || contains(text, "argue for") {
		return ModelResponse{Text: "an argued position", FinishReason: "stop"}, nil
	}
	return ModelResponse{Text: "unexpected", FinishReason: "stop"}, nil
}

func (debatingModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

func TestDebateArguesThenJudges(t *testing.T) {
	runtime, store, auth := debateRuntime(t, debatingModel{})
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "debate", Goal: "decide the interpretation", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	// The debaters must be real tasks, not in-memory calls: that is what makes
	// the debate auditable after the fact.
	for _, id := range []string{"debate:debater-1", "debate:debater-2"} {
		child, _, err := store.GetTask(id)
		if err != nil {
			t.Fatalf("debater %s was not created as a durable task: %v", id, err)
		}
		if child.Status != TaskCompleted {
			t.Fatalf("debater %s status=%s", id, child.Status)
		}
	}
	// The judging step produced the answer the task returns, so the debate is
	// not decoration around the model's first guess.
	if result.FinalAnswer != "the judged answer" {
		t.Fatalf("final answer=%q, want the judge's", result.FinalAnswer)
	}
}

// The three phases must advance. They are read from what happened rather than
// from the action that asked for it, because a fan-out reports Action=CallModel
// like any other model step — branching on the action would stall the strategy
// in phase one forever.
func TestDebateReachesTheJudgingPhase(t *testing.T) {
	runtime, _, auth := debateRuntime(t, debatingModel{})
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "phases", Goal: "decide something", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("the strategy did not finish: status=%s category=%s", result.Status, result.FailureCategory)
	}
	events, err := runtime.Journal().Replay("phases")
	if err != nil {
		t.Fatal(err)
	}
	debated, judged := false, false
	for _, event := range events {
		if event.Type == "SUBAGENTS_MERGED" {
			debated = true
		}
	}
	// A judged answer proves the phase advanced past the debate.
	if result.FinalAnswer == "the judged answer" {
		judged = true
	}
	if !debated || !judged {
		t.Fatalf("debated=%v judged=%v: the strategy did not complete both phases", debated, judged)
	}
}

// The number of debaters is bounded by the configured count, so a strategy
// cannot silently spawn an unbounded fan-out.
func TestDebateRespectsTheDebaterCount(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store), WithModel(debatingModel{}),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(DebateStrategy{Stances: []string{"one", "two", "three"}, Debaters: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "bounded", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	entries, err := store.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	children := 0
	for _, entry := range entries {
		if len(entry.TaskID) > 7 && entry.TaskID[:7] == "bounded" {
			children++
		}
	}
	if children != 2 {
		t.Fatalf("spawned %d debaters, want 2", children)
	}
}
