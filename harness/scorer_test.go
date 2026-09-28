package harness

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Best-of-N sampling: several attempts at one objective, ranked, best kept. The
// scorer is the piece that was missing, and there is no default on purpose.

// lengthScorer rates by summary length, which is a bad scorer — and that is the
// point: the tests must not depend on judgement the runtime does not have.
func lengthScorer() Scorer {
	return ScorerFunc(func(_ context.Context, _ string, result SubagentResult) (float64, error) {
		return float64(len(result.Summary)), nil
	})
}

func fanoutRuntime(t *testing.T, options ...Option) (*Runtime, Store, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	base := []Option{
		WithStore(store), WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithScorer("length", lengthScorer()),
		WithStrategyInstance(&fanoutExecStrategy{}),
	}
	base = append(base, options...)
	runtime, err := New(base...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, store, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// fanoutExecStrategy emits one fan-out then delegates.
type fanoutExecStrategy struct {
	sent bool
	plan *FanoutSpec
}

func (s *fanoutExecStrategy) Name() string    { return "fanout-exec" }
func (s *fanoutExecStrategy) Version() string { return "1.0.0" }

func (s *fanoutExecStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	if !s.sent {
		s.sent = true
		plan := s.plan
		if plan == nil {
			plan = &FanoutSpec{Branches: 2, Scorer: "length", Prune: 1}
		}
		return Action{Kind: ActionFanout, Fanout: plan}, nil
	}
	return ReActStrategy{}.Next(ctx, st)
}

func (s *fanoutExecStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return ReActStrategy{}.OnResult(st, result)
}

// The whole point: branches are sampled, scored, and pruned to the best.
func TestFanoutScoresAndPrunesBranches(t *testing.T) {
	runtime, store, auth := fanoutRuntime(t)
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "best", Goal: "answer something", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	// Every branch ran as a durable task: sampling does not bypass durability.
	for _, id := range []string{"best:branch-1", "best:branch-2"} {
		if _, _, err := store.GetTask(id); err != nil {
			t.Fatalf("branch %s was not a durable task: %v", id, err)
		}
	}
	events, err := runtime.Journal().Replay("best")
	if err != nil {
		t.Fatal(err)
	}
	started, scored := false, false
	for _, event := range events {
		switch event.Type {
		case "FANOUT_STARTED":
			started = true
		case "FANOUT_SCORED":
			scored = true
			// The concrete Go type of a journal payload depends on the store:
			// the in-memory backend keeps the slice as written, a JSON
			// round-trip would give []any. Asserting one representation makes
			// the test a statement about the backend rather than the behaviour.
			if count := payloadCount(event.Payload["ratings"]); count != 2 {
				t.Fatalf("scored %d branches, want 2", count)
			}
		}
	}
	if !started || !scored {
		t.Fatalf("started=%v scored=%v: the fan-out did not complete both phases", started, scored)
	}
}

// No scorer is registered by default, and using one must fail with a message
// that says which names exist.
func TestFanoutRequiresAScorer(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(WithStore(store), WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(&fanoutExecStrategy{plan: &FanoutSpec{Branches: 2, Scorer: "typo"}}))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "noscorer", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskFailed || result.FailureCategory != "STRATEGY" {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	message := stringValue(result.State.LastError["message"])
	if !contains(message, "FANOUT_SCORER_UNKNOWN") {
		t.Fatalf("the failure does not identify the cause: %q", message)
	}
}

// Fields that cannot be honoured are refused, not ignored. Accepting Temperature
// would make it decorative: ModelRequest has no field to carry it.
func TestFanoutRefusesFieldsItCannotHonour(t *testing.T) {
	cases := map[string]*FanoutSpec{
		"temperature": {Branches: 2, Scorer: "length", Temperature: 0.7},
		"rounds":      {Branches: 2, Scorer: "length", Rounds: 3},
	}
	for name, plan := range cases {
		t.Run(name, func(t *testing.T) {
			runtime, _, auth := fanoutRuntime(t, WithStrategyInstance(&fanoutExecStrategy{plan: plan}))
			result, err := runtime.Run(context.Background(), TaskRequest{
				TaskID: "refused", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
			}, auth)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if result.Status != TaskFailed || result.FailureCategory != "STRATEGY" {
				t.Fatalf("status=%s category=%s, want FAILED/STRATEGY", result.Status, result.FailureCategory)
			}
			message := stringValue(result.State.LastError["message"])
			if !contains(message, "UNSUPPORTED") {
				t.Fatalf("the refusal does not read as unsupported: %q", message)
			}
		})
	}
}

// Pruning orders best-first and keeps the requested count.
func TestPruneKeepsTheBestAndIsStable(t *testing.T) {
	scored := []scoredBranch{
		{result: SubagentResult{AgentID: "a"}, score: 1},
		{result: SubagentResult{AgentID: "b"}, score: 9},
		{result: SubagentResult{AgentID: "c"}, score: 5},
	}
	kept := pruneBranches(scored, 2)
	if len(kept) != 2 || kept[0].AgentID != "b" || kept[1].AgentID != "c" {
		t.Fatalf("pruned to %+v, want b then c", kept)
	}
	// Zero keeps everything: sampling without selecting is a valid use.
	if all := pruneBranches(scored, 0); len(all) != 3 {
		t.Fatalf("keep=0 kept %d branches, want all 3", len(all))
	}
	// Ties preserve request order, so the outcome does not depend on which
	// branch was scored first.
	tied := []scoredBranch{
		{result: SubagentResult{AgentID: "first"}, score: 4},
		{result: SubagentResult{AgentID: "second"}, score: 4},
	}
	if ordered := pruneBranches(tied, 1); ordered[0].AgentID != "first" {
		t.Fatalf("a tie reordered the branches: %+v", ordered)
	}
}

// An unscorable branch must not be silently dropped: a scorer failure is not
// evidence that the answer was bad.
func TestUnscorableBranchesAreKeptAndSortedLast(t *testing.T) {
	scored := []scoredBranch{
		{result: SubagentResult{AgentID: "broken"}, err: errors.New("scorer unavailable")},
		{result: SubagentResult{AgentID: "fine"}, score: 1},
	}
	kept := pruneBranches(scored, 0)
	if len(kept) != 2 {
		t.Fatalf("kept %d branches, want both: a scorer error must not discard work", len(kept))
	}
	if kept[0].AgentID != "fine" {
		t.Fatalf("an unscorable branch outranked a scored one: %+v", kept)
	}
}

// Personas become the branch objectives, so a fan-out can sample genuinely
// different approaches rather than the same one several times.
func TestFanoutPersonasBecomeBranchObjectives(t *testing.T) {
	runtime, store, auth := fanoutRuntime(t, WithStrategyInstance(&fanoutExecStrategy{
		plan: &FanoutSpec{Branches: 2, Scorer: "length", Personas: []string{"be terse", "be exhaustive"}},
	}))
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "personas", Goal: "explain the design", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	child, _, err := store.GetTask("personas:branch-1")
	if err != nil {
		t.Fatal(err)
	}
	if !contains(child.Goal, "be terse") {
		t.Fatalf("the persona did not reach the branch objective: %q", child.Goal)
	}
}

// payloadCount counts a journal payload slice without assuming the backend's
// representation of it.
func payloadCount(value any) int {
	switch typed := value.(type) {
	case []any:
		return len(typed)
	case []map[string]any:
		return len(typed)
	default:
		return -1
	}
}
