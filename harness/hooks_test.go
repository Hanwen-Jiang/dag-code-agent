package harness

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// The hook bus exists so an embedder does not have to poll the journal to learn
// that a task failed. Two rules shape it, and both are tested here: a hook
// failure never fails the run, and a hook cannot veto anything.

type recordingBus struct {
	mu     sync.Mutex
	events []LifecycleEvent
	fail   bool
}

func (b *recordingBus) Publish(_ context.Context, event LifecycleEvent) error {
	b.mu.Lock()
	b.events = append(b.events, event)
	b.mu.Unlock()
	if b.fail {
		return errors.New("hook delivery failed")
	}
	return nil
}

func (b *recordingBus) kinds() []LifecycleEventKind {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]LifecycleEventKind, 0, len(b.events))
	for _, event := range b.events {
		out = append(out, event.Kind)
	}
	return out
}

func hookRuntime(t *testing.T, bus HookBus) (*Runtime, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	runtime, err := New(WithStore(store),
		WithModel(NewScriptedModel([]ModelResponse{{Text: "done", FinishReason: "stop"}})),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()), WithHookBus(bus))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

func TestHookBusSeesTaskLifecycle(t *testing.T) {
	bus := &recordingBus{}
	runtime, auth := hookRuntime(t, bus)
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "hooked", Goal: "finish", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	kinds := bus.kinds()
	if len(kinds) != 2 {
		t.Fatalf("events=%v, want a start and a finish", kinds)
	}
	if kinds[0] != LifecycleTaskStarted || kinds[1] != LifecycleTaskFinished {
		t.Fatalf("events out of order: %v", kinds)
	}
	// The finish event must carry the outcome, or a bus cannot act on it.
	bus.mu.Lock()
	defer bus.mu.Unlock()
	finished := bus.events[1]
	if finished.Status != string(TaskCompleted) {
		t.Fatalf("finish status=%q, want %q", finished.Status, TaskCompleted)
	}
	if finished.TenantID != auth.TenantID || finished.SubjectID != auth.SubjectID {
		t.Fatalf("the event carries no identity: %+v", finished)
	}
}

// A broken alerting script must not stop the work it was meant to observe.
func TestHookFailureDoesNotFailTheRun(t *testing.T) {
	bus := &recordingBus{fail: true}
	runtime, auth := hookRuntime(t, bus)
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "broken", Goal: "finish anyway", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
	}, auth)
	if err != nil {
		t.Fatalf("a hook failure surfaced as a run error: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s: a hook failure changed the outcome", result.Status, result.FailureCategory)
	}
	// It is not silent either: the event was attempted.
	if len(bus.kinds()) == 0 {
		t.Fatal("the bus was never consulted")
	}
}

// A failure must still be reported, or alerting cannot do its job.
func TestHookBusSeesFailures(t *testing.T) {
	bus := &recordingBus{}
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel(nil)),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()), WithHookBus(bus))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	// No scripted responses, so the run fails at the model.
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "failed", Goal: "fail", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 2,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if len(bus.events) != 2 {
		t.Fatalf("events=%d, want a start and a finish", len(bus.events))
	}
	finished := bus.events[1]
	if finished.Status != string(TaskFailed) {
		t.Fatalf("finish status=%q, want %q so alerting can act", finished.Status, TaskFailed)
	}
	if finished.ErrorCode == "" {
		t.Fatal("a failed run reported no error code")
	}
}
