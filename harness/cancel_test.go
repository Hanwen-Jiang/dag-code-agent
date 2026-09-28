package harness

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Cancelling a live run.
//
// Runtime.run holds r.mu for the whole duration of a run, and Cancel used to
// take that same lock: it could therefore only "cancel" a task that had already
// stopped, which is the one case where cancellation is pointless. The registry
// below is guarded by its own lock so signalling reaches the run directly.

// blockingModel waits until its context is cancelled, standing in for any slow
// model call.
type blockingModel struct {
	started   chan struct{}
	once      sync.Once
	completed int
}

func newBlockingModel() *blockingModel {
	return &blockingModel{started: make(chan struct{})}
}

func (m *blockingModel) Name() string { return "blocking" }

func (m *blockingModel) Complete(ctx context.Context, _ ModelRequest) (ModelResponse, error) {
	m.once.Do(func() { close(m.started) })
	<-ctx.Done()
	return ModelResponse{}, ctx.Err()
}

func (m *blockingModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

func TestCancelInterruptsALiveRun(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	model := newBlockingModel()
	runtime, err := New(WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	runDone := make(chan RunResult, 1)
	go func() {
		result, _ := runtime.Run(context.Background(), TaskRequest{
			TaskID: "live", Goal: "block forever", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
		}, auth)
		runDone <- result
	}()

	// Wait until the run is genuinely inside the model call, so the assertion is
	// about interrupting a live run rather than one that has not started.
	select {
	case <-model.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the run never reached the model call")
	}

	cancelled := make(chan RunResult, 1)
	go func() {
		result, _ := runtime.Cancel("live", auth)
		cancelled <- result
	}()
	select {
	case <-cancelled:
		// The point of the fix: this returns while the run is still unwinding,
		// instead of blocking until the run finishes on its own.
	case <-time.After(10 * time.Second):
		t.Fatal("Cancel blocked on a live run: it queued behind the lock it was meant to interrupt")
	}

	select {
	case result := <-runDone:
		if result.Status != TaskCancelled {
			t.Fatalf("the interrupted run ended in %s, want CANCELLED", result.Status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run did not observe the cancellation")
	}
}

// Cancelling an idle task still records the cancellation durably: the signal
// path must not be the only path.
func TestCancelStillWorksForAnIdleTask(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel([]ModelResponse{{Text: "unused"}})),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	// Park a task in a gate so it is durably non-terminal without a live run.
	registry := NewToolRegistry()
	_ = registry
	state := TaskState{TaskID: "idle", Goal: "waiting", TenantID: auth.TenantID, SubjectID: auth.SubjectID, Status: TaskWaitingUser}
	state.Normalize()
	if _, err := runtime.Journal().Create(state, "TASK_ACCEPTED", map[string]any{"goal": state.Goal}); err != nil {
		t.Fatal(err)
	}

	result, err := runtime.Cancel("idle", auth)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if result.Status != TaskCancelled {
		t.Fatalf("status=%s, want CANCELLED", result.Status)
	}
	events, err := runtime.Journal().Replay("idle")
	if err != nil {
		t.Fatal(err)
	}
	recorded := false
	for _, event := range events {
		if event.Type == "TASK_CANCELLED" {
			recorded = true
		}
	}
	if !recorded {
		t.Fatal("cancelling an idle task recorded nothing")
	}
}

// A finished run must unregister itself, or the registry grows without bound and
// a later Cancel would signal a context nobody is watching.
func TestCancellationRegistryIsCleanedUp(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel([]ModelResponse{{Text: "done", FinishReason: "stop"}})),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "finished", Goal: "finish", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cancel := runtime.runCancelFunc("finished"); cancel != nil {
		t.Fatal("a finished run left its cancel function registered")
	}
}
