package harness

import (
	"context"
	"testing"
	"time"
)

// The watchdog exists for the case cancellation does not cover: nothing is
// watching, and a stalled task holds the runtime lock forever.

func watchdogRuntime(t *testing.T, model ModelAdapter, policy WatchdogPolicy) (*Runtime, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	runtime, err := New(WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()), WithWatchdog(policy))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

func runUntilSettled(t *testing.T, runtime *Runtime, auth AuthContext, taskID string) RunResult {
	t.Helper()
	handle := runtime.Start(context.Background(), TaskRequest{
		TaskID: taskID, Goal: "block forever", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	return result
}

// A task that runs too long is stopped, and the journal says the watchdog did it.
func TestWatchdogStopsAnOverlongTask(t *testing.T) {
	model := newBlockingModel()
	runtime, auth := watchdogRuntime(t, model, WatchdogPolicy{
		MaxDuration: 60 * time.Millisecond, CheckInterval: 10 * time.Millisecond,
	})
	result := runUntilSettled(t, runtime, auth, "overlong")

	if result.Status != TaskCancelled {
		t.Fatalf("status=%s, want CANCELLED", result.Status)
	}
	message := stringValue(result.State.LastError["message"])
	if !contains(message, "watchdog") || !contains(message, "time budget") {
		t.Fatalf("the journal does not attribute the stop to the watchdog: %q", message)
	}
}

// A task that stops producing events is stopped even though it is well inside
// any overall budget.
func TestWatchdogStopsAStalledTask(t *testing.T) {
	model := newBlockingModel()
	runtime, auth := watchdogRuntime(t, model, WatchdogPolicy{
		StallTimeout: 80 * time.Millisecond, CheckInterval: 10 * time.Millisecond,
	})
	result := runUntilSettled(t, runtime, auth, "stalled")

	if result.Status != TaskCancelled {
		t.Fatalf("status=%s, want CANCELLED", result.Status)
	}
	message := stringValue(result.State.LastError["message"])
	if !contains(message, "no progress") {
		t.Fatalf("the journal does not report a stall: %q", message)
	}
}

// With no policy the runtime must not interfere: a slow-but-healthy task has to
// be able to run to completion.
func TestWatchdogIsOffByDefault(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	slow := &delayedModel{delay: 120 * time.Millisecond, text: "finished"}
	runtime, err := New(WithStore(store), WithModel(slow),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "slow", Goal: "take your time", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s: a run without a watchdog was interrupted", result.Status, result.FailureCategory)
	}
}

// A caller's cancellation must remain distinguishable from a watchdog stop: they
// are different operational events.
func TestCallerCancellationIsDistinctFromWatchdog(t *testing.T) {
	model := newBlockingModel()
	runtime, auth := watchdogRuntime(t, model, WatchdogPolicy{
		MaxDuration: 10 * time.Second, CheckInterval: 50 * time.Millisecond,
	})
	handle := runtime.Start(context.Background(), TaskRequest{
		TaskID: "bycaller", Goal: "block", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	select {
	case <-model.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the run never started")
	}
	if _, err := handle.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	message := stringValue(result.State.LastError["message"])
	if contains(message, "watchdog") {
		t.Fatalf("a caller's cancellation was attributed to the watchdog: %q", message)
	}
	if !contains(message, "caller") {
		t.Fatalf("the journal does not name the cancellation cause: %q", message)
	}
}

// delayedModel answers after a delay, standing in for a legitimately slow call.
type delayedModel struct {
	delay time.Duration
	text  string
}

func (m *delayedModel) Name() string { return "delayed" }

func (m *delayedModel) Complete(ctx context.Context, _ ModelRequest) (ModelResponse, error) {
	select {
	case <-time.After(m.delay):
		return ModelResponse{Text: m.text, FinishReason: "stop"}, nil
	case <-ctx.Done():
		return ModelResponse{}, ctx.Err()
	}
}

func (m *delayedModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}
