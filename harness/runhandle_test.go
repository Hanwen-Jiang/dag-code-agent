package harness

import (
	"context"
	"testing"
	"time"
)

// A handle exists so a host does not have to block a request thread on a task
// that may run for minutes, and does not have to invent its own goroutine and
// bookkeeping to avoid that.

func handleRuntime(t *testing.T, model ModelAdapter) (*Runtime, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	runtime, err := New(WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// Start must not block on the run: that is the whole point of a handle.
func TestStartReturnsWhileTheRunIsInFlight(t *testing.T) {
	model := newBlockingModel()
	runtime, auth := handleRuntime(t, model)

	handle := runtime.Start(context.Background(), TaskRequest{
		TaskID: "held", Goal: "block", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if handle == nil || handle.TaskID != "held" {
		t.Fatalf("Start returned %#v", handle)
	}

	select {
	case <-model.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the run never started")
	}
	if handle.Settled() {
		t.Fatal("the handle reports a settled task while the run is still in flight")
	}
	if !handle.Running() {
		t.Fatal("the handle does not see the live run")
	}
	// Status reads the durable state, so it is observable mid-run.
	state, err := handle.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if state.TaskID != "held" {
		t.Fatalf("status reports task %q", state.TaskID)
	}

	// Cancelling through the handle must reach the live run.
	if _, err := handle.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Status != TaskCancelled {
		t.Fatalf("status=%s, want CANCELLED", result.Status)
	}
	if !handle.Settled() {
		t.Fatal("the handle did not settle after cancellation")
	}
}

// Waiting on a handle must not cancel the task: giving up on waiting and
// deciding to stop the work are different decisions.
func TestWaitTimeoutDoesNotCancelTheTask(t *testing.T) {
	model := newBlockingModel()
	runtime, auth := handleRuntime(t, model)

	handle := runtime.Start(context.Background(), TaskRequest{
		TaskID: "slow", Goal: "block", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	select {
	case <-model.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the run never started")
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWait()
	if _, err := handle.Wait(waitCtx); err == nil {
		t.Fatal("Wait returned without an error despite an expired context")
	}
	// The task is still running: a caller's impatience is not a cancellation.
	if !handle.Running() {
		t.Fatal("the wait timeout cancelled the task")
	}
	// Clean up so the test does not leak a goroutine.
	if _, err := handle.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
}

// A finished handle reports its result and stops claiming to be running, so the
// cancellation registry does not leak.
func TestHandleSettlesOnCompletion(t *testing.T) {
	runtime, auth := handleRuntime(t, NewScriptedModel([]ModelResponse{{Text: "done", FinishReason: "stop"}}))
	handle := runtime.Start(context.Background(), TaskRequest{
		TaskID: "quick", Goal: "finish", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
	}, auth)

	select {
	case <-handle.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the handle never settled")
	}
	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s, want COMPLETED", result.Status)
	}
	if handle.Running() {
		t.Fatal("a settled handle still reports a live run, so the registry leaked")
	}
}

// Two tasks started concurrently must not interfere: each handle owns its own
// task and its own cancellation.
func TestHandlesAreIndependent(t *testing.T) {
	runtime, auth := handleRuntime(t, NewScriptedModel([]ModelResponse{
		{Text: "one", FinishReason: "stop"},
		{Text: "two", FinishReason: "stop"},
	}))
	first := runtime.Start(context.Background(), TaskRequest{TaskID: "a", Goal: "first", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3}, auth)
	second := runtime.Start(context.Background(), TaskRequest{TaskID: "b", Goal: "second", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3}, auth)

	firstResult, err := first.Wait(context.Background())
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	secondResult, err := second.Wait(context.Background())
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if firstResult.TaskID != "a" || secondResult.TaskID != "b" {
		t.Fatalf("handles crossed: %q and %q", firstResult.TaskID, secondResult.TaskID)
	}
	if firstResult.Status != TaskCompleted || secondResult.Status != TaskCompleted {
		t.Fatalf("statuses: %s, %s", firstResult.Status, secondResult.Status)
	}
}
