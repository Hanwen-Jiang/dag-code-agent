package harness

import (
	"context"
	"testing"
	"time"
)

// Background work must outlive the caller's context, and must not outlive the
// runtime.

func backgroundRuntime(t *testing.T, model ModelAdapter) (*Runtime, Store, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	runtime, err := New(WithStore(store), WithModel(model),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))), WithIDSource(sequentialIDs()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runtime, store, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// The caller's context must not govern a background task: that is the whole
// difference from Start.
func TestBackgroundTaskSurvivesItsCaller(t *testing.T) {
	model := &delayedModel{delay: 150 * time.Millisecond, text: "finished"}
	runtime, store, auth := backgroundRuntime(t, model)
	defer store.Close()
	defer runtime.Close()

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	handle, err := runtime.StartBackground(TaskRequest{
		TaskID: "detached", Goal: "keep going", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 3,
	}, auth)
	if err != nil {
		t.Fatalf("StartBackground: %v", err)
	}
	// The caller walks away immediately.
	cancelCaller()

	result, err := handle.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Status != TaskCompleted {
		// The caller's context was never passed in, so cancelling it cannot have
		// reached the run.
		t.Fatalf("status=%s category=%s: the caller's cancellation killed background work",
			result.Status, result.FailureCategory)
	}
	_ = callerCtx
}

// A background task must be enumerable, or a host has to track it in parallel
// bookkeeping that can drift from reality.
func TestBackgroundTasksAreEnumerable(t *testing.T) {
	model := newBlockingModel()
	runtime, store, auth := backgroundRuntime(t, model)
	defer store.Close()
	defer runtime.Close()

	handle, err := runtime.StartBackground(TaskRequest{
		TaskID: "tracked", Goal: "block", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if err != nil {
		t.Fatalf("StartBackground: %v", err)
	}
	select {
	case <-model.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the background task never started")
	}
	running := runtime.BackgroundTasks()
	if len(running) != 1 || running[0] != "tracked" {
		t.Fatalf("BackgroundTasks=%v, want [tracked]", running)
	}
	// A settled task removes itself: the registry is a view of what is running.
	if _, err := handle.Cancel(context.Background()); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, err := handle.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(runtime.BackgroundTasks()) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if remaining := runtime.BackgroundTasks(); len(remaining) != 0 {
		t.Fatalf("a settled background task is still tracked: %v", remaining)
	}
}

// Close must stop background work. A task detached from everything would survive
// the runtime and leak.
func TestCloseStopsBackgroundTasks(t *testing.T) {
	model := newBlockingModel()
	runtime, store, auth := backgroundRuntime(t, model)
	defer store.Close()

	handle, err := runtime.StartBackground(TaskRequest{
		TaskID: "stopped", Goal: "block", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if err != nil {
		t.Fatalf("StartBackground: %v", err)
	}
	select {
	case <-model.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the background task never started")
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// A cancelled run reports the context error alongside its CANCELLED result;
	// that is the existing Run contract, and the result is what matters here.
	result, _ := handle.Wait(context.Background())
	if result.Status != TaskCancelled {
		t.Fatalf("status=%s, want CANCELLED after Close", result.Status)
	}
	message := stringValue(result.State.LastError["message"])
	if !contains(message, "closed") {
		t.Fatalf("the journal does not say the runtime closed: %q", message)
	}
}

// A task left non-terminal by a dead process must be findable, or the work is
// abandoned while looking durable.
func TestResumableTasksFindsAbandonedWork(t *testing.T) {
	runtime, store, auth := backgroundRuntime(t, NewScriptedModel([]ModelResponse{{Text: "x", FinishReason: "stop"}}))
	defer store.Close()
	defer runtime.Close()

	terminal := TaskState{TaskID: "done", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, Status: TaskCompleted}
	terminal.Normalize()
	if _, err := runtime.Journal().Create(terminal, "TASK_ACCEPTED", map[string]any{"goal": "g"}); err != nil {
		t.Fatal(err)
	}
	abandoned := TaskState{TaskID: "abandoned", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, Status: TaskRunning}
	abandoned.Normalize()
	if _, err := runtime.Journal().Create(abandoned, "TASK_ACCEPTED", map[string]any{"goal": "g"}); err != nil {
		t.Fatal(err)
	}
	waiting := TaskState{TaskID: "waiting", Goal: "g", TenantID: auth.TenantID, SubjectID: auth.SubjectID, Status: TaskWaitingUser}
	waiting.Normalize()
	if _, err := runtime.Journal().Create(waiting, "TASK_ACCEPTED", map[string]any{"goal": "g"}); err != nil {
		t.Fatal(err)
	}

	ids, err := runtime.ResumableTasks(auth.TenantID)
	if err != nil {
		t.Fatalf("ResumableTasks: %v", err)
	}
	found := map[string]bool{}
	for _, id := range ids {
		found[id] = true
	}
	if found["done"] {
		t.Fatal("a terminal task was offered for resume")
	}
	if !found["abandoned"] || !found["waiting"] {
		t.Fatalf("non-terminal tasks were not offered for resume: %v", ids)
	}
	// Tenant scoping: another tenant's work is not this caller's to resume.
	other, err := runtime.ResumableTasks("someone-else")
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("another tenant's tasks leaked: %v", other)
	}
}
