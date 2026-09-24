package harness

import (
	"context"
	"testing"
	"time"
)

// testEpoch is a fixed instant so clock-driven assertions stay reproducible.
var testEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestMultipleInstancesInOneProcess is the lifecycle contract from the
// embeddability design (§8): a host process may run several independent
// runtimes at once, and they must not share state.
//
// This is what makes "embed the control plane" different from "run our server":
// the embedder decides how many instances exist, which store each one owns, and
// when each one is torn down.
func TestMultipleInstancesInOneProcess(t *testing.T) {
	storeA := NewInMemoryStore()
	storeB := NewInMemoryStore()

	clockA := NewManualClock(testEpoch)
	runtimeA, err := New(WithStore(storeA), WithClock(clockA), WithModel(NewConversationModel()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storeA.Close() }()
	runtimeB, err := New(WithStore(storeB), WithModel(NewConversationModel()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storeB.Close() }()

	authA := AuthContext{TenantID: "tenant-a", SubjectID: "alice", Scopes: []string{"memory:read"}}
	authB := AuthContext{TenantID: "tenant-b", SubjectID: "bob", Scopes: []string{"memory:read"}}

	resultA, err := runtimeA.Run(context.Background(), TaskRequest{TaskID: "shared-id", Goal: "task for A", TenantID: authA.TenantID, SubjectID: authA.SubjectID}, authA)
	if err != nil {
		t.Fatalf("runtime A run: %v", err)
	}
	resultB, err := runtimeB.Run(context.Background(), TaskRequest{TaskID: "shared-id", Goal: "task for B", TenantID: authB.TenantID, SubjectID: authB.SubjectID}, authB)
	if err != nil {
		t.Fatalf("runtime B run: %v", err)
	}

	// The same task id exists in both instances without colliding, which proves
	// the runtimes do not share a store or any package-level state.
	if resultA.TaskID != resultB.TaskID {
		t.Fatalf("task ids differ: %q vs %q", resultA.TaskID, resultB.TaskID)
	}
	tasksA, err := storeA.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	tasksB, err := storeB.ListTasks()
	if err != nil {
		t.Fatal(err)
	}
	if len(tasksA) != 1 || len(tasksB) != 1 {
		t.Fatalf("instances leaked state: A=%d B=%d tasks", len(tasksA), len(tasksB))
	}
	if tasksA[0].TenantID != "tenant-a" || tasksB[0].TenantID != "tenant-b" {
		t.Fatalf("instances crossed tenants: A=%q B=%q", tasksA[0].TenantID, tasksB[0].TenantID)
	}

	// Each journal verifies only its own chain.
	if _, err := runtimeA.Journal().ReplayState("shared-id"); err != nil {
		t.Fatalf("instance A replay: %v", err)
	}
	if _, err := runtimeB.Journal().ReplayState("shared-id"); err != nil {
		t.Fatalf("instance B replay: %v", err)
	}

	// A runtime that created its own store closes it; an injected store is left
	// to its owner (design §8.1).
	owned, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if !owned.ownsStore {
		t.Fatal("runtime did not take ownership of its default store")
	}
	if runtimeA.ownsStore {
		t.Fatal("runtime claimed ownership of an injected store")
	}
	if err := owned.Close(); err != nil {
		t.Fatalf("owned runtime Close: %v", err)
	}
	// Closing an injected-store runtime must leave the store usable.
	if err := runtimeA.Close(); err != nil {
		t.Fatalf("injected runtime Close: %v", err)
	}
	if _, err := storeA.ListTasks(); err != nil {
		t.Fatalf("injected store was closed by the runtime: %v", err)
	}
}
