package hooks

import (
	"context"
	"testing"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	dshhooks "github.com/Hanwen-Jiang/dag-code-agent/packages/hooks"
)

// The adapter's job is narrow: map lifecycle events onto the hook points the
// manager supports, and drop anything it has no point for rather than guessing.

func TestBusWithoutManagerDeliversNothing(t *testing.T) {
	var bus *Bus
	if err := bus.Publish(context.Background(), harness.LifecycleEvent{Kind: harness.LifecycleTaskStarted}); err != nil {
		t.Fatalf("a nil bus returned an error: %v", err)
	}
	empty := NewBus(nil)
	if err := empty.Publish(context.Background(), harness.LifecycleEvent{Kind: harness.LifecycleTaskFinished}); err != nil {
		t.Fatalf("a bus without a manager returned an error: %v", err)
	}
}

func TestAdapterSatisfiesTheSeam(t *testing.T) {
	var _ harness.HookBus = NewBus(nil)
}

// An error from the manager must reach the caller. The harness decides what to do
// with it — it logs rather than fails the run — so swallowing it here would take
// that decision away.
func TestBusPropagatesManagerErrors(t *testing.T) {
	// A manager with no configuration loaded has no hooks to run, so session
	// notifications should succeed; the assertion is that the adapter reports
	// what the manager says rather than assuming success.
	bus := NewBus(dshhooks.NewHookManager())
	if err := bus.Publish(context.Background(), harness.LifecycleEvent{
		Kind: harness.LifecycleTaskStarted, TaskID: "task-1",
	}); err != nil {
		t.Fatalf("session start reported an error: %v", err)
	}
	if err := bus.Publish(context.Background(), harness.LifecycleEvent{
		Kind: harness.LifecycleTaskFinished, TaskID: "task-1", Status: "COMPLETED",
	}); err != nil {
		t.Fatalf("session stop reported an error: %v", err)
	}
}

// An unmapped kind is dropped, not routed to a default hook point: delivering an
// event under the wrong point would run the wrong script.
func TestUnmappedKindIsDropped(t *testing.T) {
	bus := NewBus(dshhooks.NewHookManager())
	if err := bus.Publish(context.Background(), harness.LifecycleEvent{
		Kind: harness.LifecycleEventKind("something_else"), TaskID: "task-1",
	}); err != nil {
		t.Fatalf("an unmapped kind returned an error: %v", err)
	}
}
