// Command embed_custom_store shows that the storage backend is a seam, not a
// dependency.
//
// It injects a store that wraps the in-memory backend with instrumentation.
// The same pattern is how an embedder plugs in Postgres, Redis or an
// already-existing database, and it is why the event hash chain lives in the
// core Journal rather than in the store: this wrapper cannot corrupt it.
//
//	go run ./examples/embed_custom_store
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// countingStore embeds the in-memory backend and records how often the runtime
// asks it to append an event. Only AppendEvent is overridden; every other
// method is promoted from the embedded store.
type countingStore struct {
	*harness.InMemoryStore
	appends atomic.Int64
	reads   atomic.Int64
}

func (s *countingStore) AppendEvent(state harness.TaskState, expectedVersion, expectedSequence *int, next harness.Event) error {
	s.appends.Add(1)
	return s.InMemoryStore.AppendEvent(state, expectedVersion, expectedSequence, next)
}

func (s *countingStore) Events(taskID string) ([]harness.Event, error) {
	s.reads.Add(1)
	return s.InMemoryStore.Events(taskID)
}

func main() {
	store := &countingStore{InMemoryStore: harness.NewInMemoryStore()}

	// WithStore overrides the default backend; everything else keeps its
	// default. Note the runtime does NOT close an injected store: the
	// embedder owns its lifecycle.
	runtime, err := harness.New(harness.WithStore(store))
	if err != nil {
		log.Fatalf("harness.New: %v", err)
	}
	defer func() { _ = store.Close() }()

	auth := harness.AuthContext{TenantID: "acme", SubjectID: "service", Scopes: []string{"memory:read"}}
	result := runtime.Run(context.Background(), harness.TaskRequest{
		TaskID:    "example-custom-store",
		Goal:      "verify the injected store receives every durable write",
		TenantID:  auth.TenantID,
		SubjectID: auth.SubjectID,
	}, auth)

	if _, err := runtime.Journal().ReplayState(result.TaskID); err != nil {
		log.Fatalf("ReplayState through the custom store: %v", err)
	}

	fmt.Printf("status=%s\n", result.Status)
	fmt.Printf("events the runtime appended through the injected store: %d\n", store.appends.Load())
	fmt.Printf("journal reads served by the injected store: %d\n", store.reads.Load())

	if store.appends.Load() == 0 {
		fmt.Fprintln(os.Stderr, "the injected store never saw a write: injection is not wired")
		os.Exit(1)
	}
	if result.Status != harness.TaskCompleted {
		fmt.Fprintf(os.Stderr, "expected COMPLETED, got %s (%s)\n", result.Status, result.FailureCategory)
		os.Exit(1)
	}
}
