// Command consumer is a deliberately SEPARATE Go module that imports the
// control plane the way an unrelated project would.
//
// This file is the guard for acceptance criterion C1. Because it lives in its
// own module, it cannot reach any package that is not genuinely importable:
// anything left under a former internal/ directory fails to compile here. If a
// regression hides the control plane again, `go build ./...` in this directory
// goes red.
//
//	cd examples/embed_external && go run .
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

func main() {
	// An external embedder supplies everything it cares about and accepts the
	// rest as defaults.
	store := harness.NewInMemoryStore()
	clock := harness.NewManualClock(time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC))

	runtime, err := harness.New(
		harness.WithStore(store),
		harness.WithClock(clock),
	)
	if err != nil {
		log.Fatalf("harness.New: %v", err)
	}
	defer func() { _ = store.Close() }()

	auth := harness.AuthContext{TenantID: "external", SubjectID: "consumer", Scopes: []string{"memory:read"}}
	result := runtime.Run(context.Background(), harness.TaskRequest{
		TaskID:    "external-consumer-1",
		Goal:      "prove the control plane is importable from another module",
		TenantID:  auth.TenantID,
		SubjectID: auth.SubjectID,
	}, auth)

	fmt.Printf("module: example.com/external-consumer\n")
	fmt.Printf("status: %s\n", result.Status)
	fmt.Printf("durable events: %d\n", len(result.Events))

	events, err := runtime.Journal().Replay(result.TaskID)
	if err != nil {
		log.Fatalf("journal replay from a foreign module: %v", err)
	}
	state, err := runtime.Journal().ReplayState(result.TaskID)
	if err != nil {
		log.Fatalf("state replay from a foreign module: %v", err)
	}
	fmt.Printf("verified chain: %d events, head=%s\n", len(events), events[len(events)-1].EventHash[:19]+"...")
	fmt.Printf("rebuilt state: status=%s step=%d\n", state.Status, state.Step)

	if result.Status != harness.TaskCompleted {
		fmt.Fprintf(os.Stderr, "expected COMPLETED, got %s\n", result.Status)
		os.Exit(1)
	}
}
