// Command embed_offline is the smallest possible embed of the control plane.
//
// It needs no configuration, no credentials and no external service: harness.New
// builds an in-memory SQLite store and a deterministic offline model adapter.
// Run it with:
//
//	go run ./examples/embed_offline
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

func main() {
	// Zero options. This is the floor for embeddability: if this does not
	// work, nothing else matters.
	runtime, err := harness.New()
	if err != nil {
		log.Fatalf("harness.New: %v", err)
	}
	// The runtime owns the store it created, so Close releases it.
	defer func() { _ = runtime.Close() }()

	auth := harness.AuthContext{
		TenantID:  "local",
		SubjectID: "developer",
		Scopes:    []string{"memory:read", "artifact:read", "artifact:write"},
	}
	result := runtime.Run(context.Background(), harness.TaskRequest{
		TaskID:    "example-offline",
		Goal:      "summarise what the control plane guarantees",
		TenantID:  auth.TenantID,
		SubjectID: auth.SubjectID,
	}, auth)

	fmt.Printf("status=%s events=%d steps=%d\n", result.Status, len(result.Events), result.State.Step)
	fmt.Println(result.FinalAnswer)

	// The journal is independently verifiable: it re-reads the chain, checks
	// the previous-hash links and compares the state digest bound into every
	// event against the checkpoint written with it.
	state, err := runtime.Journal().ReplayState(result.TaskID)
	if err != nil {
		log.Fatalf("ReplayState: %v", err)
	}
	fmt.Printf("replay verified: status=%s steps=%d tokens_in=%d\n", state.Status, state.Step, state.InputTokens)

	if result.Status != harness.TaskCompleted {
		fmt.Fprintf(os.Stderr, "expected COMPLETED, got %s (%s)\n", result.Status, result.FailureCategory)
		os.Exit(1)
	}
}
