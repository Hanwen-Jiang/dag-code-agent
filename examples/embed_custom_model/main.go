// Command embed_custom_model shows that the model provider and the clock are
// seams too.
//
// An embedder brings its own ModelAdapter (their gateway, their local model,
// a recorded fixture) and their own Clock, and still gets the same durable
// loop, tool policy and replay guarantees.
//
//	go run ./examples/embed_custom_model
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// staticModel is a minimal ModelAdapter. Complete decides the turn; Stream
// exists because the control plane can project the same answer incrementally.
type staticModel struct {
	answer string
}

func (m staticModel) Name() string { return "static-fixture" }

func (m staticModel) Complete(_ context.Context, req harness.ModelRequest) (harness.ModelResponse, error) {
	return harness.ModelResponse{
		Text:         m.answer,
		FinishReason: "stop",
		Usage:        harness.ModelUsage{InputTokens: harness.EstimateTokens(req.Messages), OutputTokens: 12},
	}, nil
}

func (m staticModel) Stream(_ context.Context, _ harness.ModelRequest) (<-chan harness.StreamEvent, error) {
	events := make(chan harness.StreamEvent, 3)
	go func() {
		defer close(events)
		events <- harness.StreamEvent{Type: "text", Data: map[string]any{"text": m.answer}}
		events <- harness.StreamEvent{Type: "done", Data: map[string]any{"finish_reason": "stop"}}
	}()
	return events, nil
}

func main() {
	// A fixed clock makes every event timestamp reproducible, which is what
	// makes lease expiry and time-based assertions testable.
	clock := harness.NewManualClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	model := staticModel{answer: `{"summary":"the control plane owns the journal"}`}

	runtime, err := harness.New(
		harness.WithModel(model),
		harness.WithClock(clock),
	)
	if err != nil {
		log.Fatalf("harness.New: %v", err)
	}
	defer func() { _ = runtime.Close() }()

	auth := harness.AuthContext{TenantID: "acme", SubjectID: "service", Scopes: []string{"memory:read"}}
	result := runtime.Run(context.Background(), harness.TaskRequest{
		TaskID:    "example-custom-model",
		Goal:      "return a structured summary",
		TenantID:  auth.TenantID,
		SubjectID: auth.SubjectID,
	}, auth)

	fmt.Printf("status=%s answer=%s\n", result.Status, result.FinalAnswer)

	// Every event carries the injected clock's timestamp.
	deterministic := true
	for _, event := range result.Events {
		if event.CreatedAt == "" || event.CreatedAt[:4] != "2026" {
			deterministic = false
			break
		}
	}
	fmt.Printf("model=%s events=%d timestamps_from_injected_clock=%v\n", runtime.Model().Name(), len(result.Events), deterministic)

	// Move the clock and show that the runtime observes it.
	clock.Advance(90 * time.Minute)
	fmt.Printf("clock now: %s\n", runtime.Clock().Now().UTC().Format(time.RFC3339))

	if !deterministic {
		fmt.Fprintln(os.Stderr, "event timestamps did not come from the injected clock")
		os.Exit(1)
	}
	if result.Status != harness.TaskCompleted {
		fmt.Fprintf(os.Stderr, "expected COMPLETED, got %s (%s)\n", result.Status, result.FailureCategory)
		os.Exit(1)
	}
}
