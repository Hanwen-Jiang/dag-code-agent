package harness

import (
	"context"
	"reflect"
	"testing"
)

// eventVocabulary is the set of event types the kernel may publish.
//
// Every loop topology shares this vocabulary, which is what lets replay, the
// UAT gate, DAG nodes and XAI attribution work unchanged when the topology
// changes. A strategy that needs a new event must add it here deliberately:
// the point of this test is that it cannot happen by accident.
var eventVocabulary = map[string]bool{
	"TASK_ACCEPTED":         true,
	"PLAN_COMPILED":         true,
	"TURN_STARTED":          true,
	"STEP_STARTED":          true,
	"CONTEXT_COMPILED":      true,
	"MODEL_REQUESTED":       true,
	"MODEL_PROPOSED":        true,
	"TOOL_INVOKED":          true,
	"TOOL_OBSERVED":         true,
	"TOOLS_EXECUTED":        true,
	"SUBAGENTS_REQUESTED":   true,
	"VERIFIER_REJECTED":     true,
	"STEP_COMPLETED":        true,
	"USER_GATE_OPENED":      true,
	"TURN_COMPLETED":        true,
	"TASK_COMPLETED":        true,
	"TASK_CANCELLED":        true,
	"TASK_FAILED":           true,
	"USER_GATE_RESUMED":     true,
	"USER_MESSAGE_RECEIVED": true,
}

func eventTypes(t *testing.T, runtime *Runtime, taskID string) []string {
	t.Helper()
	events, err := runtime.Journal().Replay(taskID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	types := make([]string, 0, len(events))
	for _, event := range events {
		if !eventVocabulary[event.Type] {
			t.Fatalf("event %q is outside the shared vocabulary: a new topology must extend eventVocabulary deliberately", event.Type)
		}
		types = append(types, event.Type)
	}
	return types
}

// TestReActEventVocabularyAndOrdering pins the sequence the kernel emits for
// the default topology. It is the guard that kernelising the loop did not
// change what the journal records.
func TestReActEventVocabularyAndOrdering(t *testing.T) {
	runtime, store, auth, request := newRuntimeForTest(t, []ModelResponse{
		{ToolCalls: []ToolCall{{CallID: "c1", Name: "echo", Arguments: map[string]any{"value": "hi"}}}},
		{Text: "done", FinishReason: "stop"},
	})
	defer store.Close()
	_ = runtime.Tools().Registry.Register(ToolSpec{
		Name: "echo", Description: "echo",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	})

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}

	want := []string{
		"TASK_ACCEPTED",
		"PLAN_COMPILED",
		"TURN_STARTED",
		"STEP_STARTED", "CONTEXT_COMPILED", "MODEL_REQUESTED", "MODEL_PROPOSED",
		"TOOL_INVOKED", "TOOL_OBSERVED", "TOOLS_EXECUTED", "STEP_COMPLETED",
		"STEP_STARTED", "CONTEXT_COMPILED", "MODEL_REQUESTED", "MODEL_PROPOSED",
		"STEP_COMPLETED", "TURN_COMPLETED", "TASK_COMPLETED",
	}
	got := eventTypes(t, runtime, request.TaskID)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event sequence changed\n got: %v\nwant: %v", got, want)
	}
}

// TestStrategyIdentityIsJournaledAndVerified makes the recorded topology real:
// recording it without checking would let a task resume under a different
// strategy and silently produce a trajectory that never happened.
func TestStrategyIdentityIsJournaledAndVerified(t *testing.T) {
	runtime, store, auth, request := newRuntimeForTest(t, []ModelResponse{{NeedUser: map[string]any{"kind": "approval", "question": "go?"}}})
	defer store.Close()

	waiting, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if waiting.Status != TaskWaitingUser {
		t.Fatalf("status=%s, want WAITING_USER", waiting.Status)
	}

	// The turn records which topology produced it.
	events, err := runtime.Journal().Replay(request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	recorded := false
	for _, event := range events {
		if event.Type != "TURN_STARTED" {
			continue
		}
		identity, ok := event.Payload["strategy"].(map[string]any)
		if !ok {
			t.Fatal("TURN_STARTED did not record the strategy identity")
		}
		if identity["name"] != reactStrategyName || identity["version"] != reactStrategyVersion {
			t.Fatalf("recorded identity=%v", identity)
		}
		recorded = true
	}
	if !recorded {
		t.Fatal("no TURN_STARTED event")
	}

	// Resuming the same task under a different topology must be refused.
	other, err := New(WithStore(store), WithModel(NewConversationModel()), WithStrategyInstance(namedStrategy{name: "reflection", version: "9.9.9"}))
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := other.Resume(context.Background(), request.TaskID, "continue", auth)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Status != TaskFailed || resumed.FailureCategory != "STRATEGY" {
		t.Fatalf("resumed under a foreign strategy: status=%s category=%s", resumed.Status, resumed.FailureCategory)
	}
}

// TestCustomStrategySharesVocabulary proves the extension point works end to
// end: an embedder's topology runs on the same kernel and publishes only known
// events.
func TestCustomStrategySharesVocabulary(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(WithStore(store), WithModel(NewConversationModel()), WithStrategyInstance(namedStrategy{name: "immediate", version: "1.0.0"}))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if runtime.Strategy().Name() != "immediate" {
		t.Fatalf("strategy=%s", runtime.Strategy().Name())
	}
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject"}
	result, err := runtime.Run(context.Background(), TaskRequest{TaskID: "custom-strategy", Goal: "finish at once", TenantID: auth.TenantID, SubjectID: auth.SubjectID}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	// No model step happened: the strategy finished immediately, and the
	// kernel still produced a well-formed, verifiable journal.
	types := eventTypes(t, runtime, "custom-strategy")
	for _, want := range []string{"TURN_STARTED", "STEP_COMPLETED", "TURN_COMPLETED", "TASK_COMPLETED"} {
		found := false
		for _, got := range types {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("custom strategy journal %v is missing %s", types, want)
		}
	}
	for _, got := range types {
		if got == "MODEL_REQUESTED" {
			t.Fatalf("custom strategy did not ask for a model but the journal records one: %v", types)
		}
	}
	if _, err := runtime.Journal().ReplayState("custom-strategy"); err != nil {
		t.Fatalf("ReplayState for a custom topology: %v", err)
	}
}

// TestUnregisteredStrategyIsRejected keeps a typo in configuration from
// silently falling back to a different topology.
func TestUnregisteredStrategyIsRejected(t *testing.T) {
	if _, err := New(WithStrategy("does-not-exist")); err == nil {
		t.Fatal("an unregistered strategy name was accepted")
	}
}
