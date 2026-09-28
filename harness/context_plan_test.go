package harness

import (
	"context"
	"strings"
	"testing"
)

// The context lifecycle replaced an amputation (keep the stable messages and
// the last one, discard everything between) with a staged pipeline. These tests
// pin the properties that make the replacement an improvement rather than a
// different way to lose context.

func contextTestState(turns int, observations int) TaskState {
	state := TaskState{TaskID: "ctx", Goal: "compile the model context", TenantID: "tenant", SubjectID: "subject"}
	for i := 0; i < turns; i++ {
		state.UserMessages = append(state.UserMessages, strings.Repeat("turn ", 10)+string(rune('a'+i)))
	}
	for i := 0; i < observations; i++ {
		state.Observations = append(state.Observations, map[string]any{
			"tool": "echo", "call_id": string(rune('a' + i)), "status": ToolSucceeded,
			"value": map[string]any{"payload": strings.Repeat("observation ", 200)},
		})
	}
	return state
}

func buildContext(t *testing.T, state TaskState, window int, store Store) ContextPacket {
	t.Helper()
	builder := NewContextBuilder(nil, NewSkillRegistry(), NewToolRegistry(), NewPrefixCache(4))
	if store != nil {
		if artifacts, ok := store.(ArtifactStore); ok {
			builder.Spill = artifactSpillStore{artifacts: artifacts}
		}
	}
	auth := AuthContext{TenantID: state.TenantID, SubjectID: state.SubjectID}
	packet, err := builder.BuildWithContext(context.Background(), TaskRequest{
		TaskID: state.TaskID, Goal: state.Goal, TenantID: state.TenantID, SubjectID: state.SubjectID,
		ContextWindow: window,
	}, auth, state, nil)
	if err != nil {
		t.Fatalf("BuildWithContext(window=%d): %v", window, err)
	}
	return packet
}

// TestStablePrefixSurvivesCompaction is the prompt-cache rule: compaction may
// only touch the non-stable suffix. If it rewrote the pinned prefix the cache
// key would change and the cache would go permanently cold.
func TestStablePrefixSurvivesCompaction(t *testing.T) {
	state := contextTestState(8, 4)
	full := buildContext(t, state, 1_000_000, nil)
	compact := buildContext(t, state, 200, nil)

	if !compact.Plan.Compacted {
		t.Fatal("a 200-token budget did not trigger compaction")
	}
	// The pinned prefix is the system/developer pair plus the goal.
	if len(compact.Messages) < 3 || len(full.Messages) < 3 {
		t.Fatalf("unexpected message counts: compact=%d full=%d", len(compact.Messages), len(full.Messages))
	}
	for i := 0; i < 3; i++ {
		left, _ := CanonicalJSON(full.Messages[i])
		right, _ := CanonicalJSON(compact.Messages[i])
		if string(left) != string(right) {
			t.Fatalf("pinned message %d changed under compaction\n before: %s\n after:  %s", i, left, right)
		}
	}
}

// TestGoalIsNeverEvicted guards the task's objective. The goal is a user
// message by role, so without pinning the drop level would happily evict it.
func TestGoalIsNeverEvicted(t *testing.T) {
	state := contextTestState(8, 6)
	compact := buildContext(t, state, 300, nil)
	found := false
	for _, message := range compact.Messages {
		if text, ok := message["content"].(string); ok && text == state.Goal {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("the task goal was evicted from the context: %v", compact.Plan.Payload())
	}
}

// TestCompactionSummarizesInsteadOfAmputating is the behaviour change: older
// turns are represented by a summary, not silently discarded.
func TestCompactionSummarizesInsteadOfAmputating(t *testing.T) {
	state := contextTestState(8, 2)
	compact := buildContext(t, state, 400, nil)
	if len(compact.Plan.Summarized) == 0 {
		t.Fatalf("nothing was summarized: %v", compact.Plan.Payload())
	}
	summaries := 0
	for _, message := range compact.Messages {
		if stringValue(message["role"]) == "observation_summary" {
			summaries++
		}
	}
	if summaries == 0 {
		t.Fatalf("older turns vanished without a summary: %v", compact.Plan.Payload())
	}
	// The most recent turn is what the model is answering, so it stays verbatim.
	last := state.UserMessages[len(state.UserMessages)-1]
	found := false
	for _, message := range compact.Messages {
		if text, ok := message["content"].(string); ok && text == last {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("the most recent turn was summarized away")
	}
}

// TestToolResultSpillIsRecoverable proves the spill level uses real storage:
// the bulk leaves the prompt, and the artifact can be read back.
func TestToolResultSpillIsRecoverable(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	state := contextTestState(2, 1)
	// Make the single observation large enough to cross the spill threshold.
	state.Observations[0] = map[string]any{
		"tool": "read_big", "call_id": "big-1", "status": ToolSucceeded,
		"value": map[string]any{"payload": strings.Repeat("x", 8000)},
	}
	compact := buildContext(t, state, 600, store)
	if len(compact.Plan.Spilled) == 0 {
		t.Fatalf("an oversized tool result was not spilled: %v", compact.Plan.Payload())
	}
	ref := compact.Plan.Spilled[0]
	if ref.ArtifactID == "" || ref.SHA256 == "" {
		t.Fatalf("spill reference is incomplete: %+v", ref)
	}
	spilled := false
	for _, message := range compact.Messages {
		text, _ := message["content"].(string)
		if strings.Contains(text, ref.ArtifactID) && strings.Contains(text, "read_artifact") {
			spilled = true
			break
		}
	}
	if !spilled {
		t.Fatal("the context does not tell the model how to fetch the spilled value")
	}
	// The artifact is durable and readable under the same identity.
	auth := AuthContext{TenantID: state.TenantID, SubjectID: state.SubjectID}
	record, ok, err := store.GetArtifact(auth, ref.ArtifactID)
	if err != nil || !ok {
		t.Fatalf("spilled artifact is not retrievable: ok=%v err=%v", ok, err)
	}
	if record.SHA256 != ref.SHA256 {
		t.Fatalf("artifact digest mismatch: %s vs %s", record.SHA256, ref.SHA256)
	}
	rehydrated, err := artifactSpillStore{artifacts: store}.Rehydrate(context.Background(), auth, ref)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if rehydrated.Content == nil {
		t.Fatal("rehydrated segment lost its content")
	}
}

// TestContextPlanIsJournaled asserts the compaction decision reaches the
// journal, so "what the model was shown" is auditable.
func TestContextPlanIsJournaled(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	registry := NewToolRegistry()
	_ = registry.Register(ToolSpec{
		Name: "big", Description: "return a large payload",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		Handler: func(map[string]any, AuthContext) (any, error) {
			return map[string]any{"payload": strings.Repeat("y", 12000)}, nil
		},
	})
	model := NewScriptedModel([]ModelResponse{
		{ToolCalls: []ToolCall{{CallID: "c1", Name: "big", Arguments: map[string]any{}}}},
		{Text: "done", FinishReason: "stop"},
	})
	runtime, err := New(WithStore(store), WithModel(model), WithToolRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "ctx-plan", Goal: "summarise the payload", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
		ContextWindow: 900,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	events, err := runtime.Journal().Replay("ctx-plan")
	if err != nil {
		t.Fatal(err)
	}
	plans := 0
	for _, event := range events {
		if event.Type != "CONTEXT_COMPILED" {
			continue
		}
		raw, ok := event.Payload["context_plan"].(map[string]any)
		if !ok {
			continue
		}
		plans++
		if compacted, _ := raw["compacted"].(bool); !compacted {
			t.Fatalf("a plan was recorded without compaction: %v", raw)
		}
	}
	if plans == 0 {
		t.Fatal("compaction happened but no context_plan was recorded")
	}
}

// TestUncompactedRunRecordsNoPlan keeps the fidelity promise: a run that fits
// its budget produces exactly the journal it produced before the context
// lifecycle work landed, so existing journals and tests stay comparable.
func TestUncompactedRunRecordsNoPlan(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel([]ModelResponse{{Text: "ok", FinishReason: "stop"}})))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject"}
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "ctx-no-plan", Goal: "answer briefly", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events, err := runtime.Journal().Replay("ctx-no-plan")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == "CONTEXT_COMPILED" {
			if _, present := event.Payload["context_plan"]; present {
				t.Fatal("an uncompacted run recorded a context_plan")
			}
		}
	}
}
