package harness

import (
	"context"
	"testing"
	"time"
)

// Voting and explicit tool search are the last two action kinds a strategy can
// emit without the kernel refusing them. Both reuse machinery that already
// existed, which is the point: a topology should not need new kernel concepts.

// votingModel answers the same objective with an answer chosen per agent, so a
// vote has something to decide between.
// votingModel answers the same objective differently per agent so a vote has
// something to decide between.
//
// It holds no state: an earlier version counted calls in a plain map, which the
// race detector correctly flagged because voters run concurrently. The counter
// was never read, so the fix was to delete it rather than to lock it — adding a
// mutex would have preserved dead state and hidden the real lesson.
type votingModel struct{}

func (m *votingModel) Name() string { return "voting" }

func (m *votingModel) Complete(_ context.Context, request ModelRequest) (ModelResponse, error) {
	text := requestText(request)
	switch {
	case contains(text, "VOTE-QUESTION"):
		// Two voters agree, one dissents.
		if contains(text, "agent three") {
			return ModelResponse{Text: "the minority answer", FinishReason: "stop"}, nil
		}
		return ModelResponse{Text: "the majority answer", FinishReason: "stop"}, nil
	default:
		return ModelResponse{Text: "parent answer", FinishReason: "stop"}, nil
	}
}

func (m *votingModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

type votingStrategy struct{ sent bool }

func (s *votingStrategy) Name() string    { return "voting-test" }
func (s *votingStrategy) Version() string { return "1.0.0" }

func (s *votingStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	if !s.sent && st.Step == 0 {
		s.sent = true
		return Action{Kind: ActionVote, Subagents: []SubagentSpec{
			{AgentID: "voter-one", Objective: "VOTE-QUESTION agent one"},
			{AgentID: "voter-two", Objective: "VOTE-QUESTION agent two"},
			{AgentID: "voter-three", Objective: "VOTE-QUESTION agent three"},
		}}, nil
	}
	return ReActStrategy{}.Next(ctx, st)
}

func (s *votingStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return ReActStrategy{}.OnResult(st, result)
}

func TestVoteResolvesToMajority(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(&votingModel{}),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(&votingStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "vote", Goal: "decide something", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	var merged map[string]any
	for _, observation := range result.State.Observations {
		if stringValue(observation["type"]) == "subagents_merged" {
			merged, _ = observation["merged"].(map[string]any)
		}
	}
	if merged == nil {
		t.Fatal("the vote produced no merged result")
	}
	if got := stringValue(merged["winner"]); got != "the majority answer" {
		t.Fatalf("winner=%q, want the majority answer (2 of 3)", got)
	}
	if votes := payloadInt(merged["winner_votes"]); votes != 2 {
		t.Fatalf("winner_votes=%d, want 2", votes)
	}
	if tied, _ := merged["tied"].(bool); tied {
		t.Fatal("a 2-of-3 decision was reported as tied")
	}
	// Each voter is a real child task, so a vote is auditable like any fan-out.
	if _, _, err := store.GetTask("vote:voter-two"); err != nil {
		t.Fatalf("a voter was not created as a durable task: %v", err)
	}
}

// A failed voter must not be able to outvote a real answer with its empty
// summary.
func TestVoteIgnoresFailedVoters(t *testing.T) {
	merged := mergeSubagentResults([]SubagentResult{
		{AgentID: "a", Status: "SUCCEEDED", Summary: "answer"},
		{AgentID: "b", Status: "FAILED", Error: "boom"},
		{AgentID: "c", Status: "FAILED", Error: "boom"},
	}, false, true)
	if got := stringValue(merged["winner"]); got != "answer" {
		t.Fatalf("winner=%q, want the only successful answer", got)
	}
	if votes := payloadInt(merged["winner_votes"]); votes != 1 {
		t.Fatalf("winner_votes=%d, want 1 (failures must not vote)", votes)
	}
}

// A tie is reported rather than broken arbitrarily.
func TestVoteReportsTies(t *testing.T) {
	merged := mergeSubagentResults([]SubagentResult{
		{AgentID: "a", Status: "SUCCEEDED", Summary: "left"},
		{AgentID: "b", Status: "SUCCEEDED", Summary: "right"},
	}, false, true)
	if tied, _ := merged["tied"].(bool); !tied {
		t.Fatal("an evenly split vote was not reported as tied")
	}
	if status := stringValue(merged["status"]); status != "CONFLICT" {
		t.Fatalf("status=%s, want CONFLICT for a tie", status)
	}
}

// A strategy can resolve tool schemas itself, through the same scoped path the
// model uses.
type searchingStrategy struct {
	sent bool
	seen bool
}

func (s *searchingStrategy) Name() string    { return "searching-test" }
func (s *searchingStrategy) Version() string { return "1.0.0" }

func (s *searchingStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	if !s.sent {
		s.sent = true
		return Action{Kind: ActionSearchTools, Search: "read_file"}, nil
	}
	return ReActStrategy{}.Next(ctx, st)
}

func (s *searchingStrategy) OnResult(st LoopState, result StepResult) LoopState {
	if result.Action == ActionSearchTools {
		s.seen = true
	}
	return ReActStrategy{}.OnResult(st, result)
}

func TestStrategyCanSearchTools(t *testing.T) {
	_, _, auth := catalogRuntime(t, 8, 3)
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(NewScriptedModel([]ModelResponse{{Text: "final", FinishReason: "stop"}})),
		WithToolRegistry(catalogTestRegistry(t)),
		WithToolCatalogThreshold(3),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(&searchingStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "searched", Goal: "find a tool", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}
	events, err := runtime.Journal().Replay("searched")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Type == "TOOL_SEARCHED" && stringValue(event.Payload["query"]) == "read_file" {
			found = true
		}
	}
	if !found {
		t.Fatal("the strategy-initiated tool search was not recorded")
	}
}

// Without the resolver registered, the strategy is told how to fix it rather
// than getting an opaque failure.
func TestSearchToolsWithoutResolverExplainsItself(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(NewScriptedModel([]ModelResponse{{Text: "final", FinishReason: "stop"}})),
		WithToolRegistry(catalogTestRegistry(t)),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(&searchingStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "noresolver", Goal: "find a tool", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 4,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskFailed || result.FailureCategory != "STRATEGY" {
		t.Fatalf("status=%s category=%s, want FAILED/STRATEGY", result.Status, result.FailureCategory)
	}
	message := stringValue(result.State.LastError["message"])
	if !contains(message, "WithToolCatalogThreshold") {
		t.Fatalf("the failure does not say how to fix it: %q", message)
	}
}

// catalogTestRegistry builds a registry large enough to trigger deferral.
func catalogTestRegistry(t *testing.T) *ToolRegistry {
	t.Helper()
	registry := NewToolRegistry()
	for i := 0; i < 6; i++ {
		name := "generic_tool_" + string(rune('a'+i))
		if err := registry.Register(ToolSpec{
			Name: name, Description: "a generic tool", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.Register(ToolSpec{
		Name: "read_file", Description: "read a file", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}
