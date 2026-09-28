package harness

import (
	"context"
	"errors"
	"testing"
)

// The strategy seam is the foundation every loop topology will be built on, so
// its contract is pinned by tests rather than by documentation alone.

func TestReActStrategyDecisions(t *testing.T) {
	strategy := ReActStrategy{}
	ctx := context.Background()

	// A fresh state asks for the model.
	action, err := strategy.Next(ctx, LoopState{TaskID: "t1"})
	if err != nil || action.Kind != ActionCallModel {
		t.Fatalf("fresh next=%+v err=%v, want ActionCallModel", action, err)
	}
	if action.Model == nil || action.Model.TaskID != "t1" {
		t.Fatalf("model request not carried: %+v", action.Model)
	}

	// A model response proposing tools must run them before asking again.
	state := strategy.OnResult(LoopState{TaskID: "t1"}, StepResult{
		Action:    ActionCallModel,
		ModelText: "thinking",
		ToolCalls: []ToolCall{{CallID: "c1", Name: "read", Arguments: map[string]any{"path": "a"}}},
	})
	action, err = strategy.Next(ctx, state)
	if err != nil || action.Kind != ActionCallTools {
		t.Fatalf("after tool proposal next=%+v err=%v, want ActionCallTools", action, err)
	}
	if len(action.Tools) != 1 || action.Tools[0].CallID != "c1" {
		t.Fatalf("tool calls not passed through: %+v", action.Tools)
	}

	// After the tools run, the loop asks the model again.
	state = strategy.OnResult(state, StepResult{Action: ActionCallTools, Tools: []ToolResult{{CallID: "c1", Name: "read", Status: ToolSucceeded}}})
	if len(state.LastTool) != 1 || len(state.PendingToolCalls) != 0 {
		t.Fatalf("tool results not folded: %+v", state)
	}
	action, err = strategy.Next(ctx, state)
	if err != nil || action.Kind != ActionCallModel {
		t.Fatalf("after tools next=%+v err=%v, want ActionCallModel", action, err)
	}

	// A structured final answer finishes the run.
	state = strategy.OnResult(state, StepResult{Action: ActionCallModel, ModelText: `{"status":"done"}`, FinalCandidate: `{"status":"done"}`})
	action, err = strategy.Next(ctx, state)
	if err != nil || action.Kind != ActionFinish {
		t.Fatalf("after final answer next=%+v err=%v, want ActionFinish", action, err)
	}
	if action.Finish == nil || action.Finish.Answer != `{"status":"done"}` || action.Finish.Category != "" {
		t.Fatalf("finish spec=%+v", action.Finish)
	}
}

func TestReActStrategyGateOutranksPendingWork(t *testing.T) {
	strategy := ReActStrategy{}
	// A user gate must win even when the same response also proposed tools:
	// running tools after the model asked to pause would act without consent.
	state := strategy.OnResult(LoopState{TaskID: "t"}, StepResult{
		Action:    ActionCallModel,
		ToolCalls: []ToolCall{{CallID: "c1", Name: "write", Arguments: map[string]any{"path": "a"}}},
		NeedUser:  map[string]any{"kind": "approval", "question": "delete the file?"},
	})
	action, err := strategy.Next(context.Background(), state)
	if err != nil || action.Kind != ActionAskUser {
		t.Fatalf("next=%+v err=%v, want ActionAskUser", action, err)
	}
}

func TestReActStrategyStopsWhenBudgetIsExhausted(t *testing.T) {
	strategy := ReActStrategy{}
	state := LoopState{TaskID: "t", Budget: BudgetSnapshot{MaxTokens: 100, UsedTokens: 100}}
	action, err := strategy.Next(context.Background(), state)
	if err != nil || action.Kind != ActionFinish {
		t.Fatalf("next=%+v err=%v, want ActionFinish", action, err)
	}
	if action.Finish == nil || action.Finish.Category != "BUDGET" {
		t.Fatalf("finish=%+v, want category BUDGET", action.Finish)
	}
	// A final answer still outranks an exhausted budget: the work is done.
	state.FinalCandidate = "answer"
	action, err = strategy.Next(context.Background(), state)
	if err != nil || action.Kind != ActionFinish || action.Finish.Category != "" {
		t.Fatalf("final answer over budget next=%+v err=%v", action, err)
	}
}

func TestBudgetSnapshotHelpers(t *testing.T) {
	unlimited := BudgetSnapshot{}
	if unlimited.RemainingTokens() != -1 || unlimited.RemainingToolCalls() != -1 || unlimited.Exhausted() {
		t.Fatalf("unlimited snapshot misreported: %+v", unlimited)
	}
	partial := budgetSnapshot(&BudgetLedger{MaxTokens: 100, UsedTokens: 40, MaxToolCalls: 5, UsedToolCalls: 2})
	if partial.RemainingTokens() != 60 || partial.RemainingToolCalls() != 3 || partial.Exhausted() {
		t.Fatalf("partial snapshot misreported: %+v", partial)
	}
	full := budgetSnapshot(&BudgetLedger{MaxToolCalls: 5, UsedToolCalls: 5})
	if !full.Exhausted() {
		t.Fatalf("exhausted tool budget not reported: %+v", full)
	}
}

func TestValidateActionRejectsMalformedActions(t *testing.T) {
	cases := []Action{
		// ActionCallModel is deliberately absent: the kernel builds the request
		// from the compiled context packet, so a bare call_model is valid and
		// requiring a Model field that is then ignored was misleading.
		{Kind: ActionCallTools},
		{Kind: ActionSpawn},
		{Kind: ActionHandoff},
		{Kind: ActionFanout, Fanout: &FanoutSpec{Branches: 0}},
		{Kind: ActionFinish},
		{Kind: ActionSearchTools, Search: "  "},
		{Kind: ActionKind("nonsense")},
	}
	for _, action := range cases {
		if err := ValidateAction(action); !errors.Is(err, ErrStrategyContract) {
			t.Fatalf("ValidateAction(%q) err=%v, want ErrStrategyContract", action.Kind, err)
		}
	}
	valid := []Action{
		// A bare call_model: the strategy asks for a model call and the kernel
		// supplies the request. Guidance is how it adds to that request.
		{Kind: ActionCallModel},
		{Kind: ActionCallModel, Guidance: []string{"an instruction"}},
		{Kind: ActionCallTools, Tools: []ToolCall{{CallID: "c"}}},
		{Kind: ActionSpawn, Subagents: []SubagentSpec{{AgentID: "a"}}},
		{Kind: ActionHandoff, Handoff: &SubagentSpec{AgentID: "a"}},
		{Kind: ActionFanout, Fanout: &FanoutSpec{Branches: 3}},
		{Kind: ActionFinish, Finish: &FinishSpec{Answer: "done"}},
		{Kind: ActionSearchTools, Search: "browser"},
	}
	for _, action := range valid {
		if err := ValidateAction(action); err != nil {
			t.Fatalf("ValidateAction(%q) err=%v, want nil", action.Kind, err)
		}
	}
}

func TestStrategyRegistryRejectsSilentReplacement(t *testing.T) {
	registry := NewStrategyRegistry(ReActStrategy{})
	if _, ok := registry.Get("react"); !ok {
		t.Fatal("built-in strategy not registered")
	}
	// Re-registering the same name must fail: replacing a topology silently
	// would replay historical runs under different semantics.
	if err := registry.Register(ReActStrategy{}); !errors.Is(err, ErrStrategyContract) {
		t.Fatalf("duplicate register err=%v, want ErrStrategyContract", err)
	}
	if err := registry.Register(nil); !errors.Is(err, ErrStrategyContract) {
		t.Fatalf("nil register err=%v, want ErrStrategyContract", err)
	}
	if err := registry.Register(namelessStrategy{}); !errors.Is(err, ErrStrategyContract) {
		t.Fatalf("nameless register err=%v, want ErrStrategyContract", err)
	}
	if err := registry.Register(versionlessStrategy{}); !errors.Is(err, ErrStrategyContract) {
		t.Fatalf("versionless register err=%v, want ErrStrategyContract", err)
	}
	// A genuinely new strategy registers and is listed deterministically.
	if err := registry.Register(namedStrategy{name: "reflection", version: "0.1.0"}); err != nil {
		t.Fatalf("register new strategy: %v", err)
	}
	infos := registry.List()
	if len(infos) != 2 || infos[0].Name != "react" || infos[1].Name != "reflection" {
		t.Fatalf("List()=%+v, want react then reflection", infos)
	}
	if want := (ReActStrategy{}).Version(); infos[0].Version != want {
		t.Fatalf("version not reported: %+v", infos[0])
	}
}

// namedStrategy is a minimal custom strategy, i.e. what an embedder writes.
type namedStrategy struct {
	name    string
	version string
}

func (s namedStrategy) Name() string    { return s.name }
func (s namedStrategy) Version() string { return s.version }
func (s namedStrategy) Next(context.Context, LoopState) (Action, error) {
	return Action{Kind: ActionFinish, Finish: &FinishSpec{Answer: "ok"}}, nil
}
func (s namedStrategy) OnResult(st LoopState, _ StepResult) LoopState { return st }

type namelessStrategy struct{}

func (namelessStrategy) Name() string                                    { return "  " }
func (namelessStrategy) Version() string                                 { return "1" }
func (namelessStrategy) Next(context.Context, LoopState) (Action, error) { return Action{}, nil }
func (namelessStrategy) OnResult(st LoopState, _ StepResult) LoopState   { return st }

type versionlessStrategy struct{}

func (versionlessStrategy) Name() string                                    { return "versionless" }
func (versionlessStrategy) Version() string                                 { return "" }
func (versionlessStrategy) Next(context.Context, LoopState) (Action, error) { return Action{}, nil }
func (versionlessStrategy) OnResult(st LoopState, _ StepResult) LoopState   { return st }
