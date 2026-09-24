package looptest_test

import (
	"context"
	"testing"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/harness/looptest"
)

// The built-in topology must pass the suite it defines: if ReAct cannot satisfy
// the contract, the contract is wrong or the kernel is.
func TestReActStrategyConformance(t *testing.T) {
	looptest.RunStrategyConformance(t, func() harness.Strategy { return harness.ReActStrategy{} })
}

// A custom topology written the way an embedder would write one must pass too.
// This is the point of the suite: it is the self-check an external author runs.
func TestCustomStrategyConformance(t *testing.T) {
	looptest.RunStrategyConformance(t, func() harness.Strategy { return echoToolStrategy{} })
}

// echoToolStrategy is a minimal custom topology: it asks the model once, runs
// whatever tools came back, then finishes. It exercises the same kernel path as
// ReAct but opens with a tool-only step.
type echoToolStrategy struct{}

func (echoToolStrategy) Name() string    { return "echo-tools" }
func (echoToolStrategy) Version() string { return "1.0.0" }

func (echoToolStrategy) Next(_ context.Context, st harness.LoopState) (harness.Action, error) {
	if len(st.LastTool) > 0 || st.FinalCandidate != "" {
		return harness.Action{Kind: harness.ActionFinish, Finish: &harness.FinishSpec{Answer: "finished"}}, nil
	}
	if len(st.PendingToolCalls) > 0 {
		return harness.Action{Kind: harness.ActionCallTools, Tools: st.PendingToolCalls}, nil
	}
	return harness.Action{Kind: harness.ActionCallModel, Model: &harness.ModelRequest{TaskID: st.TaskID}}, nil
}

func (echoToolStrategy) OnResult(st harness.LoopState, result harness.StepResult) harness.LoopState {
	next := st
	next.PendingToolCalls = nil
	next.LastTool = nil
	next.FinalCandidate = ""
	switch result.Action {
	case harness.ActionCallModel:
		next.LastText = result.ModelText
		next.PendingToolCalls = result.ToolCalls
		next.FinalCandidate = result.FinalCandidate
	case harness.ActionCallTools:
		next.LastTool = result.Tools
	}
	return next
}
