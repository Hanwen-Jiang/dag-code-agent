package harness

import (
	"context"
)

// ResearchStrategy: gather evidence over several rounds, then synthesize.
//
// It is the one topology here that is iterative rather than a single pass.
// Reflection, CoT, Debate and Tree of Thoughts each decide once; research has a
// natural stopping problem — the model does not know when it has enough, so left
// alone it either stops after one search or keeps looking forever.
//
// This bounds it from both sides: a gather phase that tells the model to look
// before answering, and a closing phase that tells it to answer with what it has
// once the round budget is spent. Neither is enforceable inside the model, so the
// strategy's contribution is making the intent explicit at the right moment
// rather than adding a mechanism.
//
// It does not fan out. Parallel searches would make the rounds cheaper but the
// stopping problem harder, and the value here is the bound, not the throughput.
type ResearchStrategy struct {
	// Inner is the wrapped topology. Nil means ReAct.
	Inner Strategy
	// Rounds is how many tool-bearing steps to allow before requiring synthesis.
	// Zero selects two.
	Rounds int
}

const (
	researchStrategyName    = "research"
	researchStrategyVersion = "1.0.0"
	researchRoundsKey       = "research_rounds"
	researchGatherPreamble  = "Before answering, gather evidence: search and read what you need, and cite what you found. Do not answer from memory alone when the task depends on facts you have not checked."
	researchClosingPreamble = "Stop gathering. The evidence you have is what you must answer from. Produce the final answer now, and state plainly what remains unverified rather than searching again."
)

func (s ResearchStrategy) inner() Strategy {
	if s.Inner == nil {
		return ReActStrategy{}
	}
	return s.Inner
}

func (s ResearchStrategy) rounds() int {
	if s.Rounds > 0 {
		return s.Rounds
	}
	return 2
}

func (s ResearchStrategy) Name() string    { return researchStrategyName }
func (s ResearchStrategy) Version() string { return researchStrategyVersion }

// Next delegates, adding the phase-appropriate instruction to model calls.
func (s ResearchStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	action, err := s.inner().Next(ctx, st)
	if err != nil {
		return Action{}, err
	}
	if action.Kind != ActionCallModel {
		return action, nil
	}
	if scratchInt(st, researchRoundsKey) < s.rounds() {
		// The gather instruction is repeated while rounds remain: unlike a
		// one-shot preamble, a model that has already searched once still needs
		// to know that searching again is allowed.
		action.Guidance = append(append([]string{}, action.Guidance...), researchGatherPreamble)
		return action, nil
	}
	action.Guidance = append(append([]string{}, action.Guidance...), researchClosingPreamble)
	return action, nil
}

// OnResult counts rounds that actually gathered something.
//
// A round is a step that ran tools, not a step that happened: counting model
// calls would spend the budget on deliberation and close the phase before any
// evidence existed.
func (s ResearchStrategy) OnResult(st LoopState, result StepResult) LoopState {
	next := s.inner().OnResult(st, result)
	if next.Scratch == nil {
		next.Scratch = map[string]any{}
	}
	if len(result.Tools) > 0 {
		next.Scratch[researchRoundsKey] = scratchInt(st, researchRoundsKey) + 1
	}
	return next
}

// compile-time assertion
var _ Strategy = ResearchStrategy{}
