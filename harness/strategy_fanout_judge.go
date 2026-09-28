package harness

import (
	"context"
	"strings"
)

// Shared skeleton for "gather several independent attempts, then judge".
//
// Debate and Tree of Thoughts are the same phase machine: refuse-if-delegation-
// is-impossible, run a fan-out, then make one judging call whose result becomes
// the answer. Only the prompts and the specs differ.
//
// They were written apart at first, and the duplication was real rather than
// theoretical: the refusal handling below was added to Debate and would have had
// to be added again, identically, to every later topology. A bug fixed in one
// copy and not the other is the failure mode this avoids.
//
// What the skeleton does NOT hide: the phases are read from what happened, not
// from the action that requested it, because a fan-out reports Action=CallModel
// like any other model step.

// fanoutJudgePlan is the topology-specific half.
type fanoutJudgePlan struct {
	// PhaseKey namespaces the scratch counter, so two topologies in one process
	// cannot read each other's phase.
	PhaseKey string
	// Kind is the action that starts the gather.
	Kind ActionKind
	// Specs builds the attempts.
	Specs func(st LoopState) []SubagentSpec
	// Judge builds the judging instruction.
	Judge func(st LoopState) string
	// NoAttempts is the judging instruction when the gather was refused.
	NoAttempts string
}

// next drives the three phases.
func (p fanoutJudgePlan) next(ctx context.Context, st LoopState, inner Strategy) (Action, error) {
	switch scratchInt(st, p.PhaseKey) {
	case 0:
		if st.FanoutRejected != nil {
			// Retrying a refused fan-out is what a strategy does when it cannot
			// tell refusal from "not yet", and it ends with the backstop killing
			// the run rather than with an answer.
			return Action{Kind: ActionCallModel, Guidance: []string{p.NoAttempts}}, nil
		}
		return Action{Kind: p.Kind, Subagents: p.Specs(st)}, nil
	case 1:
		return Action{Kind: ActionCallModel, Guidance: []string{p.Judge(st)}}, nil
	default:
		return inner.Next(ctx, st)
	}
}

// onResult advances the phases and folds the inner strategy's result.
func (p fanoutJudgePlan) onResult(st LoopState, result StepResult, inner Strategy) LoopState {
	next := inner.OnResult(st, result)
	if next.Scratch == nil {
		next.Scratch = map[string]any{}
	}
	if scratchInt(st, p.PhaseKey) == 0 && (len(result.SubagentResults) > 0 || st.FanoutRejected != nil) {
		// Either the gather happened or it was refused; both mean the topology
		// moves on rather than asking again.
		next.Scratch[p.PhaseKey] = 1
		return next
	}
	if scratchInt(st, p.PhaseKey) == 1 && result.Action == ActionCallModel && strings.TrimSpace(result.ModelText) != "" {
		next.Scratch[p.PhaseKey] = 2
	}
	return next
}

// judgeRefused is the shared wording for judging without any attempts.
const judgeRefused = "The attempts could not be gathered. Answer the task directly, and note in one sentence what a second opinion would have added."
