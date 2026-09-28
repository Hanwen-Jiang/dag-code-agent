package harness

import (
	"context"
	"fmt"
	"strings"
)

// DebateStrategy: argue the question from opposing positions, then judge.
//
// It is a different topology from Reflection and CoT in a way that matters — it
// spends a fan-out before answering, on the theory that an answer which survived
// an argument is better than one that did not. Whether that theory holds is the
// embedder's call; the runtime's job is to make the topology expressible and to
// keep it bounded.
//
// Two things it does NOT do, stated so the shape is not mistaken for more than it
// is: the debaters do not see each other's arguments (they run concurrently and
// independently), so this is parallel advocacy rather than an exchange of
// rebuttals; and there is exactly one judging step, not a converging debate.
type DebateStrategy struct {
	// Inner is the wrapped topology. Nil means ReAct.
	Inner Strategy
	// Debaters is how many positions to argue. Zero selects two.
	Debaters int
	// Stances are the positions. Zero values select a default pair.
	Stances []string
}

const (
	debateStrategyName    = "debate"
	debateStrategyVersion = "1.0.0"
	// debatePhaseKey tracks progress through the debate.
	debatePhaseKey = "debate_phase"
)

func (s DebateStrategy) inner() Strategy {
	if s.Inner == nil {
		return ReActStrategy{}
	}
	return s.Inner
}

func (s DebateStrategy) debaters() int {
	if s.Debaters > 0 {
		return s.Debaters
	}
	return len(s.stances())
}

func (s DebateStrategy) stances() []string {
	if len(s.Stances) > 0 {
		return s.Stances
	}
	return []string{
		"argue for the most direct interpretation of the task",
		"argue for the interpretation that is most likely to be wrong if the obvious reading is mistaken",
	}
}

func (s DebateStrategy) Name() string    { return debateStrategyName }
func (s DebateStrategy) Version() string { return debateStrategyVersion }

// Next drives the three phases: argue, judge, then delegate.
func (s DebateStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	switch debatePhase(st) {
	case 0:
		// A refused fan-out must not be retried. Retrying is what a strategy
		// does when it cannot tell refusal from "not yet", and it ends with the
		// loop backstop killing the run rather than with an answer.
		if st.FanoutRejected != nil {
			return Action{Kind: ActionCallModel, Guidance: []string{s.judgeInstruction(st)}}, nil
		}
		return Action{Kind: ActionVote, Subagents: s.debateSpecs(st)}, nil
	case 1:
		return Action{Kind: ActionCallModel, Guidance: []string{s.judgeInstruction(st)}}, nil
	default:
		return s.inner().Next(ctx, st)
	}
}

func (s DebateStrategy) OnResult(st LoopState, result StepResult) LoopState {
	next := s.inner().OnResult(st, result)
	if next.Scratch == nil {
		next.Scratch = map[string]any{}
	}
	// The phase is read from what happened, not from the action that asked for
	// it: a fan-out reports Action=CallModel like any other model step, so
	// branching on the action here would leave the strategy stuck in phase one
	// forever.
	if debatePhase(st) == 0 && (len(result.SubagentResults) > 0 || st.FanoutRejected != nil) {
		// Either the debate happened or it was refused; both mean the strategy
		// moves on rather than asking again.
		next.Scratch[debatePhaseKey] = 1
		return next
	}
	if debatePhase(st) == 1 && result.Action == ActionCallModel && strings.TrimSpace(result.ModelText) != "" {
		next.Scratch[debatePhaseKey] = 2
	}
	return next
}

// debateSpecs builds one debater per stance, bounded by the configured count.
func (s DebateStrategy) debateSpecs(st LoopState) []SubagentSpec {
	stances := s.stances()
	limit := s.debaters()
	if limit > len(stances) {
		limit = len(stances)
	}
	specs := make([]SubagentSpec, 0, limit)
	for index := 0; index < limit; index++ {
		specs = append(specs, SubagentSpec{
			AgentID:   fmt.Sprintf("debater-%d", index+1),
			Role:      "advocate",
			Objective: fmt.Sprintf("%s. Task: %s", stances[index], st.Goal),
		})
	}
	return specs
}

// judgeInstruction tells the judge what to do with the arguments.
//
// It points at the observations instead of repeating the arguments: the merged
// debate is already in the context the model is about to receive, and copying it
// into a directive would send it twice and spend the budget twice.
func (s DebateStrategy) judgeInstruction(st LoopState) string {
	if len(st.LastSubs) == 0 {
		// The debate was refused, so there is nothing to weigh. Saying so keeps
		// the judge from looking for arguments that were never produced.
		return "You are the judge, but the debate could not be held. Answer the task directly, " +
			"and note in one sentence what a second opinion would have added."
	}
	return fmt.Sprintf(
		"You are the judge. %d positions on this task were argued and their results are in the observations above. "+
			"Weigh them against the task, decide which is better supported, and produce the final answer. "+
			"If the arguments do not settle it, say what would.", len(st.LastSubs))
}

// debatePhase reads the phase from strategy scratch, tolerating a JSON
// round-trip that turned the int into a float.
func debatePhase(st LoopState) int {
	return scratchInt(st, debatePhaseKey)
}

// compile-time assertion
var _ Strategy = DebateStrategy{}
