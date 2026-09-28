package harness

import (
	"context"
	"fmt"
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

// plan is the shared fan-out-then-judge skeleton, configured for argument.
func (s DebateStrategy) plan() fanoutJudgePlan {
	return fanoutJudgePlan{
		PhaseKey:   debatePhaseKey,
		Kind:       ActionVote,
		Specs:      s.debateSpecs,
		Judge:      s.judgeInstruction,
		NoAttempts: judgeRefused,
	}
}

// Next drives the three phases: argue, judge, then delegate.
func (s DebateStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	return s.plan().next(ctx, st, s.inner())
}

func (s DebateStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return s.plan().onResult(st, result, s.inner())
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
	return fmt.Sprintf(
		"You are the judge. %d positions on this task were argued and their results are in the observations above. "+
			"Weigh them against the task, decide which is better supported, and produce the final answer. "+
			"If the arguments do not settle it, say what would.", len(st.LastSubs))
}

// compile-time assertion
var _ Strategy = DebateStrategy{}
