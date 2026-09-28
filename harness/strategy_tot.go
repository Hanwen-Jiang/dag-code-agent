package harness

import (
	"context"
	"fmt"
)

// TreeOfThoughtsStrategy: explore several approaches, then keep the best.
//
// Distinct from Debate rather than a rename of it. Debate asks for positions on
// one question and judges which is better supported; this asks for different ways
// of doing the task and keeps the one that worked. The evidence they produce is
// different in kind — an argument is not an attempt — so the judging instruction
// differs even though the phase machine does not.
//
// It is breadth over depth: the branches are independent and none is expanded
// further. A real tree would expand the promising branch and try again, which
// needs a second gather and a comparison between rounds; this does one round,
// which is what can be bounded with the evidence available today.
type TreeOfThoughtsStrategy struct {
	// Inner is the wrapped topology. Nil means ReAct.
	Inner Strategy
	// Branches is how many approaches to try. Zero selects two.
	Branches int
	// Approaches are the angle for each branch. Zero values select a default set.
	Approaches []string
}

const (
	totStrategyName    = "tot"
	totStrategyVersion = "1.0.0"
	totPhaseKey        = "tot_phase"
)

func (s TreeOfThoughtsStrategy) inner() Strategy {
	if s.Inner == nil {
		return ReActStrategy{}
	}
	return s.Inner
}

func (s TreeOfThoughtsStrategy) approaches() []string {
	if len(s.Approaches) > 0 {
		return s.Approaches
	}
	return []string{
		"solve the task the straightforward way, and report what you actually did",
		"solve the task by first checking whether the obvious reading is wrong, and report what you found",
	}
}

func (s TreeOfThoughtsStrategy) branches() int {
	count := s.Branches
	if count <= 0 {
		count = len(s.approaches())
	}
	if count > len(s.approaches()) {
		count = len(s.approaches())
	}
	return count
}

func (s TreeOfThoughtsStrategy) Name() string    { return totStrategyName }
func (s TreeOfThoughtsStrategy) Version() string { return totStrategyVersion }

func (s TreeOfThoughtsStrategy) plan() fanoutJudgePlan {
	return fanoutJudgePlan{
		PhaseKey:   totPhaseKey,
		Kind:       ActionSpawn,
		Specs:      s.branchSpecs,
		Judge:      s.judgeInstruction,
		NoAttempts: judgeRefused,
	}
}

func (s TreeOfThoughtsStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	return s.plan().next(ctx, st, s.inner())
}

func (s TreeOfThoughtsStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return s.plan().onResult(st, result, s.inner())
}

// branchSpecs builds one attempt per approach.
func (s TreeOfThoughtsStrategy) branchSpecs(st LoopState) []SubagentSpec {
	approaches := s.approaches()
	specs := make([]SubagentSpec, 0, s.branches())
	for index := 0; index < s.branches(); index++ {
		specs = append(specs, SubagentSpec{
			AgentID:   fmt.Sprintf("branch-%d", index+1),
			Role:      "explorer",
			Objective: fmt.Sprintf("%s. Task: %s", approaches[index], st.Goal),
		})
	}
	return specs
}

// judgeInstruction asks for a decision between attempts, not a summary of them.
//
// A model asked to "consider" several attempts tends to describe all of them and
// answer none, which leaves the run without the thing the fan-out was spent to
// get. The instruction therefore asks for one answer and permits synthesis only
// when the attempts genuinely compose.
func (s TreeOfThoughtsStrategy) judgeInstruction(st LoopState) string {
	return fmt.Sprintf(
		"%d independent attempts at this task are in the observations above, each taking a different approach. "+
			"Compare what each actually achieved and produce the final answer from the attempt that worked. "+
			"Synthesize across them only if they genuinely compose; otherwise pick one and say why the others fell short.",
		len(st.LastSubs))
}

// compile-time assertion
var _ Strategy = TreeOfThoughtsStrategy{}
