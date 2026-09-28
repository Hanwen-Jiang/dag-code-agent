package harness

import (
	"context"
	"strings"
)

// ReflectionStrategy is a decorator over another topology.
//
// The design claimed this would be roughly a forty-line decorator, and it is —
// which is the interesting result: it demonstrates that the strategy seam is
// sufficient for a genuinely different topology without touching the kernel, the
// journal vocabulary or the store.
//
// It intercepts a would-be final answer and asks the model to check that answer
// against the goal, correcting it if needed. That costs one extra model call per
// task and catches the common failure where a model stops at a plausible answer
// without verifying it against what was actually asked.
//
// The pure-Next constraint (contract rule R2) is what shapes the bookkeeping:
// Next may not mutate anything, so the round counter is advanced in OnResult,
// which is the only place a strategy is allowed to fold state back.
type ReflectionStrategy struct {
	// Inner is the wrapped topology. Nil means ReAct.
	Inner Strategy
	// MaxRounds bounds the extra verification passes. Zero means one.
	MaxRounds int
}

const (
	reflectionStrategyName    = "reflection"
	reflectionStrategyVersion = "1.0.0"
	// reflectionRoundsKey counts completed verification passes.
	reflectionRoundsKey = "reflection_rounds"
)

func (s ReflectionStrategy) inner() Strategy {
	if s.Inner == nil {
		return ReActStrategy{}
	}
	return s.Inner
}

func (s ReflectionStrategy) rounds() int {
	if s.MaxRounds <= 0 {
		return 1
	}
	return s.MaxRounds
}

func (s ReflectionStrategy) Name() string    { return reflectionStrategyName }
func (s ReflectionStrategy) Version() string { return reflectionStrategyVersion }

// Next delegates to the inner topology and intercepts a final answer until the
// verification budget is spent.
func (s ReflectionStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	action, err := s.inner().Next(ctx, st)
	if err != nil {
		return Action{}, err
	}
	if action.Kind != ActionFinish {
		return action, nil
	}
	if scratchInt(st, reflectionRoundsKey) >= s.rounds() {
		return action, nil
	}
	return Action{Kind: ActionCallModel, Model: reflectionRequest(st)}, nil
}

// OnResult folds the inner result back and records a completed verification pass.
func (s ReflectionStrategy) OnResult(st LoopState, result StepResult) LoopState {
	next := s.inner().OnResult(st, result)
	if next.Scratch == nil {
		next.Scratch = map[string]any{}
	}
	// A verification pass is exactly "the model was called while a final
	// candidate already existed": the answer had been produced, and the
	// decorator asked for it to be checked instead of accepting it.
	if result.Action == ActionCallModel && strings.TrimSpace(result.ModelText) != "" && st.FinalCandidate != "" {
		next.Scratch[reflectionRoundsKey] = scratchInt(st, reflectionRoundsKey) + 1
	}
	return next
}

// reflectionRequest asks for a verification pass.
//
// It is written as a correction request rather than a critique request: a model
// asked only to critique tends to describe problems and stop, leaving the run
// without a corrected answer. Asking for the answer itself, corrected if
// necessary, keeps the output in the shape the run needs.
func reflectionRequest(st LoopState) *ModelRequest {
	var b strings.Builder
	b.WriteString("Verification pass. Re-read the goal and your answer below.\n\nGoal:\n")
	b.WriteString(st.Goal)
	b.WriteString("\n\nYour answer:\n")
	b.WriteString(st.FinalCandidate)
	b.WriteString("\n\nCheck the answer against the goal and against any evidence you gathered. ")
	b.WriteString("If it is incomplete, unsupported, or does not answer what was actually asked, ")
	b.WriteString("produce the corrected final answer. If it is correct, restate it unchanged. ")
	b.WriteString("Reply with the final answer only.")
	return &ModelRequest{
		TaskID: st.TaskID,
		Messages: []map[string]any{
			{"role": "system", "content": b.String()},
			{"role": "user", "content": st.Goal},
		},
	}
}

// scratchInt reads an int from strategy scratch, tolerating a JSON round-trip
// that turned it into a float.
func scratchInt(st LoopState, key string) int {
	if st.Scratch == nil {
		return 0
	}
	switch value := st.Scratch[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return 0
	}
}

// compile-time assertion
var _ Strategy = ReflectionStrategy{}
