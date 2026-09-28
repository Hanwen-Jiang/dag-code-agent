package harness

import (
	"context"
	"strings"
)

// CoTStrategy is a decorator that asks the model to reason before acting.
//
// It is the first strategy to use the guidance channel, and that is the point of
// building the channel: before it, a strategy could only choose WHICH action to
// take, never what the model saw while taking it. A topology that wanted a
// reasoning preamble had to replace the compiled request — discarding the task's
// own history to add one sentence to it.
//
// The directive is sent on the first model call of a turn and nowhere else. A
// preamble repeated every step is noise the model learns to skip, and it would
// consume budget proportional to the step count for no additional effect.
type CoTStrategy struct {
	// Inner is the wrapped topology. Nil means ReAct.
	Inner Strategy
}

const (
	cotStrategyName    = "cot"
	cotStrategyVersion = "1.0.0"
	cotPreamble        = "Before acting, reason through the task step by step: what is being asked, what you already know, what is missing, and which action closes the gap. Then take the single next action. State your reasoning briefly; do not restate the whole history."
)

func (s CoTStrategy) inner() Strategy {
	if s.Inner == nil {
		return ReActStrategy{}
	}
	return s.Inner
}

func (s CoTStrategy) Name() string    { return cotStrategyName }
func (s CoTStrategy) Version() string { return cotStrategyVersion }

// Next delegates and attaches the reasoning directive while the turn is still
// opening.
//
// "Opening" is read from the state rather than tracked in scratch: the first
// model call of a turn is the one where no step has completed yet, so the
// directive needs no bookkeeping that a resumed run could lose.
func (s CoTStrategy) Next(ctx context.Context, st LoopState) (Action, error) {
	action, err := s.inner().Next(ctx, st)
	if err != nil {
		return Action{}, err
	}
	if action.Kind != ActionCallModel {
		return action, nil
	}
	if !turnIsOpening(st) {
		return action, nil
	}
	action.Guidance = append(append([]string{}, action.Guidance...), cotPreamble)
	return action, nil
}

func (s CoTStrategy) OnResult(st LoopState, result StepResult) LoopState {
	return s.inner().OnResult(st, result)
}

// turnIsOpening reports whether the model has not yet acted this turn.
//
// A turn that has produced an observation, a tool result or a subagent result is
// no longer opening, so the preamble is not repeated.
func turnIsOpening(st LoopState) bool {
	if len(st.LastTool) > 0 || len(st.LastSubs) > 0 {
		return false
	}
	if strings.TrimSpace(st.LastText) != "" {
		return false
	}
	return true
}

// compile-time assertion
var _ Strategy = CoTStrategy{}
