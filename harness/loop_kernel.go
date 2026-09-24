package harness

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
)

// The loop kernel executes Actions and owns every invariant: journal commits,
// budget charging, tool policy, validation, cancellation and replay. A
// Strategy only decides what happens next.
//
// Fidelity note. The kernel must reproduce the pre-strategy event sequence
// exactly, because the journal is hash-chained and each STEP_COMPLETED embeds
// the digest of the state it materializes. Two consequences shaped this file:
//
//  1. Tool execution stays INSIDE the model step. Splitting "run the tools the
//     model just proposed" into its own step would emit an extra STEP_STARTED
//     and change every subsequent event hash.
//  2. Terminal branches commit their own STEP_COMPLETED, after mutating state,
//     exactly where the original inline code did. The model step therefore
//     commits STEP_COMPLETED only for the outcomes that continue the loop.
//
// The payoff is that a different topology can still replace the decisions
// without touching any of this.

// loopKernel drives one run from the current TaskState to a terminal result.
type loopKernel struct {
	runtime  *Runtime
	strategy Strategy
	request  TaskRequest
	auth     AuthContext
	// scratch is strategy-private state, persisted with the task so a resumed
	// run observes the same value.
	scratch map[string]any
	// rng is the sanctioned randomness source. It is seeded from the task id and
	// strategy so a replayed run draws the same numbers.
	rng *rand.Rand
}

// run is the kernel loop. It returns exactly what the pre-strategy loop
// returned for the ReAct topology.
func (k *loopKernel) run(ctx context.Context, state TaskState) RunResult {
	r := k.runtime
	request := k.request
	taskID := state.TaskID

	// Loop-detection counters for this task (preserved from the inline loop).
	seen := r.loopSeen[taskID]
	if seen == nil {
		seen = map[string]int{}
		r.loopSeen[taskID] = seen
	}
	// The strategy sees a read-only projection of the task state.
	view := k.loopState(state)

	for state.Step < request.MaxSteps {
		// Cancellation is checked before anything durable happens.
		select {
		case <-ctx.Done():
			state.Status = TaskCancelled
			state.LastError = map[string]any{"code": "CANCELLED", "message": ctx.Err().Error()}
			_ = r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskCancelled, "reason": "context_cancelled"})
			_ = r.commitEvent(&state, "TASK_CANCELLED", state.LastError)
			return r.result(state, "")
		default:
		}

		action, err := k.strategy.Next(ctx, view)
		if err != nil {
			return r.failAndCommit(state, "STRATEGY", err)
		}
		if err := ValidateAction(action); err != nil {
			return r.failAndCommit(state, "STRATEGY", err)
		}

		switch action.Kind {
		case ActionCallModel:
			result, nextState, terminal := k.stepModel(ctx, state, seen)
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionCallTools:
			// A step that runs tools without asking the model. Strategies that
			// want this (plan-execute, retry-a-tool) emit it explicitly.
			result, nextState, terminal := k.stepTools(ctx, state, action.Tools, seen)
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionAskUser:
			return k.stepAskUser(state, view.NeedUser)

		case ActionFinish:
			return k.stepFinish(state, action.Finish)

		default:
			// Actions whose execution lands in a later milestone must fail
			// loudly rather than silently loop.
			return r.failAndCommit(state, "STRATEGY", fmt.Errorf("%w: strategy %s emitted unsupported action %q", ErrStrategyContract, k.strategy.Name(), action.Kind))
		}
	}
	return r.failAndCommit(state, "LIMIT", errors.New("MAX_STEPS_EXCEEDED"))
}

// loopState projects the durable task state into the read-only view a Strategy
// sees.
func (k *loopKernel) loopState(state TaskState) LoopState {
	return LoopState{
		TaskID:   state.TaskID,
		Goal:     state.Goal,
		TurnID:   state.CurrentTurnID,
		Step:     state.Step,
		Turns:    state.Turns,
		Plan:     append([]PlanStep{}, state.CurrentPlan...),
		Budget:   budgetSnapshot(k.runtime.budget),
		Scratch:  k.scratch,
		Clock:    k.runtime.clock,
		RNG:      k.rng,
		LastText: state.FinalAnswer,
	}
}

// refresh re-reads the counters a strategy may observe after a step.
func (k *loopKernel) refresh(view LoopState, state TaskState) LoopState {
	view.Step = state.Step
	view.Turns = state.Turns
	view.Budget = budgetSnapshot(k.runtime.budget)
	view.Scratch = k.scratch
	return view
}

// stepModel performs one model step: compile context, call the model, validate
// the output and execute whatever the response proposed.
//
// It returns the StepResult for the strategy, the updated state, and a
// non-nil result when the run reached a terminal state inside the step.
func (k *loopKernel) stepModel(ctx context.Context, state TaskState, seen map[string]int) (StepResult, TaskState, *RunResult) {
	r := k.runtime
	request := k.request
	stepNumber := state.Step + 1

	if err := r.commitEvent(&state, "STEP_STARTED", map[string]any{
		"turn_id": state.CurrentTurnID,
		"step":    stepNumber,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}

	packet, err := r.contextBuilder.Build(request, k.auth, state, nil)
	if err != nil {
		result := r.failAndCommit(state, "CONTEXT", err)
		return StepResult{}, state, &result
	}
	modelRequest := ModelRequest{TaskID: state.TaskID, Messages: packet.ProviderMessages(), Tools: packet.Tools, ResponseSchema: request.ResponseSchema, Model: request.Model, ReasoningEffort: request.ReasoningEffort, MaxOutputTokens: request.MaxOutputTokens, PrefixCacheKey: packet.PrefixCacheKey}
	requestDigest, _ := Digest(modelRequest)
	state.LastModelRequestDigest = requestDigest
	if err := r.commitEvent(&state, "CONTEXT_COMPILED", map[string]any{
		"turn_id":          state.CurrentTurnID,
		"step":             stepNumber,
		"compiler_version": r.contextBuilder.CompilerVersion,
		"estimated_tokens": packet.EstimatedTokens,
		"budget_tokens":    packet.BudgetTokens,
		"prefix_cache_key": packet.PrefixCacheKey,
		"prefix_cache_hit": packet.PrefixCacheHit,
		"exact_invariants": packet.ExactInvariants,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	// The complete provider projection is logged before the adapter is called.
	// This is the Harness invariant that model-visible input can be
	// reconstructed from the event journal alone.
	if err := r.commitEvent(&state, "MODEL_REQUESTED", map[string]any{
		"turn_id":        state.CurrentTurnID,
		"step":           stepNumber,
		"request_digest": requestDigest,
		"model_request":  modelRequest,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}

	started := r.clock.Now()
	response, err := r.model.Complete(ctx, modelRequest)
	if err != nil {
		category := "MODEL"
		if me, ok := err.(*ModelError); ok && !me.Retryable {
			category = "MODEL_PERMANENT"
		}
		result := r.failAndCommit(state, category, err)
		return StepResult{}, state, &result
	}
	response.Usage.LatencyMS = float64(r.clock.Now().Sub(started).Microseconds()) / 1000
	state.ModelRound++
	state.Step++
	state.InputTokens += response.Usage.InputTokens
	state.OutputTokens += response.Usage.OutputTokens
	state.CachedTokens += response.Usage.CachedInputTokens
	state.CostUSD += response.Usage.CostUSD
	responseDigest, _ := Digest(response)
	state.LastModelResponseDigest = responseDigest
	if err := r.commitEvent(&state, "MODEL_PROPOSED", map[string]any{
		"turn_id":         state.CurrentTurnID,
		"step":            stepNumber,
		"request_digest":  requestDigest,
		"response_digest": responseDigest,
		"response":        modelResponsePayload(response),
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	if r.budget != nil {
		if err := r.budget.Charge(response.Usage, len(response.ToolCalls)); err != nil {
			result := r.failAndCommit(state, "BUDGET", err)
			return StepResult{}, state, &result
		}
	}

	// A gate outranks everything the response also proposed: running tools
	// after the model asked to pause would act without consent. The gate and
	// final branches stay terminal here so their event order is unchanged.
	model := response
	if response.NeedUser != nil {
		return StepResult{Action: ActionCallModel, Model: &model, ModelText: response.Text, NeedUser: response.NeedUser}, state, nil
	}
	if len(response.ToolCalls) > 0 {
		result, nextState, terminal := k.stepTools(ctx, state, response.ToolCalls, seen)
		result.Model = &model
		result.ModelText = response.Text
		return result, nextState, terminal
	}
	if len(response.SubagentRequests) > 0 {
		// Executing subagents is the next milestone; recording the request
		// preserves today's behaviour and the existing event vocabulary.
		state.Observations = append(state.Observations, map[string]any{"type": "subagents_requested", "count": len(response.SubagentRequests)})
		if err := r.commitEvent(&state, "SUBAGENTS_REQUESTED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "requests": response.SubagentRequests}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
		if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "subagents"}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
		return StepResult{Action: ActionCallModel, Model: &model, ModelText: response.Text}, state, nil
	}

	// No gate, no tools, no subagents: the response must be a final answer.
	if strings.TrimSpace(response.Text) == "" {
		result := r.failAndCommit(state, "MODEL_OUTPUT", errors.New("empty model response"))
		return StepResult{}, state, &result
	}
	if err := ValidateStructured(response.Text, request.ResponseSchema); err != nil {
		result := r.failAndCommit(state, "STRUCTURED_OUTPUT", err)
		return StepResult{}, state, &result
	}
	if r.verifier != nil {
		if err := r.verifier(state, response.Text); err != nil {
			state.RetryCount++
			state.LastError = map[string]any{"code": "VERIFIER_FAILED", "message": err.Error()}
			state.Observations = append(state.Observations, state.LastError)
			if state.RetryCount >= 2 {
				result := r.failAndCommit(state, "VERIFIER", err)
				return StepResult{}, state, &result
			}
			if commitErr := r.commitEvent(&state, "VERIFIER_REJECTED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "error": state.LastError}); commitErr != nil {
				result := r.failAndCommit(state, "STORE", commitErr)
				return StepResult{}, state, &result
			}
			if commitErr := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "verifier_rejected"}); commitErr != nil {
				result := r.failAndCommit(state, "STORE", commitErr)
				return StepResult{}, state, &result
			}
			// A rejection consumes the step and asks the model again.
			return StepResult{Action: ActionCallModel, Model: &model, ModelText: response.Text}, state, nil
		}
	}
	return StepResult{Action: ActionCallModel, Model: &model, ModelText: response.Text, FinalCandidate: response.Text}, state, nil
}

// stepTools runs a batch of tool calls and commits the observations.
func (k *loopKernel) stepTools(ctx context.Context, state TaskState, calls []ToolCall, seen map[string]int) (StepResult, TaskState, *RunResult) {
	r := k.runtime
	request := k.request
	stepNumber := state.Step

	if state.ToolCalls+len(calls) > request.MaxToolCalls {
		result := r.failAndCommit(state, "BUDGET", errors.New("TOOL_BUDGET_EXCEEDED"))
		return StepResult{}, state, &result
	}
	toolObservations := []map[string]any{}
	results := make([]ToolResult, 0, len(calls))
	for _, call := range calls {
		fingerprint, _ := Digest(map[string]any{"name": call.Name, "arguments": call.Arguments})
		seen[fingerprint]++
		if seen[fingerprint] > 3 {
			result := r.failAndCommit(state, "LOOP", errors.New("LOOP_DETECTED"))
			return StepResult{}, state, &result
		}
		if err := r.commitEvent(&state, "TOOL_INVOKED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "call": toolCallPayload(call)}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
		toolResult := r.tools.Execute(ctx, call, k.auth)
		state.ToolCalls++
		observation := map[string]any{"tool": call.Name, "call_id": call.CallID, "status": toolResult.Status, "value": toolResult.Value, "error_code": toolResult.ErrorCode, "attempts": toolResult.Attempts}
		toolObservations = append(toolObservations, observation)
		state.Observations = append(state.Observations, observation)
		results = append(results, toolResult)
		if err := r.commitEvent(&state, "TOOL_OBSERVED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "call_id": call.CallID, "observation": observation}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
	}
	if err := r.commitEvent(&state, "TOOLS_EXECUTED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "results": toolObservations}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "tool_calls"}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	return StepResult{Action: ActionCallTools, Tools: results}, state, nil
}

// stepAskUser opens a user gate. State mutations happen before STEP_COMPLETED
// so the event digest matches the pre-strategy behaviour.
func (k *loopKernel) stepAskUser(state TaskState, gate map[string]any) RunResult {
	r := k.runtime
	if gate == nil {
		gate = map[string]any{}
	}
	state.Status = TaskWaitingUser
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	state.Metadata["pending_user"] = gate
	state.Observations = append(state.Observations, map[string]any{"type": "need_user", "data": gate})
	if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": state.Step, "outcome": "user_gate"}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	if err := r.commitEvent(&state, "USER_GATE_OPENED", map[string]any{"turn_id": state.CurrentTurnID, "gate": gate}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	_ = r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskWaitingUser})
	return r.result(state, "")
}

// stepFinish completes the task with the strategy's answer.
func (k *loopKernel) stepFinish(state TaskState, finish *FinishSpec) RunResult {
	r := k.runtime
	if finish == nil {
		return r.failAndCommit(state, "STRATEGY", fmt.Errorf("%w: finish without a spec", ErrStrategyContract))
	}
	if finish.Category != "" {
		return r.failAndCommit(state, finish.Category, errors.New(stringValueOr(finish.Answer, "strategy requested failure")))
	}
	state.Status = TaskCompleted
	state.FinalAnswer = finish.Answer
	state.Observations = append(state.Observations, map[string]any{"type": "final", "text": finish.Answer})
	if r.memory != nil {
		_, _ = r.memory.Write(k.auth, "episodic", "tasks", state.TaskID, finish.Answer, nil, .8, "", 0)
	}
	if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": state.Step, "outcome": "final"}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	if err := r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskCompleted}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	if err := r.commitEvent(&state, "TASK_COMPLETED", map[string]any{"final_answer": finish.Answer, "model_round": state.ModelRound, "turn_id": state.CurrentTurnID}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	return r.result(state, finish.Answer)
}

func stringValueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
