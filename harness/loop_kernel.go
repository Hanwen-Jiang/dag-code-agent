package harness

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
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
	// ctx is the run's context. Terminal branches commit TURN_COMPLETED through
	// it so the per-run observation channel can report how many frames it had
	// to drop (see turnCompletedPayload in stream.go).
	ctx context.Context
	// ledger is this run's budget. A subagent gets its own bounded slice of the
	// parent's, so one runaway child cannot spend the whole tree's budget.
	ledger *BudgetLedger
	// depth is how deep in a fan-out this run is.
	depth int
}

// budget returns the ledger this run charges against.
func (k *loopKernel) budget() *BudgetLedger {
	if k.ledger != nil {
		return k.ledger
	}
	return k.runtime.budget
}

// run is the kernel loop. It returns exactly what the pre-strategy loop
// returned for the ReAct topology.
func (k *loopKernel) run(ctx context.Context, state TaskState) RunResult {
	r := k.runtime
	request := k.request
	taskID := state.TaskID
	k.ctx = ctx

	// Every iteration must consume something. The loop below is bounded by
	// state.Step, and Step only advances on a MODEL CALL — so a strategy
	// emitting actions that never call the model (ActionCallTools, ActionSpawn)
	// would spin forever with no budget consumed and nothing to stop it,
	// hanging the run while it holds the runtime lock. That is reachable from
	// any custom strategy, so it needs a backstop rather than a convention.
	maxIterations := request.MaxSteps + request.MaxToolCalls + maxLoopSlack
	iteration := 0
	// Loop detection is owned by the guard, which keeps its own per-task
	// history. Clearing it when the run ends bounds that state and gives a
	// resumed turn a fresh window.
	if r.loopGuard != nil {
		defer r.loopGuard.Reset(taskID)
	}
	// The strategy sees a read-only projection of the task state.
	view := k.loopState(state)

	for state.Step < request.MaxSteps {
		if iteration >= maxIterations {
			return r.failAndCommit(state, "LIMIT", fmt.Errorf("MAX_ITERATIONS_EXCEEDED (%d): the strategy is repeating an action that consumes no budget", maxIterations))
		}
		iteration++
		// Cancellation is checked before anything durable happens.
		select {
		case <-ctx.Done():
			state.Status = TaskCancelled
			state.LastError = map[string]any{"code": "CANCELLED", "message": ctx.Err().Error()}
			_ = r.commitEvent(&state, "TURN_COMPLETED", r.turnCompletedPayload(ctx, map[string]any{"turn_id": state.CurrentTurnID, "status": TaskCancelled, "reason": "context_cancelled"}))
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
			result, nextState, terminal := k.stepModel(ctx, state)
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionCallTools:
			// A step that runs tools without asking the model. Strategies that
			// want this (plan-execute, retry-a-tool) emit it explicitly.
			result, nextState, terminal := k.stepTools(ctx, state, action.Tools)
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionSpawn:
			// The strategy decides to delegate. The model-proposed path exists
			// too, and both converge on the same fan-out so the journal does not
			// depend on which one asked.
			result, nextState, terminal := k.executeFanout(ctx, state, state.Step, action.Subagents, "strategy")
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionHandoff:
			// A handoff is a fan-out of exactly one. The decided semantics are
			// that a subagent RETURNS its result, so a handoff cannot transfer
			// task ownership: the parent keeps the journal and produces the
			// final answer from what came back.
			result, nextState, terminal := k.executeFanout(ctx, state, state.Step, []SubagentSpec{*action.Handoff}, "handoff")
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionVote:
			// Several children answer the same question and the parent acts on
			// the majority. The voters are ordinary subagent specs, so a vote
			// costs the same as a fan-out and reuses its durability.
			result, nextState, terminal := k.executeFanout(ctx, state, state.Step, action.Subagents, "vote")
			state = nextState
			if terminal != nil {
				return *terminal
			}
			view = k.strategy.OnResult(k.refresh(view, state), result)

		case ActionSearchTools:
			// The strategy asks for tool schemas explicitly, which is what makes
			// a deferred catalogue usable by a strategy rather than only by the
			// model.
			result, nextState, terminal := k.stepSearchTools(ctx, state, action.Search)
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
		Budget:   budgetSnapshot(k.budget()),
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
	view.Budget = budgetSnapshot(k.budget())
	view.Scratch = k.scratch
	return view
}

// stepModel performs one model step: compile context, call the model, validate
// the output and execute whatever the response proposed.
//
// It returns the StepResult for the strategy, the updated state, and a
// non-nil result when the run reached a terminal state inside the step.
func (k *loopKernel) stepModel(ctx context.Context, state TaskState) (StepResult, TaskState, *RunResult) {
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

	packet, err := r.contextBuilder.BuildWithContext(ctx, request, k.auth, state, nil)
	if err != nil {
		result := r.failAndCommit(state, "CONTEXT", err)
		return StepResult{}, state, &result
	}
	modelRequest := ModelRequest{TaskID: state.TaskID, Messages: packet.ProviderMessages(), Tools: packet.Tools, ResponseSchema: request.ResponseSchema, Model: request.Model, ReasoningEffort: request.ReasoningEffort, MaxOutputTokens: request.MaxOutputTokens, PrefixCacheKey: packet.PrefixCacheKey}
	requestDigest, _ := Digest(modelRequest)
	state.LastModelRequestDigest = requestDigest
	contextPayload := map[string]any{
		"turn_id":          state.CurrentTurnID,
		"step":             stepNumber,
		"compiler_version": r.contextBuilder.CompilerVersion,
		"estimated_tokens": packet.EstimatedTokens,
		"budget_tokens":    packet.BudgetTokens,
		"prefix_cache_key": packet.PrefixCacheKey,
		"prefix_cache_hit": packet.PrefixCacheHit,
		"exact_invariants": packet.ExactInvariants,
	}
	// The compaction decision is only recorded when it happened, so a run that
	// fits its budget produces exactly the journal it produced before the
	// context-lifecycle work landed.
	if packet.Plan.Compacted {
		contextPayload["context_plan"] = packet.Plan.Payload()
	}
	if err := r.commitEvent(&state, "CONTEXT_COMPILED", contextPayload); err != nil {
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
	response, err := r.completeModel(ctx, &state, modelRequest)
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
	if ledger := k.budget(); ledger != nil {
		if err := ledger.Charge(response.Usage, len(response.ToolCalls)); err != nil {
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
		result, nextState, terminal := k.stepTools(ctx, state, response.ToolCalls)
		result.Model = &model
		result.ModelText = response.Text
		return result, nextState, terminal
	}
	if len(response.SubagentRequests) > 0 {
		return k.stepSubagents(ctx, state, stepNumber, model, response)
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
func (k *loopKernel) stepTools(ctx context.Context, state TaskState, calls []ToolCall) (StepResult, TaskState, *RunResult) {
	r := k.runtime
	request := k.request
	stepNumber := state.Step

	if state.ToolCalls+len(calls) > request.MaxToolCalls {
		result := r.failAndCommit(state, "BUDGET", errors.New("TOOL_BUDGET_EXCEEDED"))
		return StepResult{}, state, &result
	}
	// Loop detection runs before anything executes, so a batch the guard has
	// already flagged does not spend budget confirming what it suspects.
	guarded, terminal, nudged := k.applyLoopGuard(state, calls)
	if terminal != nil {
		return StepResult{}, guarded, terminal
	}
	if nudged {
		// The batch is skipped, not merely annotated. Running a call the guard
		// just flagged spends budget to reinforce the loop the model is in.
		return StepResult{Action: ActionCallModel}, guarded, nil
	}
	state = guarded

	// A batch only runs concurrently when every member declares itself safe.
	if r.tools.batchCanRunConcurrently(calls) {
		return k.stepToolsConcurrent(ctx, state, calls)
	}
	toolObservations := []map[string]any{}
	results := make([]ToolResult, 0, len(calls))
	for _, call := range calls {
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
	_ = r.commitEvent(&state, "TURN_COMPLETED", r.turnCompletedPayload(k.ctx, map[string]any{"turn_id": state.CurrentTurnID, "status": TaskWaitingUser}))
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
	if err := r.commitEvent(&state, "TURN_COMPLETED", r.turnCompletedPayload(k.ctx, map[string]any{"turn_id": state.CurrentTurnID, "status": TaskCompleted})); err != nil {
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

// stepToolsConcurrent runs a batch of declared-safe tools in parallel.
//
// Replay safety is the constraint that shapes this: the journal must not depend
// on which tool happened to finish first. So the invariant is that RESULTS ARE
// COMMITTED IN REQUEST ORDER. Execution is concurrent; the loop-detection pass,
// the event order and the observation order are all driven by the input slice.
//
// The batch events are emitted only here, which is why a single-tool step (or a
// batch containing an undeclared tool) keeps the exact event sequence it had
// before parallel execution existed.
func (k *loopKernel) stepToolsConcurrent(ctx context.Context, state TaskState, calls []ToolCall) (StepResult, TaskState, *RunResult) {
	r := k.runtime
	stepNumber := state.Step

	batch := make([]map[string]any, 0, len(calls))
	for _, call := range calls {
		batch = append(batch, toolCallPayload(call))
	}
	if err := r.commitEvent(&state, "TOOL_BATCH_STARTED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": stepNumber, "parallelism": r.tools.parallelism(), "calls": batch,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	for _, call := range calls {
		if err := r.commitEvent(&state, "TOOL_INVOKED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "call": toolCallPayload(call)}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
	}

	results := make([]ToolResult, len(calls))
	sem := make(chan struct{}, r.tools.parallelism())
	var wg sync.WaitGroup
	for i := range calls {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[index] = ToolResult{CallID: calls[index].CallID, Name: calls[index].Name, Status: ToolFailed, ErrorCode: "CANCELLED", ErrorMessage: ctx.Err().Error()}
				return
			}
			defer func() { <-sem }()
			results[index] = r.tools.Execute(ctx, calls[index], k.auth)
		}(i)
	}
	wg.Wait()

	// Observations are appended and committed in request order, so the journal
	// is identical to a sequential run of the same batch.
	toolObservations := []map[string]any{}
	for i, call := range calls {
		state.ToolCalls++
		observation := map[string]any{"tool": call.Name, "call_id": call.CallID, "status": results[i].Status, "value": results[i].Value, "error_code": results[i].ErrorCode, "attempts": results[i].Attempts}
		toolObservations = append(toolObservations, observation)
		state.Observations = append(state.Observations, observation)
		if err := r.commitEvent(&state, "TOOL_OBSERVED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "call_id": call.CallID, "observation": observation}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
	}
	if err := r.commitEvent(&state, "TOOL_BATCH_COMPLETED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": stepNumber, "results": toolObservations,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
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

// applyLoopGuard consults the loop guard for a batch about to run.
//
// The guard is the only component that decides a run is stuck. Its Nudge verdict
// is the reason the seam exists at all: the previous implementation could only
// kill the task, whereas a nudge tells the model what pattern it repeated and
// lets it try something else. A nudge skips the batch — re-running a call the
// guard has already flagged as unproductive spends budget to reinforce the loop.
//
// The returned RunResult is non-nil only for a terminal verdict. A nudge returns
// the state with the directive recorded and no terminal result.
func (k *loopKernel) applyLoopGuard(state TaskState, calls []ToolCall) (TaskState, *RunResult, bool) {
	r := k.runtime
	if r.loopGuard == nil || len(calls) == 0 {
		return state, nil, false
	}
	verdict := r.loopGuard.Check(LoopObservation{
		TaskID:    state.TaskID,
		Step:      state.Step,
		ToolCalls: calls,
	})
	switch verdict.Decision {
	case LoopForceStop:
		err := errors.New("LOOP_DETECTED")
		if verdict.Message != "" {
			err = fmt.Errorf("LOOP_DETECTED: %s", verdict.Message)
		}
		result := r.failAndCommit(state, "LOOP", err)
		return state, &result, false
	case LoopNudge:
		directive := verdict.Directive()
		state.Observations = append(state.Observations, map[string]any{
			"type": "loop_nudge", "message": verdict.Message, "evidence": verdict.Evidence, "directive": directive,
		})
		if err := r.commitEvent(&state, "LOOP_NUDGE", map[string]any{
			"turn_id": state.CurrentTurnID, "step": state.Step,
			"message": verdict.Message, "evidence": verdict.Evidence, "directive": directive,
		}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return state, &result, false
		}
		if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{
			"turn_id": state.CurrentTurnID, "step": state.Step, "outcome": "loop_nudge",
		}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return state, &result, false
		}
		return state, nil, true
	default:
		return state, nil, false
	}
}

// stepSubagents executes a fan-out and merges the results into the parent.
//
// Every child is a durable task running the same kernel loop, so a fan-out is
// resumable and auditable rather than a set of in-memory calls. The parent never
// yields ownership: it commits every event, and the children's contributions
// arrive as an observation it can read on the next model call.
func (k *loopKernel) stepSubagents(ctx context.Context, state TaskState, stepNumber int, model ModelResponse, response ModelResponse) (StepResult, TaskState, *RunResult) {
	r := k.runtime

	if err := r.commitEvent(&state, "SUBAGENTS_REQUESTED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": stepNumber,
		"requests": response.SubagentRequests, "count": len(response.SubagentRequests),
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}

	specs := subagentSpecsFromProposals(response.SubagentRequests)
	if len(specs) == 0 {
		// Every proposal was unusable (no objective). Failing the step is
		// clearer than silently continuing with no work done.
		result := r.failAndCommit(state, "MODEL_OUTPUT", errors.New("SUBAGENT_PROPOSALS_INVALID"))
		return StepResult{}, state, &result
	}
	result, next, terminal := k.executeFanout(ctx, state, stepNumber, specs, "model")
	if terminal != nil {
		return result, next, terminal
	}
	// The strategy still needs to see what the model proposed, which the shared
	// fan-out path has no reason to know about.
	result.Model = &model
	result.ModelText = response.Text
	return result, next, nil
}

// executeFanout runs a fan-out and folds the results into the parent.
//
// Shared by the two ways a fan-out can start, because they differ only in who
// decided: the model proposing subagents, or the strategy emitting ActionSpawn
// itself. The journal must not depend on which, so the events, ordering and
// merge are identical and only the origin field differs.
func (k *loopKernel) executeFanout(ctx context.Context, state TaskState, stepNumber int, specs []SubagentSpec, origin string) (StepResult, TaskState, *RunResult) {
	r := k.runtime
	policy := r.subagentPolicy
	if policy.MaxParallel <= 0 {
		policy = DefaultSubagentPolicy()
	}

	if k.depth >= policy.MaxDepth {
		// Nesting is refused rather than truncated: a task that asked to
		// delegate must learn it cannot, not silently receive nothing.
		if err := r.commitEvent(&state, "SUBAGENT_REJECTED", map[string]any{
			"turn_id": state.CurrentTurnID, "step": stepNumber,
			"reason": ErrSubagentDepthExceeded.Error(), "depth": k.depth, "max_depth": policy.MaxDepth,
		}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
		state.Observations = append(state.Observations, map[string]any{
			"type": "subagents_rejected", "reason": ErrSubagentDepthExceeded.Error(), "max_depth": policy.MaxDepth,
		})
		if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{
			"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "subagents_rejected",
		}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
		return StepResult{Action: ActionCallModel}, state, nil
	}

	for _, spec := range specs {
		if err := r.commitEvent(&state, "SUBAGENT_SPAWNED", map[string]any{
			"turn_id": state.CurrentTurnID, "step": stepNumber,
			"agent_id": spec.AgentID, "role": spec.Role, "objective": spec.Objective, "max_steps": spec.MaxSteps,
			"origin": origin,
		}); err != nil {
			result := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result
		}
	}

	// A model can ask to fan out on every single step, and the detectors reason
	// about repeated tool calls, so spawns are presented in that shape. Without
	// this an unbounded fan-out is invisible to the guard: it would see no
	// repetition at all while the task spawned children forever.
	spawnCalls := make([]ToolCall, 0, len(specs))
	for _, spec := range specs {
		spawnCalls = append(spawnCalls, ToolCall{
			CallID: "subagent:" + spec.AgentID, Name: "subagent",
			Arguments: map[string]any{"agent_id": spec.AgentID, "objective": spec.Objective},
		})
	}
	guarded, terminal, nudged := k.applyLoopGuard(state, spawnCalls)
	if terminal != nil {
		return StepResult{}, guarded, terminal
	}
	state = guarded
	if nudged {
		return StepResult{Action: ActionCallModel}, state, nil
	}

	runner := r.newSubagentRunner(state, k.auth, k.budget())
	results := runner.run(ctx, specs)

	for _, result := range results {
		eventType := "SUBAGENT_COMPLETED"
		if result.Status != "SUCCEEDED" {
			eventType = "SUBAGENT_FAILED"
		}
		if err := r.commitEvent(&state, eventType, map[string]any{
			"turn_id": state.CurrentTurnID, "step": stepNumber,
			"agent_id": result.AgentID, "status": result.Status, "summary": result.Summary, "error": result.Error,
		}); err != nil {
			result2 := r.failAndCommit(state, "STORE", err)
			return StepResult{}, state, &result2
		}
	}

	merged := mergeSubagentResults(results, policy.RequireConsensus, origin == "vote")
	if err := r.commitEvent(&state, "SUBAGENTS_MERGED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": stepNumber, "merged": merged,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	state.Observations = append(state.Observations, map[string]any{"type": "subagents_merged", "merged": merged})
	if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "subagents",
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	return StepResult{Action: ActionCallModel, SubagentResults: results}, state, nil
}

// stepSearchTools resolves a tool query into schemas and hands them to the model.
//
// It calls the same tool_search handler the model would call, so a strategy has
// no privileged path: the query is subject to the same scope filtering, and it
// cannot surface a tool the caller is not allowed to use.
func (k *loopKernel) stepSearchTools(ctx context.Context, state TaskState, query string) (StepResult, TaskState, *RunResult) {
	r := k.runtime
	if r.tools == nil || r.tools.Registry == nil {
		result := r.failAndCommit(state, "STRATEGY", errors.New("SEARCH_TOOLS_UNAVAILABLE: no tool registry"))
		return StepResult{}, state, &result
	}
	spec, ok := r.tools.Registry.Get(toolSearchToolName)
	if !ok {
		// Telling the strategy how to fix this is more useful than a bare
		// failure, because the resolver is opt-in.
		result := r.failAndCommit(state, "STRATEGY", fmt.Errorf("SEARCH_TOOLS_UNAVAILABLE: %s is not registered; enable WithToolCatalogThreshold", toolSearchToolName))
		return StepResult{}, state, &result
	}
	value, err := spec.Handler(map[string]any{"query": query}, k.auth)
	if err != nil {
		result := r.failAndCommit(state, "TOOL", err)
		return StepResult{}, state, &result
	}
	observation := map[string]any{"type": "tool_search", "query": query, "result": value}
	state.Observations = append(state.Observations, observation)
	if err := r.commitEvent(&state, "TOOL_SEARCHED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": state.Step, "query": query, "result": value,
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{
		"turn_id": state.CurrentTurnID, "step": state.Step, "outcome": "tool_search",
	}); err != nil {
		result := r.failAndCommit(state, "STORE", err)
		return StepResult{}, state, &result
	}
	return StepResult{Action: ActionSearchTools}, state, nil
}
