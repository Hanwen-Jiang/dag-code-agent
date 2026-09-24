package harness

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

type RuntimeError struct {
	Code     string
	Message  string
	Category string
}

func (e *RuntimeError) Error() string {
	if e.Message != "" {
		return e.Code + ": " + e.Message
	}
	return e.Code
}

type VerifyFunc func(TaskState, string) error

type RunResult struct {
	TaskID          string             `json:"task_id"`
	Status          string             `json:"status"`
	State           TaskState          `json:"state"`
	FinalAnswer     string             `json:"final_answer,omitempty"`
	FailureCategory string             `json:"failure_category,omitempty"`
	Events          []Event            `json:"events"`
	Metrics         map[string]float64 `json:"metrics"`
}

type Runtime struct {
	Store    *Store
	Model    ModelAdapter
	Tools    *ToolRuntime
	Context  *ContextBuilder
	Memory   *MemoryManager
	Planner  Planner
	Verifier VerifyFunc
	Budget   *BudgetLedger
	mu       sync.Mutex
	loopSeen map[string]map[string]int
}

func NewRuntime(store *Store, model ModelAdapter, tools *ToolRuntime, contextBuilder *ContextBuilder, memory *MemoryManager) *Runtime {
	return &Runtime{Store: store, Model: model, Tools: tools, Context: contextBuilder, Memory: memory, Planner: RulePlanner{}, Budget: &BudgetLedger{}, loopSeen: map[string]map[string]int{}}
}

// commitEvent is the only path the agent loop uses to publish durable
// lifecycle facts. Keeping the event append next to the materialized state
// update makes the model/tool boundary replayable after a worker restart.
func (r *Runtime) commitEvent(state *TaskState, eventType string, payload map[string]any) error {
	_, err := r.Store.CommitStateEvent(*state, eventType, payload, nil, nil)
	return err
}

func toolCallPayload(call ToolCall) map[string]any {
	return map[string]any{"call_id": call.CallID, "name": call.Name, "arguments": call.Arguments}
}

func modelResponsePayload(response ModelResponse) map[string]any {
	toolCalls := make([]map[string]any, 0, len(response.ToolCalls))
	for _, call := range response.ToolCalls {
		toolCalls = append(toolCalls, toolCallPayload(call))
	}
	return map[string]any{
		"text":              response.Text,
		"tool_calls":        toolCalls,
		"subagent_requests": response.SubagentRequests,
		"need_user":         response.NeedUser,
		"finish_reason":     response.FinishReason,
		"usage":             response.Usage,
	}
}

func (r *Runtime) Run(ctx context.Context, request TaskRequest, auth AuthContext) RunResult {
	request = request.WithDefaults()
	if err := validateIdentity(request, auth); err != nil {
		return failedResult(request, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, _, err := r.Store.GetTask(request.TaskID); err == nil {
		stored := requestFromState(existing)
		if stored.Goal != request.Goal || stored.TenantID != request.TenantID || stored.SubjectID != request.SubjectID {
			return failedStateResult(existing, &RuntimeError{Code: "TASK_IDENTITY_MISMATCH", Message: "task id is already bound to another goal or identity", Category: "AUTH"})
		}
		return r.continueRun(ctx, request.TaskID, auth)
	}
	state := TaskState{TaskID: request.TaskID, Goal: request.Goal, TenantID: request.TenantID, SubjectID: request.SubjectID, Status: TaskReady, Metadata: map[string]any{"request": request}}
	state.Normalize()
	if r.Planner != nil {
		state.CurrentPlan = r.Planner.Plan(request, state)
	}
	if _, err := r.Store.CreateTaskWithEvent(state, "TASK_ACCEPTED", map[string]any{"goal": request.Goal, "tenant_id": request.TenantID, "subject_id": request.SubjectID}); err != nil {
		return failedResult(request, err)
	}
	if len(state.CurrentPlan) > 0 {
		state.Status = TaskRunning
		_, _ = r.Store.CommitStateEvent(state, "PLAN_COMPILED", map[string]any{"steps": state.CurrentPlan}, nil, nil)
	}
	return r.continueRun(ctx, request.TaskID, auth)
}

func validateIdentity(request TaskRequest, auth AuthContext) error {
	if request.TaskID == "" || request.Goal == "" {
		return &RuntimeError{Code: "INVALID_TASK", Message: "task id and goal are required", Category: "INPUT"}
	}
	if request.TenantID != auth.TenantID || request.SubjectID != auth.SubjectID {
		return &RuntimeError{Code: "TASK_IDENTITY_MISMATCH", Message: "tenant or subject does not match auth context", Category: "AUTH"}
	}
	for _, scope := range request.RequiredScopes {
		if !auth.HasScope(scope) {
			return &RuntimeError{Code: "REQUIRED_SCOPE_MISSING", Message: scope, Category: "AUTH"}
		}
	}
	return nil
}

func (r *Runtime) continueRun(ctx context.Context, taskID string, auth AuthContext) RunResult {
	state, _, err := r.Store.GetTask(taskID)
	if err != nil {
		return failedResult(TaskRequest{TaskID: taskID}, err)
	}
	request := requestFromState(state)
	if err := validateIdentity(request, auth); err != nil {
		return failedStateResult(state, err)
	}
	if state.Status == TaskCompleted || state.Status == TaskFailed || state.Status == TaskCancelled {
		return r.result(state, "")
	}
	state.Status = TaskRunning
	state.Turns++
	state.CurrentTurnID = NewID("TURN")
	if err := r.commitEvent(&state, "TURN_STARTED", map[string]any{
		"turn_id": state.CurrentTurnID,
		"turn":    state.Turns,
		"resumed": state.Step > 0 || len(state.UserMessages) > 0,
	}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	seen := r.loopSeen[taskID]
	if seen == nil {
		seen = map[string]int{}
		r.loopSeen[taskID] = seen
	}
	for state.Step < request.MaxSteps {
		select {
		case <-ctx.Done():
			state.Status = TaskCancelled
			state.LastError = map[string]any{"code": "CANCELLED", "message": ctx.Err().Error()}
			_ = r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskCancelled, "reason": "context_cancelled"})
			_ = r.commitEvent(&state, "TASK_CANCELLED", state.LastError)
			return r.result(state, "")
		default:
		}
		stepNumber := state.Step + 1
		if err := r.commitEvent(&state, "STEP_STARTED", map[string]any{
			"turn_id": state.CurrentTurnID,
			"step":    stepNumber,
		}); err != nil {
			return r.failAndCommit(state, "STORE", err)
		}
		packet, err := r.Context.Build(request, auth, state, nil)
		if err != nil {
			return r.failAndCommit(state, "CONTEXT", err)
		}
		modelRequest := ModelRequest{TaskID: taskID, Messages: packet.ProviderMessages(), Tools: packet.Tools, ResponseSchema: request.ResponseSchema, Model: request.Model, ReasoningEffort: request.ReasoningEffort, MaxOutputTokens: request.MaxOutputTokens, PrefixCacheKey: packet.PrefixCacheKey}
		requestDigest, _ := Digest(modelRequest)
		state.LastModelRequestDigest = requestDigest
		if err := r.commitEvent(&state, "CONTEXT_COMPILED", map[string]any{
			"turn_id":          state.CurrentTurnID,
			"step":             stepNumber,
			"compiler_version": r.Context.CompilerVersion,
			"estimated_tokens": packet.EstimatedTokens,
			"budget_tokens":    packet.BudgetTokens,
			"prefix_cache_key": packet.PrefixCacheKey,
			"prefix_cache_hit": packet.PrefixCacheHit,
			"exact_invariants": packet.ExactInvariants,
		}); err != nil {
			return r.failAndCommit(state, "STORE", err)
		}
		// The complete provider projection is logged before the adapter is
		// called. This is the Harness invariant that model-visible input can be
		// reconstructed from the event journal alone.
		if err := r.commitEvent(&state, "MODEL_REQUESTED", map[string]any{
			"turn_id":        state.CurrentTurnID,
			"step":           stepNumber,
			"request_digest": requestDigest,
			"model_request":  modelRequest,
		}); err != nil {
			return r.failAndCommit(state, "STORE", err)
		}
		started := time.Now()
		response, err := r.Model.Complete(ctx, modelRequest)
		if err != nil {
			category := "MODEL"
			if me, ok := err.(*ModelError); ok && !me.Retryable {
				category = "MODEL_PERMANENT"
			}
			return r.failAndCommit(state, category, err)
		}
		response.Usage.LatencyMS = float64(time.Since(started).Microseconds()) / 1000
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
			return r.failAndCommit(state, "STORE", err)
		}
		if r.Budget != nil {
			if err := r.Budget.Charge(response.Usage, len(response.ToolCalls)); err != nil {
				return r.failAndCommit(state, "BUDGET", err)
			}
		}
		if response.NeedUser != nil {
			state.Status = TaskWaitingUser
			state.Metadata["pending_user"] = response.NeedUser
			state.Observations = append(state.Observations, map[string]any{"type": "need_user", "data": response.NeedUser})
			if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "user_gate"}); err != nil {
				return r.failAndCommit(state, "STORE", err)
			}
			if err := r.commitEvent(&state, "USER_GATE_OPENED", map[string]any{"turn_id": state.CurrentTurnID, "gate": response.NeedUser}); err != nil {
				return r.failAndCommit(state, "STORE", err)
			}
			_ = r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskWaitingUser})
			return r.result(state, "")
		}
		if len(response.ToolCalls) > 0 {
			if state.ToolCalls+len(response.ToolCalls) > request.MaxToolCalls {
				return r.failAndCommit(state, "BUDGET", errors.New("TOOL_BUDGET_EXCEEDED"))
			}
			toolObservations := []map[string]any{}
			for _, call := range response.ToolCalls {
				fingerprint, _ := Digest(map[string]any{"name": call.Name, "arguments": call.Arguments})
				seen[fingerprint]++
				if seen[fingerprint] > 3 {
					return r.failAndCommit(state, "LOOP", errors.New("LOOP_DETECTED"))
				}
				if err := r.commitEvent(&state, "TOOL_INVOKED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "call": toolCallPayload(call)}); err != nil {
					return r.failAndCommit(state, "STORE", err)
				}
				result := r.Tools.Execute(ctx, call, auth)
				state.ToolCalls++
				observation := map[string]any{"tool": call.Name, "call_id": call.CallID, "status": result.Status, "value": result.Value, "error_code": result.ErrorCode, "attempts": result.Attempts}
				toolObservations = append(toolObservations, observation)
				state.Observations = append(state.Observations, observation)
				if err := r.commitEvent(&state, "TOOL_OBSERVED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "call_id": call.CallID, "observation": observation}); err != nil {
					return r.failAndCommit(state, "STORE", err)
				}
			}
			if err := r.commitEvent(&state, "TOOLS_EXECUTED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "results": toolObservations}); err != nil {
				return r.failAndCommit(state, "STORE", err)
			}
			if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "tool_calls"}); err != nil {
				return r.failAndCommit(state, "STORE", err)
			}
			continue
		}
		if len(response.SubagentRequests) > 0 {
			state.Observations = append(state.Observations, map[string]any{"type": "subagents_requested", "count": len(response.SubagentRequests)})
			if err := r.commitEvent(&state, "SUBAGENTS_REQUESTED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "requests": response.SubagentRequests}); err != nil {
				return r.failAndCommit(state, "STORE", err)
			}
			if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "subagents"}); err != nil {
				return r.failAndCommit(state, "STORE", err)
			}
			continue
		}
		if strings.TrimSpace(response.Text) == "" {
			return r.failAndCommit(state, "MODEL_OUTPUT", errors.New("empty model response"))
		}
		if err := ValidateStructured(response.Text, request.ResponseSchema); err != nil {
			return r.failAndCommit(state, "STRUCTURED_OUTPUT", err)
		}
		if r.Verifier != nil {
			if err := r.Verifier(state, response.Text); err != nil {
				state.RetryCount++
				state.LastError = map[string]any{"code": "VERIFIER_FAILED", "message": err.Error()}
				state.Observations = append(state.Observations, state.LastError)
				if state.RetryCount >= 2 {
					return r.failAndCommit(state, "VERIFIER", err)
				}
				if commitErr := r.commitEvent(&state, "VERIFIER_REJECTED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "error": state.LastError}); commitErr != nil {
					return r.failAndCommit(state, "STORE", commitErr)
				}
				if commitErr := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "verifier_rejected"}); commitErr != nil {
					return r.failAndCommit(state, "STORE", commitErr)
				}
				continue
			}
		}
		state.Status = TaskCompleted
		state.FinalAnswer = response.Text
		state.Observations = append(state.Observations, map[string]any{"type": "final", "text": response.Text})
		if r.Memory != nil {
			_, _ = r.Memory.Write(auth, "episodic", "tasks", taskID, response.Text, nil, .8, "", 0)
		}
		if err := r.commitEvent(&state, "STEP_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "step": stepNumber, "outcome": "final"}); err != nil {
			return r.failAndCommit(state, "STORE", err)
		}
		if err := r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskCompleted}); err != nil {
			return r.failAndCommit(state, "STORE", err)
		}
		if err := r.commitEvent(&state, "TASK_COMPLETED", map[string]any{"final_answer": response.Text, "model_round": state.ModelRound, "turn_id": state.CurrentTurnID}); err != nil {
			return r.failAndCommit(state, "STORE", err)
		}
		return r.result(state, response.Text)
	}
	return r.failAndCommit(state, "LIMIT", errors.New("MAX_STEPS_EXCEEDED"))
}

func requestFromState(state TaskState) TaskRequest {
	var request TaskRequest
	if raw, ok := state.Metadata["request"]; ok {
		b, _ := json.Marshal(raw)
		_ = json.Unmarshal(b, &request)
	}
	if request.TaskID == "" {
		request.TaskID = state.TaskID
	}
	if request.Goal == "" {
		request.Goal = state.Goal
	}
	if request.TenantID == "" {
		request.TenantID = state.TenantID
	}
	if request.SubjectID == "" {
		request.SubjectID = state.SubjectID
	}
	return request.WithDefaults()
}
func (r *Runtime) failAndCommit(state TaskState, category string, err error) RunResult {
	state.Status = TaskFailed
	state.LastError = map[string]any{"code": category, "message": err.Error()}
	_ = r.commitEvent(&state, "TURN_COMPLETED", map[string]any{"turn_id": state.CurrentTurnID, "status": TaskFailed, "failure_category": category})
	_ = r.commitEvent(&state, "TASK_FAILED", state.LastError)
	return r.resultWithFailure(state, category)
}
func failedResult(request TaskRequest, err error) RunResult {
	category := "INPUT"
	if runtimeErr, ok := err.(*RuntimeError); ok && runtimeErr.Category != "" {
		category = runtimeErr.Category
	}
	state := TaskState{TaskID: request.TaskID, Goal: request.Goal, Status: TaskFailed, LastError: map[string]any{"code": category, "message": err.Error()}}
	return RunResult{TaskID: request.TaskID, Status: TaskFailed, FailureCategory: category, State: state, Metrics: map[string]float64{}}
}
func failedStateResult(state TaskState, err error) RunResult {
	state.Status = TaskFailed
	state.LastError = map[string]any{"code": "AUTH", "message": err.Error()}
	return RunResult{TaskID: state.TaskID, Status: TaskFailed, State: state, FailureCategory: "AUTH", Metrics: map[string]float64{}}
}
func (r *Runtime) result(state TaskState, answer string) RunResult {
	return r.resultWithFailure(state, "")
}
func (r *Runtime) resultWithFailure(state TaskState, category string) RunResult {
	events, _ := r.Store.Events(state.TaskID)
	metrics := map[string]float64{
		"model_rounds":        float64(state.ModelRound),
		"tool_calls":          float64(state.ToolCalls),
		"event_count":         float64(len(events)),
		"steps":               float64(state.Step),
		"turns":               float64(state.Turns),
		"input_tokens":        float64(state.InputTokens),
		"output_tokens":       float64(state.OutputTokens),
		"cached_input_tokens": float64(state.CachedTokens),
		"cost_usd":            state.CostUSD,
	}
	return RunResult{TaskID: state.TaskID, Status: state.Status, State: state, FinalAnswer: state.FinalAnswer, FailureCategory: category, Events: events, Metrics: metrics}
}

func (r *Runtime) Resume(ctx context.Context, taskID, userMessage string, auth AuthContext) RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, _, err := r.Store.GetTask(taskID)
	if err != nil {
		return failedResult(TaskRequest{TaskID: taskID}, err)
	}
	request := requestFromState(state)
	if err := validateIdentity(request, auth); err != nil {
		return failedStateResult(state, err)
	}
	if state.Status != TaskWaitingUser {
		state.LastError = map[string]any{"code": "TASK_NOT_WAITING_USER", "message": "task is not waiting for user input"}
		return r.resultWithFailure(state, "STATE")
	}
	delete(state.Metadata, "pending_user")
	state.UserMessages = append(state.UserMessages, userMessage)
	state.Observations = append(state.Observations, map[string]any{"type": "user_response", "message": userMessage})
	if _, err := r.Store.CommitStateEvent(state, "USER_GATE_RESUMED", map[string]any{"message": userMessage}, nil, nil); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	return r.continueRun(ctx, taskID, auth)
}

// SendMessage is the conversational session entrypoint. Unlike Resume, it
// also permits a completed or failed task to start a fresh turn, so a client
// can keep one session while iterating on a code change.
func (r *Runtime) SendMessage(ctx context.Context, taskID, userMessage string, auth AuthContext) RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, _, err := r.Store.GetTask(taskID)
	if err != nil {
		return failedResult(TaskRequest{TaskID: taskID}, err)
	}
	request := requestFromState(state)
	if err := validateIdentity(request, auth); err != nil {
		return failedStateResult(state, err)
	}
	if strings.TrimSpace(userMessage) == "" {
		return r.resultWithFailure(state, "INPUT")
	}
	state.Status = TaskRunning
	state.FinalAnswer = ""
	state.LastError = nil
	delete(state.Metadata, "pending_user")
	state.UserMessages = append(state.UserMessages, userMessage)
	state.Observations = append(state.Observations, map[string]any{"type": "user_message", "message": userMessage})
	if _, err := r.Store.CommitStateEvent(state, "USER_MESSAGE_RECEIVED", map[string]any{"message": userMessage}, nil, nil); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	return r.continueRun(ctx, taskID, auth)
}
func (r *Runtime) Cancel(taskID string, auth AuthContext) RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, _, err := r.Store.GetTask(taskID)
	if err != nil {
		return failedResult(TaskRequest{TaskID: taskID}, err)
	}
	request := requestFromState(state)
	if err := validateIdentity(request, auth); err != nil {
		return failedStateResult(state, err)
	}
	if state.Status == TaskCompleted || state.Status == TaskFailed || state.Status == TaskCancelled {
		return r.result(state, "")
	}
	state.Status = TaskCancelled
	state.LastError = map[string]any{"code": "CANCELLED", "message": "cancelled by caller"}
	_, _ = r.Store.CommitStateEvent(state, "TASK_CANCELLED", state.LastError, nil, nil)
	return r.result(state, "")
}
func (r *Runtime) Recover(taskID string) (TaskState, error) {
	return r.Store.ReplayState(taskID)
}
func (r *Runtime) Replay(taskID string) ([]Event, error) { return r.Store.Replay(taskID) }
