package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
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

// Runtime executes agent tasks against a durable journal.
//
// The fields are unexported on purpose. Every durable fact the runtime
// publishes must be written through the journal, so that materialized state is
// never updated without the matching event. Use the read-only accessors for
// inspection and build the runtime with New + options to change collaborators.
type Runtime struct {
	store          CoreStore
	journal        *Journal
	model          ModelAdapter
	tools          *ToolRuntime
	contextBuilder *ContextBuilder
	memory         *MemoryManager
	planner        Planner
	verifier       VerifyFunc
	budget         *BudgetLedger
	clock          Clock
	logger         *slog.Logger
	workspaceStore WorkspaceStore
	mailbox        *Mailbox
	ownsStore      bool

	// strategy decides the loop topology; strategies is the registry that
	// names resolve against.
	strategy   Strategy
	strategies *StrategyRegistry
	// loopGuard decides when a run is stuck.
	loopGuard LoopGuard
	// streamBuffer bounds a streamed run's observation queue.
	streamBuffer int
	// subagentPolicy bounds fan-out.
	subagentPolicy SubagentPolicy
	// watchdog bounds how long a live run may take or stay silent.
	watchdog WatchdogPolicy
	// hookBus receives lifecycle events.
	hookBus HookBus
	// scorers ranks fan-out branches by name.
	scorers map[string]Scorer
	// running maps a live task to the function that cancels its context. It is
	// guarded by its own lock rather than r.mu, because r.mu is held for the
	// whole duration of a run: a caller asking to cancel would have to wait for
	// the very thing it is trying to stop.
	runningMu sync.Mutex
	running   map[string]context.CancelCauseFunc
	// baseCtx is the parent of every background task. Background work must be
	// owned by the runtime, not by context.Background(): a task detached from
	// everything would survive Close and leak past the runtime's lifetime.
	baseCtx    context.Context
	baseCancel context.CancelCauseFunc
	// background tracks live background tasks, guarded by its own lock: a
	// background task holds r.mu while it runs, so tracking must not need it.
	backgroundMu sync.Mutex
	background   map[string]*RunHandle
	// newID generates identifiers created during a run; injectable so a
	// deterministic test can assert a byte-identical journal.
	newID func(string) string
	// emitter is the optional runtime-level observation channel (see
	// WithEmitter/SetEmitter in stream.go). It is read while a run holds mu,
	// which is what serializes it against SetEmitter; a nil emitter means the
	// run never leaves the modelled Complete path.
	emitter Emitter

	mu sync.Mutex
}

// Store returns the persistence backend (read-only use).
func (r *Runtime) Store() CoreStore { return r.store }

// Journal returns the event journal owned by this runtime.
func (r *Runtime) Journal() *Journal { return r.journal }

// Model returns the configured model adapter.
func (r *Runtime) Model() ModelAdapter { return r.model }

// Tools returns the tool runtime.
func (r *Runtime) Tools() *ToolRuntime { return r.tools }

// ContextBuilder returns the context compiler.
func (r *Runtime) ContextBuilder() *ContextBuilder { return r.contextBuilder }

// Memory returns the memory manager, if one is wired.
func (r *Runtime) Memory() *MemoryManager { return r.memory }

// Clock returns the time source used by this runtime.
func (r *Runtime) Clock() Clock { return r.clock }

// id creates an identifier with the configured generator.
func (r *Runtime) id(prefix string) string {
	if r.newID == nil {
		return NewID(prefix)
	}
	return r.newID(prefix)
}

// LoopGuard returns the guard deciding when a run is stuck.
func (r *Runtime) LoopGuard() LoopGuard { return r.loopGuard }

// Strategy returns the loop topology this runtime runs.
func (r *Runtime) Strategy() Strategy { return r.strategy }

// Strategies lists the registered topologies, for introspection.
func (r *Runtime) Strategies() []StrategyInfo {
	if r.strategies == nil {
		return nil
	}
	return r.strategies.List()
}

// RegisterStrategy adds a topology to this runtime's registry. It fails when
// the name is already taken at a different version: silently replacing a
// topology would replay historical runs under new semantics.
func (r *Runtime) RegisterStrategy(strategy Strategy) error {
	if r.strategies == nil {
		r.strategies = NewStrategyRegistry()
	}
	return r.strategies.Register(strategy)
}

// WorkspaceStore exposes the optional shared-state projection.
func (r *Runtime) WorkspaceStore() WorkspaceStore { return r.workspaceStore }

// Mailbox exposes the optional protocol-aware collaboration channel.
//
// It returns nil when the configured store does not implement MailboxStore
// (for example a minimal third-party CoreStore), so callers must check for nil
// rather than relying on every call failing.
func (r *Runtime) Mailbox() *Mailbox { return r.mailbox }

// SetModel replaces the model adapter. Unlike the exported field this
// replaces, the swap is guarded so it cannot interleave with a running turn.
func (r *Runtime) SetModel(model ModelAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.model = model
}

// SetVerifier replaces the output verifier.
func (r *Runtime) SetVerifier(verifier VerifyFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.verifier = verifier
}

// Close releases the store when the runtime created it. A store supplied with
// WithStore is owned by the embedder and is left open.
func (r *Runtime) Close() error {
	// Background tasks first: leaving them running against a closed store would
	// turn an orderly shutdown into a burst of store errors.
	if r.baseCancel != nil {
		r.baseCancel(errRuntimeClosed)
	}
	if !r.ownsStore || r.store == nil {
		return nil
	}
	return r.store.Close()
}

// errRuntimeClosed is the cause reported to background tasks stopped by Close.
var errRuntimeClosed = errors.New("harness: runtime closed")

// NewRuntime builds a runtime from explicit collaborators.
//
// Deprecated: use New with WithStore/WithModel/WithTools/WithContextBuilder/
// WithMemoryManager. This constructor is retained for one minor cycle.
func NewRuntime(store CoreStore, model ModelAdapter, tools *ToolRuntime, contextBuilder *ContextBuilder, memory *MemoryManager) (*Runtime, error) {
	return New(
		WithStore(store),
		WithModel(model),
		WithTools(tools),
		WithContextBuilder(contextBuilder),
		WithMemoryManager(memory),
	)
}

// commitEvent is the only path the agent loop uses to publish durable
// lifecycle facts. The journal computes the chain fields and the store
// enforces the optimistic-concurrency preconditions, so a materialized state
// can never be published without the matching event.
func (r *Runtime) commitEvent(state *TaskState, eventType string, payload map[string]any) error {
	_, err := r.journal.Commit(*state, eventType, payload)
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

// failureCategoryStore marks a failure caused by the control plane's own
// storage rather than by the task or the model.
const failureCategoryStore = "STORE"

// ErrInfrastructure reports a failure of the control plane itself (store or
// journal) rather than an outcome of the task. Callers should surface it as an
// operational error, not as "the agent gave up".
var ErrInfrastructure = errors.New("infrastructure failure")

// finish pairs a run result with the error a Go caller expects.
//
// The rule is deliberate (see the embeddability design, §5.4):
//
//   - a completed task, or a task that failed on its own merits (model output
//     rejected, budget exhausted, verification failed), returns (result, nil);
//     callers inspect result.Status and result.FailureCategory.
//   - a cancelled or timed-out context returns (result, ctx.Err()) so callers
//     can observe context.Canceled / context.DeadlineExceeded.
//   - a store/journal failure returns (result, ErrInfrastructure-wrapped err).
func (r *Runtime) finish(ctx context.Context, result RunResult) (RunResult, error) {
	// Every terminal outcome passes through here, which is why the finished
	// event is published at this single point rather than at each return.
	defer func() {
		code := ""
		if value, ok := result.State.LastError["code"].(string); ok {
			code = value
		}
		r.publishHook(ctx, LifecycleEvent{
			Kind: LifecycleTaskFinished, TaskID: result.TaskID,
			TenantID: result.State.TenantID, SubjectID: result.State.SubjectID,
			Step: result.State.Step, Status: string(result.Status),
			ErrorCode: code,
		})
	}()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if result.FailureCategory == failureCategoryStore {
		detail := ""
		if message, ok := result.State.LastError["message"].(string); ok {
			detail = message
		}
		if code, ok := result.State.LastError["code"].(string); ok && detail == "" {
			detail = code
		}
		if detail == "" {
			detail = "control-plane store failure"
		}
		return result, fmt.Errorf("%w: %s", ErrInfrastructure, detail)
	}
	return result, nil
}

// Run executes a task to completion (or to a gate/failure) and returns the
// durable result. Calling it with an existing task id resumes that task
// instead of starting over.
func (r *Runtime) Run(ctx context.Context, request TaskRequest, auth AuthContext) (RunResult, error) {
	result, err := r.finish(ctx, r.run(ctx, request, auth))
	r.logRun(result, err)
	return result, err
}

// logRun reports one durable outcome. It is deliberately debug-level: an
// embedded control plane must not spam the host application's logs.
func (r *Runtime) logRun(result RunResult, err error) {
	if r.logger == nil {
		return
	}
	r.logger.Debug("harness run finished",
		"task_id", result.TaskID,
		"status", result.Status,
		"failure_category", result.FailureCategory,
		"steps", result.State.Step,
		"events", len(result.Events),
		"err", err)
}

// Resume answers an open user gate.
func (r *Runtime) Resume(ctx context.Context, taskID, userMessage string, auth AuthContext) (RunResult, error) {
	return r.finish(ctx, r.resume(ctx, taskID, userMessage, auth))
}

// SendMessage appends a user message to an existing task and continues it.
func (r *Runtime) SendMessage(ctx context.Context, taskID, userMessage string, auth AuthContext) (RunResult, error) {
	return r.finish(ctx, r.sendMessage(ctx, taskID, userMessage, auth))
}

// Cancel marks a task cancelled and durably records the request.
func (r *Runtime) Cancel(taskID string, auth AuthContext) (RunResult, error) {
	return r.finish(context.Background(), r.cancel(taskID, auth))
}

func (r *Runtime) run(ctx context.Context, request TaskRequest, auth AuthContext) RunResult {
	request = request.WithDefaults()
	if err := validateIdentity(request, auth); err != nil {
		return failedResult(request, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Registered under the run lock but signalling through its own, so Cancel
	// reaches a live run instead of queueing behind it.
	runCtx, endRun := r.beginRun(ctx, request.TaskID)
	defer endRun()
	ctx = runCtx
	// Published here rather than in beginRun, which has no identity to report.
	r.publishHook(ctx, LifecycleEvent{
		Kind: LifecycleTaskStarted, TaskID: request.TaskID,
		TenantID: request.TenantID, SubjectID: request.SubjectID,
	})
	if existing, _, err := r.store.GetTask(request.TaskID); err == nil {
		stored := requestFromState(existing)
		if stored.Goal != request.Goal || stored.TenantID != request.TenantID || stored.SubjectID != request.SubjectID {
			return failedStateResult(existing, &RuntimeError{Code: "TASK_IDENTITY_MISMATCH", Message: "task id is already bound to another goal or identity", Category: "AUTH"})
		}
		return r.continueRun(ctx, request.TaskID, auth)
	}
	state := TaskState{TaskID: request.TaskID, Goal: request.Goal, TenantID: request.TenantID, SubjectID: request.SubjectID, Status: TaskReady, Metadata: map[string]any{"request": request}}
	state.Normalize()
	if r.planner != nil {
		state.CurrentPlan = r.planner.Plan(request, state)
	}
	if _, err := r.journal.Create(state, "TASK_ACCEPTED", map[string]any{"goal": request.Goal, "tenant_id": request.TenantID, "subject_id": request.SubjectID}); err != nil {
		return failedResult(request, err)
	}
	if len(state.CurrentPlan) > 0 {
		state.Status = TaskRunning
		_, _ = r.journal.Commit(state, "PLAN_COMPILED", map[string]any{"steps": state.CurrentPlan})
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
	return r.continueRunWithLedger(ctx, taskID, auth, nil)
}

// continueRunWithLedger drives a task with its own budget ledger.
//
// It deliberately does not take r.mu: a subagent runs while its parent's run
// holds that lock, so taking it here would deadlock a fan-out against its own
// parent.
func (r *Runtime) continueRunWithLedger(ctx context.Context, taskID string, auth AuthContext, ledger *BudgetLedger) RunResult {
	return r.continueRunFor(ctx, taskID, auth, ledger, r.strategy)
}

// continueRunFor drives a task under an explicit topology.
//
// A subagent runs under a simple one rather than inheriting its parent's.
// Inheriting is wrong in both directions: a debater holding its own debate is
// incoherent, and a topology whose whole purpose is to fan out would fan out
// again inside every child — which is not hypothetical. A debate child did
// exactly that and spent 134 iterations before the loop backstop stopped it.
// The depth limit already says a child may not delegate; running a topology that
// requires delegation contradicts it.
func (r *Runtime) continueRunFor(ctx context.Context, taskID string, auth AuthContext, ledger *BudgetLedger, strategy Strategy) RunResult {
	state, _, err := r.store.GetTask(taskID)
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
	// A journal that already ran under a different topology must not continue
	// under this one: replaying a task with different decisions would produce a
	// trajectory that never happened.
	if err := r.verifyStrategyIdentity(state); err != nil {
		return r.failAndCommit(state, "STRATEGY", err)
	}
	state.Status = TaskRunning
	state.Turns++
	state.CurrentTurnID = r.id("TURN")
	if err := r.commitEvent(&state, "TURN_STARTED", map[string]any{
		"turn_id":  state.CurrentTurnID,
		"turn":     state.Turns,
		"resumed":  state.Step > 0 || len(state.UserMessages) > 0,
		"strategy": strategyIdentity(r.strategy),
	}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	if strategy == nil {
		strategy = ReActStrategy{}
	}
	kernel := &loopKernel{
		runtime:  r,
		strategy: strategy,
		request:  request,
		auth:     auth,
		scratch:  loadStrategyScratch(state),
		rng:      newStrategyRand(state.TaskID, r.strategy),
		ledger:   ledger,
		depth:    taskDepth(state),
	}
	return kernel.run(ctx, state)
}

// strategyIdentity is the journaled form of a topology. Name plus version, so a
// replay can tell whether the decisions that produced a trajectory are the ones
// being replayed.
func strategyIdentity(strategy Strategy) map[string]any {
	if strategy == nil {
		return map[string]any{"name": "none", "version": ""}
	}
	return map[string]any{"name": strategy.Name(), "version": strategy.Version()}
}

// verifyStrategyIdentity compares the topology recorded in the journal with the
// one about to run. Recording without checking would be decorative.
func (r *Runtime) verifyStrategyIdentity(state TaskState) error {
	events, err := r.store.Events(state.TaskID)
	if err != nil || len(events) == 0 {
		return nil
	}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type != "TURN_STARTED" {
			continue
		}
		recorded, ok := events[i].Payload["strategy"].(map[string]any)
		if !ok {
			// Journals written before strategy identity existed carry no claim.
			return nil
		}
		name, _ := recorded["name"].(string)
		version, _ := recorded["version"].(string)
		current := strategyIdentity(r.strategy)
		if name != current["name"] || version != current["version"] {
			return fmt.Errorf("%w: task ran under %s@%s but this runtime uses %v@%v",
				ErrStrategyContract, name, version, current["name"], current["version"])
		}
		return nil
	}
	return nil
}

// loadStrategyScratch reads strategy-private state from the task metadata, so a
// resumed run observes what the strategy left behind.
func loadStrategyScratch(state TaskState) map[string]any {
	scratch := map[string]any{}
	raw, ok := state.Metadata[strategyScratchKey]
	if !ok {
		return scratch
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return scratch
	}
	if err := json.Unmarshal(encoded, &scratch); err != nil {
		return map[string]any{}
	}
	return scratch
}

// strategyScratchKey is where strategy-private state is persisted. Keeping it
// in the task metadata means it travels with the checkpoint for free.
const strategyScratchKey = "strategy_scratch"

// newStrategyRand seeds the sanctioned randomness source from the task and the
// topology, so a replayed run draws the same numbers.
func newStrategyRand(taskID string, strategy Strategy) *rand.Rand {
	name := "none"
	if strategy != nil {
		name = strategy.Name() + "@" + strategy.Version()
	}
	seed, _ := Digest(map[string]any{"task_id": taskID, "strategy": name})
	var h uint64
	for _, b := range []byte(seed) {
		h = h*1099511628211 ^ uint64(b)
	}
	return rand.New(rand.NewSource(int64(h)))
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
	events, _ := r.store.Events(state.TaskID)
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

func (r *Runtime) resume(ctx context.Context, taskID, userMessage string, auth AuthContext) RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	runCtx, endRun := r.beginRun(ctx, taskID)
	defer endRun()
	ctx = runCtx
	state, _, err := r.store.GetTask(taskID)
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
	if _, err := r.journal.Commit(state, "USER_GATE_RESUMED", map[string]any{"message": userMessage}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	return r.continueRun(ctx, taskID, auth)
}

// SendMessage is the conversational session entrypoint. Unlike Resume, it
// also permits a completed or failed task to start a fresh turn, so a client
// can keep one session while iterating on a code change.
func (r *Runtime) sendMessage(ctx context.Context, taskID, userMessage string, auth AuthContext) RunResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	runCtx, endRun := r.beginRun(ctx, taskID)
	defer endRun()
	ctx = runCtx
	state, _, err := r.store.GetTask(taskID)
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
	if _, err := r.journal.Commit(state, "USER_MESSAGE_RECEIVED", map[string]any{"message": userMessage}); err != nil {
		return r.failAndCommit(state, "STORE", err)
	}
	return r.continueRun(ctx, taskID, auth)
}
func (r *Runtime) cancel(taskID string, auth AuthContext) RunResult {
	// Signal a live run FIRST and without the run lock. Waiting for r.mu here
	// would block until the run finished, which is precisely what cancellation
	// is supposed to avoid. The run observes the cancelled context and durably
	// records its own termination; the lock is taken below only to cover a task
	// that is not running at all.
	if cancel := r.runCancelFunc(taskID); cancel != nil {
		cancel(ErrCancelledByCaller)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state, _, err := r.store.GetTask(taskID)
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
	_, _ = r.journal.Commit(state, "TASK_CANCELLED", state.LastError)
	return r.result(state, "")
}
func (r *Runtime) Recover(taskID string) (TaskState, error) {
	return r.journal.ReplayState(taskID)
}
func (r *Runtime) Replay(taskID string) ([]Event, error) { return r.journal.Replay(taskID) }

// taskDepth reads how deep in a fan-out a task is. It is read from the journaled
// state rather than from the caller, so the limit cannot be bypassed by a
// request that claims to be shallower than it is.
func taskDepth(state TaskState) int {
	if depth, ok := state.Metadata[subagentDepthKey].(int); ok && depth > 0 {
		return depth
	}
	if depth, ok := state.Metadata[subagentDepthKey].(float64); ok && depth > 0 {
		return int(depth)
	}
	return 0
}

// registerRun records how to cancel a live task.
func (r *Runtime) registerRun(taskID string, cancel context.CancelCauseFunc) {
	r.runningMu.Lock()
	defer r.runningMu.Unlock()
	if r.running == nil {
		r.running = map[string]context.CancelCauseFunc{}
	}
	r.running[taskID] = cancel
}

func (r *Runtime) unregisterRun(taskID string) {
	r.runningMu.Lock()
	defer r.runningMu.Unlock()
	delete(r.running, taskID)
}

// runCancelFunc returns the cancel function for a live task, if any.
func (r *Runtime) runCancelFunc(taskID string) context.CancelCauseFunc {
	r.runningMu.Lock()
	defer r.runningMu.Unlock()
	return r.running[taskID]
}

// beginRun makes a run cancellable and returns the context it should use.
//
// Every entry point that can hold the runtime lock for a long time goes through
// this, so Cancel works against a running task rather than only against one that
// has already stopped.
func (r *Runtime) beginRun(ctx context.Context, taskID string) (context.Context, func()) {
	// A cause rather than a bare cancel: "who stopped this and why" belongs in
	// the journal, and a caller-initiated stop is not the same event as a
	// watchdog breaking a stall.
	runCtx, cancel := context.WithCancelCause(ctx)
	r.registerRun(taskID, cancel)

	// Supervision starts with the run, so no caller has to remember to arm it.
	r.startWatchdog(runCtx, cancel, taskID)
	return runCtx, func() {
		r.unregisterRun(taskID)
		cancel(nil)
	}
}

// ErrCancelledByCaller records an explicit cancellation request. The cause is
// journaled, so an operator can tell a deliberate stop apart from a watchdog
// breaking a stall without reading logs.
var ErrCancelledByCaller = errors.New("cancelled by caller")
