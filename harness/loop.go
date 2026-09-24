package harness

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// This file defines the loop kernel's extension point: the seam between "what
// to do next" (a Strategy) and "how it is durably executed" (the kernel).
//
// The kernel owns every invariant — journal commits, budget charging, tool
// policy, retries, cancellation and replay. A Strategy only decides the next
// Action. That split is what makes a new loop topology a small declaration
// instead of a second copy of the durable machinery.
//
// Contract for implementations (enforced by harness/looptest):
//
//	R1 Actions only. A Strategy never writes to the journal, never mutates
//	   task state, and never executes anything itself; it returns an Action.
//	R2 Determinism. Next and OnResult must be pure functions of (LoopState,
//	   StepResult). No time.Now, no global rand, no I/O — replay requires the
//	   same inputs to produce the same trajectory. Use LoopState.Clock and
//	   LoopState.RNG, whose values are journaled.
//	R3 Bounded. One call returns exactly one Action and must not loop. The
//	   kernel enforces MaxSteps.
//	R4 No privilege escalation. Tool calls are proposed through
//	   ActionCallTools and still pass Authorizer, HookBus and the budget.

// ActionKind enumerates what the kernel can be asked to do.
type ActionKind string

const (
	ActionCallModel   ActionKind = "call_model"
	ActionCallTools   ActionKind = "call_tools"
	ActionSpawn       ActionKind = "spawn_subagents"
	ActionFanout      ActionKind = "fanout"
	ActionVote        ActionKind = "vote"
	ActionHandoff     ActionKind = "handoff"
	ActionRunDAG      ActionKind = "run_dag"
	ActionSearchTools ActionKind = "search_tools"
	ActionAskUser     ActionKind = "ask_user"
	ActionFinish      ActionKind = "finish"
)

// FanoutSpec describes one branch set for tree search or debate topologies.
// It is declared here so strategies can express them, and executed by the
// kernel like any other action.
type FanoutSpec struct {
	Branches    int
	Temperature float64
	Scorer      string   // named scorer the kernel resolves
	Prune       int      // keep the best N branches
	Personas    []string // debate roles
	Rounds      int
	Metadata    map[string]any
}

// FinishSpec terminates a run.
type FinishSpec struct {
	Answer   string
	Category string // empty means the task completed normally
	Metadata map[string]any
}

// Action is one instruction to the kernel. Exactly one payload field is
// meaningful, selected by Kind.
type Action struct {
	Kind      ActionKind
	Model     *ModelRequest
	Tools     []ToolCall
	Subagents []SubagentSpec
	Fanout    *FanoutSpec
	Handoff   *SubagentSpec
	Finish    *FinishSpec
	Search    string
}

// BudgetSnapshot is a read-only view of the ledger, so a strategy can adapt to
// the remaining budget without being able to spend it.
type BudgetSnapshot struct {
	MaxTokens     int
	UsedTokens    int
	MaxToolCalls  int
	UsedToolCalls int
	MaxCostUSD    float64
	UsedCostUSD   float64
}

// RemainingTokens returns the tokens left, or -1 when the budget is unlimited.
func (b BudgetSnapshot) RemainingTokens() int {
	if b.MaxTokens <= 0 {
		return -1
	}
	return b.MaxTokens - b.UsedTokens
}

// RemainingToolCalls returns the tool calls left, or -1 when unlimited.
func (b BudgetSnapshot) RemainingToolCalls() int {
	if b.MaxToolCalls <= 0 {
		return -1
	}
	return b.MaxToolCalls - b.UsedToolCalls
}

// Exhausted reports whether any configured budget has been reached.
func (b BudgetSnapshot) Exhausted() bool {
	if b.MaxTokens > 0 && b.UsedTokens >= b.MaxTokens {
		return true
	}
	if b.MaxToolCalls > 0 && b.UsedToolCalls >= b.MaxToolCalls {
		return true
	}
	return b.MaxCostUSD > 0 && b.UsedCostUSD >= b.MaxCostUSD
}

func budgetSnapshot(ledger *BudgetLedger) BudgetSnapshot {
	if ledger == nil {
		return BudgetSnapshot{}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return BudgetSnapshot{
		MaxTokens:     ledger.MaxTokens,
		UsedTokens:    ledger.UsedTokens,
		MaxToolCalls:  ledger.MaxToolCalls,
		UsedToolCalls: ledger.UsedToolCalls,
		MaxCostUSD:    ledger.MaxCostUSD,
		UsedCostUSD:   ledger.UsedCostUSD,
	}
}

// LoopState is the read-only snapshot a Strategy sees. It is deliberately not
// *TaskState: a strategy cannot assign to any of it, so "a strategy cannot
// corrupt task state" is a property of the type system rather than a rule in a
// document.
type LoopState struct {
	TaskID string
	Goal   string
	TurnID string
	Step   int
	Turns  int
	Plan   []string

	Budget BudgetSnapshot

	// Pending* carry what the previous StepResult proposed and the kernel has
	// not executed yet, so a strategy can decide in which order to run them.
	PendingToolCalls []ToolCall
	PendingSubagents []SubagentSpec

	// LastText is the most recent model text; FinalCandidate is set when that
	// text satisfied the structured-output contract.
	LastText       string
	FinalCandidate string
	NeedUser       map[string]any
	LastTool       []ToolResult
	LastSubs       []SubagentResult

	// Scratch is strategy-private state. The kernel persists it with the
	// checkpoint, so a resumed run observes the same Scratch.
	Scratch map[string]any

	// Clock and RNG are the only sanctioned sources of time and randomness.
	Clock Clock
	RNG   *rand.Rand
}

// Now returns the current time from the sanctioned clock, or the zero time
// when no clock is attached.
func (s LoopState) Now() time.Time {
	if s.Clock == nil {
		return time.Time{}
	}
	return s.Clock.Now()
}

// StepResult is what the kernel reports back after executing an Action.
//
// The kernel owns parsing untrusted model output, so the decision-relevant
// fields (ToolCalls, Subagents, NeedUser, FinalCandidate) arrive already
// normalized: a Strategy never has to interpret a raw map from the model.
// Model is kept for observability and tests.
type StepResult struct {
	Action ActionKind

	Model          *ModelResponse
	ModelText      string
	ToolCalls      []ToolCall
	Subagents      []SubagentSpec
	NeedUser       map[string]any
	FinalCandidate string

	Tools           []ToolResult
	SubagentResults []SubagentResult

	Err error
}

// Strategy decides the next Action. Implementations must honour the contract
// documented at the top of this file.
type Strategy interface {
	// Name identifies the topology; it is journaled with the run.
	Name() string
	// Version participates in replay identity. Bump it whenever the decisions
	// this strategy makes change, or an old task would be replayed under new
	// semantics.
	Version() string
	// Next returns the next action. It must not perform I/O.
	Next(ctx context.Context, st LoopState) (Action, error)
	// OnResult folds a completed step back into the state that Next observes.
	OnResult(st LoopState, result StepResult) LoopState
}

// ErrStrategyContract reports a strategy that violated the loop contract.
var ErrStrategyContract = errors.New("strategy contract violation")

// ValidateAction rejects an Action the kernel cannot execute. Keeping this in
// the core means a custom strategy fails loudly at the seam instead of
// misbehaving midway through a durable run.
func ValidateAction(a Action) error {
	switch a.Kind {
	case ActionCallModel:
		if a.Model == nil {
			return fmt.Errorf("%w: %s requires Model", ErrStrategyContract, a.Kind)
		}
	case ActionCallTools:
		if len(a.Tools) == 0 {
			return fmt.Errorf("%w: %s requires at least one tool call", ErrStrategyContract, a.Kind)
		}
	case ActionSpawn:
		if len(a.Subagents) == 0 {
			return fmt.Errorf("%w: %s requires at least one subagent spec", ErrStrategyContract, a.Kind)
		}
	case ActionHandoff:
		if a.Handoff == nil {
			return fmt.Errorf("%w: %s requires Handoff", ErrStrategyContract, a.Kind)
		}
	case ActionFanout:
		if a.Fanout == nil || a.Fanout.Branches < 1 {
			return fmt.Errorf("%w: %s requires Fanout with at least one branch", ErrStrategyContract, a.Kind)
		}
	case ActionFinish:
		if a.Finish == nil {
			return fmt.Errorf("%w: %s requires Finish", ErrStrategyContract, a.Kind)
		}
	case ActionSearchTools:
		if strings.TrimSpace(a.Search) == "" {
			return fmt.Errorf("%w: %s requires a query", ErrStrategyContract, a.Kind)
		}
	case ActionVote, ActionRunDAG, ActionAskUser:
		// Payload is carried in the action's metadata by the strategies that
		// use them; nothing further to check here.
	default:
		return fmt.Errorf("%w: unknown action kind %q", ErrStrategyContract, a.Kind)
	}
	return nil
}

// ReActStrategy is the topology the runtime has always used: call the model,
// run whatever tools it asked for, feed the observations back, and stop when
// the model produces a final answer. It is the default and the reference
// implementation of the Strategy contract.
type ReActStrategy struct{}

const (
	reactStrategyName    = "react"
	reactStrategyVersion = "1.0.0"
)

func (ReActStrategy) Name() string    { return reactStrategyName }
func (ReActStrategy) Version() string { return reactStrategyVersion }

func (ReActStrategy) Next(_ context.Context, st LoopState) (Action, error) {
	// A user gate outranks everything: the model explicitly asked to pause.
	if st.NeedUser != nil {
		return Action{Kind: ActionAskUser}, nil
	}
	// Execute what the previous model response proposed before asking again.
	if len(st.PendingToolCalls) > 0 {
		return Action{Kind: ActionCallTools, Tools: st.PendingToolCalls}, nil
	}
	if len(st.PendingSubagents) > 0 {
		return Action{Kind: ActionSpawn, Subagents: st.PendingSubagents}, nil
	}
	// A structured final answer that passed the schema contract ends the run.
	if st.FinalCandidate != "" {
		return Action{Kind: ActionFinish, Finish: &FinishSpec{Answer: st.FinalCandidate}}, nil
	}
	if st.Budget.Exhausted() {
		return Action{Kind: ActionFinish, Finish: &FinishSpec{Category: "BUDGET", Metadata: map[string]any{"reason": "budget exhausted"}}}, nil
	}
	return Action{Kind: ActionCallModel, Model: &ModelRequest{TaskID: st.TaskID}}, nil
}

// OnResult folds a step result back into the state the next decision sees.
// The kernel has already journaled, budgeted and normalized the step by this
// point, so only the normalized fields are read here.
func (ReActStrategy) OnResult(st LoopState, result StepResult) LoopState {
	next := st
	next.PendingToolCalls = nil
	next.PendingSubagents = nil
	next.NeedUser = nil
	next.LastTool = nil
	next.LastSubs = nil
	switch result.Action {
	case ActionCallTools:
		next.FinalCandidate = ""
		next.LastTool = result.Tools
	case ActionSpawn, ActionHandoff:
		next.FinalCandidate = ""
		next.LastSubs = result.SubagentResults
	case ActionCallModel:
		next.LastText = result.ModelText
		next.PendingToolCalls = result.ToolCalls
		next.PendingSubagents = result.Subagents
		next.NeedUser = result.NeedUser
		next.FinalCandidate = result.FinalCandidate
	}
	return next
}

// Compile-time assertions for the strategies shipped in the core.
var _ Strategy = ReActStrategy{}

// StrategyInfo describes a registered strategy for introspection.
type StrategyInfo struct {
	Name    string
	Version string
}

// StrategyRegistry holds the strategies a runtime can run. Built-ins are
// registered by New; embedders add their own with RegisterStrategy.
type StrategyRegistry struct {
	strategies map[string]Strategy
}

func NewStrategyRegistry(strategies ...Strategy) *StrategyRegistry {
	registry := &StrategyRegistry{strategies: map[string]Strategy{}}
	for _, strategy := range strategies {
		if strategy == nil {
			continue
		}
		registry.strategies[strategy.Name()] = strategy
	}
	return registry
}

// Register adds a strategy. Re-registering the same name with a different
// version is rejected: replacing a topology silently would make historical
// runs replay under different semantics.
func (r *StrategyRegistry) Register(strategy Strategy) error {
	if r == nil || strategy == nil {
		return fmt.Errorf("%w: nil strategy", ErrStrategyContract)
	}
	name := strings.TrimSpace(strategy.Name())
	if name == "" {
		return fmt.Errorf("%w: strategy name is required", ErrStrategyContract)
	}
	if strings.TrimSpace(strategy.Version()) == "" {
		return fmt.Errorf("%w: strategy %s has no version", ErrStrategyContract, name)
	}
	if r.strategies == nil {
		r.strategies = map[string]Strategy{}
	}
	if existing, ok := r.strategies[name]; ok {
		if existing.Version() == strategy.Version() {
			return fmt.Errorf("%w: strategy %s@%s already registered", ErrStrategyContract, name, strategy.Version())
		}
		return fmt.Errorf("%w: strategy %s@%s already registered (cannot silently replace %s)", ErrStrategyContract, name, strategy.Version(), existing.Version())
	}
	r.strategies[name] = strategy
	return nil
}

// Get resolves a strategy by name.
func (r *StrategyRegistry) Get(name string) (Strategy, bool) {
	if r == nil {
		return nil, false
	}
	strategy, ok := r.strategies[name]
	return strategy, ok
}

// List returns the registered strategies in name order, for introspection and
// for /health style endpoints.
func (r *StrategyRegistry) List() []StrategyInfo {
	if r == nil {
		return nil
	}
	infos := make([]StrategyInfo, 0, len(r.strategies))
	for _, strategy := range r.strategies {
		infos = append(infos, StrategyInfo{Name: strategy.Name(), Version: strategy.Version()})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}
