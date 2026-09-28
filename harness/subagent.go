package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Subagent execution.
//
// A model could always ask for subagents: ModelResponse.SubagentRequests was
// parsed and the request was recorded as a SUBAGENTS_REQUESTED event. Nothing
// ran. The coordinator in multi_agent.go was never called either, and it could
// only have made a single Complete call per subagent — no tools, no steps, no
// journal.
//
// A subagent here is a durable child task that runs the SAME kernel loop as its
// parent: its own journal, its own tool loop, its own step budget. That is what
// makes the two properties below possible, and neither is available to a
// fire-and-forget goroutine:
//
//   - crash reuse: a child that already completed is not re-run, so a fan-out
//     interrupted midway does not repeat the work it finished.
//   - audit: the child's trajectory is replayable on its own, with the same
//     hash chain guarantees as any other task.
//
// Handoff semantics (decided in the foundation design): a subagent RETURNS its
// result to the parent. The parent keeps task ownership and the only journal
// write path; there is no control transfer and no "current agent" global.

// SubagentPolicy bounds a fan-out.
type SubagentPolicy struct {
	// MaxParallel bounds how many children run at once.
	MaxParallel int
	// MaxDepth bounds nesting. The default of 1 lets a task delegate but not
	// delegate again, because fan-out grows multiplicatively and an unbounded
	// tree is indistinguishable from a runaway.
	MaxDepth int
	// DefaultMaxSteps is a child's step budget when it does not ask for one.
	DefaultMaxSteps int
	// BudgetShare is the fraction of the parent's remaining token budget each
	// child may spend. It bounds a single runaway child without letting one
	// starve the others.
	BudgetShare float64
	// RequireConsensus marks a fan-out whose disagreeing children are reported
	// as a conflict rather than silently merged.
	RequireConsensus bool
}

// DefaultSubagentPolicy returns the policy used when none is configured.
func DefaultSubagentPolicy() SubagentPolicy {
	return SubagentPolicy{
		MaxParallel:      defaultSubagentParallelism,
		MaxDepth:         defaultSubagentDepth,
		DefaultMaxSteps:  defaultSubagentSteps,
		BudgetShare:      defaultSubagentBudgetShare,
		RequireConsensus: false,
	}
}

const (
	defaultSubagentParallelism = 4
	defaultSubagentDepth       = 1
	defaultSubagentSteps       = 6
	defaultSubagentBudgetShare = 0.25
	// parentTaskKey links a child to the task that spawned it.
	parentTaskKey = "parent_task_id"
	// subagentDepthKey records nesting depth so the limit can be enforced
	// without trusting the request.
	subagentDepthKey = "subagent_depth"
)

// WithSubagentPolicy configures fan-out.
func WithSubagentPolicy(policy SubagentPolicy) Option {
	return func(c *runtimeConfig) error {
		c.subagentPolicy = policy
		return nil
	}
}

// subagentSpecsFromProposals normalizes the raw maps a model produced.
//
// Parsing untrusted model output is the kernel's job (see StepResult), so this
// is where a malformed proposal is rejected rather than at the execution point.
func subagentSpecsFromProposals(proposals []map[string]any) []SubagentSpec {
	specs := make([]SubagentSpec, 0, len(proposals))
	for index, proposal := range proposals {
		if proposal == nil {
			continue
		}
		spec := SubagentSpec{
			AgentID:   stringValue(proposal["agent_id"]),
			Role:      stringValue(proposal["role"]),
			Objective: stringValue(proposal["objective"]),
		}
		if spec.AgentID == "" {
			// A stable id is required: it is half of the child task id, so an
			// unstable one would defeat crash reuse.
			spec.AgentID = fmt.Sprintf("agent-%d", index+1)
		}
		spec.Tags = stringSlice(proposal["tags"])
		spec.AllowedToolNames = stringSlice(proposal["allowed_tool_names"])
		if steps, ok := proposal["max_steps"].(float64); ok && steps > 0 {
			spec.MaxSteps = int(steps)
		}
		if strings.TrimSpace(spec.Objective) == "" {
			// An objective-less child has nothing to do; skipping it is more
			// honest than running a task whose goal is the empty string.
			continue
		}
		specs = append(specs, spec)
	}
	return specs
}

func stringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := stringValue(item); text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		if typed == "" {
			return nil
		}
		return strings.Split(typed, ",")
	default:
		return nil
	}
}

// subagentRunner executes a fan-out for one parent step.
type subagentRunner struct {
	runtime *Runtime
	policy  SubagentPolicy
	auth    AuthContext
	parent  TaskState
	// depth is the depth the children will run at.
	depth int
	// ledger is the parent's budget ledger, used to size each child's share.
	ledger *BudgetLedger
}

func (r *Runtime) newSubagentRunner(parent TaskState, auth AuthContext, ledger *BudgetLedger) *subagentRunner {
	policy := r.subagentPolicy
	if policy.MaxParallel <= 0 {
		policy = DefaultSubagentPolicy()
	}
	depth := 1
	if recorded, ok := parent.Metadata[subagentDepthKey].(int); ok {
		depth = recorded + 1
	}
	return &subagentRunner{runtime: r, policy: policy, auth: auth, parent: parent, depth: depth, ledger: ledger}
}

// run executes every spec, bounded by MaxParallel, and returns the results in
// request order so the parent's journal does not depend on completion order.
func (s *subagentRunner) run(ctx context.Context, specs []SubagentSpec) []SubagentResult {
	results := make([]SubagentResult, len(specs))
	sem := make(chan struct{}, s.policy.MaxParallel)
	var wg sync.WaitGroup
	for i := range specs {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[index] = SubagentResult{AgentID: specs[index].AgentID, Status: "FAILED", Error: ctx.Err().Error()}
				return
			}
			defer func() { <-sem }()
			results[index] = s.spawn(ctx, specs[index])
		}(i)
	}
	wg.Wait()
	return results
}

// spawn runs one child, reusing a completed one.
func (s *subagentRunner) spawn(ctx context.Context, spec SubagentSpec) SubagentResult {
	childID := s.parent.TaskID + ":" + spec.AgentID

	if state, _, err := s.runtime.store.GetTask(childID); err == nil {
		switch state.Status {
		case TaskCompleted:
			// Crash reuse: the work is already durable, so it is reported rather
			// than repeated. This is the whole reason a subagent is a task.
			return SubagentResult{
				AgentID: spec.AgentID,
				Status:  "SUCCEEDED",
				Summary: state.FinalAnswer,
				Usage:   ModelUsage{InputTokens: state.InputTokens, OutputTokens: state.OutputTokens, CostUSD: state.CostUSD},
			}
		case TaskRunning, TaskReady, TaskWaitingUser:
			// A child left non-terminal by an interrupted run: resuming it is
			// exactly the recovery the journal supports.
			return s.drive(ctx, childID, spec)
		}
		// FAILED or CANCELLED children are re-attempted: memoizing a failure
		// would make a transient error permanent.
	}

	request := s.childRequest(spec, childID)
	state := TaskState{
		TaskID:    childID,
		Goal:      spec.Objective,
		TenantID:  s.auth.TenantID,
		SubjectID: s.auth.SubjectID,
		Status:    TaskReady,
		Metadata: map[string]any{
			"request":         request,
			parentTaskKey:     s.parent.TaskID,
			subagentDepthKey:  s.depth,
			"subagent_role":   spec.Role,
			"subagent_origin": "fanout",
		},
	}
	state.Normalize()
	if s.runtime.planner != nil {
		state.CurrentPlan = s.runtime.planner.Plan(request, state)
	}
	if _, err := s.runtime.journal.Create(state, "TASK_ACCEPTED", map[string]any{
		"goal": request.Goal, "tenant_id": state.TenantID, "subject_id": state.SubjectID,
		"parent_task_id": s.parent.TaskID, "subagent_role": spec.Role,
	}); err != nil {
		// A duplicate id means another writer won the race; its result is used.
		if existing, _, getErr := s.runtime.store.GetTask(childID); getErr == nil && existing.Status == TaskCompleted {
			return SubagentResult{AgentID: spec.AgentID, Status: "SUCCEEDED", Summary: existing.FinalAnswer}
		}
		return SubagentResult{AgentID: spec.AgentID, Status: "FAILED", Error: err.Error()}
	}
	return s.drive(ctx, childID, spec)
}

// drive runs the child through the kernel loop.
//
// It must NOT call Runtime.run: the parent's run holds r.mu for its whole
// duration, so a child that took the same lock would deadlock against its own
// parent.
func (s *subagentRunner) drive(ctx context.Context, childID string, spec SubagentSpec) SubagentResult {
	result := s.runtime.continueRunWithLedger(ctx, childID, s.auth, s.childLedger())
	if result.Status != TaskCompleted {
		return SubagentResult{
			AgentID: spec.AgentID,
			Status:  "FAILED",
			Error:   stringValue(result.State.LastError["message"]),
			Summary: result.FailureCategory,
			Usage:   ModelUsage{InputTokens: result.State.InputTokens, OutputTokens: result.State.OutputTokens, CostUSD: result.State.CostUSD},
		}
	}
	return SubagentResult{
		AgentID: spec.AgentID,
		Status:  "SUCCEEDED",
		Summary: result.FinalAnswer,
		Usage:   ModelUsage{InputTokens: result.State.InputTokens, OutputTokens: result.State.OutputTokens, CostUSD: result.State.CostUSD},
	}
}

// childRequest derives the child's request from the parent's, so a child
// inherits the model, policy version, constraints and context window.
func (s *subagentRunner) childRequest(spec SubagentSpec, childID string) TaskRequest {
	request := requestFromState(s.parent)
	request.TaskID = childID
	request.Goal = spec.Objective
	request.TenantID = s.auth.TenantID
	request.SubjectID = s.auth.SubjectID
	request.MaxSteps = spec.MaxSteps
	if request.MaxSteps <= 0 {
		request.MaxSteps = s.policy.DefaultMaxSteps
	}
	// A child must not fan out again beyond the policy's depth.
	request.Metadata = map[string]any{
		parentTaskKey:    s.parent.TaskID,
		subagentDepthKey: s.depth,
	}
	return request
}

// childLedger gives the child a bounded slice of the parent's remaining budget.
func (s *subagentRunner) childLedger() *BudgetLedger {
	if s.ledger == nil {
		return nil
	}
	remaining := budgetSnapshot(s.ledger).RemainingTokens()
	share := s.policy.BudgetShare
	if share <= 0 || share > 1 {
		share = defaultSubagentBudgetShare
	}
	ledger := &BudgetLedger{MaxToolCalls: 0}
	if remaining > 0 {
		ledger.MaxTokens = int(float64(remaining) * share)
	}
	// Cost is inherited rather than divided: the parent's ceiling still applies
	// to the tree as a whole.
	ledger.MaxCostUSD = 0
	return ledger
}

// mergeSubagentResults aggregates a fan-out into the payload recorded on the
// parent and injected into its context.
func mergeSubagentResults(results []SubagentResult, requireConsensus bool) map[string]any {
	if len(results) == 0 {
		return map[string]any{"status": "FAILED", "error": "NO_SUBAGENTS"}
	}
	status := "SUCCEEDED"
	summaries := map[string]int{}
	for _, result := range results {
		if result.Status != "SUCCEEDED" {
			status = "FAILED"
		}
		key := strings.TrimSpace(result.Summary)
		if key == "" {
			key = result.Error
		}
		summaries[key]++
	}
	merged := map[string]any{"status": status, "count": len(results), "results": results}
	if requireConsensus && len(summaries) > 1 {
		// Disagreement is reported rather than averaged away: a silent merge of
		// conflicting conclusions is worse than an explicit conflict.
		merged["status"] = "CONFLICT"
		conflicts := make([]map[string]any, 0, len(summaries))
		for summary, count := range summaries {
			conflicts = append(conflicts, map[string]any{"summary": summary, "count": count})
		}
		merged["conflicts"] = conflicts
	}
	return merged
}

// ErrSubagentDepthExceeded reports a fan-out beyond the configured depth.
var ErrSubagentDepthExceeded = errors.New("SUBAGENT_DEPTH_EXCEEDED")
