package harness

import (
	"context"
	"fmt"
	"sync"
)

type AgentFactory func(SubagentSpec) ModelAdapter

type MultiAgentCoordinator struct {
	Factory     AgentFactory
	Context     *ContextBuilder
	Tools       *ToolRegistry
	Runtime     *ToolRuntime
	MaxParallel int
}

func (c *MultiAgentCoordinator) FanOut(ctx context.Context, specs []SubagentSpec, request TaskRequest, auth AuthContext) ([]SubagentResult, error) {
	if c.MaxParallel <= 0 {
		c.MaxParallel = 4
	}
	results := make([]SubagentResult, len(specs))
	sem := make(chan struct{}, c.MaxParallel)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	for i, spec := range specs {
		wg.Add(1)
		go func(index int, spec SubagentSpec) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				mu.Unlock()
				return
			}
			defer func() { <-sem }()
			adapter := c.Factory(spec)
			if adapter == nil {
				results[index] = SubagentResult{AgentID: spec.AgentID, Status: "FAILED", Error: "missing adapter"}
				return
			}
			subRequest := request
			subRequest.TaskID = request.TaskID + ":" + spec.AgentID
			subRequest.Goal = spec.Objective
			if spec.MaxSteps > 0 {
				subRequest.MaxSteps = spec.MaxSteps
			}
			state := TaskState{TaskID: subRequest.TaskID, Goal: spec.Objective, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Status: TaskReady}
			packet, err := c.Context.Build(subRequest, auth, state, nil)
			if err != nil {
				results[index] = SubagentResult{AgentID: spec.AgentID, Status: "FAILED", Error: err.Error()}
				return
			}
			messages := packet.ProviderMessages()
			response, err := adapter.Complete(ctx, ModelRequest{TaskID: subRequest.TaskID, Messages: messages, Tools: filteredTools(c.Tools, spec.AllowedToolNames, auth), Model: subRequest.Model, ReasoningEffort: subRequest.ReasoningEffort, MaxOutputTokens: subRequest.MaxOutputTokens})
			if err != nil {
				results[index] = SubagentResult{AgentID: spec.AgentID, Status: "FAILED", Error: err.Error()}
				return
			}
			results[index] = SubagentResult{AgentID: spec.AgentID, Status: "SUCCEEDED", Summary: response.Text, Usage: response.Usage}
		}(i, spec)
	}
	wg.Wait()
	return results, firstErr
}
func filteredTools(registry *ToolRegistry, allowed []string, auth AuthContext) []map[string]any {
	if registry == nil {
		return nil
	}
	return registry.SchemasForAuth(allowed, auth)
}

func (c *MultiAgentCoordinator) Merge(results []SubagentResult, requireConsensus bool) map[string]any {
	summary := map[string]any{"status": "SUCCEEDED", "results": results}
	if len(results) == 0 {
		summary["status"] = "FAILED"
		summary["error"] = "NO_SUBAGENTS"
		return summary
	}
	counts := map[string]int{}
	for _, r := range results {
		if r.Status != "SUCCEEDED" {
			summary["status"] = "FAILED"
		}
		normalized := r.Summary
		if normalized == "" {
			normalized = r.Error
		}
		counts[normalized]++
	}
	if requireConsensus && len(counts) > 1 {
		summary["status"] = "CONFLICT"
		summary["conflicts"] = counts
	}
	return summary
}

func (c *MultiAgentCoordinator) String() string {
	return fmt.Sprintf("multi-agent(max_parallel=%d)", c.MaxParallel)
}
