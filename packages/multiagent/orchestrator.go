package multiagent

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// TeamOrchestrator manages the multi-agent pool and team lifecycle.
type TeamOrchestrator struct {
	config TeamConfig

	mu        sync.Mutex
	agents    map[string]*Agent
	teams     map[string]*AgentTeam
	running   int // teams currently executing
	teamCount int // deterministic team ID counter
}

// NewTeamOrchestrator creates a multi-agent team system.
func NewTeamOrchestrator(config TeamConfig) *TeamOrchestrator {
	if config.DefaultTeamSize <= 0 {
		config.DefaultTeamSize = 3
	}
	if config.CoordinationStrategy == "" {
		config.CoordinationStrategy = SequentialStrategy
	}
	if config.MinFitScore <= 0 {
		config.MinFitScore = 0.0
	}
	return &TeamOrchestrator{
		config: config,
		agents: make(map[string]*Agent),
		teams:  make(map[string]*AgentTeam),
	}
}

// Config returns the orchestrator configuration.
func (o *TeamOrchestrator) Config() TeamConfig { return o.config }

// RegisterAgent adds a specialist agent to the pool.
func (o *TeamOrchestrator) RegisterAgent(agent *Agent) error {
	if agent == nil {
		return fmt.Errorf("nil agent")
	}
	if agent.ID == "" {
		return fmt.Errorf("agent requires an ID")
	}
	if agent.Role == "" {
		return fmt.Errorf("agent %s requires a role", agent.ID)
	}
	if agent.LLM == nil && o.config.DefaultLLM == nil {
		return fmt.Errorf("agent %s has no LLM and no default LLM is configured", agent.ID)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, exists := o.agents[agent.ID]; exists {
		return fmt.Errorf("agent already registered: %s", agent.ID)
	}
	o.agents[agent.ID] = agent
	return nil
}

// Agent returns a registered agent by ID.
func (o *TeamOrchestrator) Agent(id string) (*Agent, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	a, ok := o.agents[id]
	return a, ok
}

// AgentIDs returns the sorted pool of agent IDs.
func (o *TeamOrchestrator) AgentIDs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	ids := make([]string, 0, len(o.agents))
	for id := range o.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Team returns a team by ID.
func (o *TeamOrchestrator) Team(id string) (*AgentTeam, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	t, ok := o.teams[id]
	return t, ok
}

// FormTeam assembles the optimal agent team for a query.
func (o *TeamOrchestrator) FormTeam(ctx context.Context, query string) (*AgentTeam, error) {
	if query == "" {
		return nil, fmt.Errorf("empty task query")
	}
	requirements := analyzeQueryRequirements(query)

	selected := o.selectBestAgents(requirements)
	if len(selected) == 0 {
		if !o.config.FallbackToSingle {
			return nil, fmt.Errorf("no agents match the query and fallback is disabled")
		}
		fallback := o.fallbackAgent()
		if fallback == nil {
			return nil, fmt.Errorf("no available agents in pool")
		}
		selected = []*Agent{fallback}
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.teamCount++
	team := &AgentTeam{
		ID:             fmt.Sprintf("team_%04d", o.teamCount),
		TaskQuery:      query,
		Strategy:       o.config.CoordinationStrategy,
		AssignedAgents: selected,
		Status:         TeamPending,
		CreatedAt:      time.Now(),
		sharedContext:  map[string]any{"requirements": requirements},
	}
	o.teams[team.ID] = team
	return team, nil
}

// fallbackAgent picks the most capable generalist (highest verified capability
// level, deterministic tie-break by ID).
func (o *TeamOrchestrator) fallbackAgent() *Agent {
	var best *Agent
	bestScore := -1
	var ids []string
	for id := range o.agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		a := o.agents[id]
		score := 0
		for _, c := range a.Capabilities {
			if c.Verified {
				score += c.Level
			}
		}
		if a.Role == RoleGeneralist {
			score += 5
		}
		if best == nil || score > bestScore {
			best, bestScore = a, score
		}
	}
	return best
}

// Execute forms a team for the query and runs it to completion.
func (o *TeamOrchestrator) Execute(ctx context.Context, query string) (*TeamResult, error) {
	team, err := o.FormTeam(ctx, query)
	if err != nil {
		return nil, err
	}
	return o.RunTeam(ctx, team.ID)
}

// RunTeam executes a pending team according to its coordination strategy.
func (o *TeamOrchestrator) RunTeam(ctx context.Context, teamID string) (*TeamResult, error) {
	o.mu.Lock()
	if o.config.MaxConcurrentTeams > 0 && o.running >= o.config.MaxConcurrentTeams {
		o.mu.Unlock()
		return nil, fmt.Errorf("max concurrent teams (%d) reached", o.config.MaxConcurrentTeams)
	}
	team, ok := o.teams[teamID]
	if !ok {
		o.mu.Unlock()
		return nil, fmt.Errorf("team not found: %s", teamID)
	}
	if team.Status == TeamRunning {
		o.mu.Unlock()
		return nil, fmt.Errorf("team %s is already running", teamID)
	}
	o.running++
	team.Status = TeamRunning
	o.mu.Unlock()

	defer func() {
		o.mu.Lock()
		o.running--
		o.mu.Unlock()
	}()

	var result *TeamResult
	var err error
	switch team.Strategy {
	case PipelineStrategy:
		result, err = o.executePipeline(ctx, team)
	case MasterWorkerStrategy:
		result, err = o.executeMasterWorker(ctx, team)
	case ConsensusStrategy:
		result, err = o.executeConsensus(ctx, team)
	case SequentialStrategy:
		result, err = o.executeSequential(ctx, team)
	default:
		result, err = o.executeSequential(ctx, team)
	}

	if err != nil {
		team.Status = TeamFailure
		now := time.Now()
		team.CompletedAt = &now
		if result == nil {
			result = &TeamResult{TeamID: team.ID, Query: team.TaskQuery, Status: TeamFailure, Error: err.Error(), CompletedAt: &now}
		}
		result.Status = TeamFailure
		result.Error = err.Error()
		return result, err
	}

	team.Status = TeamSuccess
	now := time.Now()
	team.CompletedAt = &now
	result.Status = TeamSuccess
	result.CompletedAt = &now
	return result, nil
}

// llmFor resolves the LLM an agent should use.
func (o *TeamOrchestrator) llmFor(a *Agent) LLM {
	if a.LLM != nil {
		return a.LLM
	}
	return o.config.DefaultLLM
}

// runAgent executes a single agent on the task with the given shared context.
func (o *TeamOrchestrator) runAgent(ctx context.Context, team *AgentTeam, a *Agent, extraContext map[string]any) AgentOutput {
	llm := o.llmFor(a)
	start := time.Now()
	prompt := buildPrompt(a, team.TaskQuery, mergeContext(team.SharedContext(), extraContext))

	output := AgentOutput{AgentID: a.ID, Role: a.Role, Success: true}
	if llm == nil {
		output.Success = false
		output.Error = "no LLM configured"
		a.RecordOutcome(false, 0, 0)
		return output
	}

	content, err := llm.Complete(ctx, prompt)
	latency := float64(time.Since(start))
	tokens := estimateTokens(content)
	if err != nil {
		output.Success = false
		output.Error = err.Error()
	} else {
		output.Content = content
	}
	output.LatencyMs = latency
	output.Tokens = tokens
	a.RecordOutcome(output.Success, latency, tokens)
	return output
}

// buildPrompt renders a deterministic prompt for an agent.
func buildPrompt(a *Agent, query string, shared map[string]any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are a %s agent", a.Role)
	if a.Domain != "" {
		fmt.Fprintf(&b, " specializing in %s", a.Domain)
	}
	fmt.Fprintf(&b, ". Task: %s\n", query)
	keys := make([]string, 0, len(shared))
	for k := range shared {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "Context %s: %v\n", k, shared[k])
	}
	return b.String()
}

func mergeContext(a, b map[string]any) map[string]any {
	out := make(map[string]any, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func estimateTokens(s string) int {
	if s == "" {
		return 0
	}
	n := len(s) / 4
	if n == 0 {
		n = 1
	}
	return n
}

// stepFromOutput converts an agent output into a recorded step.
func stepFromOutput(out AgentOutput) AgentStep {
	return AgentStep{
		AgentID:   out.AgentID,
		AgentRole: out.Role,
		Output:    out.Content,
		LatencyMs: out.LatencyMs,
		Tokens:    out.Tokens,
	}
}

// ---- Strategy: sequential ----

// executeSequential runs agents one after another; each agent sees the
// previous agent's output.
func (o *TeamOrchestrator) executeSequential(ctx context.Context, team *AgentTeam) (*TeamResult, error) {
	result := &TeamResult{TeamID: team.ID, Query: team.TaskQuery, CreatedAt: team.CreatedAt}
	var previous string
	for i, a := range team.AssignedAgents {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		extra := map[string]any{"step": i}
		if previous != "" {
			extra["previous_output"] = previous
		}
		out := o.runAgent(ctx, team, a, extra)
		team.addIntermediate(out)
		result.Steps = append(result.Steps, stepFromOutput(out))
		if !out.Success {
			return nil, fmt.Errorf("agent %s failed: %s", a.ID, out.Error)
		}
		previous = out.Content
	}
	// Contradictions between sequentially chained outputs are still flagged.
	result.Output = previous
	result.Conflicts = detectConflicts(team.AssignedAgents, team.Intermediate())
	return result, nil
}

// ---- Strategy: pipeline ----

// stageRoleOrder defines the deterministic pipeline stage ordering.
var stageRoleOrder = []Role{RoleResearcher, RoleCoder, RoleReviewer, RoleManager, RoleGeneralist}

// executePipeline groups agents into stages by role and runs each stage's
// agents in parallel, passing the aggregated stage output to the next stage.
func (o *TeamOrchestrator) executePipeline(ctx context.Context, team *AgentTeam) (*TeamResult, error) {
	stages := groupAgentsByStage(team.AssignedAgents)
	result := &TeamResult{TeamID: team.ID, Query: team.TaskQuery, CreatedAt: team.CreatedAt}
	var stageOutputs []string
	for stageIdx, stage := range stages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		outputs, err := o.runStageParallel(ctx, team, stage, map[string]any{
			"stage":        stageIdx,
			"stage_output": strings.Join(stageOutputs, "\n"),
		})
		if err != nil {
			return nil, err
		}
		aggregated := make([]string, 0, len(outputs))
		for _, out := range outputs {
			team.addIntermediate(out)
			result.Steps = append(result.Steps, stepFromOutput(out))
			if out.Success {
				aggregated = append(aggregated, out.Content)
			}
		}
		stageOutputs = append(stageOutputs, strings.Join(aggregated, "\n"))
	}
	result.Output = strings.Join(stageOutputs, "\n---\n")
	result.Conflicts = detectConflicts(team.AssignedAgents, team.Intermediate())
	return result, nil
}

// runStageParallel runs a stage's agents in parallel and returns their
// outputs in the stage's deterministic order.
func (o *TeamOrchestrator) runStageParallel(ctx context.Context, team *AgentTeam, stage []*Agent, extra map[string]any) ([]AgentOutput, error) {
	var wg sync.WaitGroup
	outputs := make([]AgentOutput, len(stage))
	for i, a := range stage {
		wg.Add(1)
		go func(i int, a *Agent) {
			defer wg.Done()
			outputs[i] = o.runAgent(ctx, team, a, extra)
		}(i, a)
	}
	wg.Wait()

	allFailed := true
	var firstErr string
	for i, out := range outputs {
		if out.Success {
			allFailed = false
		} else if firstErr == "" {
			firstErr = fmt.Sprintf("agent %s failed: %s", stage[i].ID, out.Error)
		}
	}
	if allFailed {
		return outputs, fmt.Errorf("%s", firstErr)
	}
	return outputs, nil
}

func groupAgentsByStage(agents []*Agent) [][]*Agent {
	stages := make([][]*Agent, 0, len(stageRoleOrder))
	placed := make(map[string]bool)
	for _, role := range stageRoleOrder {
		var stage []*Agent
		for _, a := range agents {
			if a.Role == role && !placed[a.ID] {
				stage = append(stage, a)
				placed[a.ID] = true
			}
		}
		if len(stage) > 0 {
			sort.Slice(stage, func(i, j int) bool { return stage[i].ID < stage[j].ID })
			stages = append(stages, stage)
		}
	}
	return stages
}

// ---- Strategy: master-worker ----

// executeMasterWorker fans the task out to all worker agents in parallel,
// then the master agent aggregates their results. The master is the agent
// with RoleManager, or the first agent if none.
func (o *TeamOrchestrator) executeMasterWorker(ctx context.Context, team *AgentTeam) (*TeamResult, error) {
	agents := team.AssignedAgents
	if len(agents) == 0 {
		return nil, fmt.Errorf("team has no agents")
	}
	master := agents[0]
	workers := agents[1:]
	for i, a := range agents {
		if a.Role == RoleManager {
			master = a
			workers = append(append([]*Agent{}, agents[:i]...), agents[i+1:]...)
			break
		}
	}
	if len(workers) == 0 {
		// Single-agent fallback: master does the work directly.
		out := o.runAgent(ctx, team, master, map[string]any{"role": "master"})
		team.addIntermediate(out)
		return &TeamResult{
			TeamID:    team.ID,
			Query:     team.TaskQuery,
			CreatedAt: team.CreatedAt,
			Steps:     []AgentStep{stepFromOutput(out)},
			Output:    out.Content,
		}, nil
	}

	// Fan-out: run workers in parallel.
	var wg sync.WaitGroup
	workerOuts := make([]AgentOutput, len(workers))
	for i, w := range workers {
		wg.Add(1)
		go func(i int, w *Agent) {
			defer wg.Done()
			workerOuts[i] = o.runAgent(ctx, team, w, map[string]any{"role": "worker"})
		}(i, w)
	}
	wg.Wait()

	result := &TeamResult{TeamID: team.ID, Query: team.TaskQuery, CreatedAt: team.CreatedAt}
	var workerFragments []string
	for i, out := range workerOuts {
		team.addIntermediate(out)
		result.Steps = append(result.Steps, stepFromOutput(out))
		if !out.Success {
			return nil, fmt.Errorf("worker %s failed: %s", workers[i].ID, out.Error)
		}
		workerFragments = append(workerFragments, fmt.Sprintf("[%s] %s", workers[i].ID, out.Content))
	}

	// Aggregation: master consolidates all worker results.
	masterExtra := map[string]any{
		"role":         "master",
		"worker_outputs": strings.Join(workerFragments, "\n"),
	}
	masterOut := o.runAgent(ctx, team, master, masterExtra)
	team.addIntermediate(masterOut)
	result.Steps = append(result.Steps, stepFromOutput(masterOut))
	if !masterOut.Success {
		return nil, fmt.Errorf("master %s failed: %s", master.ID, masterOut.Error)
	}
	result.Output = masterOut.Content
	result.Conflicts = detectConflicts(workers, team.Intermediate())
	return result, nil
}

// ---- Strategy: consensus ----

// executeConsensus asks every agent for an answer in parallel and takes the
// majority vote. Ties and splits are reported as conflicts.
func (o *TeamOrchestrator) executeConsensus(ctx context.Context, team *AgentTeam) (*TeamResult, error) {
	agents := team.AssignedAgents
	if len(agents) == 0 {
		return nil, fmt.Errorf("team has no agents")
	}

	var wg sync.WaitGroup
	outputs := make([]AgentOutput, len(agents))
	for i, a := range agents {
		wg.Add(1)
		go func(i int, a *Agent) {
			defer wg.Done()
			outputs[i] = o.runAgent(ctx, team, a, map[string]any{"role": "voter"})
		}(i, a)
	}
	wg.Wait()

	result := &TeamResult{TeamID: team.ID, Query: team.TaskQuery, CreatedAt: team.CreatedAt}
	var votes []string
	for i, out := range outputs {
		team.addIntermediate(out)
		result.Steps = append(result.Steps, stepFromOutput(out))
		if !out.Success {
			return nil, fmt.Errorf("voter %s failed: %s", agents[i].ID, out.Error)
		}
		votes = append(votes, normalizeVote(out.Content))
	}

	// Majority vote: a vote wins with strictly more than half the ballots.
	tallies := make(map[string]int)
	for _, v := range votes {
		tallies[v]++
	}
	majorityNeeded := len(votes)/2 + 1
	winner := ""
	for v, count := range tallies {
		if count >= majorityNeeded && (winner == "" || count > tallies[winner]) {
			winner = v
		}
	}

	if winner == "" {
		conflicts := detectConflicts(agents, team.Intermediate())
		conflicts = append(conflicts, Conflict{
			Type:   "no_majority",
			Agents: sortedIDs(agents),
			Detail: fmt.Sprintf("votes: %v", tallies),
		})
		// Deterministic fallback: first agent's (by ID order) raw output.
		for i, a := range agents {
			if a.ID == sortedIDs(agents)[0] {
				result.Output = outputs[i].Content
				break
			}
		}
		result.Conflicts = conflicts
		return result, nil
	}

	result.Output = rawForVote(winner, outputs)
	result.Conflicts = detectConflicts(agents, team.Intermediate())
	return result, nil
}

// ---- Conflict detection ----

// detectConflicts inspects a set of agent outputs for contradictions and
// duplicates:
//
//   - contradiction: two agents both emit a "CONCLUSION: ..." line with
//     differing content;
//   - duplicate: two or more agents produced identical non-empty output
//     (redundant work).
func detectConflicts(agents []*Agent, outputs []AgentOutput) []Conflict {
	var conflicts []Conflict
	n := len(outputs)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			ai, aj := outputs[i].AgentID, outputs[j].AgentID
			ca, cb := conclusionOf(outputs[i].Content), conclusionOf(outputs[j].Content)
			if ca != "" && cb != "" && ca != cb {
				conflicts = append(conflicts, Conflict{
					Type:   "contradiction",
					Agents: sortedStrings([]string{ai, aj}),
					Detail: fmt.Sprintf("%s concluded %q but %s concluded %q", ai, ca, aj, cb),
				})
			}
		}
	}
	// Duplicates.
	byContent := make(map[string][]string)
	for _, out := range outputs {
		if strings.TrimSpace(out.Content) != "" {
			byContent[out.Content] = append(byContent[out.Content], out.AgentID)
		}
	}
	for content, ids := range byContent {
		if len(ids) >= 2 {
			conflicts = append(conflicts, Conflict{
				Type:   "duplicate",
				Agents: sortedStrings(ids),
				Detail: fmt.Sprintf("agents produced identical output (%d chars)", len(content)),
			})
		}
	}
	sort.Slice(conflicts, func(i, j int) bool {
		if conflicts[i].Type != conflicts[j].Type {
			return conflicts[i].Type < conflicts[j].Type
		}
		return conflicts[i].Detail < conflicts[j].Detail
	})
	return conflicts
}

// conclusionOf extracts the deterministic "CONCLUSION:" line from an output.
func conclusionOf(content string) string {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(line), "CONCLUSION:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "CONCLUSION:"))
		}
	}
	return ""
}

// normalizeVote lowercases and trims a vote for equality comparison.
func normalizeVote(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// rawForVote maps a normalized vote back to the first raw output that matches.
func rawForVote(normalized string, outputs []AgentOutput) string {
	for _, out := range outputs {
		if normalizeVote(out.Content) == normalized {
			return out.Content
		}
	}
	return ""
}

func sortedIDs(agents []*Agent) []string {
	ids := make([]string, 0, len(agents))
	for _, a := range agents {
		ids = append(ids, a.ID)
	}
	return sortedStrings(ids)
}

func sortedStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}
