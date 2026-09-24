// Package multiagent implements multi-agent team orchestration: agent
// registration, team formation, task assignment, fan-out execution, result
// aggregation, and conflict detection. The core is pure Go and deterministic;
// the only external dependency is the pluggable LLM interface (no third-party
// LLM client).
package multiagent

import (
	"context"
	"sync"
	"time"
)

// Role is a specialist role an agent can hold in a team.
type Role string

const (
	RoleResearcher Role = "researcher"
	RoleCoder      Role = "coder"
	RoleReviewer   Role = "reviewer"
	RoleManager    Role = "manager"
	RoleGeneralist Role = "generalist"
)

// CoordinationStrategy defines how agents coordinate within a team.
type CoordinationStrategy string

const (
	SequentialStrategy   CoordinationStrategy = "sequential"    // one after another
	PipelineStrategy     CoordinationStrategy = "pipeline"      // parallel stages
	MasterWorkerStrategy CoordinationStrategy = "master_worker" // central coordinator
	ConsensusStrategy    CoordinationStrategy = "consensus"     // all vote
)

// TeamStatus is the lifecycle state of a team.
type TeamStatus string

const (
	TeamPending TeamStatus = "pending"
	TeamRunning TeamStatus = "running"
	TeamSuccess TeamStatus = "success"
	TeamFailure TeamStatus = "failure"
)

// Capability is a named expertise of an agent.
type Capability struct {
	Name     string
	Level    int // 1-5 expertise level
	Verified bool
}

// LLM is the pluggable language-model interface. The orchestrator never
// depends on a concrete provider; tests inject a deterministic fake.
type LLM interface {
	// Name identifies the provider (for logs and stats).
	Name() string
	// Complete produces the model output for a prompt.
	Complete(ctx context.Context, prompt string) (string, error)
}

// Agent is a single specialist in the pool.
type Agent struct {
	ID           string
	Role         Role
	Domain       string // e.g. "python", "database", "frontend"
	ModelID      string // underlying model identifier
	Capabilities []Capability

	// Pluggable brain. If nil, the agent uses the orchestrator's default LLM.
	LLM LLM

	// Configuration.
	Temperature float64
	MaxTokens   int
	TimeoutMs   int64

	mu sync.Mutex
	// Performance stats (updated after each execution).
	executions    int
	successes     int
	totalLatency  float64
	totalTokens   int
}

// RecordOutcome folds an execution into the agent's running stats.
func (a *Agent) RecordOutcome(success bool, latencyMs float64, tokens int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.executions++
	if success {
		a.successes++
	}
	a.totalLatency += latencyMs
	a.totalTokens += tokens
}

// AvgLatencyMs returns the average latency per execution.
func (a *Agent) AvgLatencyMs() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.executions == 0 {
		return 0
	}
	return a.totalLatency / float64(a.executions)
}

// SuccessRate returns the fraction of successful executions.
func (a *Agent) SuccessRate() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.executions == 0 {
		return 0
	}
	return float64(a.successes) / float64(a.executions)
}

// TotalExecutions returns how many times the agent has run.
func (a *Agent) TotalExecutions() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.executions
}

// AgentOutput is the result of one agent working on a task.
type AgentOutput struct {
	AgentID   string
	Role      Role
	Content   string
	LatencyMs float64
	Tokens    int
	Success   bool
	Error     string
}

// Conflict is a detected disagreement between agents.
type Conflict struct {
	Type   string   // "contradiction", "duplicate", "no_majority"
	Agents []string // involved agent IDs (sorted)
	Detail string
}

// AgentStep is one recorded step in a team execution.
type AgentStep struct {
	AgentID   string
	AgentRole Role
	Output    string
	LatencyMs float64
	Tokens    int
}

// AgentTeam is a dynamic grouping of specialists for a task.
type AgentTeam struct {
	ID             string
	TaskQuery      string
	Strategy       CoordinationStrategy
	AssignedAgents []*Agent
	Status         TeamStatus
	CreatedAt      time.Time
	CompletedAt    *time.Time

	mu              sync.Mutex
	sharedContext   map[string]any
	intermediate    []AgentOutput
}

// SharedContext returns a defensive copy of the team's shared context.
func (t *AgentTeam) SharedContext() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]any, len(t.sharedContext))
	for k, v := range t.sharedContext {
		out[k] = v
	}
	return out
}

// SetSharedValue stores a value in the shared context.
func (t *AgentTeam) SetSharedValue(k string, v any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sharedContext[k] = v
}

// Intermediate returns a copy of the intermediate per-agent results.
func (t *AgentTeam) Intermediate() []AgentOutput {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]AgentOutput, len(t.intermediate))
	copy(out, t.intermediate)
	return out
}

func (t *AgentTeam) addIntermediate(out AgentOutput) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.intermediate = append(t.intermediate, out)
}

// TeamResult is the final outcome of a team execution.
type TeamResult struct {
	TeamID    string
	Query     string
	Status    TeamStatus
	Output    string // aggregated final output
	Steps     []AgentStep
	Conflicts []Conflict
	Error     string
	CreatedAt time.Time
	CompletedAt *time.Time
}

// QueryRequirements captures the analysis of a task query.
type QueryRequirements struct {
	TaskType        string
	RequiredDomains []string
	Complexity      int // 1..5
	Urgent          bool
	Keywords        []string
}

// TeamConfig holds team orchestration parameters.
type TeamConfig struct {
	DefaultTeamSize      int
	MaxConcurrentTeams   int
	FallbackToSingle     bool
	CoordinationStrategy CoordinationStrategy

	// DefaultLLM is used by any agent that has no per-agent LLM.
	DefaultLLM LLM

	// MinFitScore is the minimum agent fit score to be selected.
	MinFitScore float64
}

// DefaultTeamConfig returns reasonable defaults.
func DefaultTeamConfig() TeamConfig {
	return TeamConfig{
		DefaultTeamSize:      3,
		MaxConcurrentTeams:   8,
		FallbackToSingle:     true,
		CoordinationStrategy: SequentialStrategy,
		MinFitScore:          0.0,
	}
}
