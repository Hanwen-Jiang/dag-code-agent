// Package harness contains the provider-neutral Agent Harness control plane.
//
// The package deliberately treats model output as data.  Only the runtime and
// its policy checks may commit task state, invoke side effects, or advance a
// long-running plan.
package harness

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
)

const (
	TaskReady       = "READY"
	TaskRunning     = "RUNNING"
	TaskWaitingUser = "WAITING_USER"
	TaskCompleted   = "COMPLETED"
	TaskFailed      = "FAILED"
	TaskCancelled   = "CANCELLED"
	ToolSucceeded   = "SUCCEEDED"
	ToolFailed      = "FAILED"
	ToolUnknown     = "UNKNOWN"
)

// UTCNow is kept behind a function so tests can compare event ordering while
// still receiving RFC3339 timestamps in production.
func UTCNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func NewID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b)
}

// CanonicalJSON normalizes structs and typed slices into the generic JSON data
// model before hashing. encoding/json sorts map keys, while struct field order
// follows source declaration order; the round trip removes that discrepancy so
// persisted payloads and replayed payloads hash identically.
func CanonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var normalized any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}

func Digest(v any) (string, error) {
	b, err := CanonicalJSON(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

type AuthContext struct {
	TenantID  string   `json:"tenant_id"`
	SubjectID string   `json:"subject_id"`
	Roles     []string `json:"roles,omitempty"`
	Scopes    []string `json:"scopes,omitempty"`
	TraceID   string   `json:"trace_id,omitempty"`
}

func (a AuthContext) HasScope(scope string) bool {
	for _, item := range a.Scopes {
		if item == "*" || item == scope {
			return true
		}
	}
	return false
}

type TaskRequest struct {
	TaskID          string         `json:"task_id"`
	Goal            string         `json:"goal"`
	TenantID        string         `json:"tenant_id"`
	SubjectID       string         `json:"subject_id"`
	Constraints     []string       `json:"constraints,omitempty"`
	MaxSteps        int            `json:"max_steps"`
	MaxToolCalls    int            `json:"max_tool_calls"`
	ContextWindow   int            `json:"context_window_tokens"`
	RequiredScopes  []string       `json:"required_scopes,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	Model           string         `json:"model,omitempty"`
	ModelRevision   string         `json:"model_revision,omitempty"`
	PolicyVersion   string         `json:"policy_version,omitempty"`
	ReasoningEffort string         `json:"reasoning_effort,omitempty"`
	MaxOutputTokens int            `json:"max_output_tokens,omitempty"`
	ResponseSchema  map[string]any `json:"response_schema,omitempty"`
}

func (r TaskRequest) WithDefaults() TaskRequest {
	if r.MaxSteps <= 0 {
		r.MaxSteps = 32
	}
	if r.MaxToolCalls <= 0 {
		r.MaxToolCalls = 64
	}
	if r.ContextWindow <= 0 {
		r.ContextWindow = 16000
	}
	if r.ReasoningEffort == "" {
		r.ReasoningEffort = "low"
	}
	if r.PolicyVersion == "" {
		r.PolicyVersion = "policy-v1"
	}
	if r.MaxOutputTokens <= 0 {
		r.MaxOutputTokens = 2048
	}
	if r.Metadata == nil {
		r.Metadata = map[string]any{}
	}
	return r
}

type TaskState struct {
	TaskID                  string           `json:"task_id"`
	Goal                    string           `json:"goal"`
	TenantID                string           `json:"tenant_id"`
	SubjectID               string           `json:"subject_id"`
	Status                  string           `json:"status"`
	Turns                   int              `json:"turns"`
	CurrentTurnID           string           `json:"current_turn_id,omitempty"`
	Step                    int              `json:"step"`
	ModelRound              int              `json:"model_round"`
	ToolCalls               int              `json:"tool_calls"`
	InputTokens             int              `json:"input_tokens"`
	OutputTokens            int              `json:"output_tokens"`
	CachedTokens            int              `json:"cached_input_tokens"`
	CostUSD                 float64          `json:"cost_usd"`
	RetryCount              int              `json:"retry_count"`
	LastModelRequestDigest  string           `json:"last_model_request_digest,omitempty"`
	LastModelResponseDigest string           `json:"last_model_response_digest,omitempty"`
	CurrentPlan             []PlanStep       `json:"current_plan,omitempty"`
	CompletedSteps          []string         `json:"completed_steps,omitempty"`
	Observations            []map[string]any `json:"observations,omitempty"`
	Artifacts               []map[string]any `json:"artifacts,omitempty"`
	MemoryIDs               []string         `json:"memory_ids,omitempty"`
	UserMessages            []string         `json:"user_messages,omitempty"`
	LastError               map[string]any   `json:"last_error,omitempty"`
	FinalAnswer             string           `json:"final_answer,omitempty"`
	Metadata                map[string]any   `json:"metadata,omitempty"`
}

func (s TaskState) LastErrorString() string {
	if s.LastError == nil {
		return ""
	}
	if value, ok := s.LastError["message"].(string); ok {
		return value
	}
	return fmt.Sprint(s.LastError)
}

func (s *TaskState) Normalize() {
	if s.Status == "" {
		s.Status = TaskReady
	}
	if s.Observations == nil {
		s.Observations = []map[string]any{}
	}
	if s.Artifacts == nil {
		s.Artifacts = []map[string]any{}
	}
	if s.MemoryIDs == nil {
		s.MemoryIDs = []string{}
	}
	if s.UserMessages == nil {
		s.UserMessages = []string{}
	}
	if s.Metadata == nil {
		s.Metadata = map[string]any{}
	}
}

type ToolCall struct {
	CallID    string         `json:"call_id"`
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

type ModelUsage struct {
	InputTokens       int     `json:"input_tokens"`
	OutputTokens      int     `json:"output_tokens"`
	CachedInputTokens int     `json:"cached_input_tokens"`
	LatencyMS         float64 `json:"latency_ms"`
	CostUSD           float64 `json:"cost_usd"`
}

func (u ModelUsage) TotalTokens() int { return u.InputTokens + u.OutputTokens }

type ModelRequest struct {
	TaskID          string           `json:"task_id"`
	Messages        []map[string]any `json:"messages"`
	Tools           []map[string]any `json:"tools,omitempty"`
	ResponseSchema  map[string]any   `json:"response_schema,omitempty"`
	Model           string           `json:"model,omitempty"`
	ReasoningEffort string           `json:"reasoning_effort,omitempty"`
	MaxOutputTokens int              `json:"max_output_tokens,omitempty"`
	Stream          bool             `json:"stream,omitempty"`
	PrefixCacheKey  string           `json:"prefix_cache_key,omitempty"`
}

type ModelResponse struct {
	Text             string           `json:"text,omitempty"`
	ToolCalls        []ToolCall       `json:"tool_calls,omitempty"`
	SubagentRequests []map[string]any `json:"subagent_requests,omitempty"`
	NeedUser         map[string]any   `json:"need_user,omitempty"`
	FinishReason     string           `json:"finish_reason,omitempty"`
	Usage            ModelUsage       `json:"usage"`
	Raw              map[string]any   `json:"raw,omitempty"`
}

func (r ModelResponse) IsFinal() bool {
	return strings.TrimSpace(r.Text) != "" && len(r.ToolCalls) == 0 && len(r.SubagentRequests) == 0 && r.NeedUser == nil
}

type StreamEvent struct {
	Type string         `json:"event_type"`
	Data map[string]any `json:"data,omitempty"`
}

type ToolResult struct {
	CallID           string  `json:"call_id"`
	Name             string  `json:"name"`
	Status           string  `json:"status"`
	Value            any     `json:"value,omitempty"`
	ErrorCode        string  `json:"error_code,omitempty"`
	ErrorMessage     string  `json:"error_message,omitempty"`
	Attempts         int     `json:"attempts"`
	IdempotentReplay bool    `json:"idempotent_replay,omitempty"`
	LatencyMS        float64 `json:"latency_ms"`
}

func (r ToolResult) OK() bool { return r.Status == ToolSucceeded }

type ToolHandler func(arguments map[string]any, auth AuthContext) (any, error)

type ToolSpec struct {
	Name                string         `json:"name"`
	Description         string         `json:"description"`
	InputSchema         map[string]any `json:"input_schema"`
	Handler             ToolHandler    `json:"-"`
	RequiredScopes      []string       `json:"required_scopes,omitempty"`
	SideEffect          string         `json:"side_effect"`
	IdempotencyRequired bool           `json:"idempotency_required,omitempty"`
	TimeoutSeconds      float64        `json:"timeout_seconds"`
	MaxRetries          int            `json:"max_retries"`
	Tags                []string       `json:"tags,omitempty"`
}

func (t ToolSpec) SchemaForModel() map[string]any {
	return map[string]any{"type": "function", "function": map[string]any{
		"name": t.Name, "description": t.Description, "parameters": t.InputSchema,
	}}
}

type MemoryRecord struct {
	MemoryID   string   `json:"memory_id"`
	TenantID   string   `json:"tenant_id"`
	SubjectID  string   `json:"subject_id"`
	MemoryType string   `json:"memory_type"`
	Namespace  string   `json:"namespace"`
	Key        string   `json:"key"`
	Content    string   `json:"content"`
	Tags       []string `json:"tags,omitempty"`
	Importance float64  `json:"importance"`
	Confidence float64  `json:"confidence"`
	Version    int      `json:"version"`
	Source     string   `json:"source"`
	CreatedAt  string   `json:"created_at"`
	ExpiresAt  string   `json:"expires_at,omitempty"`
	Supersedes string   `json:"supersedes,omitempty"`
	Tombstone  bool     `json:"tombstone,omitempty"`
}

type SkillManifest struct {
	SkillID              string   `json:"skill_id"`
	Version              string   `json:"version"`
	Title                string   `json:"title"`
	Instructions         string   `json:"instructions"`
	Tags                 []string `json:"tags,omitempty"`
	ToolNames            []string `json:"tool_names,omitempty"`
	RequiredScopes       []string `json:"required_scopes,omitempty"`
	Status               string   `json:"status"`
	MinModelCapabilities []string `json:"min_model_capabilities,omitempty"`
}

type ContextPacket struct {
	TaskID          string           `json:"task_id"`
	Goal            string           `json:"goal"`
	Messages        []map[string]any `json:"messages"`
	State           map[string]any   `json:"state"`
	Memories        []map[string]any `json:"memories,omitempty"`
	Skills          []map[string]any `json:"skills,omitempty"`
	Tools           []map[string]any `json:"tools,omitempty"`
	ExactInvariants []string         `json:"exact_invariants,omitempty"`
	Excluded        []string         `json:"excluded_categories,omitempty"`
	EstimatedTokens int              `json:"estimated_tokens"`
	BudgetTokens    int              `json:"budget_tokens"`
	PrefixCacheKey  string           `json:"prefix_cache_key,omitempty"`
	PrefixCacheHit  bool             `json:"prefix_cache_hit"`
}

type SubagentSpec struct {
	AgentID          string   `json:"agent_id"`
	Role             string   `json:"role"`
	Objective        string   `json:"objective"`
	Tags             []string `json:"tags,omitempty"`
	AllowedToolNames []string `json:"allowed_tool_names,omitempty"`
	MaxSteps         int      `json:"max_steps"`
}

type SubagentResult struct {
	AgentID   string           `json:"agent_id"`
	Status    string           `json:"status"`
	Summary   string           `json:"summary,omitempty"`
	Artifacts []map[string]any `json:"artifacts,omitempty"`
	Error     string           `json:"error,omitempty"`
	Usage     ModelUsage       `json:"usage"`
}

type PlanStep struct {
	StepID       string   `json:"step_id"`
	Objective    string   `json:"objective"`
	Dependencies []string `json:"dependencies,omitempty"`
	Status       string   `json:"status"`
	Attempts     int      `json:"attempts"`
}

type EvaluationCase struct {
	CaseID         string         `json:"case_id"`
	Goal           string         `json:"goal"`
	Expected       map[string]any `json:"expected,omitempty"`
	FixtureVersion string         `json:"fixture_version,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type FeedbackSignal struct {
	FeedbackID string         `json:"feedback_id"`
	TaskID     string         `json:"task_id"`
	TenantID   string         `json:"tenant_id"`
	SubjectID  string         `json:"subject_id"`
	Source     string         `json:"source"`
	Stage      string         `json:"stage"`
	Outcome    string         `json:"outcome"`
	Category   string         `json:"category,omitempty"`
	Rating     int            `json:"rating,omitempty"`
	Comment    string         `json:"comment,omitempty"`
	Variant    string         `json:"variant,omitempty"`
	Cohort     string         `json:"cohort,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	CreatedAt  string         `json:"created_at"`
}

func ContainsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func Intersects(a, b []string) bool {
	set := map[string]struct{}{}
	for _, item := range a {
		set[item] = struct{}{}
	}
	for _, item := range b {
		if _, ok := set[item]; ok {
			return true
		}
	}
	return false
}

func lexicalTerms(value string) []string {
	parts := strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	seen := map[string]bool{}
	for _, part := range parts {
		if len([]rune(part)) > 1 {
			seen[part] = true
		}
		runes := []rune(part)
		hasHan := false
		for _, r := range runes {
			if unicode.Is(unicode.Han, r) {
				hasHan = true
				break
			}
		}
		if hasHan {
			for i := 0; i+1 < len(runes); i++ {
				seen[string(runes[i:i+2])] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for term := range seen {
		out = append(out, term)
	}
	sort.Strings(out)
	return out
}

func lexicalOverlap(query, document string) int {
	documentTerms := map[string]bool{}
	for _, term := range lexicalTerms(document) {
		documentTerms[term] = true
	}
	score := 0
	for _, term := range lexicalTerms(query) {
		if documentTerms[term] {
			score++
		}
	}
	return score
}

func SortedStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
