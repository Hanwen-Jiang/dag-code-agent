package skills

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// SkillStatus is the lifecycle status of a skill.
type SkillStatus string

const (
	// StatusActive skills are eligible for retrieval and execution.
	StatusActive SkillStatus = "ACTIVE"
	// StatusDeprecated skills are kept for audit/export but never retrieved.
	StatusDeprecated SkillStatus = "DEPRECATED"
	// StatusPending skills await human review.
	StatusPending SkillStatus = "PENDING"
)

// PatternDefinition defines a reusable reasoning pattern (a skill).
type PatternDefinition struct {
	ID              string `yaml:"id" json:"id"`
	Name            string `yaml:"name" json:"name"`
	Description     string `yaml:"description" json:"description"`
	Trigger         PatternTrigger `yaml:"trigger" json:"trigger"`
	ExecutionPlan   []SkillAction  `yaml:"execution_plan" json:"execution_plan"`
	ContextRequirements []string   `yaml:"context_requirements,omitempty" json:"context_requirements,omitempty"`

	// Learning versioning.
	Status        SkillStatus `yaml:"status" json:"status"`
	Version       int         `yaml:"version" json:"version"`
	LastUsed      time.Time   `yaml:"last_used" json:"last_used"`
	UsageCount    int         `yaml:"usage_count" json:"usage_count"`
	AvgLatencyMs  float64     `yaml:"avg_latency_ms" json:"avg_latency_ms"`
	AvgTokensUsed int         `yaml:"avg_tokens_used" json:"avg_tokens_used"`
	SuccessRate   float64     `yaml:"success_rate" json:"success_rate"`

	// Training data.
	ExampleQueries  []string      `yaml:"example_queries,omitempty" json:"example_queries,omitempty"`
	TestCases       []PatternTest `yaml:"test_cases,omitempty" json:"test_cases,omitempty"`
	ContributorType string        `yaml:"contributor_type" json:"contributor_type"`
	Hash            string        `yaml:"hash,omitempty" json:"hash,omitempty"`
}

// PatternTrigger specifies when to apply a pattern.
type PatternTrigger struct {
	Type           TriggerType `yaml:"type" json:"type"`
	QueryEmbedding []float32   `yaml:"query_embedding,omitempty" json:"query_embedding,omitempty"`
	Keywords       []string    `yaml:"keywords,omitempty" json:"keywords,omitempty"`
	DomainFilters  []string    `yaml:"domain_filters,omitempty" json:"domain_filters,omitempty"`
}

// TriggerType is the trigger matching strategy.
type TriggerType string

const (
	EmbeddingMatch TriggerType = "embedding_match"
	KeywordMatch   TriggerType = "keyword_match"
	DomainSpecific TriggerType = "domain_specific"
)

// SkillAction represents one step in the skill execution plan.
type SkillAction struct {
	ActionType  string                 `yaml:"action_type" json:"action_type"`
	TargetTool  string                 `yaml:"target_tool,omitempty" json:"target_tool,omitempty"`
	Parameters  map[string]interface{} `yaml:"parameters,omitempty" json:"parameters,omitempty"`
	Prompt      string                 `yaml:"prompt,omitempty" json:"prompt,omitempty"`
	Condition   string                 `yaml:"condition,omitempty" json:"condition,omitempty"`
	Expectation string                 `yaml:"expectation,omitempty" json:"expectation,omitempty"`
	TimeoutMs   int64                  `yaml:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
}

// PatternTest defines an automated test for skill validation.
type PatternTest struct {
	InputQuery     string         `yaml:"input_query" json:"input_query"`
	ExpectedOutput string         `yaml:"expected_output,omitempty" json:"expected_output,omitempty"`
	GroundTruth    []string       `yaml:"ground_truth,omitempty" json:"ground_truth,omitempty"`
	Constraint     TestConstraint `yaml:"constraint,omitempty" json:"constraint,omitempty"`
}

// TestConstraint bounds a pattern test.
type TestConstraint struct {
	MaxTokens        int      `yaml:"max_tokens,omitempty" json:"max_tokens,omitempty"`
	MaxTimeMs        int64    `yaml:"max_time_ms,omitempty" json:"max_time_ms,omitempty"`
	NoForbiddenTools []string `yaml:"no_forbidden_tools,omitempty" json:"no_forbidden_tools,omitempty"`
}

// ExecutionRecord tracks a past task execution for pattern mining.
type ExecutionRecord struct {
	TaskID       string
	Query        string
	Steps        []TraceStep
	Solution     string
	IsSuccessful bool
	LatencyMs    float64
	TokensUsed   int
	Domain       string
	Complexity   int
}

// TraceStep is one step of an agent execution trace.
type TraceStep struct {
	Reasoning     string
	ToolCalls     []TraceToolCall
	LoopDecision  TraceLoopDecision
	PermissionCheck *TracePermission
	StateAfterLLM TraceLLMState
}

// TraceToolCall is a single tool invocation inside a trace step.
type TraceToolCall struct {
	Name string
	Args map[string]interface{}
}

// TraceLoopDecision records a loop-detection decision at a step.
type TraceLoopDecision struct {
	Triggered bool
	Decision  string
}

// TracePermission records a permission decision at a step.
type TracePermission struct {
	Allowed bool
	Code    string
}

// TraceLLMState records post-LLM state at a step.
type TraceLLMState struct {
	MemoryQuery  string
	RetrievedDocs []string
}

// ExecutionResult is the outcome of running a skill.
type ExecutionResult struct {
	Success   bool
	LatencyMs float64
	TokensUsed int
	Error     string
}

// SkillPackage is a portable, shareable skill export.
type SkillPackage struct {
	ID            string
	Name          string
	Description   string
	Trigger       PatternTrigger
	ExecutionPlan []SkillAction
	TestCases     []PatternTest
	Version       int
	UsageStats    *SkillMetrics
	ExportedAt    time.Time
}

// ExecutionPlan is a validated, ready-to-run plan materialized from a pattern.
type ExecutionPlan struct {
	PatternID string
	Steps     []SkillAction
}

// SkillMetrics tracks performance statistics for a skill.
type SkillMetrics struct {
	ID              string
	TotalExecutions int
	SuccessCount    int
	AvgLatencyMs    float64
	AvgTokensUsed   int
}

// ScoredSkill couples a pattern with its fit score for ranking.
type ScoredSkill struct {
	Pattern *PatternDefinition
	Score   float64
}

// SkillConstraints are hard constraints a skill must satisfy to be selected.
type SkillConstraints struct {
	MaxLatencyMs    float64
	MaxTokensUsed   int
	RequiredContext []string
}

// TestResults is the outcome of structurally validating a skill's test cases.
type TestResults struct {
	Passed   bool
	PassRate float64 // 0..1
	Failures []string
}

// LearningInsights summarizes correction-learning analytics.
type LearningInsights struct {
	TotalCorrectionsReceived   int
	CorrectionsConvertedToSkills int
	IterativelyImprovedSkills  int
	TypeDistribution           map[RulePattern]int
	AverageTimeToDeployment    time.Duration
}

// ExecutionHistory tracks past task executions for pattern mining.
type ExecutionHistory struct {
	mu         sync.Mutex
	executions []ExecutionRecord
	limit      int
}

// NewExecutionHistory creates a bounded in-memory history (limit <= 0: 10000).
func NewExecutionHistory(limit int) *ExecutionHistory {
	if limit <= 0 {
		limit = 10000
	}
	return &ExecutionHistory{limit: limit}
}

// Add appends a record, evicting the oldest beyond the limit.
func (h *ExecutionHistory) Add(record ExecutionRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.executions = append(h.executions, record)
	if len(h.executions) > h.limit {
		h.executions = h.executions[len(h.executions)-h.limit:]
	}
}

// Records returns a copy of the stored records in chronological order.
func (h *ExecutionHistory) Records() []ExecutionRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]ExecutionRecord, len(h.executions))
	copy(out, h.executions)
	return out
}

// ============================
// Deterministic text utilities
// ============================

// tokenize lowercases and splits text into alphanumeric words.
func tokenize(text string) []string {
	text = strings.ToLower(text)
	var words []string
	var sb strings.Builder
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
			continue
		}
		if sb.Len() > 0 {
			words = append(words, sb.String())
			sb.Reset()
		}
	}
	if sb.Len() > 0 {
		words = append(words, sb.String())
	}
	return words
}

// stopWords are ignored when extracting keywords.
var stopWords = map[string]bool{
	"a": true, "an": true, "the": true, "is": true, "are": true, "was": true,
	"were": true, "be": true, "been": true, "of": true, "to": true, "in": true,
	"on": true, "for": true, "with": true, "and": true, "or": true, "by": true,
	"at": true, "from": true, "it": true, "this": true, "that": true, "please": true,
}

// extractKeywords pulls the most informative words out of a query.
func extractKeywords(query string) []string {
	seen := make(map[string]bool)
	var keywords []string
	for _, w := range tokenize(query) {
		if len(w) < 3 || stopWords[w] || seen[w] {
			continue
		}
		seen[w] = true
		keywords = append(keywords, w)
	}
	return keywords
}

// keywordOverlap counts how many of a are present in b.
func keywordOverlap(a, b []string) int {
	set := make(map[string]bool, len(b))
	for _, w := range b {
		set[w] = true
	}
	count := 0
	for _, w := range a {
		if set[w] {
			count++
		}
	}
	return count
}

// hashString returns a stable FNV-1a hex digest of s.
func hashString(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return fmt.Sprintf("%016x", h.Sum64())
}

// generateEmbedding produces a deterministic 64-dim "embedding" from a text
// using hashed word features and L2 normalization. It is not semantic, but it
// is stable across runs and correlates shared vocabulary with high cosine
// similarity, which is all the pattern matcher needs offline.
func generateEmbedding(text string) []float32 {
	const dim = 64
	vec := make([]float32, dim)
	for _, w := range tokenize(text) {
		h := fnv.New64a()
		_, _ = h.Write([]byte(w))
		sum := h.Sum64()
		idx := int(sum % uint64(dim))
		weight := float32(1.0)
		vec[idx] += weight
	}
	norm := float64(0)
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vec {
			vec[i] = float32(float64(vec[i]) / norm)
		}
	}
	return vec
}

// cosineSimilarity returns the cosine similarity of two vectors.
// Mismatched dimensions or zero vectors yield 0.
func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}

// sortStrings sorts a copy of a string slice.
func sortStrings(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}

// containsString reports whether s is in slice.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// now is the package clock, overridable in tests.
var now = time.Now
