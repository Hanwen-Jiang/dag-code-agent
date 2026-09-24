// Package skills implements skill discovery, learning, versioning and
// lifecycle management (ACTIVE / DEPRECATED) for reusable agent capabilities.
package skills

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// RecognizerConfig holds pattern analysis parameters.
type RecognizerConfig struct {
	MinPatternConfidence   float64 // minimum success rate for a mined pattern to be kept
	MaxSimilarityThreshold float64 // similarity above which a new pattern merges into an existing one
	RequireToolSequence    bool    // whether tool sequence matters for pattern matching

	// Learning preferences.
	AutoExtractFromSuccesses bool // mine skills from successful executions
	AutoExtractFromFailures  bool // mine anti-patterns from failures
	RequireHumanApproval     bool // mined skills go to review instead of the ACTIVE pool
}

// DefaultRecognizerConfig returns standard defaults.
func DefaultRecognizerConfig() RecognizerConfig {
	return RecognizerConfig{
		MinPatternConfidence:   0.75,
		MaxSimilarityThreshold: 0.85,
		RequireToolSequence:    true,
		AutoExtractFromSuccesses: true,
		AutoExtractFromFailures:  false,
		RequireHumanApproval:     true,
	}
}

// SkillStore is the persistence backend for skill definitions.
type SkillStore interface {
	Save(pattern *PatternDefinition) error
	Load(id string) (*PatternDefinition, error)
	List() ([]*PatternDefinition, error)
	Delete(id string) error
	// UpdateVersion persists the replacement and records the supersession.
	UpdateVersion(oldID, newID string) error
}

// StoreCallback observes state changes (store-ready, approval requests).
type StoreCallback func(pattern *PatternDefinition, action string)

// PatternRecognizer identifies reusable reasoning patterns from task
// execution and manages their versioning and lifecycle.
type PatternRecognizer struct {
	config  RecognizerConfig
	store   SkillStore // optional; nil = in-memory only

	mu       sync.RWMutex
	patterns map[string]*PatternDefinition
	history  *ExecutionHistory

	// pendingApprovals holds mined patterns awaiting human review.
	pendingApprovals map[string]*PatternDefinition
	// onStore fires whenever a pattern is stored, approved or deprecated.
	onStore StoreCallback
	// idCounter makes IDs deterministic for tests when time would collide.
	idCounter int
}

// NewPatternRecognizer creates the pattern recognition system.
func NewPatternRecognizer(config RecognizerConfig) *PatternRecognizer {
	return &PatternRecognizer{
		config:           config,
		patterns:         make(map[string]*PatternDefinition),
		history:          NewExecutionHistory(10000),
		pendingApprovals: make(map[string]*PatternDefinition),
	}
}

// SetStore attaches a persistence backend.
func (r *PatternRecognizer) SetStore(store SkillStore) { r.store = store }

// SetStoreCallback installs an observer for state transitions.
func (r *PatternRecognizer) SetStoreCallback(cb StoreCallback) { r.onStore = cb }

// PendingApprovals returns a copy of the review queue IDs.
func (r *PatternRecognizer) PendingApprovals() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.pendingApprovals))
	for id := range r.pendingApprovals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// RecordExecution stores a task result and, when configured, mines a skill
// from it (synchronously and deterministically).
func (r *PatternRecognizer) RecordExecution(ctx context.Context, record ExecutionRecord) {
	r.history.Add(record)

	learn := false
	if record.IsSuccessful && r.config.AutoExtractFromSuccesses {
		learn = true
	}
	if !record.IsSuccessful && r.config.AutoExtractFromFailures {
		learn = true
	}
	if !learn {
		return
	}
	pattern := r.MinePattern(record)
	if pattern == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.config.RequireHumanApproval {
		r.pendingApprovals[pattern.ID] = pattern
		if r.onStore != nil {
			r.onStore(pattern, "pending_approval")
		}
		return
	}
	if err := r.storeLocked(pattern); err != nil {
		// Dropped on store error; the caller can inspect PendingApprovals.
		r.pendingApprovals[pattern.ID] = pattern
	}
}

// MinePattern extracts a reusable pattern from a single execution record.
// It returns nil when the record does not meet quality thresholds
// (too few steps, permission denials, excessive token usage, low success).
func (r *PatternRecognizer) MinePattern(record ExecutionRecord) *PatternDefinition {
	analysis := analyzeExecutionPattern(record.Steps)

	// Quality gates.
	if len(analysis.ToolSequence) < 2 {
		return nil
	}
	if hasPermissionDenials(record.Steps) {
		return nil
	}
	if record.TokensUsed > 10000 {
		return nil
	}
	successRate := 0.0
	if record.IsSuccessful {
		successRate = 1.0
	}
	if successRate < r.config.MinPatternConfidence {
		return nil
	}

	// Duplicate detection: if an equivalent pattern already exists (same tool
	// sequence fingerprint) or is highly similar, update it instead of
	// creating a new versioned skill.
	existing := r.findEquivalent(analysis, record)
	if existing != nil {
		r.mergeWithExisting(existing, record)
		return existing
	}

	pattern := &PatternDefinition{
		ID:        r.nextID("skill", record.Query, analysis.ToolSequence),
		Name:      inferSkillName(record.Query, analysis.ToolSequence),
		Trigger: PatternTrigger{
			Type:          EmbeddingMatch,
			QueryEmbedding: generateEmbedding(record.Query),
			Keywords:      extractKeywords(record.Query),
			DomainFilters: domainFilters(record),
		},
		ExecutionPlan:       buildSkillActions(analysis.ToolSequence),
		ContextRequirements: identifyContextRequirements(analysis),
		Status:              StatusActive,
		Version:             1,
		UsageCount:          1,
		AvgLatencyMs:        record.LatencyMs,
		AvgTokensUsed:       record.TokensUsed,
		SuccessRate:         successRate,
		ExampleQueries:      []string{record.Query},
		ContributorType:     "auto_learned",
		TestCases:           r.generateTestCases(record),
		Hash:                computePatternHash(analysis.ToolSequence),
	}
	return pattern
}

// Approve promotes a pending pattern into the ACTIVE pool.
func (r *PatternRecognizer) Approve(patternID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	pattern, ok := r.pendingApprovals[patternID]
	if !ok {
		return fmt.Errorf("pattern not in approval queue: %s", patternID)
	}
	pattern.Status = StatusActive
	pattern.ContributorType = "human_approved"
	pattern.Version++
	delete(r.pendingApprovals, patternID)
	return r.storeLocked(pattern)
}

// Reject removes a pending pattern from the queue.
func (r *PatternRecognizer) Reject(patternID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pendingApprovals[patternID]; !ok {
		return fmt.Errorf("pattern not in approval queue: %s", patternID)
	}
	delete(r.pendingApprovals, patternID)
	return nil
}

// StorePattern adds or replaces an ACTIVE pattern (used for imports and
// tests).
func (r *PatternRecognizer) StorePattern(pattern *PatternDefinition) error {
	if pattern == nil {
		return fmt.Errorf("nil pattern")
	}
	if pattern.Status == "" {
		pattern.Status = StatusActive
	}
	if pattern.Version == 0 {
		pattern.Version = 1
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.storeLocked(pattern)
}

func (r *PatternRecognizer) storeLocked(pattern *PatternDefinition) error {
	if r.store != nil {
		if err := r.store.Save(pattern); err != nil {
			return err
		}
	}
	r.patterns[pattern.ID] = pattern
	if r.onStore != nil {
		r.onStore(pattern, "stored")
	}
	return nil
}

// GetPattern returns a copy of a pattern by ID.
func (r *PatternRecognizer) GetPattern(id string) (*PatternDefinition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.patterns[id]
	if !ok {
		return nil, false
	}
	cp := *p
	return &cp, true
}

// ListActive returns all ACTIVE patterns sorted by ID.
func (r *PatternRecognizer) ListActive() []*PatternDefinition {
	return r.listByStatus(StatusActive)
}

// ListDeprecated returns all DEPRECATED patterns sorted by ID.
func (r *PatternRecognizer) ListDeprecated() []*PatternDefinition {
	return r.listByStatus(StatusDeprecated)
}

func (r *PatternRecognizer) listByStatus(status SkillStatus) []*PatternDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*PatternDefinition, 0)
	for _, p := range r.patterns {
		if p.Status == status {
			cp := *p
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// MarkDeprecated transitions an ACTIVE skill to DEPRECATED. Deprecated skills
// are never returned by retrieval but remain queryable for audit/export.
func (r *PatternRecognizer) MarkDeprecated(patternID, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.patterns[patternID]
	if !ok {
		return fmt.Errorf("pattern not found: %s", patternID)
	}
	if p.Status == StatusDeprecated {
		return nil // idempotent
	}
	p.Status = StatusDeprecated
	if reason != "" {
		p.Description = strings.TrimRight(p.Description, " ") + " [deprecated: " + reason + "]"
	}
	if r.store != nil {
		if err := r.store.Save(p); err != nil {
			return err
		}
	}
	if r.onStore != nil {
		r.onStore(p, "deprecated")
	}
	return nil
}

// Reactivate moves a DEPRECATED skill back to ACTIVE (e.g. after a fix).
func (r *PatternRecognizer) Reactivate(patternID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.patterns[patternID]
	if !ok {
		return fmt.Errorf("pattern not found: %s", patternID)
	}
	p.Status = StatusActive
	if r.store != nil {
		if err := r.store.Save(p); err != nil {
			return err
		}
	}
	if r.onStore != nil {
		r.onStore(p, "reactivated")
	}
	return nil
}

// UpdateVersion creates the next version of a skill (version bump + hash
// change) and deprecates the old version, keeping both retrievable-by-ID.
func (r *PatternRecognizer) UpdateVersion(patternID string, mutate func(*PatternDefinition)) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	old, ok := r.patterns[patternID]
	if !ok {
		return "", fmt.Errorf("pattern not found: %s", patternID)
	}

	updated := *old // full copy
	updated.ID = r.nextID("skill", updated.Name, []string{fmt.Sprintf("v%d", updated.Version)})
	updated.Version = old.Version + 1
	updated.LastUsed = time.Time{}
	mutate(&updated)
	updated.Hash = computePatternHash(actionFingerprints(updated.ExecutionPlan))

	r.patterns[updated.ID] = &updated
	r.patterns[patternID].Status = StatusDeprecated

	if r.store != nil {
		if err := r.store.Save(&updated); err != nil {
			return "", err
		}
		if err := r.store.Save(r.patterns[patternID]); err != nil {
			return "", err
		}
		if err := r.store.UpdateVersion(patternID, updated.ID); err != nil {
			return "", err
		}
	}
	if r.onStore != nil {
		r.onStore(&updated, "version_updated")
		r.onStore(r.patterns[patternID], "deprecated")
	}
	return updated.ID, nil
}

// updateUsageMetrics folds an execution outcome into a pattern's running
// statistics (rolling averages, success rate, usage count).
func (r *PatternRecognizer) updateUsageMetrics(pattern *PatternDefinition, result ExecutionResult) {
	count := float64(pattern.UsageCount)
	pattern.UsageCount++
	if pattern.UsageCount == 1 {
		pattern.AvgLatencyMs = result.LatencyMs
		pattern.AvgTokensUsed = result.TokensUsed
	} else {
		pattern.AvgLatencyMs = (pattern.AvgLatencyMs*count + result.LatencyMs) / float64(pattern.UsageCount)
		pattern.AvgTokensUsed = int((float64(pattern.AvgTokensUsed)*count + float64(result.TokensUsed)) / float64(pattern.UsageCount))
	}
	if result.Success {
		pattern.SuccessRate = (pattern.SuccessRate * count + 1.0) / float64(pattern.UsageCount)
	} else {
		pattern.SuccessRate = (pattern.SuccessRate * count) / float64(pattern.UsageCount)
	}
	pattern.LastUsed = now()
}

// RetrieveMatchingPatterns finds ACTIVE skills applicable to the query.
// Matching uses embedding cosine similarity and keyword overlap; results are
// sorted by relevance (descending) and truncated to 5.
func (r *PatternRecognizer) RetrieveMatchingPatterns(ctx context.Context, query string) []*PatternDefinition {
	queryEmb := generateEmbedding(query)
	queryWords := tokenize(query)

	type hit struct {
		pattern *PatternDefinition
		score   float64
	}
	var hits []hit
	seen := make(map[string]bool)

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, pattern := range r.patterns {
		if pattern.Status != StatusActive {
			continue // DEPRECATED/PENDING are never retrieved
		}
		if seen[pattern.ID] {
			continue
		}
		score := 0.0
		emb := cosineSimilarity(queryEmb, pattern.Trigger.QueryEmbedding)
		if pattern.Trigger.Type == EmbeddingMatch && emb > 0.3 {
			score = float64(emb) * 0.6
		}
		overlap := float64(keywordOverlap(queryWords, pattern.Trigger.Keywords))
		if len(pattern.Trigger.Keywords) > 0 {
			overlapScore := overlap / float64(len(pattern.Trigger.Keywords))
			score += overlapScore * 0.4
		}
		if score > 0.05 {
			seen[pattern.ID] = true
			hits = append(hits, hit{pattern: pattern, score: score})
		}
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].score != hits[j].score {
			return hits[i].score > hits[j].score
		}
		return hits[i].pattern.ID < hits[j].pattern.ID // deterministic tie-break
	})

	const maxResults = 5
	if len(hits) > maxResults {
		hits = hits[:maxResults]
	}
	out := make([]*PatternDefinition, 0, len(hits))
	for _, h := range hits {
		cp := *h.pattern
		out = append(out, &cp)
	}
	return out
}

// ApplyPattern validates and materializes a pattern into an ExecutionPlan.
// Actions are validated in order; the first failed condition/expectation
// aborts with an error. Metrics are updated on success.
func (r *PatternRecognizer) ApplyPattern(ctx context.Context, pattern *PatternDefinition, query string, executor ActionExecutor) (*ExecutionPlan, error) {
	if pattern == nil {
		return nil, fmt.Errorf("nil pattern")
	}
	if pattern.Status == StatusDeprecated {
		return nil, fmt.Errorf("pattern %s is deprecated", pattern.ID)
	}

	plan := &ExecutionPlan{PatternID: pattern.ID, Steps: make([]SkillAction, 0, len(pattern.ExecutionPlan))}
	result := ExecutionResult{Success: true}
	for i, action := range pattern.ExecutionPlan {
		if action.Condition != "" && !evalCondition(action.Condition, query) {
			return nil, fmt.Errorf("action %d condition not met: %s", i, action.Condition)
		}
		if executor != nil && action.ActionType == "call_tool" {
			output, err := executor.ExecuteAction(ctx, action)
			if err != nil {
				result = ExecutionResult{Success: false, Error: err.Error()}
				r.recordOutcome(pattern.ID, result)
				return nil, fmt.Errorf("action %d failed: %w", i, err)
			}
			_ = output
		}
		plan.Steps = append(plan.Steps, action)
	}
	r.recordOutcome(pattern.ID, result)
	return plan, nil
}

// ActionExecutor executes "call_tool" skill actions. Nil means validation-only
// mode (actions are planned but not executed).
type ActionExecutor interface {
	ExecuteAction(ctx context.Context, action SkillAction) (map[string]interface{}, error)
}

func (r *PatternRecognizer) recordOutcome(patternID string, result ExecutionResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.patterns[patternID]; ok {
		r.updateUsageMetrics(p, result)
	}
}

// History returns the execution history backing this recognizer.
func (r *PatternRecognizer) History() *ExecutionHistory { return r.history }

// RecordOutcome folds an execution result into a stored pattern's running
// statistics (in place). Unknown pattern IDs are ignored.
func (r *PatternRecognizer) RecordOutcome(patternID string, result ExecutionResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.patterns[patternID]; ok {
		r.updateUsageMetrics(p, result)
	}
}

// ============================
// Internal pattern mining
// ============================

// ExecutionAnalysis is the extracted structure of an execution.
type ExecutionAnalysis struct {
	ToolSequence []string
	Observations []string
	HasMemory    bool
	HasLoopGuard bool
}

func analyzeExecutionPattern(steps []TraceStep) ExecutionAnalysis {
	var analysis ExecutionAnalysis
	for _, step := range steps {
		for _, tc := range step.ToolCalls {
			analysis.ToolSequence = append(analysis.ToolSequence, tc.Name)
		}
		if step.Reasoning != "" {
			analysis.Observations = append(analysis.Observations, step.Reasoning)
		}
		if step.StateAfterLLM.MemoryQuery != "" {
			analysis.HasMemory = true
		}
		if step.LoopDecision.Triggered {
			analysis.HasLoopGuard = true
		}
	}
	return analysis
}

func hasPermissionDenials(steps []TraceStep) bool {
	for _, step := range steps {
		if step.PermissionCheck != nil && !step.PermissionCheck.Allowed {
			return true
		}
	}
	return false
}

func domainFilters(record ExecutionRecord) []string {
	if record.Domain == "" {
		return nil
	}
	return []string{record.Domain}
}

// inferSkillName derives a human-readable skill name from the query and the
// first tool used.
func inferSkillName(query string, toolSequence []string) string {
	keywords := extractKeywords(query)
	if len(keywords) == 0 {
		return "unnamed_skill"
	}
	head := keywords[0]
	if len(keywords) > 1 {
		head += "_" + keywords[1]
	}
	if len(toolSequence) > 0 {
		head += "_" + strings.SplitN(toolSequence[0], ":", 2)[0]
	}
	return head
}

func buildSkillActions(toolSequence []string) []SkillAction {
	actions := make([]SkillAction, 0, len(toolSequence))
	for _, entry := range toolSequence {
		name := entry
		if idx := strings.Index(entry, ":"); idx > 0 {
			name = entry[:idx]
		}
		actions = append(actions, SkillAction{
			ActionType: "call_tool",
			TargetTool: name,
			TimeoutMs:  30000,
		})
	}
	return actions
}

func identifyContextRequirements(analysis ExecutionAnalysis) []string {
	var reqs []string
	if analysis.HasMemory {
		reqs = append(reqs, "memory")
	}
	if analysis.HasLoopGuard {
		reqs = append(reqs, "loop_guard")
	}
	return reqs
}

func (r *PatternRecognizer) generateTestCases(record ExecutionRecord) []PatternTest {
	tests := make([]PatternTest, 0, 1)
	if record.Solution != "" {
		tests = append(tests, PatternTest{
			InputQuery:     record.Query,
			ExpectedOutput: record.Solution,
			GroundTruth:    []string{record.Solution},
		})
	}
	return tests
}

func (r *PatternRecognizer) findEquivalent(analysis ExecutionAnalysis, record ExecutionRecord) *PatternDefinition {
	fingerprint := computePatternHash(analysis.ToolSequence)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.patterns {
		if p.Hash == fingerprint && p.Status == StatusActive {
			return p
		}
	}
	// Near-duplicate by embedding similarity.
	for _, p := range r.patterns {
		if p.Status != StatusActive {
			continue
		}
		sim := float64(cosineSimilarity(generateEmbedding(record.Query), p.Trigger.QueryEmbedding))
		if sim >= r.config.MaxSimilarityThreshold &&
			computePatternHash(analysis.ToolSequence) == p.Hash {
			return p
		}
	}
	return nil
}

// mergeWithExisting folds another execution into an existing pattern: bumps
// usage stats and merges keywords, keeping the pattern fresh.
func (r *PatternRecognizer) mergeWithExisting(existing *PatternDefinition, record ExecutionRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if record.Query != "" && !containsString(existing.ExampleQueries, record.Query) {
		existing.ExampleQueries = append(existing.ExampleQueries, record.Query)
	}
	for _, kw := range extractKeywords(record.Query) {
		if !containsString(existing.Trigger.Keywords, kw) {
			existing.Trigger.Keywords = append(existing.Trigger.Keywords, kw)
		}
	}
	r.updateUsageMetrics(existing, ExecutionResult{
		Success:    record.IsSuccessful,
		LatencyMs:  record.LatencyMs,
		TokensUsed: record.TokensUsed,
	})
	if r.store != nil {
		_ = r.store.Save(existing)
	}
}

// computePatternHash fingerprints a tool sequence (or action list) for
// duplicate detection.
func computePatternHash(sequence []string) string {
	return hashString(strings.Join(sequence, "->"))
}

// actionFingerprints extracts the tool sequence from an execution plan.
func actionFingerprints(plan []SkillAction) []string {
	out := make([]string, 0, len(plan))
	for _, a := range plan {
		out = append(out, a.TargetTool)
	}
	return out
}

// evalCondition evaluates the limited guard-condition language used by skills:
// "contains:<word>" requires the word in the query; "eq:<value>" compares the
// whole query; an empty condition is always true.
func evalCondition(condition, query string) bool {
	condition = strings.TrimSpace(condition)
	if condition == "" {
		return true
	}
	if strings.HasPrefix(condition, "contains:") {
		return strings.Contains(strings.ToLower(query), strings.ToLower(strings.TrimPrefix(condition, "contains:")))
	}
	if strings.HasPrefix(condition, "eq:") {
		return query == strings.TrimPrefix(condition, "eq:")
	}
	return false
}

func (r *PatternRecognizer) nextID(prefix, query string, sequence []string) string {
	r.idCounter++
	key := fmt.Sprintf("%s:%s:%s:%d", prefix, hashString(query), strings.Join(sequence, ","), r.idCounter)
	return prefix + "_" + hashString(key)[:12]
}
