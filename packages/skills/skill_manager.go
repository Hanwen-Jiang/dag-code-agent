package skills

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// SkillManager orchestrates the skill lifecycle: storage, retrieval,
// approval, versioning, metrics, and portable export/import.
type SkillManager struct {
	patternRecognizer *PatternRecognizer
	feedbackHandler   *FeedbackHandler
	skillStore        SkillStore

	mu sync.RWMutex
	// Pending approvals for human review (patternID -> candidate).
	pendingApprovals map[string]*PatternDefinition
	// Performance tracking per skill.
	metrics map[string]SkillMetrics
}

// NewSkillManager creates the skill management system.
func NewSkillManager(storage SkillStore, config RecognizerConfig) *SkillManager {
	m := &SkillManager{
		patternRecognizer: NewPatternRecognizer(config),
		skillStore:        storage,
		pendingApprovals:  make(map[string]*PatternDefinition),
		metrics:           make(map[string]SkillMetrics),
	}
	if storage != nil {
		m.patternRecognizer.SetStore(storage)
	}
	m.feedbackHandler = NewFeedbackHandler(m, DefaultFeedbackConfig())
	return m
}

// PatternRecognizer exposes the recognizer for advanced use.
func (m *SkillManager) PatternRecognizer() *PatternRecognizer { return m.patternRecognizer }

// FeedbackHandler exposes the feedback handler for correction learning.
func (m *SkillManager) FeedbackHandler() *FeedbackHandler { return m.feedbackHandler }

// SetFeedbackConfig replaces the feedback handler configuration.
func (m *SkillManager) SetFeedbackConfig(cfg FeedbackConfig) {
	m.feedbackHandler = NewFeedbackHandler(m, cfg)
}

// LoadAllSkills initializes the in-memory index from persistent storage.
func (m *SkillManager) LoadAllSkills(ctx context.Context) error {
	if m.skillStore == nil {
		return nil
	}
	patterns, err := m.skillStore.List()
	if err != nil {
		return fmt.Errorf("failed to load skills: %w", err)
	}
	for _, pattern := range patterns {
		if err := m.patternRecognizer.StorePattern(pattern); err != nil {
			return err
		}
	}
	return nil
}

// EnqueueForApproval places a candidate skill in the review queue.
func (m *SkillManager) EnqueueForApproval(pattern *PatternDefinition) error {
	if pattern == nil {
		return fmt.Errorf("nil pattern")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pattern.Status == "" {
		pattern.Status = StatusPending
	}
	pattern.Status = StatusPending
	m.pendingApprovals[pattern.ID] = pattern
	return nil
}

// PendingApprovalIDs lists the queued candidates, sorted.
func (m *SkillManager) PendingApprovalIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.pendingApprovals))
	for id := range m.pendingApprovals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// SubmitForApproval resolves a queued candidate based on the human review.
// userReview is one of: "approve", "reject", "modify".
func (m *SkillManager) SubmitForApproval(patternID, userReview string) error {
	m.mu.Lock()
	pattern, exists := m.pendingApprovals[patternID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("pattern not found in approvals queue: %s", patternID)
	}
	switch userReview {
	case "approve":
		// Validate the test suite structurally before production.
		results := m.runAutomatedTests(pattern)
		if results.PassRate < 0.9 {
			delete(m.pendingApprovals, patternID)
			m.mu.Unlock()
			return fmt.Errorf("automated tests failed with %.0f%% pass rate: %s",
				results.PassRate*100, strings.Join(results.Failures, "; "))
		}
		delete(m.pendingApprovals, patternID)
		m.mu.Unlock()

		pattern.Status = StatusActive
		pattern.ContributorType = "human_approved"
		if err := m.patternRecognizer.StorePattern(pattern); err != nil {
			return err
		}
		m.markDeployed(patternID)
		return nil

	case "reject":
		delete(m.pendingApprovals, patternID)
		m.mu.Unlock()
		return nil

	case "modify":
		// Kept in the queue for re-review; the caller re-submits after editing.
		m.mu.Unlock()
		return nil

	default:
		m.mu.Unlock()
		return fmt.Errorf("unknown review decision %q (want approve|reject|modify)", userReview)
	}
}

// LearnFromHumanCorrection turns a corrected output into a skill candidate
// via the feedback handler and returns the candidate ID.
func (m *SkillManager) LearnFromHumanCorrection(
	ctx context.Context,
	originalOutput, correctedOutput string,
	query string,
	context map[string]interface{},
) (string, error) {
	record := CorrectionRecord{
		Query:           query,
		OriginalOutput:  originalOutput,
		CorrectedOutput: correctedOutput,
		Context:         context,
	}
	before := m.PendingApprovalIDs()
	if err := m.feedbackHandler.ProcessCorrection(ctx, record); err != nil {
		return "", err
	}
	// The handler enqueues the candidate under a deterministic ID; find it
	// as the new entry in the queue.
	seen := make(map[string]bool, len(before))
	for _, id := range before {
		seen[id] = true
	}
	var newID string
	for _, id := range m.PendingApprovalIDs() {
		if !seen[id] {
			newID = id
		}
	}
	if newID == "" {
		return "", fmt.Errorf("no skill candidate was enqueued for correction")
	}
	return newID, nil
}

// markDeployed flips the corresponding correction record to deployed so
// learning insights reflect the outcome.
func (m *SkillManager) markDeployed(patternID string) {
	if h := m.feedbackHandler; h != nil {
		h.mu.Lock()
		for _, record := range h.correctionHistory {
			candidateID := fmt.Sprintf("skill_correction_%s", hashString(record.ID+record.Query)[:12])
			if candidateID == patternID {
				record.Deployed = true
			}
		}
		h.mu.Unlock()
	}
}

// GetBestSkill retrieves the optimal ACTIVE skill for the query, respecting
// constraints. Returns nil when nothing scores high enough.
func (m *SkillManager) GetBestSkill(ctx context.Context, query string, constraints SkillConstraints) *PatternDefinition {
	candidates := m.patternRecognizer.RetrieveMatchingPatterns(ctx, query)
	if len(candidates) == 0 {
		return nil
	}

	type scored struct {
		pattern *PatternDefinition
		score   float64
	}
	var scoredList []scored
	for _, pattern := range candidates {
		if !meetsConstraints(pattern, constraints) {
			continue
		}
		scoredList = append(scoredList, scored{pattern: pattern, score: computeFitScore(pattern, query, constraints)})
	}
	if len(scoredList) == 0 {
		return nil
	}
	sort.Slice(scoredList, func(i, j int) bool {
		if scoredList[i].score != scoredList[j].score {
			return scoredList[i].score > scoredList[j].score
		}
		return scoredList[i].pattern.ID < scoredList[j].pattern.ID
	})

	best := scoredList[0]
	const minFitScore = 0.3
	if best.score >= minFitScore {
		return best.pattern
	}
	return nil
}

// meetsConstraints enforces hard constraints on a candidate.
func meetsConstraints(pattern *PatternDefinition, c SkillConstraints) bool {
	if c.MaxLatencyMs > 0 && pattern.AvgLatencyMs > c.MaxLatencyMs {
		return false
	}
	if c.MaxTokensUsed > 0 && pattern.AvgTokensUsed > c.MaxTokensUsed {
		return false
	}
	for _, req := range c.RequiredContext {
		if !containsString(pattern.ContextRequirements, req) {
			return false
		}
	}
	return true
}

// computeFitScore blends retrieval similarity, quality, and usage.
func computeFitScore(pattern *PatternDefinition, query string, c SkillConstraints) float64 {
	words := tokenize(query)
	overlap := 0.0
	if len(pattern.Trigger.Keywords) > 0 {
		overlap = float64(keywordOverlap(words, pattern.Trigger.Keywords)) / float64(len(pattern.Trigger.Keywords))
	}
	quality := 0.5*pattern.SuccessRate + 0.3*overlap + 0.2*min1(float64(pattern.UsageCount)/10)
	return quality
}

func min1(v float64) float64 {
	if v > 1 {
		return 1
	}
	if v < 0 {
		return 0
	}
	return v
}

// UpdateSkillMetrics records a skill execution result in its running stats.
func (m *SkillManager) UpdateSkillMetrics(patternID string, result ExecutionResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	metrics, exists := m.metrics[patternID]
	if !exists {
		metrics = SkillMetrics{ID: patternID}
	}
	metrics.TotalExecutions++
	if result.Success {
		metrics.SuccessCount++
	}
	count := float64(metrics.TotalExecutions)
	metrics.AvgLatencyMs = (metrics.AvgLatencyMs*(count-1) + result.LatencyMs) / count
	metrics.AvgTokensUsed = int((float64(metrics.AvgTokensUsed)*(count-1) + float64(result.TokensUsed)) / count)
	m.metrics[patternID] = metrics

	// Also fold into the pattern's own running stats (in place).
	m.patternRecognizer.RecordOutcome(patternID, result)
}

// GetStats returns the current performance stats for a skill.
func (m *SkillManager) GetStats(patternID string) *SkillMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if metrics, exists := m.metrics[patternID]; exists {
		return &metrics
	}
	return nil
}

// runAutomatedTests structurally validates a skill's test cases:
// every test must have a query and an expected output (or ground truth), and
// at least one test must exist. This is a deterministic structural check that
// gates approval (not a semantic execution).
func (m *SkillManager) runAutomatedTests(pattern *PatternDefinition) TestResults {
	results := TestResults{Passed: true}
	if len(pattern.TestCases) == 0 {
		results.Passed = false
		results.PassRate = 0
		results.Failures = append(results.Failures, "skill has no test cases")
		return results
	}

	passed := 0
	for i, tc := range pattern.TestCases {
		if strings.TrimSpace(tc.InputQuery) == "" {
			results.Failures = append(results.Failures, fmt.Sprintf("test %d: missing input query", i))
			continue
		}
		if strings.TrimSpace(tc.ExpectedOutput) == "" && len(tc.GroundTruth) == 0 {
			results.Failures = append(results.Failures, fmt.Sprintf("test %d: missing expected output", i))
			continue
		}
		passed++
	}
	results.PassRate = float64(passed) / float64(len(pattern.TestCases))
	results.Passed = results.PassRate >= 0.9 && len(results.Failures) == 0
	return results
}

// ExportSkillPackage exports a skill for sharing across deployments.
func (m *SkillManager) ExportSkillPackage(patternID string) (*SkillPackage, error) {
	pattern, ok := m.patternRecognizer.GetPattern(patternID)
	if !ok {
		return nil, fmt.Errorf("skill not found: %s", patternID)
	}
	stats := m.GetStats(patternID)
	return &SkillPackage{
		ID:            pattern.ID,
		Name:          pattern.Name,
		Description:   pattern.Description,
		Trigger:       pattern.Trigger,
		ExecutionPlan: pattern.ExecutionPlan,
		TestCases:     pattern.TestCases,
		Version:       pattern.Version,
		UsageStats:    stats,
		ExportedAt:    now(),
	}, nil
}

// ImportSkillPackage loads a shared skill package after validation.
func (m *SkillManager) ImportSkillPackage(pkg *SkillPackage) error {
	if pkg == nil {
		return fmt.Errorf("nil package")
	}
	pattern := &PatternDefinition{
		ID:              pkg.ID,
		Name:            pkg.Name,
		Description:     pkg.Description,
		Trigger:         pkg.Trigger,
		ExecutionPlan:   pkg.ExecutionPlan,
		TestCases:       pkg.TestCases,
		Version:         pkg.Version,
		Status:          StatusActive,
		ContributorType: "shared_skill",
	}
	if pattern.Version == 0 {
		pattern.Version = 1
	}

	// Reject packages that fail structural validation.
	results := m.runAutomatedTests(pattern)
	if !results.Passed {
		return fmt.Errorf("import rejected: validation tests failed: %s", strings.Join(results.Failures, "; "))
	}
	return m.patternRecognizer.StorePattern(pattern)
}
