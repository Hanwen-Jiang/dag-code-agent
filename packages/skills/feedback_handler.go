package skills

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// FeedbackConfig holds learning parameters.
type FeedbackConfig struct {
	AutoLearnThreshold float64       // auto-learn if confidence > threshold
	HumanReviewTimeout time.Duration // wait time before auto-deploying

	// Correction types to learn from.
	LearnFromExplicitFeedback bool // star ratings, text comments
	LearnFromImplicitFeedback bool // successful retries after failure
	LearnFromComparisons      bool // side-by-side output comparisons

	// Validation requirements.
	RequireTestCoverage  bool
	MinTestCasesPerSkill int
}

// DefaultFeedbackConfig returns standard defaults.
func DefaultFeedbackConfig() FeedbackConfig {
	return FeedbackConfig{
		AutoLearnThreshold:      0.85,
		HumanReviewTimeout:      24 * time.Hour,
		LearnFromExplicitFeedback: true,
		LearnFromImplicitFeedback: true,
		LearnFromComparisons:      true,
		RequireTestCoverage:       true,
		MinTestCasesPerSkill:      1,
	}
}

// CorrectionRecord stores a human correction for future learning.
type CorrectionRecord struct {
	ID              string
	Timestamp       time.Time
	TaskID          string
	Query           string
	OriginalOutput  string
	CorrectedOutput string
	Context         map[string]interface{}
	UserID          string
	Satisfaction    int // 1-5 rating if explicit feedback

	// Analysis results (filled by the handler).
	InferredRule *InferredRule
	TestGenerated bool
	Deployed      bool
	Version       int
}

// OutputPreference is the user's choice in a comparison.
type OutputPreference string

const (
	PreferenceA OutputPreference = "a"
	PreferenceB OutputPreference = "b"
)

// RulePattern classifies the kind of change a correction expresses.
type RulePattern string

const (
	CodeFix          RulePattern = "code_fix"
	TextModification RulePattern = "text_modification"
	LogicChange      RulePattern = "logic_change"
	FormattingFix    RulePattern = "formatting_fix"
	SecurityPatch    RulePattern = "security_patch"
)

// InferredRule captures the rule extracted from a correction.
type InferredRule struct {
	Description     string
	PatternType     RulePattern
	RuleTemplate    string
	GuardConditions []string
	Scope           string // when does this rule apply?
}

// DiffResult is the line-based difference between two outputs.
type DiffResult struct {
	AddedLines   []string
	RemovedLines []string
	ContextLines []string
	Ratio        float64 // 0..1, how much the outputs differ
}

// DiffAnalyzer compares two outputs and extracts differences.
type DiffAnalyzer struct {
	config DiffConfig
}

// DiffConfig defines diff analysis parameters.
type DiffConfig struct {
	LineBased              bool
	ContextWindowSize      int
	ExtractSemanticChanges bool
}

// NewDiffAnalyzer creates a diff analyzer.
func NewDiffAnalyzer(config DiffConfig) *DiffAnalyzer {
	return &DiffAnalyzer{config: config}
}

// Analyze computes a deterministic line-based diff.
func (d *DiffAnalyzer) Analyze(original, corrected string) *DiffResult {
	origLines := splitLines(original)
	corrLines := splitLines(corrected)

	origSet := make(map[string]bool, len(origLines))
	corrSet := make(map[string]bool, len(corrLines))
	for _, l := range origLines {
		if strings.TrimSpace(l) != "" {
			origSet[l] = true
		}
	}
	for _, l := range corrLines {
		if strings.TrimSpace(l) != "" {
			corrSet[l] = true
		}
	}

	res := &DiffResult{}
	for _, l := range origLines {
		if !corrSet[l] {
			res.RemovedLines = append(res.RemovedLines, l)
		}
	}
	for _, l := range corrLines {
		if !origSet[l] {
			res.AddedLines = append(res.AddedLines, l)
		}
	}

	union := len(origSet) + len(corrSet) - overlapCount(origSet, corrSet)
	if union == 0 {
		res.Ratio = 0
	} else {
		res.Ratio = float64(len(res.AddedLines)+len(res.RemovedLines)) / float64(union)
	}
	sort.Strings(res.AddedLines)
	sort.Strings(res.RemovedLines)
	return res
}

func splitLines(s string) []string {
	var lines []string
	for _, l := range strings.Split(s, "\n") {
		lines = append(lines, strings.TrimRight(l, "\r"))
	}
	return lines
}

func overlapCount(a, b map[string]bool) int {
	n := 0
	for k := range a {
		if b[k] {
			n++
		}
	}
	return n
}

// FeedbackHandler manages human-in-the-loop learning from corrections.
type FeedbackHandler struct {
	config       FeedbackConfig
	skillManager *SkillManager
	diffAnalyzer *DiffAnalyzer

	mu                sync.Mutex
	correctionHistory []*CorrectionRecord
	correctionCounter int
}

// NewFeedbackHandler creates the correction learning system.
func NewFeedbackHandler(skillMgr *SkillManager, config FeedbackConfig) *FeedbackHandler {
	return &FeedbackHandler{
		config:       config,
		skillManager: skillMgr,
		diffAnalyzer: NewDiffAnalyzer(DiffConfig{
			LineBased:           true,
			ContextWindowSize:   5,
			ExtractSemanticChanges: true,
		}),
		correctionHistory: make([]*CorrectionRecord, 0),
	}
}

// ProcessCorrection handles an incoming human correction: diff analysis,
// rule inference, test generation, and submission for skill approval.
func (h *FeedbackHandler) ProcessCorrection(ctx context.Context, record CorrectionRecord) error {
	if record.ID == "" {
		h.mu.Lock()
		h.correctionCounter++
		record.ID = fmt.Sprintf("corr_%d", h.correctionCounter)
		record.Timestamp = now()
		h.mu.Unlock()
	}

	h.mu.Lock()
	h.correctionHistory = append(h.correctionHistory, &record)
	h.mu.Unlock()

	// Step 1: analyze the difference.
	diff := h.diffAnalyzer.Analyze(record.OriginalOutput, record.CorrectedOutput)

	// Step 2: infer the underlying rule.
	rule := h.inferRule(diff, record.Query, record.Context)
	record.InferredRule = rule

	// Step 3: generate test cases.
	testCases := h.generateTestCases(rule, record)
	if h.config.RequireTestCoverage && len(testCases) < h.config.MinTestCasesPerSkill {
		return fmt.Errorf("insufficient test coverage (%d tests generated, need %d)",
			len(testCases), h.config.MinTestCasesPerSkill)
	}
	record.TestGenerated = len(testCases) > 0

	// Step 4: create the candidate skill and enqueue for approval.
	candidate := h.createCandidateSkill(rule, record, testCases)
	return h.skillManager.EnqueueForApproval(candidate)
}

// inferRule derives a generalizable rule from a specific correction.
func (h *FeedbackHandler) inferRule(diff *DiffResult, query string, context map[string]interface{}) *InferredRule {
	patternType := classifyDiffPattern(diff)

	template := ""
	switch patternType {
	case CodeFix:
		template = "replace removed code lines with corrected lines"
	case TextModification:
		template = "apply textual corrections to the original output"
	case LogicChange:
		template = "restructure logic per corrected output"
	case FormattingFix:
		template = "reformat output to match corrected layout"
	case SecurityPatch:
		template = "apply security hardening per corrected output"
	default:
		template = "adjust output per corrected example"
	}

	guards := make([]string, 0, 1)
	if query != "" {
		guards = append(guards, "contains:"+firstKeyword(query))
	}

	return &InferredRule{
		Description:     summarizeCorrection(diff),
		PatternType:     patternType,
		RuleTemplate:    template,
		GuardConditions: guards,
		Scope:           determineScope(context, query),
	}
}

// createCandidateSkill builds the formal skill definition from a rule.
func (h *FeedbackHandler) createCandidateSkill(rule *InferredRule, record CorrectionRecord, testCases []PatternTest) *PatternDefinition {
	id := fmt.Sprintf("skill_correction_%s", hashString(record.ID+record.Query)[:12])
	return &PatternDefinition{
		ID:          id,
		Name:        "Correction: " + rule.Description,
		Description: fmt.Sprintf("Derived from human correction %s. %s", record.ID, rule.Description),
		Trigger: PatternTrigger{
			Type:           KeywordMatch,
			QueryEmbedding: generateEmbedding(record.Query),
			Keywords:       extractKeywords(record.Query),
		},
		ExecutionPlan: []SkillAction{{
			ActionType: "apply_rule",
			Parameters: map[string]interface{}{
				"rule":   rule.RuleTemplate,
				"input":  "context.input",
				"output": "result",
			},
			Expectation: "matches_corrected_output",
			Condition:   strings.Join(rule.GuardConditions, " AND "),
		}},
		ContextRequirements: identifyContextRequires(record.Context),
		TestCases:           testCases,
		Status:              StatusPending,
		Version:             1,
		ContributorType:     "human_feedback",
	}
}

// generateTestCases creates validation tests from a correction: the exact
// case plus a paraphrase variation.
func (h *FeedbackHandler) generateTestCases(rule *InferredRule, record CorrectionRecord) []PatternTest {
	var testCases []PatternTest

	// Test 1: the exact original query -> corrected output.
	testCases = append(testCases, PatternTest{
		InputQuery:     record.Query,
		ExpectedOutput: record.CorrectedOutput,
		GroundTruth:    []string{record.CorrectedOutput},
	})

	// Test 2: a variation of the query.
	if variation := varyQuery(record.Query); variation != record.Query && variation != "" {
		testCases = append(testCases, PatternTest{
			InputQuery:     variation,
			ExpectedOutput: record.CorrectedOutput,
			GroundTruth:    []string{record.CorrectedOutput},
		})
	}

	return testCases
}

// ProcessComparisonFeedback handles a side-by-side output comparison as an
// implicit correction (the chosen output wins).
func (h *FeedbackHandler) ProcessComparisonFeedback(
	ctx context.Context,
	query string,
	outputA string,
	outputB string,
	userChoice OutputPreference,
	context map[string]interface{},
) error {
	if !h.config.LearnFromComparisons {
		return nil
	}
	var winner, loser string
	if userChoice == PreferenceA {
		winner, loser = outputA, outputB
	} else {
		winner, loser = outputB, outputA
	}

	h.mu.Lock()
	h.correctionCounter++
	record := CorrectionRecord{
		ID:              fmt.Sprintf("comparison_%d", h.correctionCounter),
		Timestamp:       now(),
		Query:           query,
		OriginalOutput:  loser,
		CorrectedOutput: winner,
		Context:         context,
	}
	h.mu.Unlock()

	return h.ProcessCorrection(ctx, record)
}

// GetLearningInsights provides analytics on correction patterns.
func (h *FeedbackHandler) GetLearningInsights() LearningInsights {
	h.mu.Lock()
	defer h.mu.Unlock()

	deployed := 0
	improved := 0
	typeCounts := make(map[RulePattern]int)
	for _, record := range h.correctionHistory {
		if record.Deployed {
			deployed++
		}
		if record.Version > 1 {
			improved++
		}
		if record.InferredRule != nil {
			typeCounts[record.InferredRule.PatternType]++
		}
	}

	return LearningInsights{
		TotalCorrectionsReceived:      len(h.correctionHistory),
		CorrectionsConvertedToSkills:  deployed,
		IterativelyImprovedSkills:     improved,
		TypeDistribution:              typeCounts,
	}
}

// classifyDiffPattern categorizes the correction by its diff signature.
func classifyDiffPattern(diff *DiffResult) RulePattern {
	joined := strings.ToLower(strings.Join(append(append([]string{}, diff.AddedLines...), diff.RemovedLines...), " "))
	switch {
	case containsAny(joined, "password", "secret", "token", "sudo", "rm -rf", "chmod", "api_key", "credential"):
		return SecurityPatch
	case containsAny(joined, "if ", "else", "return", "func ", "for ", "==", "while", "def ", "=>"):
		return CodeFix
	case containsAny(joined, "because", "therefore", "instead", "however", "should"):
		return LogicChange
	case len(diff.AddedLines)+len(diff.RemovedLines) <= 2 && diff.Ratio < 0.4:
		return FormattingFix
	default:
		return TextModification
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func firstKeyword(query string) string {
	kw := extractKeywords(query)
	if len(kw) == 0 {
		return query
	}
	return kw[0]
}

func varyQuery(query string) string {
	q := strings.TrimSpace(query)
	if q == "" {
		return ""
	}
	// Simple deterministic paraphrase: drop a trailing period, prefix "Also".
	trimmed := strings.TrimSuffix(q, ".")
	return "Also: " + trimmed
}

func summarizeCorrection(diff *DiffResult) string {
	if len(diff.AddedLines) == 0 && len(diff.RemovedLines) == 0 {
		return "no change"
	}
	parts := make([]string, 0, 2)
	if len(diff.AddedLines) > 0 {
		parts = append(parts, fmt.Sprintf("%d line(s) added", len(diff.AddedLines)))
	}
	if len(diff.RemovedLines) > 0 {
		parts = append(parts, fmt.Sprintf("%d line(s) removed", len(diff.RemovedLines)))
	}
	return strings.Join(parts, ", ")
}

func determineScope(context map[string]interface{}, query string) string {
	if d, ok := context["domain"].(string); ok && d != "" {
		return d
	}
	if t, ok := context["task_type"].(string); ok && t != "" {
		return t
	}
	if query != "" {
		return firstKeyword(query)
	}
	return "general"
}

func identifyContextRequires(context map[string]interface{}) []string {
	if context == nil {
		return nil
	}
	keys := make([]string, 0, len(context))
	for k := range context {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
