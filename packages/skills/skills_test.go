package skills

import (
	"context"
	"testing"
)

func successRecord(query string, domain string, tools ...string) ExecutionRecord {
	var steps []TraceStep
	for _, tool := range tools {
		steps = append(steps, TraceStep{
			Reasoning: "doing work",
			ToolCalls: []TraceToolCall{{Name: tool, Args: map[string]interface{}{"k": "v"}}},
		})
	}
	return ExecutionRecord{
		TaskID:       "t1",
		Query:        query,
		Steps:        steps,
		Solution:     "the answer",
		IsSuccessful: true,
		LatencyMs:    120,
		TokensUsed:   800,
		Domain:       domain,
	}
}

func TestPatternDiscoveryAndVersioning(t *testing.T) {
	cfg := DefaultRecognizerConfig()
	cfg.RequireHumanApproval = false // auto-store for deterministic test
	r := NewPatternRecognizer(cfg)
	store := NewMemoryStore()
	r.SetStore(store)

	// A multi-step success is mined into an ACTIVE skill.
	r.RecordExecution(context.Background(), successRecord("summarize the database schema", "database", "bash", "read_file"))

	active := r.ListActive()
	if len(active) != 1 {
		t.Fatalf("expected 1 active pattern, got %d", len(active))
	}
	p := active[0]
	if p.Status != StatusActive {
		t.Fatalf("expected ACTIVE, got %s", p.Status)
	}
	if len(p.ExecutionPlan) != 2 {
		t.Fatalf("expected 2-step plan, got %d", len(p.ExecutionPlan))
	}
	if p.Version != 1 {
		t.Fatalf("expected version 1, got %d", p.Version)
	}

	// A near-duplicate execution merges into the existing pattern (same tool
	// sequence => same hash) instead of creating a new skill.
	r.RecordExecution(context.Background(), successRecord("summarize the database schema again", "database", "bash", "read_file"))
	if got := len(r.ListActive()); got != 1 {
		t.Fatalf("duplicate execution should merge, got %d active patterns", got)
	}
	if p, ok := r.GetPattern(p.ID); !ok || p.UsageCount < 2 {
		t.Fatalf("expected merged usage count >= 2, got %+v", p)
	}

	// Retrieval finds the ACTIVE skill by keyword/embedding overlap.
	ctx := context.Background()
	hits := r.RetrieveMatchingPatterns(ctx, "summarize the database schema")
	if len(hits) == 0 || hits[0].ID != p.ID {
		t.Fatalf("expected retrieval to find %s, got %+v", p.ID, hits)
	}

	// Versioning: update the skill, old version becomes DEPRECATED, new one
	// ACTIVE at version 2.
	newID, err := r.UpdateVersion(p.ID, func(up *PatternDefinition) {
		up.Description = "improved description"
	})
	if err != nil {
		t.Fatalf("UpdateVersion: %v", err)
	}
	old, _ := r.GetPattern(p.ID)
	if old.Status != StatusDeprecated {
		t.Fatalf("old version should be DEPRECATED, got %s", old.Status)
	}
	updated, _ := r.GetPattern(newID)
	if updated.Status != StatusActive || updated.Version != 2 {
		t.Fatalf("new version should be ACTIVE v2, got %s v%d", updated.Status, updated.Version)
	}
	if linked, ok := store.VersionOf(p.ID); !ok || linked != newID {
		t.Fatalf("store version link missing: %v %v", linked, ok)
	}

	// Deprecated skills are never retrieved.
	hits = r.RetrieveMatchingPatterns(ctx, "summarize the database schema")
	for _, h := range hits {
		if h.ID == p.ID {
			t.Fatalf("deprecated pattern %s was retrieved", p.ID)
		}
	}

	// Reactivate moves it back to ACTIVE.
	if err := r.Reactivate(p.ID); err != nil {
		t.Fatalf("Reactivate: %v", err)
	}
	if re, _ := r.GetPattern(p.ID); re.Status != StatusActive {
		t.Fatalf("expected reactivation, got %s", re.Status)
	}

	// MarkDeprecated again + idempotency.
	if err := r.MarkDeprecated(p.ID, "obsolete"); err != nil {
		t.Fatalf("MarkDeprecated: %v", err)
	}
	if err := r.MarkDeprecated(p.ID, "obsolete"); err != nil {
		t.Fatalf("MarkDeprecated idempotent: %v", err)
	}
	if len(r.ListDeprecated()) != 1 {
		t.Fatalf("expected 1 deprecated, got %d", len(r.ListDeprecated()))
	}
}

func TestApprovalWorkflow(t *testing.T) {
	cfg := DefaultRecognizerConfig() // RequireHumanApproval = true
	m := NewSkillManager(NewMemoryStore(), cfg)

	// Mined skill lands in the review queue, not the active pool.
	m.PatternRecognizer().RecordExecution(context.Background(),
		successRecord("refactor the login endpoint", "web", "bash", "code_edit", "run_tests"))

	pending := m.PatternRecognizer().PendingApprovals()
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %v", pending)
	}
	id := pending[0]
	if got := len(m.PatternRecognizer().ListActive()); got != 0 {
		t.Fatalf("nothing should be active before approval, got %d", got)
	}

	// Approve => becomes ACTIVE with human_approved provenance.
	if err := m.PatternRecognizer().Approve(id); err != nil {
		t.Fatalf("Approve: %v", err)
	}
	p, ok := m.PatternRecognizer().GetPattern(id)
	if !ok || p.Status != StatusActive || p.ContributorType != "human_approved" {
		t.Fatalf("approval failed: %+v ok=%v", p, ok)
	}
}

func TestFeedbackCorrectionToSkill(t *testing.T) {
	m := NewSkillManager(NewMemoryStore(), DefaultRecognizerConfig())
	ctx := context.Background()

	id, err := m.LearnFromHumanCorrection(ctx,
		"func main() { println(x) }\n",
		"func main() { println(x + 1) }\n",
		"fix the off-by-one in the counter",
		map[string]interface{}{"domain": "go"},
	)
	if err != nil {
		t.Fatalf("LearnFromHumanCorrection: %v", err)
	}

	// The candidate is in the manager's approval queue.
	queued := m.PendingApprovalIDs()
	if len(queued) != 1 || queued[0] != id {
		t.Fatalf("expected queued candidate %s, got %v", id, queued)
	}

	// Approve it; the structural tests must pass (they were generated with
	// an expected output).
	if err := m.SubmitForApproval(id, "approve"); err != nil {
		t.Fatalf("SubmitForApproval: %v", err)
	}
	p, ok := m.PatternRecognizer().GetPattern(id)
	if !ok || p.Status != StatusActive {
		t.Fatalf("corrected skill should be active: ok=%v", ok)
	}
	if len(p.TestCases) < 1 {
		t.Fatalf("expected generated test cases, got %d", len(p.TestCases))
	}

	// Insights reflect the correction.
	insights := m.FeedbackHandler().GetLearningInsights()
	if insights.TotalCorrectionsReceived != 1 {
		t.Fatalf("insights total = %d, want 1", insights.TotalCorrectionsReceived)
	}
	if insights.CorrectionsConvertedToSkills != 1 {
		t.Fatalf("insights deployed = %d, want 1", insights.CorrectionsConvertedToSkills)
	}
}

func TestGetBestSkill(t *testing.T) {
	m := NewSkillManager(NewMemoryStore(), DefaultRecognizerConfig())
	r := m.PatternRecognizer()

	if err := r.StorePattern(&PatternDefinition{
		ID:        "skill_go_tests",
		Name:      "run go tests",
		Trigger: PatternTrigger{
			Type:     KeywordMatch,
			Keywords: []string{"go", "test", "run"},
		},
		ExecutionPlan: []SkillAction{{ActionType: "call_tool", TargetTool: "bash"}},
		Status:        StatusActive,
		Version:       1,
		SuccessRate:   1.0,
	}); err != nil {
		t.Fatal(err)
	}

	best := m.GetBestSkill(context.Background(), "please run the go test suite", SkillConstraints{})
	if best == nil || best.ID != "skill_go_tests" {
		t.Fatalf("expected skill_go_tests, got %+v", best)
	}

	// Constraint filtering: max latency below the pattern's average excludes it.
	strict := m.GetBestSkill(context.Background(), "please run the go test suite",
		SkillConstraints{MaxTokensUsed: 0, MaxLatencyMs: 0, RequiredContext: []string{"memory"}})
	if strict != nil {
		t.Fatalf("constraint (memory) should exclude the pattern, got %+v", strict)
	}
}

func TestExportImport(t *testing.T) {
	src := NewSkillManager(NewMemoryStore(), DefaultRecognizerConfig())
	if err := src.PatternRecognizer().StorePattern(&PatternDefinition{
		ID:      "shared_skill",
		Name:    "shared",
		Trigger: PatternTrigger{Type: KeywordMatch, Keywords: []string{"shared"}},
		ExecutionPlan: []SkillAction{{ActionType: "call_tool", TargetTool: "bash"}},
		Status:        StatusActive,
		Version:       1,
		TestCases: []PatternTest{{
			InputQuery:     "shared case",
			ExpectedOutput: "ok",
			GroundTruth:    []string{"ok"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	pkg, err := src.ExportSkillPackage("shared_skill")
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	dst := NewSkillManager(NewMemoryStore(), DefaultRecognizerConfig())
	if err := dst.ImportSkillPackage(pkg); err != nil {
		t.Fatalf("import: %v", err)
	}
	if p, ok := dst.PatternRecognizer().GetPattern("shared_skill"); !ok || p.ContributorType != "shared_skill" {
		t.Fatalf("imported pattern missing or wrong provenance: %+v ok=%v", p, ok)
	}

	// Import rejects packages without valid tests.
	bad := &SkillPackage{
		ID:        "bad_skill",
		Name:      "bad",
		ExecutionPlan: []SkillAction{{ActionType: "call_tool", TargetTool: "bash"}},
		Version:   1,
		TestCases: []PatternTest{{InputQuery: ""}},
	}
	if err := dst.ImportSkillPackage(bad); err == nil {
		t.Fatal("import should reject a package with invalid tests")
	}
	if _, ok := dst.PatternRecognizer().GetPattern("bad_skill"); ok {
		t.Fatal("bad_skill was stored despite failing validation")
	}
}

func TestMetricsRollingAverage(t *testing.T) {
	m := NewSkillManager(NewMemoryStore(), DefaultRecognizerConfig())
	if err := m.PatternRecognizer().StorePattern(&PatternDefinition{
		ID:      "m1",
		Status:  StatusActive,
		Version: 1,
	}); err != nil {
		t.Fatal(err)
	}

	m.UpdateSkillMetrics("m1", ExecutionResult{Success: true, LatencyMs: 100, TokensUsed: 10})
	m.UpdateSkillMetrics("m1", ExecutionResult{Success: false, LatencyMs: 300, TokensUsed: 30})

	stats := m.GetStats("m1")
	if stats == nil {
		t.Fatal("no stats")
	}
	if stats.TotalExecutions != 2 || stats.SuccessCount != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if stats.AvgLatencyMs != 200 {
		t.Fatalf("avg latency = %v, want 200", stats.AvgLatencyMs)
	}
	if stats.AvgTokensUsed != 20 {
		t.Fatalf("avg tokens = %v, want 20", stats.AvgTokensUsed)
	}

	// Pattern-level success rate should be 0.5 after one success/one failure.
	if p, _ := m.PatternRecognizer().GetPattern("m1"); p.SuccessRate != 0.5 {
		t.Fatalf("pattern success rate = %v, want 0.5", p.SuccessRate)
	}
}
