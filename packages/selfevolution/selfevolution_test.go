package selfevolution

import (
	"context"
	"testing"
	"time"
)

// stubRunner returns a fixed output for every task.
type stubRunner struct{ output string }

func (s stubRunner) Run(ctx context.Context, task Task) (string, int, float64, error) {
	return s.output, 3, 0.001, nil
}

func TestEvaluateSuccessWithGroundTruth(t *testing.T) {
	e := NewEvaluator(DefaultEvaluationConfig()).WithRunner(stubRunner{output: "The answer is 42"})
	tasks := []Task{{ID: "t1", Type: "math", Query: "what is 6*7?", GroundTruth: []string{"42", "answer"}}}

	results, err := e.Evaluate(context.Background(), tasks)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if !results[0].OK {
		t.Fatalf("expected success, got failure (output=%q)", results[0].Output)
	}

	sum := e.Aggregate(results)
	if sum.SuccessRate != 1.0 {
		t.Fatalf("success rate = %v, want 1.0", sum.SuccessRate)
	}
	if sum.Samples != 1 {
		t.Fatalf("samples = %d, want 1", sum.Samples)
	}
}

func TestEvaluateFailureWhenConceptMissing(t *testing.T) {
	e := NewEvaluator(DefaultEvaluationConfig()).WithRunner(stubRunner{output: "I have no idea."})
	tasks := []Task{{ID: "t1", Type: "qa", Query: "capital of France?", GroundTruth: []string{"Paris"}}}

	results, err := e.Evaluate(context.Background(), tasks)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if results[0].OK {
		t.Fatal("expected failure when ground truth concept is missing")
	}
	if results[0].FailureCategory != "incomplete_output" {
		t.Fatalf("failure category = %q, want incomplete_output", results[0].FailureCategory)
	}

	sum := e.Aggregate(results)
	if sum.SuccessRate != 0 {
		t.Fatalf("success rate = %v, want 0", sum.SuccessRate)
	}
	if sum.Samples != 1 || sum.FailureCounts["incomplete_output"] != 1 {
		t.Fatalf("unexpected aggregate: %+v", sum)
	}
}

func TestAggregateLatencyAndCost(t *testing.T) {
	// Two successes, one failure; verify mean latency and cost/success.
	cases := []TaskResult{
		{TaskID: "a", OK: true, Steps: 1, LatencyMS: 10, CostUSD: 0.001},
		{TaskID: "b", OK: true, Steps: 2, LatencyMS: 30, CostUSD: 0.002},
		{TaskID: "c", OK: false, LatencyMS: 20, FailureCategory: "timeout"},
	}
	sum := NewEvaluator(DefaultEvaluationConfig()).Aggregate(cases)
	if sum.Samples != 3 {
		t.Fatalf("samples = %d, want 3", sum.Samples)
	}
	if sum.SuccessRate != 2.0/3.0 {
		t.Fatalf("success rate = %v, want %v", sum.SuccessRate, 2.0/3.0)
	}
	// mean latency over all samples = (10+30+20)/3 = 20
	if sum.MeanLatencyMs != 20 {
		t.Fatalf("mean latency = %v, want 20", sum.MeanLatencyMs)
	}
	// cost per success = (0.001+0.002)/2 = 0.0015
	want := 0.0015
	if sum.CostPerSuccess < want-1e-9 || sum.CostPerSuccess > want+1e-9 {
		t.Fatalf("cost per success = %v, want %v", sum.CostPerSuccess, want)
	}
}

func TestAblationStudyRunAndDeltas(t *testing.T) {
	cfg := DefaultAblationConfig()
	if len(cfg.Components) == 0 {
		t.Fatal("default ablation config must have components")
	}
	study := NewAblationStudy(cfg)

	dataset := []Task{
		// Ground-truth concepts are words the deterministic baseline runner
		// (which echoes the query) will cover, so the baseline succeeds and
		// ablations measurably degrade it.
		{ID: "t1", Type: "math", Query: "multiply 6 by 7", GroundTruth: []string{"6 by 7"}},
		{ID: "t2", Type: "qa", Query: "name the capital city of France", GroundTruth: []string{"France"}},
	}
	study.SetDataset(dataset)

	ev := NewEvaluator(DefaultEvaluationConfig())
	if err := study.Run(context.Background(), ev); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rep := study.LastReport()
	if rep == nil {
		t.Fatal("LastReport is nil after Run")
	}
	if rep.DatasetSize != len(dataset) {
		t.Fatalf("DatasetSize = %d, want %d", rep.DatasetSize, len(dataset))
	}
	if rep.Baseline == nil {
		t.Fatal("Baseline is nil")
	}
	if len(rep.Variants) != len(cfg.Components) {
		t.Fatalf("variants = %d, want %d", len(rep.Variants), len(cfg.Components))
	}
	// The default baseline runner echoes the query, so it covers the chosen
	// ground truth and the baseline success rate must be 1.0.
	if rep.Baseline.SuccessRate != 1.0 {
		t.Fatalf("baseline success rate = %v, want 1.0 (report: %s)", rep.Baseline.SuccessRate, rep.FormatReport())
	}
	_ = rep.FormatReport()
}

func TestAblationStudyRequiresDataset(t *testing.T) {
	study := NewAblationStudy(DefaultAblationConfig())
	if err := study.Run(context.Background(), NewEvaluator(DefaultEvaluationConfig())); err == nil {
		t.Fatal("expected error when no dataset is set")
	}
}

func TestFeedbackAnalyzerReport(t *testing.T) {
	fa := NewFeedbackAnalyzer(DefaultFeedbackConfig())
	base := time.Now()

	fa.Record(FeedbackSignal{FeedbackID: "f1", TaskID: "t1", Source: "user", Rating: 5, Outcome: "success", CreatedAt: base})
	fa.Record(FeedbackSignal{FeedbackID: "f2", TaskID: "t2", Source: "user", Rating: 4, Comment: "slow", Outcome: "success", CreatedAt: base.Add(time.Minute)})
	fa.Record(FeedbackSignal{FeedbackID: "f3", TaskID: "t3", Source: "user", Rating: 1, Comment: "wrong answer", Outcome: "failure", CreatedAt: base.Add(2 * time.Minute)})

	rep := fa.Analyze()
	if rep == nil {
		t.Fatal("Analyze returned nil")
	}
	if rep.Samples != 3 {
		t.Fatalf("samples = %d, want 3", rep.Samples)
	}
	// avg = (5+4+1)/3 = 3.333...
	want := 10.0 / 3.0
	if rep.AvgRating < want-1e-9 || rep.AvgRating > want+1e-9 {
		t.Fatalf("avg rating = %v, want %v", rep.AvgRating, want)
	}
	// negative rate = 1/3 (rating <= 2)
	wantNeg := 1.0 / 3.0
	if rep.NegativeRate < wantNeg-1e-9 || rep.NegativeRate > wantNeg+1e-9 {
		t.Fatalf("negative rate = %v, want %v", rep.NegativeRate, wantNeg)
	}
}

func TestABTestStickyAssignmentAndReport(t *testing.T) {
	p := NewABTestPlatform(DefaultABTestConfig())
	exp := p.StartExperiment("exp1", []string{"control", "treatment"})
	if exp == "" {
		t.Fatal("StartExperiment returned empty id")
	}

	// Sticky assignment: same subject always gets the same variant.
	v1 := p.Assign(exp, "user-42")
	v2 := p.Assign(exp, "user-42")
	if v1 != v2 {
		t.Fatalf("assignment not sticky: %q vs %q", v1, v2)
	}
	if v1 != "control" && v1 != "treatment" {
		t.Fatalf("unexpected variant %q", v1)
	}

	// Record clearly separated outcomes so the test is deterministic.
	for i := 0; i < 20; i++ {
		p.RecordOutcome(exp, "control", 1.0)
		p.RecordOutcome(exp, "treatment", 2.0)
	}

	rep := p.Report(exp)
	if rep == nil {
		t.Fatal("Report returned nil")
	}
	if rep.ExperimentID != exp {
		t.Fatalf("ExperimentID = %q, want %q", rep.ExperimentID, exp)
	}
	if len(rep.Stats) != 2 {
		t.Fatalf("stats = %d, want 2", len(rep.Stats))
	}
	// treatment mean should exceed control mean -> positive lift.
	if rep.Lift <= 0 {
		t.Fatalf("lift = %v, want > 0", rep.Lift)
	}
	// With 20 vs 20 samples of constant 1 vs 2, the t-test is highly
	// significant.
	if !rep.Significant {
		t.Fatalf("expected significant result, p=%v", rep.PValue)
	}
	if rep.Recommendation == "" {
		t.Fatal("expected a non-empty recommendation")
	}
}

func TestABTestInsufficientSamplesNotSignificant(t *testing.T) {
	p := NewABTestPlatform(DefaultABTestConfig())
	exp := p.StartExperiment("exp2", []string{"a", "b"})
	// Single samples each: no statistical power.
	p.RecordOutcome(exp, "a", 1.0)
	p.RecordOutcome(exp, "b", 1.1)
	rep := p.Report(exp)
	if rep == nil {
		t.Fatal("Report returned nil")
	}
	if rep.Significant {
		t.Fatalf("expected not significant with tiny samples, p=%v", rep.PValue)
	}
}

func TestFailureCatalogRecordLookupSuggest(t *testing.T) {
	c := NewFailureCatalog()

	c.Record(FailureRecord{
		Signature: "sig-1",
		Category:  "timeout",
		TaskID:    "t1",
		Details:   "step exceeded 30s",
		Count:     1,
		LastSeen:  time.Now(),
	})
	// Same signature aggregates.
	c.Record(FailureRecord{Signature: "sig-1", Category: "timeout", Count: 1, LastSeen: time.Now()})

	got := c.Lookup("sig-1")
	if got == nil {
		t.Fatal("Lookup returned nil")
	}
	if got.Count != 2 {
		t.Fatalf("count = %d, want 2", got.Count)
	}

	if len(c.List()) != 1 {
		t.Fatalf("List length = %d, want 1", len(c.List()))
	}

	// Playbook for timeout starts with "extend_deadline".
	if s := c.SuggestRecovery("timeout"); s == "" {
		t.Fatal("SuggestRecovery returned empty")
	}
	// Unknown category falls back to the default escalation.
	if s := c.SuggestRecovery("does_not_exist"); s == "" {
		t.Fatal("SuggestRecovery default returned empty")
	}

	// Learning: a recorded recovery overrides the playbook suggestion.
	c.MarkRecovered("sig-1", "custom_recovery")
	if s := c.SuggestRecovery("timeout"); s != "custom_recovery" {
		t.Fatalf("SuggestRecovery = %q, want learned custom_recovery", s)
	}
}
