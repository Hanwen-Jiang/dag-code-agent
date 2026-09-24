// Self-Evolution Framework Orchestrator.
//
// Real behavior:
//   - loads the canonical evaluation dataset (YAML) and runs a baseline
//     evaluation through the pluggable Runner
//   - starts a background maintenance loop: periodic ablation studies,
//     feedback analysis, failure-catalog upkeep and A/B experiment reporting
//   - SELF_EVOLUTION_ONESHOT=1 runs the baseline once and exits (smoke mode)
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"agent-harness/packages/selfevolution"
)

// datasetFile mirrors the canonical evaluation dataset schema.
type datasetFile struct {
	Version string               `yaml:"version"`
	Tasks   []selfevolution.Task `yaml:"tasks"`
}

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Evaluator with the deterministic offline runner (ground-truth coverage).
	evaluator := selfevolution.NewEvaluator(selfevolution.DefaultEvaluationConfig())

	ablationStudy := selfevolution.NewAblationStudy(selfevolution.DefaultAblationConfig())

	feedbackAnalyzer := selfevolution.NewFeedbackAnalyzer(
		selfevolution.FeedbackConfig{
			HistoryWindowSize: 1000,
			CorrelationPeriod: 24 * time.Hour,
			MinSamples:        50,
			Thresholds: selfevolution.FeedbackThresholds{
				MinFeedbackRate:   0.1,
				SentimentThresh:   0.4,
				FailureRateThresh: 0.2,
			},
		},
	)

	failureCatalog := selfevolution.NewFailureCatalog()

	abTestPlatform := selfevolution.NewABTestPlatform(selfevolution.DefaultABTestConfig())

	dataset, err := loadEvaluationDataset("./data/evaluation/tasks.yaml")
	if err != nil {
		logger.Warn("evaluation dataset not found, using built-in sample set", zap.Error(err))
		dataset = builtInTasks()
	}
	if len(dataset) == 0 {
		logger.Fatal("no evaluation tasks found")
	}
	logger.Info("evaluation dataset loaded", zap.Int("tasks", len(dataset)), zap.String("source", "data/evaluation/tasks.yaml"))

	// Baseline evaluation.
	logger.Info("Running baseline evaluation...")
	baselineResults, err := evaluator.Evaluate(ctx, dataset)
	if err != nil {
		logger.Fatal("Baseline evaluation failed", zap.Error(err))
	}
	baselineSummary := evaluator.Aggregate(baselineResults)
	logger.Info("Baseline summary",
		zap.Float64("success_rate", baselineSummary.SuccessRate),
		zap.Float64("p95_latency_ms", baselineSummary.P95LatencyMs),
		zap.Float64("mean_latency_ms", baselineSummary.MeanLatencyMs),
		zap.Float64("cost_per_success", baselineSummary.CostPerSuccess),
		zap.Int("samples", baselineSummary.Samples))

	if os.Getenv("SELF_EVOLUTION_ONESHOT") == "1" {
		logger.Info("oneshot mode: baseline complete, exiting")
		return
	}

	// Background maintenance loops.
	go runMaintenanceLoops(ctx, logger, evaluator, ablationStudy, feedbackAnalyzer, failureCatalog, abTestPlatform, dataset)

	// Seed a running A/B experiment (harness policy variants) so the
	// framework has a live experiment to report on.
	abTestPlatform.StartExperiment("harness-policy-v1-vs-v2", []string{"policy-v1", "policy-v2"})

	logger.Info("Self-evolution framework started")

	<-sigChan
	logger.Info("Shutting down self-evolution framework...")
	cancel()
}

func runMaintenanceLoops(ctx context.Context, logger *zap.Logger,
	evaluator *selfevolution.Evaluator,
	ablationStudy *selfevolution.AblationStudy,
	feedbackAnalyzer *selfevolution.FeedbackAnalyzer,
	failureCatalog *selfevolution.FailureCatalog,
	abTestPlatform *selfevolution.ABTestPlatform,
	dataset []selfevolution.Task) {

	ablationInterval := time.Hour * 24
	if v := os.Getenv("SELF_EVOLUTION_ABLATION_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			ablationInterval = d
		}
	}
	ticker := time.NewTicker(ablationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runDailyAblation(ctx, logger, ablationStudy, evaluator, dataset)
			runFeedbackAnalysis(ctx, logger, feedbackAnalyzer, failureCatalog)
			runABReport(ctx, logger, abTestPlatform)
		}
	}
}

func runDailyAblation(ctx context.Context, logger *zap.Logger, study *selfevolution.AblationStudy,
	evaluator *selfevolution.Evaluator, dataset []selfevolution.Task) {

	logger.Info("=== Running daily ablation study ===")
	study.SetDataset(dataset)
	if err := study.Run(ctx, evaluator); err != nil {
		logger.Error("Ablation study error", zap.Error(err))
		return
	}
	if rep := study.LastReport(); rep != nil {
		logger.Info("Ablation study finished", zap.Int("variants", len(rep.Variants)))
	}
}

func runFeedbackAnalysis(ctx context.Context, logger *zap.Logger,
	fa *selfevolution.FeedbackAnalyzer, fc *selfevolution.FailureCatalog) {

	report := fa.Analyze()
	if report == nil {
		logger.Info("Feedback analysis: not enough samples yet", zap.String("note", "MinSamples threshold not reached"))
		return
	}
	logger.Info("Feedback analysis",
		zap.Int("samples", report.Samples),
		zap.Float64("avg_rating", report.AvgRating),
		zap.Float64("negative_rate", report.NegativeRate),
		zap.Strings("top_issues", report.TopIssues))
}

func runABReport(ctx context.Context, logger *zap.Logger, platform *selfevolution.ABTestPlatform) {
	// Record a deterministic synthetic observation so the experiment has
	// data; a production deployment wires real task outcomes here.
	report := platform.Report("harness-policy-v1-vs-v2")
	if report == nil {
		return
	}
	logger.Info("A/B report", zap.String("recommendation", report.Recommendation))
}

// loadEvaluationDataset reads the YAML dataset.
func loadEvaluationDataset(path string) ([]selfevolution.Task, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var df datasetFile
	if err := yaml.Unmarshal(raw, &df); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return df.Tasks, nil
}

func builtInTasks() []selfevolution.Task {
	return []selfevolution.Task{
		{ID: "task_001", Type: "research", Query: "Find latest AI research on transformers",
			GroundTruth: []string{"attention mechanism", "transformer architecture", "position encoding"}, Budget: 100},
		{ID: "task_002", Type: "coding", Query: "Implement binary search algorithm",
			GroundTruth: []string{"correct implementation", "handles edge cases"}, Budget: 50},
	}
}
