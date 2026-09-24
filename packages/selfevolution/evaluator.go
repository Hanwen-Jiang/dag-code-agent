// Package selfevolution provides evaluation, ablation, feedback, failure
// catalog and A/B testing primitives for a self-improving agent harness.
// Everything is deterministic and stdlib-only so tests are stable offline.
package selfevolution

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Task is one unit of evaluation work.
type Task struct {
	ID          string
	Type        string
	Query       string
	GroundTruth []string // concepts the output must cover
	Budget      int
}

// EvaluationConfig configures an Evaluator.
type EvaluationConfig struct {
	// ModelTier is recorded for provenance (does not affect scoring).
	ModelTier string
	// BudgetLimitUSD caps per-run cost (informational for the default runner).
	BudgetLimitUSD float64
	// LatencyPerStepMS is the virtual milliseconds the default runner charges
	// per step. The evaluator uses a virtual clock, so latency is fully
	// deterministic (no wall-clock reads).
	LatencyPerStepMS float64
	// MinSubstringLen is the minimum length for substring ground-truth matches.
	MinSubstringLen int
	// MaxSteps bounds the steps the default runner reports per task.
	MaxSteps int
}

// DefaultEvaluationConfig returns standard evaluation parameters.
func DefaultEvaluationConfig() EvaluationConfig {
	return EvaluationConfig{
		ModelTier:        "production",
		BudgetLimitUSD:   100.0,
		LatencyPerStepMS: 25,
		MinSubstringLen:  4,
		MaxSteps:         8,
	}
}

// TaskResult is the outcome of evaluating one task.
type TaskResult struct {
	TaskID          string
	OK              bool
	Steps           int
	LatencyMS       float64
	CostUSD         float64
	FailureCategory string // "" on success, else a stable category
	Output          string
}

// Summary aggregates a set of TaskResults.
type Summary struct {
	SuccessRate    float64
	P95LatencyMs   float64
	MeanLatencyMs  float64
	CostPerSuccess float64
	FailureCounts  map[string]int
	Samples        int
}

// Runner executes a single task. Implementations must be deterministic for
// reproducible evaluations.
type Runner interface {
	Run(ctx context.Context, task Task) (output string, steps int, costUSD float64, err error)
}

// Evaluator runs tasks through a pluggable Runner and aggregates results.
type Evaluator struct {
	config EvaluationConfig
	runner Runner
	// clock is a deterministic virtual time source; it advances by the
	// runner-reported steps so latency never depends on wall time.
	nowMS float64
}

// NewEvaluator creates an evaluator. The default runner is a deterministic
// keyword/coverage scorer that works fully offline.
func NewEvaluator(cfg EvaluationConfig) *Evaluator {
	if cfg.LatencyPerStepMS <= 0 {
		cfg.LatencyPerStepMS = 25
	}
	if cfg.MinSubstringLen <= 0 {
		cfg.MinSubstringLen = 4
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 8
	}
	return &Evaluator{
		config: cfg,
		runner: newDefaultRunner(cfg),
	}
}

// WithRunner replaces the evaluator's runner (used by ablation wrappers).
func (e *Evaluator) WithRunner(r Runner) *Evaluator {
	if r == nil {
		panic("selfevolution: WithRunner(nil)")
	}
	e.runner = r
	return e
}

// Runner exposes the configured runner (for wrapping).
func (e *Evaluator) Runner() Runner { return e.runner }

// Config returns a copy of the evaluator configuration.
func (e *Evaluator) Config() EvaluationConfig { return e.config }

// Evaluate runs every task through the runner (sequentially, in input order)
// and returns one TaskResult per task in the same order.
func (e *Evaluator) Evaluate(ctx context.Context, tasks []Task) ([]TaskResult, error) {
	results := make([]TaskResult, 0, len(tasks))
	for i, task := range tasks {
		if err := ctx.Err(); err != nil {
			return results, err
		}
		res := e.evaluateOne(ctx, task)
		if res.TaskID == "" {
			res.TaskID = fmt.Sprintf("task-%d", i)
		}
		results = append(results, res)
	}
	return results, nil
}

// evaluateOne runs one task and derives success/failure. Latency comes from a
// virtual clock advanced by the reported step count, so it is deterministic.
func (e *Evaluator) evaluateOne(ctx context.Context, task Task) TaskResult {
	res := TaskResult{TaskID: task.ID}
	start := e.nowMS

	output, steps, cost, err := e.runner.Run(ctx, task)
	if steps < 0 {
		steps = 0
	}
	// Advance the virtual clock: at least one virtual "tick" per task plus the
	// per-step cost, so identical inputs always yield identical latency.
	e.nowMS = start + float64(1+steps)*e.config.LatencyPerStepMS

	res.Output = output
	res.Steps = steps
	res.CostUSD = cost
	res.LatencyMS = e.nowMS - start

	if err != nil {
		res.OK = false
		res.FailureCategory = classifyRunnerError(err)
		return res
	}

	covered, missing := coversGroundTruth(task.GroundTruth, output, e.config.MinSubstringLen)
	res.OK = covered
	if !covered {
		res.FailureCategory = "incomplete_output"
		if len(missing) > 0 {
			sort.Strings(missing)
			res.Output = output + " [missing: " + strings.Join(missing, ", ") + "]"
		}
	}
	return res
}

// coversGroundTruth reports whether every ground-truth concept is covered by
// the output. Matching is case-insensitive: a concept of MinSubstringLen+
// characters matches on substring; shorter concepts must match as whole words.
func coversGroundTruth(truth []string, output string, minSubstringLen int) (bool, []string) {
	if len(truth) == 0 {
		return true, nil
	}
	hay := normalize(output)
	var missing []string
	for _, concept := range truth {
		c := normalize(concept)
		if c == "" {
			continue
		}
		if !coveredConcept(hay, c, minSubstringLen) {
			missing = append(missing, concept)
		}
	}
	return len(missing) == 0, missing
}

// normalize lowercases and collapses whitespace.
func normalize(s string) string {
	return strings.Join(strings.Fields(strings.ToLower(s)), " ")
}

// coveredConcept decides how one concept is matched (see coversGroundTruth).
func coveredConcept(hay, concept string, minSubstringLen int) bool {
	if len(concept) >= minSubstringLen {
		return strings.Contains(hay, concept)
	}
	// Short concepts require a whole-word match to avoid spurious hits.
	return wordContains(hay, concept)
}

// wordContains reports whether word appears as a standalone word in text.
func wordContains(text, word string) bool {
	if word == "" {
		return false
	}
	for _, w := range strings.Split(text, " ") {
		if w == word {
			return true
		}
	}
	return false
}

// classifyRunnerError maps a runner error to a stable failure category.
func classifyRunnerError(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return "timeout"
	case strings.Contains(msg, "budget") || strings.Contains(msg, "cost"):
		return "budget_exceeded"
	case strings.Contains(msg, "loop") || strings.Contains(msg, "repeated"):
		return "loop_detected"
	case strings.Contains(msg, "permission") || strings.Contains(msg, "denied"):
		return "permission_denied"
	default:
		return "runner_error"
	}
}

// Aggregate computes summary statistics over a set of results.
func (e *Evaluator) Aggregate(results []TaskResult) *Summary {
	s := &Summary{FailureCounts: make(map[string]int)}
	s.Samples = len(results)
	if len(results) == 0 {
		return s
	}

	var latencies []float64
	successes := 0
	successCost := 0.0
	for _, r := range results {
		latencies = append(latencies, r.LatencyMS)
		if r.OK {
			successes++
			successCost += r.CostUSD
		} else if r.FailureCategory != "" {
			s.FailureCounts[r.FailureCategory]++
		}
	}
	s.SuccessRate = float64(successes) / float64(len(results))
	s.P95LatencyMs = percentile(latencies, 95)
	s.MeanLatencyMs = mean(latencies)
	if successes > 0 {
		s.CostPerSuccess = successCost / float64(successes)
	}
	return s
}

// percentile returns the p-th percentile (0-100) using nearest-rank.
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)
	rank := int(0.5 + p/100*float64(len(sorted)))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// mean returns the arithmetic mean (0 for empty input).
func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// ---- default runner: deterministic keyword/coverage scorer ----

// defaultRunner is the offline reference runner. It simulates an agent that
// answers strictly from what the query already states: its output is a
// deterministic template over the query text. It never invents concepts, so a
// task succeeds exactly when the query text covers every ground-truth concept
// (case-insensitive; concepts of MinSubstringLen+ chars match by substring,
// shorter ones by whole word). This makes the default runner a true
// keyword/coverage scorer that works fully offline.
type defaultRunner struct {
	cfg EvaluationConfig
}

func newDefaultRunner(cfg EvaluationConfig) *defaultRunner {
	return &defaultRunner{cfg: cfg}
}

// Run implements Runner.
func (r *defaultRunner) Run(ctx context.Context, task Task) (string, int, float64, error) {
	if err := ctx.Err(); err != nil {
		return "", 0, 0, err
	}
	steps := 1 + len(task.GroundTruth)
	if steps > r.cfg.MaxSteps {
		steps = r.cfg.MaxSteps
	}
	cost := float64(steps) * 0.001
	return DeterministicRunnerOutput(task), steps, cost, nil
}

// DeterministicRunnerOutput returns the default runner's output for a task:
// a fixed template wrapping the query. Used directly and by ablation wrappers
// that want to mutate the baseline text deterministically.
func DeterministicRunnerOutput(task Task) string {
	return "Answer to [" + task.Type + "]: " + task.Query
}
