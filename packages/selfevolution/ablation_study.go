package selfevolution

import (
	"context"
	"fmt"
)

// RunnerWrapper transforms the baseline runner for one ablation.
type RunnerWrapper func(base Runner) Runner

// AblationComponent is one named knob that an ablation can disable.
type AblationComponent struct {
	Name     string
	Wrapper  RunnerWrapper
	Describe string
}

// DefaultAblationComponents returns the built-in ablations. Each wrapper
// simulates disabling one agent capability by degrading the runner
// deterministically:
//
//   - "memory": the agent forgets the query and answers from a canned phrase,
//     so coverage collapses.
//   - "skill":  the agent produces a truncated answer (half the baseline text)
//     and uses extra steps to compensate.
//   - "planner": the agent skips planning (one step) but loses the query
//     prefix, dropping some concepts.
//
// The resulting deltas (ablated vs full) are deterministic in sign on the
// standard datasets.
func DefaultAblationComponents() []AblationComponent {
	return []AblationComponent{
		{
			Name:     "memory",
			Describe: "agent loses task memory and answers from a canned phrase",
			Wrapper: func(base Runner) Runner {
				return &wrappedRunner{base: base, mutate: func(t Task, out string, steps int, cost float64) (string, int, float64, error) {
					return "Sorry, I don't have that context available.", steps + 2, cost * 1.5, nil
				}}
			},
		},
		{
			Name:     "skill",
			Describe: "agent loses skill library and produces a truncated, overlong answer",
			Wrapper: func(base Runner) Runner {
				return &wrappedRunner{base: base, mutate: func(t Task, out string, steps int, cost float64) (string, int, float64, error) {
					return out[:max(0, len(out)/2)], steps + 3, cost * 1.2, nil
				}}
			},
		},
		{
			Name:     "planner",
			Describe: "agent skips planning and drops the task framing from its answer",
			Wrapper: func(base Runner) Runner {
				return &wrappedRunner{base: base, mutate: func(t Task, out string, steps int, cost float64) (string, int, float64, error) {
					// Strip the "Answer to [<type>]: " prefix the baseline adds.
					trimmed := ""
					for i := 0; i < len(out) && i < 32; i++ {
						if out[i] == ':' && i+1 < len(out) {
							trimmed = out[i+1:]
							break
						}
					}
					if trimmed == "" {
						trimmed = out
					}
					return trimmed, max(1, steps-1), cost * 0.9, nil
				}}
			},
		},
	}
}

// wrappedRunner delegates to a base runner and post-processes the result.
type wrappedRunner struct {
	base   Runner
	mutate func(task Task, output string, steps int, cost float64) (string, int, float64, error)
}

func (w *wrappedRunner) Run(ctx context.Context, task Task) (string, int, float64, error) {
	out, steps, cost, err := w.base.Run(ctx, task)
	if err != nil {
		return "", steps, cost, err
	}
	return w.mutate(task, out, steps, cost)
}

// AblationConfig configures an AblationStudy.
type AblationConfig struct {
	// Components are the knobs to ablate (one variant each). Empty means the
	// built-in defaults.
	Components []AblationComponent
}

// DefaultAblationConfig returns the standard ablation configuration.
func DefaultAblationConfig() AblationConfig {
	return AblationConfig{Components: DefaultAblationComponents()}
}

// AblationVariantResult is the stored outcome for one ablation.
type AblationVariantResult struct {
	Component    string
	Description  string
	Summary      *Summary
	SuccessDelta float64 // ablated success - baseline success
	LatencyDelta float64 // ablated mean latency - baseline mean latency (ms)
	CostDelta    float64 // ablated cost/success - baseline cost/success
}

// AblationReport is the stored report of a completed study.
type AblationReport struct {
	DatasetSize int
	Baseline    *Summary
	Variants    []AblationVariantResult
}

// AblationStudy re-runs a fixed dataset with the full evaluator and with each
// component ablated, then stores the deltas.
type AblationStudy struct {
	config     AblationConfig
	dataset    []Task
	lastReport *AblationReport
}

// NewAblationStudy creates a study with the given configuration.
func NewAblationStudy(cfg AblationConfig) *AblationStudy {
	if len(cfg.Components) == 0 {
		cfg = DefaultAblationConfig()
	}
	return &AblationStudy{config: cfg}
}

// SetDataset fixes the evaluation dataset the study runs over. It must be
// called (or the dataset passed to Run) before Run.
func (a *AblationStudy) SetDataset(tasks []Task) {
	a.dataset = tasks
}

// Run executes the full configuration plus one variant per configured
// component, all against the same dataset and evaluator, and stores the
// AblationReport. The evaluator's runner is used as the baseline; each
// component's wrapper wraps it for its variant. Deterministic: no randomness,
// fixed dataset order.
func (a *AblationStudy) Run(ctx context.Context, evaluator *Evaluator) error {
	tasks := a.dataset
	if len(tasks) == 0 {
		return fmt.Errorf("ablation study: no dataset set; call SetDataset first")
	}

	components := a.config.Components

	// Baseline: full configuration.
	baseRunner := evaluator.Runner()
	full := evaluator.WithRunner(baseRunner)
	baseResults, err := full.Evaluate(ctx, tasks)
	if err != nil {
		return fmt.Errorf("baseline evaluation: %w", err)
	}
	baseline := full.Aggregate(baseResults)

	report := &AblationReport{DatasetSize: len(tasks), Baseline: baseline}

	for _, comp := range components {
		ablated := evaluator.WithRunner(comp.Wrapper(baseRunner))
		varResults, err := ablated.Evaluate(ctx, tasks)
		if err != nil {
			return fmt.Errorf("ablation %q: %w", comp.Name, err)
		}
		sum := ablated.Aggregate(varResults)
		vr := AblationVariantResult{
			Component:    comp.Name,
			Description:  comp.Describe,
			Summary:      sum,
			SuccessDelta: sum.SuccessRate - baseline.SuccessRate,
			LatencyDelta: sum.MeanLatencyMs - baseline.MeanLatencyMs,
			CostDelta:    sum.CostPerSuccess - baseline.CostPerSuccess,
		}
		report.Variants = append(report.Variants, vr)
	}

	a.lastReport = report
	return nil
}

// LastReport returns the most recent report (nil before Run succeeds).
func (a *AblationStudy) LastReport() *AblationReport {
	return a.lastReport
}

// FormatReport renders a deterministic text report.
func (r *AblationReport) FormatReport() string {
	if r == nil {
		return "no ablation report available"
	}
	lines := []string{
		fmt.Sprintf("Ablation study over %d tasks", r.DatasetSize),
		fmt.Sprintf("baseline: success=%.3f mean_latency=%.1fms cost_per_success=%.4f",
			r.Baseline.SuccessRate, r.Baseline.MeanLatencyMs, r.Baseline.CostPerSuccess),
	}
	for _, v := range r.Variants {
		lines = append(lines, fmt.Sprintf(
			"- %s (%s): success %+.3f, latency %+.1fms, cost_per_success %+.4f",
			v.Component, v.Description, v.SuccessDelta, v.LatencyDelta, v.CostDelta))
	}
	return joinLines(lines)
}

func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}
