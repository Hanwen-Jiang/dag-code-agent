package harness

type PromotionPolicy struct {
	MinSamples         int
	MinSuccessDelta    float64
	MaxP95LatencyDelta float64
	MaxMeanStepsDelta  float64
}

type PromotionDecision struct {
	Baseline  EvaluationResult   `json:"baseline"`
	Candidate EvaluationResult   `json:"candidate"`
	Delta     map[string]float64 `json:"delta"`
	Promote   bool               `json:"promote"`
	Reasons   []string           `json:"reasons"`
}

// EvaluatePromotion is the controlled self-evolution boundary: a candidate
// configuration advances only through a fixed fixture set and explicit
// outcome, latency and trajectory guardrails.
func EvaluatePromotion(cases []EvaluationCase, baseline, candidate Evaluator, policy PromotionPolicy) PromotionDecision {
	if policy.MinSamples <= 0 {
		policy.MinSamples = 20
	}
	results := RunAblation(cases, map[string]Evaluator{"baseline": baseline, "candidate": candidate})
	decision := PromotionDecision{Baseline: results["baseline"], Candidate: results["candidate"]}
	decision.Delta = CompareEvaluation(decision.Baseline, decision.Candidate)
	if len(cases) < policy.MinSamples {
		decision.Reasons = append(decision.Reasons, "INSUFFICIENT_SAMPLES")
	}
	if decision.Delta["delta_task_success_rate"] < policy.MinSuccessDelta {
		decision.Reasons = append(decision.Reasons, "SUCCESS_GUARDRAIL")
	}
	if decision.Delta["delta_p95_latency_ms"] > policy.MaxP95LatencyDelta {
		decision.Reasons = append(decision.Reasons, "LATENCY_GUARDRAIL")
	}
	if decision.Delta["delta_mean_steps"] > policy.MaxMeanStepsDelta {
		decision.Reasons = append(decision.Reasons, "TRAJECTORY_GUARDRAIL")
	}
	decision.Promote = len(decision.Reasons) == 0
	return decision
}
