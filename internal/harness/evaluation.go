package harness

import "math"

type EvaluationMetrics struct {
	TaskSuccessRate       float64        `json:"task_success_rate"`
	TaskSuccessRateCILow  float64        `json:"task_success_rate_ci_low"`
	TaskSuccessRateCIHigh float64        `json:"task_success_rate_ci_high"`
	MeanSteps             float64        `json:"mean_steps"`
	P95LatencyMS          float64        `json:"p95_latency_ms"`
	MeanToolCalls         float64        `json:"mean_tool_calls"`
	FailureCounts         map[string]int `json:"failure_counts,omitempty"`
}
type EvaluationResult struct {
	Variant string            `json:"variant"`
	Metrics EvaluationMetrics `json:"metrics"`
	Samples int               `json:"samples"`
}
type Evaluator func(EvaluationCase) (map[string]any, error)

func EvaluateCases(variant string, cases []EvaluationCase, run Evaluator) EvaluationResult {
	result := EvaluationResult{Variant: variant, Samples: len(cases)}
	if len(cases) == 0 {
		return result
	}
	success := 0
	steps := []float64{}
	latencies := []float64{}
	tools := []float64{}
	failures := map[string]int{}
	for _, c := range cases {
		out, err := run(c)
		if err != nil {
			failures["runner_error"]++
			continue
		}
		if ok, _ := out["ok"].(bool); ok {
			success++
		}
		steps = append(steps, floatNumber(out["steps"]))
		latencies = append(latencies, floatNumber(out["latency_ms"]))
		tools = append(tools, floatNumber(out["tool_calls"]))
		if category, ok := out["failure_category"].(string); ok && category != "" && !boolValue(out["ok"]) {
			failures[category]++
		}
	}
	rate := float64(success) / float64(len(cases))
	low, high := wilson(success, len(cases), 1.96)
	result.Metrics = EvaluationMetrics{TaskSuccessRate: rate, TaskSuccessRateCILow: low, TaskSuccessRateCIHigh: high, MeanSteps: mean(steps), P95LatencyMS: Percentile(latencies, .95), MeanToolCalls: mean(tools), FailureCounts: failures}
	return result
}
func floatNumber(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}
func boolValue(v any) bool { b, _ := v.(bool); return b }
func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sum := 0.
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}
func wilson(success, total int, z float64) (float64, float64) {
	if total == 0 {
		return 0, 0
	}
	p := float64(success) / float64(total)
	den := 1 + z*z/float64(total)
	center := (p + z*z/(2*float64(total))) / den
	spread := z * math.Sqrt(p*(1-p)/float64(total)+z*z/(4*float64(total*total))) / den
	return math.Max(0, center-spread), math.Min(1, center+spread)
}
func CompareEvaluation(a, b EvaluationResult) map[string]float64 {
	return map[string]float64{"delta_task_success_rate": b.Metrics.TaskSuccessRate - a.Metrics.TaskSuccessRate, "delta_p95_latency_ms": b.Metrics.P95LatencyMS - a.Metrics.P95LatencyMS, "delta_mean_steps": b.Metrics.MeanSteps - a.Metrics.MeanSteps}
}
func RunAblation(cases []EvaluationCase, variants map[string]Evaluator) map[string]EvaluationResult {
	out := map[string]EvaluationResult{}
	for name, run := range variants {
		out[name] = EvaluateCases(name, cases, run)
	}
	return out
}
