package selfevolution

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"
)

// ABTestPlatform manages deterministic A/B experiments with statistical
// decision making. Assignment is sticky per subject (fnv-1a hash of
// experiment|subject) so a subject always sees the same variant; outcomes are
// summarized with per-variant means and a Welch two-sample t-test (normal
// approximation for the p-value) to pick a recommendation.
type ABTestPlatform struct {
	config ABTestConfig
	mu     sync.RWMutex
	exps   map[string]*experiment
}

// ABTestConfig holds platform parameters.
type ABTestConfig struct {
	MinSamplesForDecision int // Per-variant minimum before a recommendation
	Alpha                 float64
}

// DefaultABTestConfig returns standard defaults.
func DefaultABTestConfig() ABTestConfig {
	return ABTestConfig{MinSamplesForDecision: 10, Alpha: 0.05}
}

type experiment struct {
	ID        string
	Name      string
	Variants  []string
	CreatedAt time.Time
	mu        sync.Mutex
	values    map[string][]float64 // variant -> recorded values
}

// NewABTestPlatform creates the platform.
func NewABTestPlatform(config ABTestConfig) *ABTestPlatform {
	if config.MinSamplesForDecision <= 0 {
		config.MinSamplesForDecision = 10
	}
	if config.Alpha <= 0 {
		config.Alpha = 0.05
	}
	return &ABTestPlatform{config: config, exps: map[string]*experiment{}}
}

// StartExperiment registers an experiment; returns its ID (== name).
func (p *ABTestPlatform) StartExperiment(name string, variants []string) string {
	if len(variants) == 0 {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	values := map[string][]float64{}
	for _, v := range variants {
		values[v] = []float64{}
	}
	p.exps[name] = &experiment{ID: name, Name: name, Variants: variants, CreatedAt: time.Now(), values: values}
	return name
}

// Assign deterministically and stickily maps a subject to a variant.
func (p *ABTestPlatform) Assign(experimentID, subject string) string {
	p.mu.RLock()
	exp, ok := p.exps[experimentID]
	p.mu.RUnlock()
	if !ok || len(exp.Variants) == 0 {
		return ""
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(experimentID + "|" + subject))
	return exp.Variants[int(h.Sum32())%len(exp.Variants)]
}

// RecordOutcome records one scalar observation for a variant.
func (p *ABTestPlatform) RecordOutcome(experimentID, variant string, value float64) {
	p.mu.RLock()
	exp, ok := p.exps[experimentID]
	p.mu.RUnlock()
	if !ok {
		return
	}
	exp.mu.Lock()
	defer exp.mu.Unlock()
	if _, exists := exp.values[variant]; !exists {
		return
	}
	exp.values[variant] = append(exp.values[variant], value)
}

// VariantStats is one variant's summary.
type VariantStats struct {
	Variant string  `json:"variant"`
	N       int     `json:"n"`
	Mean    float64 `json:"mean"`
	StdDev  float64 `json:"stddev"`
}

// ExperimentReport is the statistical summary of one experiment.
type ExperimentReport struct {
	ExperimentID   string         `json:"experiment_id"`
	Stats          []VariantStats `json:"stats"`
	Lift           float64        `json:"lift"` // best - baseline (first variant), in value units
	PValue         float64        `json:"p_value"`
	Significant    bool           `json:"significant"`
	Recommendation string         `json:"recommendation"`
}

// Report computes the experiment summary, or nil for an unknown experiment.
func (p *ABTestPlatform) Report(experimentID string) *ExperimentReport {
	p.mu.RLock()
	exp, ok := p.exps[experimentID]
	p.mu.RUnlock()
	if !ok {
		return nil
	}
	exp.mu.Lock()
	defer exp.mu.Unlock()

	stats := make([]VariantStats, 0, len(exp.Variants))
	for _, v := range exp.Variants {
		vals := exp.values[v]
		st := VariantStats{Variant: v, N: len(vals), Mean: mean(vals), StdDev: stddev(vals)}
		stats = append(stats, st)
	}
	rep := &ExperimentReport{ExperimentID: experimentID, Stats: stats}

	// Compare the first variant (baseline) with the best other variant.
	if len(exp.Variants) >= 2 {
		base := exp.Variants[0]
		bestVar, bestMean := base, mean(exp.values[base])
		for _, v := range exp.Variants[1:] {
			m := mean(exp.values[v])
			if m > bestMean {
				bestVar, bestMean = v, m
			}
		}
		a, b := exp.values[base], exp.values[bestVar]
		if len(a) > 0 && len(b) > 0 {
			rep.Lift = bestMean - mean(a)
			rep.PValue = welchPValue(a, b)
			minN := p.config.MinSamplesForDecision
			if minN <= 0 {
				minN = 10
			}
			rep.Significant = rep.PValue < p.config.Alpha && len(a) >= minN && len(b) >= minN
		}
		if rep.Significant && bestVar != base {
			rep.Recommendation = fmt.Sprintf("promote %s over %s (lift %.4f, p=%.4f)", bestVar, base, rep.Lift, rep.PValue)
		} else {
			rep.Recommendation = fmt.Sprintf("no significant difference (p=%.4f); keep %s", rep.PValue, base)
		}
	} else {
		rep.Recommendation = "single-variant experiment; nothing to compare"
	}
	return rep
}

func stddev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := mean(values)
	sum := 0.0
	for _, v := range values {
		d := v - m
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(values)-1))
}

// welchPValue computes a two-sided Welch t-test p-value using the normal
// approximation (sufficient for moderate/large samples; keeps the package
// dependency-free and deterministic).
func welchPValue(a, b []float64) float64 {
	na, nb := float64(len(a)), float64(len(b))
	if na < 1 || nb < 1 {
		return 1
	}
	va, vb := variance(a), variance(b)
	se := math.Sqrt(va/na + vb/nb)
	if se == 0 {
		if mean(a) == mean(b) {
			return 1
		}
		return 0
	}
	t := (mean(b) - mean(a)) / se
	// Two-sided normal approximation of the t statistic.
	return 2 * (1 - normalCDF(math.Abs(t)))
}

func variance(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := mean(values)
	sum := 0.0
	for _, v := range values {
		d := v - m
		sum += d * d
	}
	return sum / float64(len(values)-1)
}

// normalCDF is the standard normal CDF via erf (Abramowitz & Stegun 7.1.26).
func normalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}

// List returns the sorted experiment IDs (stable for tests/reports).
func (p *ABTestPlatform) List() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.exps))
	for id := range p.exps {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
