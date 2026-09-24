package harness

import (
	"sort"
	"time"
)

type TraceEvent struct {
	Sequence int            `json:"sequence"`
	Type     string         `json:"type"`
	At       string         `json:"at"`
	Data     map[string]any `json:"data,omitempty"`
}

type Trajectory struct {
	TaskID    string       `json:"task_id"`
	Events    []TraceEvent `json:"events"`
	StartedAt string       `json:"started_at"`
	EndedAt   string       `json:"ended_at"`
}

func (t *Trajectory) Append(kind string, data map[string]any) {
	t.Events = append(t.Events, TraceEvent{Sequence: len(t.Events) + 1, Type: kind, At: UTCNow(), Data: data})
}
func (t *Trajectory) Finish() { t.EndedAt = UTCNow() }
func (t Trajectory) Duration() time.Duration {
	start, _ := time.Parse(time.RFC3339Nano, t.StartedAt)
	end, _ := time.Parse(time.RFC3339Nano, t.EndedAt)
	return end.Sub(start)
}

type FailureTaxonomy struct {
	Counts map[string]int `json:"counts"`
}

func NewFailureTaxonomy() *FailureTaxonomy { return &FailureTaxonomy{Counts: map[string]int{}} }
func (f *FailureTaxonomy) Add(category string) {
	if category == "" {
		category = "UNKNOWN"
	}
	f.Counts[category]++
}
func (f *FailureTaxonomy) Snapshot() map[string]int {
	out := map[string]int{}
	for k, v := range f.Counts {
		out[k] = v
	}
	return out
}

func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	v := append([]float64(nil), values...)
	sort.Float64s(v)
	if p <= 0 {
		return v[0]
	}
	if p >= 1 {
		return v[len(v)-1]
	}
	index := p * float64(len(v)-1)
	lo := int(index)
	hi := lo + 1
	if hi >= len(v) {
		return v[lo]
	}
	return v[lo] + (v[hi]-v[lo])*(index-float64(lo))
}
