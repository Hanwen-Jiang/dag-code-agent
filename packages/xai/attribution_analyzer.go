package xai

import (
	"fmt"
	"sort"
	"strings"
)

// AttributionConfig configures an AttributionAnalyzer.
type AttributionConfig struct {
	// TopNEvents is how many of the slowest events to report as bottlenecks.
	TopNEvents int
	// TopNTools is how many tools to rank by cumulative latency.
	TopNTools int
}

// DefaultAttributionConfig returns the standard analyzer configuration.
func DefaultAttributionConfig() AttributionConfig {
	return AttributionConfig{TopNEvents: 5, TopNTools: 3}
}

// ToolLatencyShare is one tool's contribution to total trace latency.
type ToolLatencyShare struct {
	Tool           string  `json:"tool"`
	Calls          int     `json:"calls"`
	TotalLatencyMS float64 `json:"total_latency_ms"`
	Share          float64 `json:"share"` // fraction of total traced latency, [0,1]
}

// ErrorAttribution ties a single error event to the tool and iteration that
// produced it, plus a deterministic cause label.
type ErrorAttribution struct {
	Iteration int    `json:"iteration"`
	ToolName  string `json:"tool_name,omitempty"`
	Index     int    `json:"event_index"`
	Message   string `json:"message"`
	Cause     string `json:"cause"`
	Severity  string `json:"severity"`
}

// Bottleneck is one of the top-N slowest events in a trace.
type Bottleneck struct {
	Index int        `json:"index"`
	Event TraceEvent `json:"event"`
	Rank  int        `json:"rank"`
	Share float64    `json:"share"`
}

// SlowTool ranks tools by cumulative latency.
type SlowTool struct {
	Tool           string  `json:"tool"`
	Calls          int     `json:"calls"`
	TotalLatencyMS float64 `json:"total_latency_ms"`
	Share          float64 `json:"share"`
}

// CausalChainEntry is one link in the error propagation chain.
type CausalChainEntry struct {
	Index     int    `json:"index"`
	Iteration int    `json:"iteration"`
	ToolName  string `json:"tool_name,omitempty"`
	Message   string `json:"message"`
	Role      string `json:"role"` // "root" or "follow_up"
}

// AttributionReport is the deterministic attribution analysis of a trace.
type AttributionReport struct {
	TaskID            string             `json:"task_id"`
	Success           bool               `json:"success"`
	TotalLatencyMS    float64            `json:"total_latency_ms"`
	EventCount        int                `json:"event_count"`
	ErrorCount        int                `json:"error_count"`
	ToolLatencyShares []ToolLatencyShare `json:"tool_latency_shares"`
	ErrorAttributions []ErrorAttribution `json:"error_attributions"`
	Bottlenecks       []Bottleneck       `json:"bottlenecks"`
	SlowTools         []SlowTool         `json:"slow_tools"`
	CausalChain       []CausalChainEntry `json:"causal_chain,omitempty"`
	Confidence        float64            `json:"confidence"`
}

// AttributionAnalyzer explains which tools, iterations and events are
// responsible for a trace's latency, errors and failure. It is deterministic:
// the same trace always produces the same report.
type AttributionAnalyzer struct {
	config AttributionConfig
}

// NewAttributionAnalyzer creates an analyzer with the given config.
func NewAttributionAnalyzer(cfg AttributionConfig) *AttributionAnalyzer {
	if cfg.TopNEvents <= 0 || cfg.TopNTools <= 0 {
		cfg = DefaultAttributionConfig()
	}
	return &AttributionAnalyzer{config: cfg}
}

// Analyze computes the attribution report for a trace. A nil trace yields a
// report with TaskID "unknown" and zero confidence.
func (a *AttributionAnalyzer) Analyze(trace *Trace) *AttributionReport {
	if trace == nil {
		return &AttributionReport{TaskID: "unknown", Confidence: 0}
	}

	report := &AttributionReport{
		TaskID:         trace.TaskID,
		Success:        trace.Status == TraceCompleted,
		TotalLatencyMS: trace.TotalTimeMS(),
		EventCount:     len(trace.Events),
		ErrorCount:     trace.ErrorCount(),
	}

	report.ToolLatencyShares = a.toolLatencyShares(trace)
	report.SlowTools = a.slowTools(trace)
	report.Bottlenecks = a.bottlenecks(trace)
	report.ErrorAttributions = a.errorAttributions(trace)
	report.CausalChain = a.causalChain(trace)
	report.Confidence = a.confidence(trace)

	return report
}

// toolLatencyShares returns every tool's share of total traced latency,
// sorted by share descending (ties broken by tool name).
func (a *AttributionAnalyzer) toolLatencyShares(trace *Trace) []ToolLatencyShare {
	type acc struct {
		calls int
		total float64
	}
	byTool := make(map[string]*acc)
	for _, ev := range trace.Events {
		if ev.ToolName == "" {
			continue
		}
		aa, ok := byTool[ev.ToolName]
		if !ok {
			aa = &acc{}
			byTool[ev.ToolName] = aa
		}
		aa.calls++
		aa.total += ev.LatencyMS
	}

	total := trace.TotalTimeMS()
	out := make([]ToolLatencyShare, 0, len(byTool))
	for tool, aa := range byTool {
		share := 0.0
		if total > 0 {
			share = aa.total / total
		}
		out = append(out, ToolLatencyShare{
			Tool:           tool,
			Calls:          aa.calls,
			TotalLatencyMS: aa.total,
			Share:          share,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Share != out[j].Share {
			return out[i].Share > out[j].Share
		}
		return out[i].Tool < out[j].Tool
	})
	return out
}

// slowTools returns the top-N tools by cumulative latency.
func (a *AttributionAnalyzer) slowTools(trace *Trace) []SlowTool {
	shares := a.toolLatencyShares(trace)
	n := a.config.TopNTools
	if n > len(shares) {
		n = len(shares)
	}
	out := make([]SlowTool, 0, n)
	for i := 0; i < n; i++ {
		s := shares[i]
		out = append(out, SlowTool{
			Tool:           s.Tool,
			Calls:          s.Calls,
			TotalLatencyMS: s.TotalLatencyMS,
			Share:          s.Share,
		})
	}
	return out
}

// bottlenecks returns the top-N slowest individual events, ranked.
func (a *AttributionAnalyzer) bottlenecks(trace *Trace) []Bottleneck {
	type idxLat struct {
		index int
		lat   float64
	}
	idxs := make([]idxLat, 0, len(trace.Events))
	for i, ev := range trace.Events {
		idxs = append(idxs, idxLat{index: i, lat: ev.LatencyMS})
	}
	sort.SliceStable(idxs, func(i, j int) bool {
		if idxs[i].lat != idxs[j].lat {
			return idxs[i].lat > idxs[j].lat
		}
		return idxs[i].index < idxs[j].index
	})
	n := a.config.TopNEvents
	if n > len(idxs) {
		n = len(idxs)
	}
	total := trace.TotalTimeMS()
	out := make([]Bottleneck, 0, n)
	for rank := 0; rank < n; rank++ {
		share := 0.0
		if total > 0 {
			share = idxs[rank].lat / total
		}
		ev := trace.Events[idxs[rank].index]
		out = append(out, Bottleneck{
			Index: idxs[rank].index,
			Event: ev,
			Rank:  rank + 1,
			Share: share,
		})
	}
	return out
}

// errorAttributions attributes every error event to the tool and iteration
// that produced it, with a deterministic cause label.
func (a *AttributionAnalyzer) errorAttributions(trace *Trace) []ErrorAttribution {
	out := make([]ErrorAttribution, 0, trace.ErrorCount())
	for i, ev := range trace.Events {
		if ev.Error == "" {
			continue
		}
		out = append(out, ErrorAttribution{
			Iteration: ev.Iteration,
			ToolName:  ev.ToolName,
			Index:     i,
			Message:   ev.Error,
			Cause:     classifyErrorCause(ev),
			Severity:  severityForError(ev),
		})
	}
	return out
}

// causalChain builds the chain of error events in trace order. The first error
// is the root; later errors are follow-ups. When a later error involves a
// different tool than the root, the chain is marked as propagating across
// tools (informational only).
func (a *AttributionAnalyzer) causalChain(trace *Trace) []CausalChainEntry {
	entries := make([]CausalChainEntry, 0, trace.ErrorCount())
	role := "root"
	for i, ev := range trace.Events {
		if ev.Error == "" {
			continue
		}
		entries = append(entries, CausalChainEntry{
			Index:     i,
			Iteration: ev.Iteration,
			ToolName:  ev.ToolName,
			Message:   ev.Error,
			Role:      role,
		})
		role = "follow_up"
	}
	return entries
}

// confidence is a deterministic heuristic in [0,1] that expresses how well
// supported the attribution is: traces with many events, with some traced
// latency and consistent error attribution score higher.
func (a *AttributionAnalyzer) confidence(trace *Trace) float64 {
	if len(trace.Events) == 0 {
		return 0
	}
	conf := 0.5
	// More observed events -> stronger signal.
	if len(trace.Events) >= 5 {
		conf += 0.2
	}
	// If most of the wall time is accounted for by traced events, attribution
	// of latency is well supported.
	if trace.TotalTimeMS() > 0 {
		traced := 0.0
		for _, ev := range trace.Events {
			traced += ev.LatencyMS
		}
		wall := wallTimeMS(trace)
		if wall > 0 && traced/wall >= 0.8 {
			conf += 0.15
		}
	}
	// Every error attributed to a named tool is direct evidence.
	if trace.ErrorCount() > 0 {
		attributed := 0
		for _, ev := range trace.Events {
			if ev.Error != "" && ev.ToolName != "" {
				attributed++
			}
		}
		conf += 0.15 * float64(attributed) / float64(trace.ErrorCount())
	}
	if conf > 1 {
		conf = 1
	}
	return conf
}

func wallTimeMS(trace *Trace) float64 {
	if trace.StartedAt.IsZero() || trace.EndedAt.IsZero() || trace.EndedAt.Before(trace.StartedAt) {
		return 0
	}
	return float64(trace.EndedAt.Sub(trace.StartedAt).Nanoseconds()) / 1e6
}

var transientErrorMarkers = []string{
	"timeout", "timed out", "temporarily unavailable", "connection reset",
	"rate limit", "econnreset", "deadline exceeded", "503",
}

var deterministicErrorMarkers = []string{
	"permission denied", "denied", "not found", "no such file", "invalid",
	"parse", "syntax", "panic", "nil pointer", "division by zero",
}

// classifyErrorCause maps an error event to a deterministic cause label.
func classifyErrorCause(ev TraceEvent) string {
	msg := strings.ToLower(ev.Error)
	switch {
	case strings.Contains(msg, "permission") || strings.Contains(msg, "denied"):
		return "permission_denied"
	case strings.Contains(msg, "not found") || strings.Contains(msg, "no such file"):
		return "missing_resource"
	case strings.Contains(msg, "invalid") || strings.Contains(msg, "parse") || strings.Contains(msg, "syntax"):
		return "invalid_input"
	case containsAny(msg, transientErrorMarkers):
		return "transient_infrastructure"
	case strings.Contains(ev.Output, "loop") || strings.Contains(msg, "loop"):
		return "loop_detected"
	case containsAny(msg, deterministicErrorMarkers):
		return "deterministic_fault"
	default:
		return "unclassified"
	}
}

func containsAny(s string, markers []string) bool {
	for _, m := range markers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// severityForError grades an error: critical when it likely aborted the task,
// major for tool-level faults, minor otherwise.
func severityForError(ev TraceEvent) string {
	switch classifyErrorCause(ev) {
	case "permission_denied":
		return "critical"
	case "transient_infrastructure":
		return "major"
	case "deterministic_fault":
		return "major"
	default:
		return "minor"
	}
}

// Summary renders a short deterministic text summary of the report.
func (r *AttributionReport) Summary() string {
	if r.EventCount == 0 {
		return fmt.Sprintf("trace %s has no events", r.TaskID)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "trace %s: %d events, %.0f ms total, %d error(s).",
		r.TaskID, r.EventCount, r.TotalLatencyMS, r.ErrorCount)
	if len(r.SlowTools) > 0 {
		top := r.SlowTools[0]
		fmt.Fprintf(&b, " Slowest tool: %s (%.0f%% of traced latency).",
			top.Tool, top.Share*100)
	}
	if len(r.Bottlenecks) > 0 && r.Bottlenecks[0].Event.LatencyMS > 0 {
		bn := r.Bottlenecks[0]
		fmt.Fprintf(&b, " Slowest event: iteration %d%s (%.0f ms).",
			bn.Event.Iteration, toolSuffix(bn.Event.ToolName), bn.Event.LatencyMS)
	}
	if len(r.CausalChain) > 0 {
		root := r.CausalChain[0]
		fmt.Fprintf(&b, " Root error at iteration %d: %s.", root.Iteration, truncateRuneLimit(root.Message, 60))
	}
	return b.String()
}

func toolSuffix(tool string) string {
	if tool == "" {
		return ""
	}
	return " (" + tool + ")"
}
