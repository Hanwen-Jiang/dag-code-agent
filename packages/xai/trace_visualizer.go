package xai

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TraceEvent is a single recorded event in an agent execution trace.
type TraceEvent struct {
	Type      string    `json:"type"` // event type, e.g. "tool_call", "llm_call", "error"
	TaskID    string    `json:"task_id"`
	Timestamp time.Time `json:"timestamp"`
	Iteration int       `json:"iteration"`
	ToolName  string    `json:"tool_name,omitempty"`
	Output    string    `json:"output,omitempty"`
	LatencyMS float64   `json:"latency_ms,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// TraceStatus enumerates terminal states of a trace.
type TraceStatus string

const (
	TraceRunning   TraceStatus = "running"
	TraceCompleted TraceStatus = "completed"
	TraceFailed    TraceStatus = "failed"
	TraceTimedOut  TraceStatus = "timed_out"
)

// Trace holds the complete execution history for one task.
type Trace struct {
	TaskID    string       `json:"task_id"`
	StartedAt time.Time    `json:"started_at"`
	EndedAt   time.Time    `json:"ended_at"`
	Events    []TraceEvent `json:"events"`
	Status    TraceStatus  `json:"status"`
	Model     string       `json:"model,omitempty"`
}

// TraceConfig configures a TraceVisualizer.
type TraceConfig struct {
	// MaxTraces is the number of most recent traces kept in memory.
	// Defaults to 100 when zero or negative.
	MaxTraces int
}

// DefaultTraceConfig returns the standard visualizer configuration.
func DefaultTraceConfig() TraceConfig {
	return TraceConfig{MaxTraces: 100}
}

type traceCallback struct {
	id string
	cb func(TraceEvent)
}

// TraceVisualizer records agent execution traces and serves them to
// visualizers and consumers. All methods are safe for concurrent use.
type TraceVisualizer struct {
	mu        sync.RWMutex
	config    TraceConfig
	order     []string // task IDs in insertion (oldest first)
	traces    map[string]*Trace
	callbacks []traceCallback
	nextCbID  int
	now       func() time.Time
}

// NewTraceVisualizer creates a trace visualizer with the given config.
func NewTraceVisualizer(cfg TraceConfig) *TraceVisualizer {
	if cfg.MaxTraces <= 0 {
		cfg = DefaultTraceConfig()
	}
	return &TraceVisualizer{
		config: cfg,
		traces: make(map[string]*Trace),
		now:    time.Now,
	}
}

// setNowFunc injects a deterministic clock (used by tests).
func (t *TraceVisualizer) setNowFunc(f func() time.Time) {
	if f != nil {
		t.now = f
	}
}

// Record appends an event to its trace, creating the trace on first sight.
// Registered callbacks are fanned out with the recorded event.
func (t *TraceVisualizer) Record(event TraceEvent) {
	if event.TaskID == "" {
		return
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = t.now()
	}

	var snapshot []traceCallback
	t.mu.Lock()
	trace, ok := t.traces[event.TaskID]
	if !ok {
		trace = &Trace{
			TaskID:    event.TaskID,
			StartedAt: event.Timestamp,
			Status:    TraceRunning,
		}
		t.traces[event.TaskID] = trace
		t.order = append(t.order, event.TaskID)
	}
	trace.EndedAt = event.Timestamp
	trace.Events = append(trace.Events, event)
	t.evictLocked()
	snapshot = make([]traceCallback, len(t.callbacks))
	copy(snapshot, t.callbacks)
	t.mu.Unlock()

	for _, c := range snapshot {
		c.cb(event)
	}
}

// SetStatus records a terminal status for a trace.
func (t *TraceVisualizer) SetStatus(taskID string, status TraceStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if trace, ok := t.traces[taskID]; ok {
		trace.Status = status
		trace.EndedAt = t.now()
	}
}

// evictLocked drops the oldest traces beyond the configured retention.
// Caller must hold t.mu (write lock).
func (t *TraceVisualizer) evictLocked() {
	for len(t.order) > t.config.MaxTraces {
		id := t.order[0]
		t.order = t.order[1:]
		delete(t.traces, id)
	}
}

// ListTraces returns up to limit most recent traces (newest first).
// A limit <= 0 returns all retained traces.
func (t *TraceVisualizer) ListTraces(limit int) []Trace {
	t.mu.RLock()
	defer t.mu.RUnlock()

	n := len(t.order)
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Trace, 0, n)
	for i := len(t.order) - 1; i >= 0 && len(out) < n; i-- {
		if tr, ok := t.traces[t.order[i]]; ok {
			out = append(out, *tr)
		}
	}
	return out
}

// GetTrace returns a copy of the trace with the given task ID.
func (t *TraceVisualizer) GetTrace(taskID string) (*Trace, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	trace, ok := t.traces[taskID]
	if !ok {
		return nil, fmt.Errorf("trace not found: %s", taskID)
	}
	out := *trace
	out.Events = make([]TraceEvent, len(trace.Events))
	copy(out.Events, trace.Events)
	return &out, nil
}

// RegisterCallback adds an event callback and returns its ID. Callbacks are
// invoked synchronously on every Record call, including events recorded from
// other goroutines.
func (t *TraceVisualizer) RegisterCallback(cb func(TraceEvent)) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextCbID++
	id := "cb-" + strconv.Itoa(t.nextCbID)
	t.callbacks = append(t.callbacks, traceCallback{id: id, cb: cb})
	return id
}

// UnregisterCallback removes a previously registered callback by ID.
// Unknown IDs are ignored.
func (t *TraceVisualizer) UnregisterCallback(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, c := range t.callbacks {
		if c.id == id {
			t.callbacks = append(t.callbacks[:i], t.callbacks[i+1:]...)
			return
		}
	}
}

// TraceAggregate holds summary numbers computed from a trace's events.
type TraceAggregate struct {
	TotalTimeMS  float64        `json:"total_time_ms"`
	ToolCounts   map[string]int `json:"tool_counts"`
	SlowestEvent *TraceEvent    `json:"slowest_event,omitempty"`
	ErrorCount   int            `json:"error_count"`
	EventCount   int            `json:"event_count"`
	Success      bool           `json:"success"`
}

type visNode struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Label     string                 `json:"label"`
	Iteration int                    `json:"iteration"`
	ToolName  string                 `json:"tool_name,omitempty"`
	LatencyMS float64                `json:"latency_ms"`
	Status    string                 `json:"status"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type visPayload struct {
	TaskID     string         `json:"task_id"`
	Status     TraceStatus    `json:"status"`
	StartedAt  time.Time      `json:"started_at"`
	EndedAt    time.Time      `json:"ended_at"`
	Nodes      []visNode      `json:"timeline"`
	Aggregates TraceAggregate `json:"aggregates"`
}

// VisualizationJSON renders a deterministic JSON document for a trace:
// one timeline node per event (iteration, tool, latency, status) plus an
// aggregates block (total time, per-tool counts, slowest event, error count).
func (t *TraceVisualizer) VisualizationJSON(taskID string) (string, error) {
	trace, err := t.GetTrace(taskID)
	if err != nil {
		return "", err
	}

	nodes := make([]visNode, 0, len(trace.Events))
	aggs := TraceAggregate{
		ToolCounts: make(map[string]int),
		Success:    trace.Status == TraceCompleted,
	}

	var slowest TraceEvent
	for i, ev := range trace.Events {
		status := "ok"
		if ev.Error != "" {
			status = "error"
			aggs.ErrorCount++
		}
		aggs.TotalTimeMS += ev.LatencyMS
		if ev.ToolName != "" {
			aggs.ToolCounts[ev.ToolName]++
		}
		if ev.LatencyMS > slowest.LatencyMS {
			slowest = ev
		}
		nodes = append(nodes, visNode{
			ID:        fmt.Sprintf("n%d", i),
			Type:      eventKind(ev),
			Label:     fmt.Sprintf("iter %d: %s", ev.Iteration, nodeLabel(ev)),
			Iteration: ev.Iteration,
			ToolName:  ev.ToolName,
			LatencyMS: ev.LatencyMS,
			Status:    status,
			Data: map[string]interface{}{
				"output": ev.Output,
				"error":  ev.Error,
			},
		})
	}
	aggs.EventCount = len(trace.Events)
	if slowest.LatencyMS > 0 || len(trace.Events) > 0 {
		aggs.SlowestEvent = &slowest
	}

	payload := visPayload{
		TaskID:     trace.TaskID,
		Status:     trace.Status,
		StartedAt:  trace.StartedAt,
		EndedAt:    trace.EndedAt,
		Nodes:      nodes,
		Aggregates: aggs,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal visualization: %w", err)
	}
	return string(raw), nil
}

func eventKind(ev TraceEvent) string {
	if ev.Error != "" {
		return "error"
	}
	if ev.ToolName != "" {
		return "tool"
	}
	switch ev.Type {
	case "llm_call", "llm":
		return "llm"
	default:
		return "event"
	}
}

func nodeLabel(ev TraceEvent) string {
	if ev.ToolName != "" {
		return ev.ToolName
	}
	if ev.Type != "" {
		return ev.Type
	}
	return "step"
}

// TotalTimeMS returns the sum of event latencies for a trace.
func (tr *Trace) TotalTimeMS() float64 {
	var total float64
	for _, ev := range tr.Events {
		total += ev.LatencyMS
	}
	return total
}

// ErrorCount returns the number of events carrying a non-empty Error field.
func (tr *Trace) ErrorCount() int {
	n := 0
	for _, ev := range tr.Events {
		if ev.Error != "" {
			n++
		}
	}
	return n
}

// ToolCounts returns call counts per tool name.
func (tr *Trace) ToolCounts() map[string]int {
	counts := make(map[string]int)
	for _, ev := range tr.Events {
		if ev.ToolName != "" {
			counts[ev.ToolName]++
		}
	}
	return counts
}

// ErrorEvents returns all events that recorded an error, in trace order.
func (tr *Trace) ErrorEvents() []TraceEvent {
	var out []TraceEvent
	for _, ev := range tr.Events {
		if ev.Error != "" {
			out = append(out, ev)
		}
	}
	return out
}

// SlowestEvent returns the event with the largest latency (first one wins on
// ties). The zero value is returned for an empty trace.
func (tr *Trace) SlowestEvent() TraceEvent {
	var slow TraceEvent
	for _, ev := range tr.Events {
		if ev.LatencyMS > slow.LatencyMS {
			slow = ev
		}
	}
	return slow
}

// percentile returns the p-th percentile (0-100) of values using the
// nearest-rank method. It does not mutate its input.
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

// averageSlice returns the arithmetic mean of values (0 for empty input).
func averageSlice(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// float64Sums returns the sum of two float64 slices (helper for vector math).
func float64Sums(a, b []float64) []float64 {
	if len(a) != len(b) {
		return nil
	}
	out := make([]float64, len(a))
	for i := range a {
		out[i] = a[i] + b[i]
	}
	return out
}

// truncateRuneLimit truncates s to at most n runes without splitting a rune.
func truncateRuneLimit(s string, n int) string {
	if n <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}

// sanitizeJSONKey lowercases a map key and replaces unsafe characters.
func sanitizeJSONKey(key string) string {
	key = strings.ToLower(key)
	var b strings.Builder
	for _, r := range key {
		if r == '-' || r == ' ' || r == '_' {
			b.WriteByte('_')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
