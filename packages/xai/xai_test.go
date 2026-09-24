package xai

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// buildDemoTrace records a small, deterministic trace: two tool calls, one
// error, a successful finish.
func buildDemoTrace(t *testing.T, taskID string) *Trace {
	t.Helper()
	tv := NewTraceVisualizer(DefaultTraceConfig())
	base := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

	events := []TraceEvent{
		{Type: "llm_call", TaskID: taskID, Timestamp: base, Iteration: 1, Output: "planning approach", LatencyMS: 120},
		{Type: "tool_call", TaskID: taskID, Timestamp: base.Add(150 * time.Millisecond), Iteration: 2, ToolName: "workspace_read", Output: "read 3 files", LatencyMS: 45},
		{Type: "tool_call", TaskID: taskID, Timestamp: base.Add(300 * time.Millisecond), Iteration: 3, ToolName: "workspace_search", Output: "12 matches", LatencyMS: 38},
		{Type: "error", TaskID: taskID, Timestamp: base.Add(500 * time.Millisecond), Iteration: 4, ToolName: "workspace_test", Error: "go test: 2 failures", LatencyMS: 900},
		{Type: "tool_call", TaskID: taskID, Timestamp: base.Add(1500 * time.Millisecond), Iteration: 5, ToolName: "workspace_test", Output: "go test: 0 failures", LatencyMS: 850},
		{Type: "llm_call", TaskID: taskID, Timestamp: base.Add(2400 * time.Millisecond), Iteration: 6, Output: "final answer", LatencyMS: 210},
	}
	for _, ev := range events {
		tv.Record(ev)
	}
	tv.SetStatus(taskID, TraceCompleted)

	trace, err := tv.GetTrace(taskID)
	if err != nil {
		t.Fatalf("GetTrace: %v", err)
	}
	return trace
}

func TestTraceRecordAndRetrieve(t *testing.T) {
	tv := NewTraceVisualizer(DefaultTraceConfig())
	base := time.Now()
	tv.Record(TraceEvent{Type: "llm_call", TaskID: "t1", Timestamp: base, Iteration: 1, Output: "hi", LatencyMS: 10})
	tv.Record(TraceEvent{Type: "tool_call", TaskID: "t1", Timestamp: base.Add(50 * time.Millisecond), Iteration: 2, ToolName: "bash", Output: "ok", LatencyMS: 5})
	tv.SetStatus("t1", TraceCompleted)

	trace, err := tv.GetTrace("t1")
	if err != nil {
		t.Fatalf("GetTrace: %v", err)
	}
	if trace.TaskID != "t1" {
		t.Fatalf("TaskID = %q, want t1", trace.TaskID)
	}
	if len(trace.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(trace.Events))
	}
	if trace.Status != TraceCompleted {
		t.Fatalf("status = %q, want completed", trace.Status)
	}

	// ListTraces returns it.
	traces := tv.ListTraces(10)
	if len(traces) != 1 || traces[0].TaskID != "t1" {
		t.Fatalf("ListTraces = %+v", traces)
	}

	if _, err := tv.GetTrace("nope"); err == nil {
		t.Fatal("expected error for unknown trace")
	}
}

func TestVisualizationJSONIsValidJSON(t *testing.T) {
	tv := NewTraceVisualizer(DefaultTraceConfig())
	taskID := "t-json"
	base := time.Now()
	tv.Record(TraceEvent{Type: "llm_call", TaskID: taskID, Timestamp: base, Iteration: 1, Output: "x", LatencyMS: 1})
	tv.SetStatus(taskID, TraceCompleted)

	raw, err := tv.VisualizationJSON(taskID)
	if err != nil {
		t.Fatalf("VisualizationJSON: %v", err)
	}
	if strings.TrimSpace(raw) == "" {
		t.Fatal("VisualizationJSON returned empty string")
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("VisualizationJSON is not valid JSON: %v", err)
	}
	if _, err := tv.VisualizationJSON("missing"); err == nil {
		t.Fatal("expected error for unknown trace")
	}
}

func TestCallbackRegistration(t *testing.T) {
	tv := NewTraceVisualizer(DefaultTraceConfig())
	got := make(chan TraceEvent, 4)
	id := tv.RegisterCallback(func(ev TraceEvent) { got <- ev })
	if id == "" {
		t.Fatal("RegisterCallback returned empty id")
	}

	base := time.Now()
	tv.Record(TraceEvent{Type: "llm_call", TaskID: "cb1", Timestamp: base, Iteration: 1})
	select {
	case ev := <-got:
		if ev.TaskID != "cb1" {
			t.Fatalf("callback task = %q, want cb1", ev.TaskID)
		}
	default:
		t.Fatal("callback not invoked")
	}

	tv.UnregisterCallback(id)
	tv.Record(TraceEvent{Type: "llm_call", TaskID: "cb2", Timestamp: base, Iteration: 2})
	select {
	case ev := <-got:
		t.Fatalf("callback still active after unregister, got %q", ev.TaskID)
	default:
	}
}

func TestAttributionAnalyzer(t *testing.T) {
	trace := buildDemoTrace(t, "t-attr")

	analyzer := NewAttributionAnalyzer(DefaultAttributionConfig())
	rep := analyzer.Analyze(trace)
	if rep == nil {
		t.Fatal("Analyze returned nil")
	}
	if rep.TaskID != "t-attr" {
		t.Fatalf("TaskID = %q, want t-attr", rep.TaskID)
	}
	if rep.EventCount != 6 {
		t.Fatalf("EventCount = %d, want 6", rep.EventCount)
	}
	if rep.ErrorCount != 1 {
		t.Fatalf("ErrorCount = %d, want 1", rep.ErrorCount)
	}
	if !rep.Success {
		t.Fatal("trace completed, expected Success=true")
	}
	if rep.TotalLatencyMS <= 0 {
		t.Fatalf("TotalLatencyMS = %v, want > 0", rep.TotalLatencyMS)
	}
	if len(rep.ToolLatencyShares) == 0 {
		t.Fatal("expected tool latency shares")
	}
	if len(rep.ErrorAttributions) == 0 {
		t.Fatal("expected error attributions")
	}
	if rep.Summary() == "" {
		t.Fatal("Summary() returned empty")
	}
	// The workspace_test tool carried 900+850ms of the 2163ms total, so its
	// share must be the largest.
	if rep.ToolLatencyShares[0].Tool != "workspace_test" {
		t.Fatalf("top tool = %q, want workspace_test", rep.ToolLatencyShares[0].Tool)
	}
}

func TestReasoningExplainer(t *testing.T) {
	trace := buildDemoTrace(t, "t-exp")

	explainer := NewReasoningExplainer(DefaultExplainerConfig())
	ex := explainer.Generate(trace)
	if ex == nil {
		t.Fatal("Generate returned nil")
	}
	if ex.TaskID != "t-exp" {
		t.Fatalf("TaskID = %q, want t-exp", ex.TaskID)
	}
	if !ex.Success {
		t.Fatal("expected success narrative for completed trace")
	}
	if ex.Narrative == "" {
		t.Fatal("Narrative is empty")
	}
	if len(ex.Segments) == 0 {
		t.Fatal("no reasoning segments")
	}
	if ex.TopFailure == nil {
		t.Fatal("expected a top failure insight (the trace has an error)")
	}
}

func TestClassifyFailure(t *testing.T) {
	if got := ClassifyFailure("context deadline exceeded"); got == "" {
		t.Fatal("expected non-empty failure class")
	}
}
