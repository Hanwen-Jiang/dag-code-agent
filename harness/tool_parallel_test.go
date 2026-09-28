package harness

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// Parallel tool execution is only acceptable if the journal does not depend on
// which tool finished first. These tests pin that.

// slowTool builds a tool that records overlap so the test can prove concurrency
// actually happened, rather than trusting a comment.
type overlapProbe struct {
	mu      sync.Mutex
	active  int
	maxSeen int
	calls   int
}

func (p *overlapProbe) enter() {
	p.mu.Lock()
	p.active++
	p.calls++
	if p.active > p.maxSeen {
		p.maxSeen = p.active
	}
	p.mu.Unlock()
}

func (p *overlapProbe) leave() {
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
}

func (p *overlapProbe) peak() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxSeen
}

func parallelBatchRuntime(t *testing.T, probe *overlapProbe, safe bool, callsPerStep int) (*Runtime, Store, AuthContext, TaskRequest) {
	t.Helper()
	store := NewInMemoryStore()
	registry := NewToolRegistry()
	toolCalls := make([]ToolCall, 0, callsPerStep)
	for i := 0; i < callsPerStep; i++ {
		name := fmt.Sprintf("probe%d", i)
		err := registry.Register(ToolSpec{
			Name: name, Description: "records overlap",
			InputSchema:     map[string]any{"type": "object", "properties": map[string]any{}},
			ConcurrencySafe: safe,
			Handler: func(map[string]any, AuthContext) (any, error) {
				probe.enter()
				time.Sleep(20 * time.Millisecond)
				probe.leave()
				return map[string]any{"ok": true}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		toolCalls = append(toolCalls, ToolCall{CallID: fmt.Sprintf("c%d", i), Name: name, Arguments: map[string]any{}})
	}
	model := NewScriptedModel([]ModelResponse{
		{ToolCalls: toolCalls},
		{Text: "done", FinishReason: "stop"},
	})
	runtime, err := New(WithStore(store), WithModel(model), WithToolRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	request := TaskRequest{TaskID: "parallel", Goal: "run a batch", TenantID: auth.TenantID, SubjectID: auth.SubjectID}
	return runtime, store, auth, request
}

// TestDeclaredSafeToolsRunConcurrently proves the batch actually overlaps.
func TestDeclaredSafeToolsRunConcurrently(t *testing.T) {
	probe := &overlapProbe{}
	runtime, store, auth, request := parallelBatchRuntime(t, probe, true, 4)
	defer store.Close()
	defer runtime.Close()
	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	if peak := probe.peak(); peak < 2 {
		t.Fatalf("declared-safe tools did not overlap (peak concurrency %d)", peak)
	}
	// The batch is observable in the journal.
	if !containsEvent(t, runtime, request.TaskID, "TOOL_BATCH_STARTED") {
		t.Fatal("a concurrent batch did not record TOOL_BATCH_STARTED")
	}
}

// TestUndeclaredToolsStaySerial is the safety default: an unlabelled tool may
// write, so the kernel must not reorder it against its siblings.
func TestUndeclaredToolsStaySerial(t *testing.T) {
	probe := &overlapProbe{}
	runtime, store, auth, request := parallelBatchRuntime(t, probe, false, 4)
	defer store.Close()
	defer runtime.Close()
	if _, err := runtime.Run(context.Background(), request, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if peak := probe.peak(); peak != 1 {
		t.Fatalf("undeclared tools ran concurrently (peak %d)", peak)
	}
	if containsEvent(t, runtime, request.TaskID, "TOOL_BATCH_STARTED") {
		t.Fatal("a serial batch recorded batch events")
	}
}

// TestConcurrentBatchJournalIsReproducible is the replay-safety property:
// completion order must not leak into the journal.
func TestConcurrentBatchJournalIsReproducible(t *testing.T) {
	run := func() []string {
		store := NewInMemoryStore()
		defer store.Close()
		registry := NewToolRegistry()
		// Deliberately varying latencies: the slowest tool is requested first,
		// which is exactly the case where a completion-ordered commit would
		// produce a different journal.
		for i, delay := range []time.Duration{40 * time.Millisecond, 25 * time.Millisecond, 10 * time.Millisecond} {
			name := fmt.Sprintf("probe%d", i)
			_ = registry.Register(ToolSpec{
				Name: name, InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
				ConcurrencySafe: true,
				Handler: func(map[string]any, AuthContext) (any, error) {
					time.Sleep(delay)
					return map[string]any{"tool": name}, nil
				},
			})
		}
		calls := []ToolCall{
			{CallID: "c0", Name: "probe0"}, {CallID: "c1", Name: "probe1"}, {CallID: "c2", Name: "probe2"},
		}
		model := NewScriptedModel([]ModelResponse{{ToolCalls: calls}, {Text: "done", FinishReason: "stop"}})
		runtime, err := New(WithStore(store), WithModel(model), WithToolRegistry(registry))
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close()
		auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
		if _, err := runtime.Run(context.Background(), TaskRequest{
			TaskID: "reproducible", Goal: "batch", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
		}, auth); err != nil {
			t.Fatalf("Run: %v", err)
		}
		events, err := runtime.Journal().Replay("reproducible")
		if err != nil {
			t.Fatal(err)
		}
		sequence := make([]string, 0, len(events))
		for _, event := range events {
			line := event.Type
			if event.Type == "TOOL_OBSERVED" {
				line += ":" + stringValue(event.Payload["call_id"])
			}
			sequence = append(sequence, line)
		}
		return sequence
	}

	first := run()
	second := run()
	if strings.Join(first, "|") != strings.Join(second, "|") {
		t.Fatalf("concurrent batch journal is not reproducible\n first: %v\nsecond: %v", first, second)
	}
	// Observations follow request order, not completion order: c1 is faster than
	// c0, so a completion-ordered commit would list it first.
	observed := make([]string, 0, 3)
	for _, line := range first {
		if strings.HasPrefix(line, "TOOL_OBSERVED:") {
			observed = append(observed, strings.TrimPrefix(line, "TOOL_OBSERVED:"))
		}
	}
	want := []string{"c0", "c1", "c2"}
	if strings.Join(observed, ",") != strings.Join(want, ",") {
		t.Fatalf("observations are not in request order: %v", observed)
	}
}

// TestSingleToolStepKeepsItsEventSequence is the fidelity guard: parallel
// execution must not alter the journal of the common single-tool case.
func TestSingleToolStepKeepsItsEventSequence(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	registry := NewToolRegistry()
	_ = registry.Register(ToolSpec{
		Name: "echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
		ConcurrencySafe: true, SideEffect: "NONE",
		Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	})
	model := NewScriptedModel([]ModelResponse{
		{ToolCalls: []ToolCall{{CallID: "c1", Name: "echo", Arguments: map[string]any{}}}},
		{Text: "done", FinishReason: "stop"},
	})
	runtime, err := New(WithStore(store), WithModel(model), WithToolRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "single", Goal: "one tool", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, banned := range []string{"TOOL_BATCH_STARTED", "TOOL_BATCH_COMPLETED"} {
		if containsEvent(t, runtime, "single", banned) {
			t.Fatalf("a single-tool step emitted %s", banned)
		}
	}
}

func containsEvent(t *testing.T, runtime *Runtime, taskID, eventType string) bool {
	t.Helper()
	events, err := runtime.Journal().Replay(taskID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}
