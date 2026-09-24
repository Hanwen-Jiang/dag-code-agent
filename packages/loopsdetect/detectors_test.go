package loopsdetect

import (
	"strings"
	"testing"
)

func call(name string, input map[string]interface{}) ToolCall {
	return ToolCall{Name: name, Input: input, Status: "success"}
}

func TestConsecutiveDuplicate(t *testing.T) {
	d := NewConsecutiveDuplicateDetector(2)
	calls := []ToolCall{
		call("bash", map[string]interface{}{"command": "ls"}),
		call("bash", map[string]interface{}{"command": "pwd"}),
		call("bash", map[string]interface{}{"command": "ls"}),
	}
	if got := d.Check(calls); got != Continue {
		t.Fatalf("non-duplicate sequence: got %v, want Continue", got)
	}
	calls = append(calls, call("bash", map[string]interface{}{"command": "ls"}))
	if got := d.Check(calls); got != Nudge {
		t.Fatalf("duplicate run: got %v, want Nudge", got)
	}
}

func TestExactDuplicate(t *testing.T) {
	d := NewExactDuplicateDetector(10, 3)
	calls := make([]ToolCall, 0, 10)
	for i := 0; i < 10; i++ {
		calls = append(calls, call("web_search", map[string]interface{}{"q": "same query"}))
	}
	if got := d.Check(calls); got != Nudge {
		t.Fatalf("3+ identical calls: got %v, want Nudge", got)
	}
}

func TestSameToolError(t *testing.T) {
	d := NewSameToolErrorDetector(4)
	var calls []ToolCall
	for i := 0; i < 3; i++ {
		c := call("http_get", map[string]interface{}{"url": "x"})
		c.Status = "error"
		calls = append(calls, c)
	}
	if got := d.Check(calls); got != Continue {
		t.Fatalf("3 errors not enough: got %v", got)
	}
	c := call("http_get", map[string]interface{}{"url": "x"})
	c.Status = "error"
	calls = append(calls, c)
	if got := d.Check(calls); got != Nudge {
		t.Fatalf("4 consecutive errors on one tool: got %v, want Nudge", got)
	}
}

func TestFamilyNoProgress(t *testing.T) {
	d := NewFamilyNoProgressDetector()
	var calls []ToolCall
	for i := 0; i < 5; i++ {
		calls = append(calls, call("bash", map[string]interface{}{"command": "step"}))
	}
	if got := d.Check(calls); got != Nudge {
		t.Fatalf("5 shell calls in window: got %v, want Nudge", got)
	}
	for i := 5; i < 7; i++ {
		calls = append(calls, call("bash", map[string]interface{}{"command": "more"}))
	}
	if got := d.Check(calls); got != ForceStop {
		t.Fatalf("7 shell calls in window: got %v, want ForceStop", got)
	}
}

func TestNgramRepetition(t *testing.T) {
	d := NewNgramRepetitionDetector(20, 2, 3)

	// ABABAB: the 2-gram "search→edit" repeats 3 times consecutively.
	var calls []ToolCall
	for i := 0; i < 3; i++ {
		calls = append(calls, call("search", nil), call("edit", nil))
	}
	if got := d.Check(calls); got != ForceStop {
		t.Fatalf("ABABAB pattern: got %v, want ForceStop", got)
	}

	// Three non-overlapping occurrences without tight run => Nudge.
	calls = []ToolCall{
		call("search", nil), call("edit", nil),
		call("bash", nil),
		call("search", nil), call("edit", nil),
		call("bash", nil),
		call("search", nil), call("edit", nil),
	}
	if got := d.Check(calls); got != Nudge {
		t.Fatalf("repeated n-gram with breaks: got %v, want Nudge", got)
	}

	// Varied sequence: no repetition.
	calls = []ToolCall{
		call("search", nil), call("edit", nil),
		call("bash", nil), call("read", nil),
		call("write", nil), call("test", nil),
	}
	if got := d.Check(calls); got != Continue {
		t.Fatalf("varied sequence: got %v, want Continue", got)
	}
}

func TestProgressStagnation(t *testing.T) {
	d := NewProgressStagnationDetector(3)
	mk := func(out string) ToolCall {
		return ToolCall{Name: "bash", Status: "success", Output: map[string]interface{}{"result": out}}
	}
	calls := []ToolCall{
		mk("state A"),
		mk("state B"),
		mk("state B"),
	}
	if got := d.Check(calls); got != Continue {
		t.Fatalf("only 2 repeated outputs: got %v, want Continue", got)
	}
	calls = append(calls, mk("state B"))
	if got := d.Check(calls); got != Nudge {
		t.Fatalf("3 identical trailing outputs: got %v, want Nudge", got)
	}
}

func TestManagerEscalation(t *testing.T) {
	m := NewLoopDetectorManager()

	// A single repeated identical call triggers exactly the duplicate
	// detectors => Nudge.
	calls := []ToolCall{
		call("bash", map[string]interface{}{"command": "echo hi"}),
		call("bash", map[string]interface{}{"command": "echo hi"}),
	}
	res := m.Check(calls)
	if res.Decision != Nudge {
		t.Fatalf("expected Nudge, got %v (evidence: %v)", res.Decision, res.Evidence)
	}
	if m.NudgeCount() != 1 {
		t.Fatalf("nudge count = %d, want 1", m.NudgeCount())
	}

	// A different, healthy sequence resets the counter.
	healthy := []ToolCall{
		call("bash", map[string]interface{}{"command": "echo hi"}),
		call("bash", map[string]interface{}{"command": "echo there"}),
		call("bash", map[string]interface{}{"command": "echo now"}),
	}
	if res := m.Check(healthy); res.Decision != Continue {
		t.Fatalf("healthy sequence: got %v, want Continue", res.Decision)
	}
	if m.NudgeCount() != 0 {
		t.Fatalf("nudge count after healthy = %d, want 0", m.NudgeCount())
	}

	// Three consecutive Nudge checks escalate to ForceStop.
	var last *DetectorResult
	for i := 0; i < 3; i++ {
		last = m.Check(calls)
		if i < 2 && last.Decision != Nudge {
			t.Fatalf("check %d: got %v, want Nudge", i+1, last.Decision)
		}
	}
	if last.Decision != ForceStop {
		t.Fatalf("after 3 nudges: got %v, want ForceStop", last.Decision)
	}
	found := false
	for _, ev := range last.Evidence {
		if strings.Contains(ev, "escalated_from_repeated_nudges") {
			found = true
		}
	}
	if !found {
		t.Fatalf("escalation evidence missing: %v", last.Evidence)
	}
}

func TestAllDecisions(t *testing.T) {
	if got := AllDecisions([]Decision{Continue, Nudge, Continue, ForceStop, Nudge}); got != ForceStop {
		t.Fatalf("max decision = %v, want ForceStop", got)
	}
	if got := MaxDecision(ForceStop, Nudge); got != ForceStop {
		t.Fatalf("MaxDecision = %v", got)
	}
}
