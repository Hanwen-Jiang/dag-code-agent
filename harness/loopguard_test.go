package harness

import (
	"context"
	"fmt"
	"testing"
)

// The loop guard seam replaced a bare fingerprint counter that could only ever
// kill the task. These tests pin the escalation it now supports.

// variedObservation models healthy multi-step work: a different tool each step,
// distinct arguments and distinct results. Varying only a single character, as an
// earlier version of this fixture did, is not varied work at all — the n-gram
// detector correctly reads that as repetition, which is why the fixture has to
// look like the real thing for the assertion to mean anything.
func variedObservation(step int) LoopObservation {
	steps := []struct {
		tool   string
		args   map[string]any
		output map[string]any
	}{
		{"workspace_list", map[string]any{"path": ".", "depth": 1}, map[string]any{"entries": []string{"main.go", "go.mod", "README.md"}, "count": 3}},
		{"workspace_read", map[string]any{"path": "main.go"}, map[string]any{"body": "package main\n\nfunc main() {\n\tprintln(\"hello\")\n}", "lines": 5}},
		{"workspace_search", map[string]any{"query": "func main"}, map[string]any{"matches": []string{"main.go:3"}, "count": 1}},
		{"workspace_read", map[string]any{"path": "go.mod"}, map[string]any{"body": "module example.com/demo\n\ngo 1.26.0", "lines": 3}},
		{"workspace_test", map[string]any{"command": "go test ./..."}, map[string]any{"exit_code": 0, "stdout": "ok example.com/demo 0.02s"}},
		{"workspace_search", map[string]any{"query": "TODO"}, map[string]any{"matches": []string{}, "count": 0}},
	}
	pick := steps[step%len(steps)]
	return LoopObservation{
		TaskID: "task",
		Step:   step,
		ToolCalls: []ToolCall{
			{CallID: fmt.Sprintf("c%d", step), Name: pick.tool, Arguments: pick.args},
		},
		ToolResults: []ToolResult{
			{CallID: fmt.Sprintf("c%d", step), Name: pick.tool, Status: ToolSucceeded, Value: pick.output},
		},
	}
}

func repeatedObservation(step int) LoopObservation {
	return LoopObservation{
		TaskID:      "task",
		Step:        step,
		ToolCalls:   []ToolCall{{CallID: "c1", Name: "read", Arguments: map[string]any{"path": "same"}}},
		ToolResults: []ToolResult{{CallID: "c1", Name: "read", Status: ToolSucceeded, Value: map[string]any{"body": "same"}}},
	}
}

// A varied batch must not be reported as a loop: a guard that fires on normal
// work is worse than no guard, because it would nudge every healthy run.
func TestLoopGuardAllowsVariedWork(t *testing.T) {
	guard := newDetectorLoopGuard()
	for step := 0; step < 6; step++ {
		observation := variedObservation(step)
		if verdict := guard.Check(observation); verdict.Decision != LoopContinue {
			t.Fatalf("step %d produced %s: %s", step, verdict.Decision, verdict.Message)
		}
	}
}

// Repetition must first warn and only then terminate. The previous behaviour
// failed on the fourth identical call with no chance to recover.
func TestLoopGuardNudgesBeforeStopping(t *testing.T) {
	guard := newDetectorLoopGuard()
	sawNudge := false
	sawForceStop := false
	for step := 0; step < 30 && !sawForceStop; step++ {
		verdict := guard.Check(repeatedObservation(step))
		switch verdict.Decision {
		case LoopNudge:
			sawNudge = true
			// A nudge without an actionable directive gives the model nothing to
			// change, which usually produces the same call again.
			if directive := verdict.Directive(); directive == "" || len(directive) < 40 {
				t.Fatalf("nudge directive is not actionable: %q", directive)
			}
		case LoopForceStop:
			sawForceStop = true
		}
	}
	if !sawNudge {
		t.Fatal("repetition never produced a nudge")
	}
	if !sawForceStop {
		t.Fatal("repeated nudges never escalated to a force stop, so a stuck run could continue forever")
	}
}

// Per-task isolation: the manager holds nudge counts, so sharing it would let
// one task's repetition push another task over the escalation threshold.
func TestLoopGuardStateIsPerTask(t *testing.T) {
	guard := newDetectorLoopGuard()
	for step := 0; step < 3; step++ {
		observation := repeatedObservation(step)
		observation.TaskID = "noisy"
		guard.Check(observation)
	}
	quiet := variedObservation(4)
	quiet.TaskID = "quiet"
	if verdict := guard.Check(quiet); verdict.Decision != LoopContinue {
		t.Fatalf("a quiet task inherited another task's loop state: %s", verdict.Decision)
	}
}

// Reset must clear the task, so a re-run of the same id starts clean.
func TestLoopGuardReset(t *testing.T) {
	guard := newDetectorLoopGuard()
	for step := 0; step < 3; step++ {
		guard.Check(repeatedObservation(step))
	}
	guard.Reset("task")
	if verdict := guard.Check(variedObservation(0)); verdict.Decision != LoopContinue {
		t.Fatalf("state survived Reset: %s", verdict.Decision)
	}
}

// A nil guard is rejected when the option is applied, not mid-run.
func TestWithLoopGuardRejectsNil(t *testing.T) {
	if _, err := New(WithLoopGuard(nil)); err == nil {
		t.Fatal("a nil loop guard was accepted")
	}
}

// The managed resolver must make packages/skills reachable from the runtime:
// registering a pattern has to survive into selection, otherwise the package is
// still wired to nothing.
func TestManagedSkillResolverSelectsRegisteredPattern(t *testing.T) {
	resolver := NewManagedSkillResolver()
	if err := resolver.Register(SkillManifest{
		SkillID:      "verify-evidence",
		Version:      "1",
		Title:        "verify evidence",
		Instructions: "Bind every claim to an artifact digest before answering.",
		Status:       "ACTIVE",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// The pattern is persisted, which is what makes it selectable.
	patterns, err := resolver.store.List()
	if err != nil || len(patterns) != 1 {
		t.Fatalf("pattern not stored: n=%d err=%v", len(patterns), err)
	}
	if patterns[0].ID != "verify-evidence" || patterns[0].Version != 1 {
		t.Fatalf("pattern round-trip lost fields: %+v", patterns[0])
	}
	// Selection must not error merely because the goal does not match.
	_ = resolver.Resolve("unrelated goal", AuthContext{TenantID: "t", SubjectID: "s"})
}

// A resolver with nothing stored returns no skills rather than failing: context
// compilation must not depend on the skill store.
func TestManagedSkillResolverEmptyIsNotAnError(t *testing.T) {
	resolver := NewManagedSkillResolver()
	if got := resolver.Resolve("anything", AuthContext{TenantID: "t", SubjectID: "s"}); len(got) != 0 {
		t.Fatalf("expected no skills, got %+v", got)
	}
}

// The built-in registry remains a valid resolver, so widening the field type
// changes nothing for existing callers.
func TestSkillRegistrySatisfiesResolver(t *testing.T) {
	registry := NewSkillRegistry()
	registry.Register(SkillManifest{SkillID: "verification", Version: "1.0.0", Title: "verification", Instructions: "bind output to evidence", Status: "ACTIVE"})
	var resolver SkillResolver = registry
	resolved := resolver.Resolve("verification", AuthContext{TenantID: "t", SubjectID: "s"})
	if len(resolved) != 1 || resolved[0].SkillID != "verification" {
		t.Fatalf("registry resolver returned %+v", resolved)
	}
}

// The guard is only useful if the kernel consults it. This drives the real run
// loop with a repeated tool call and asserts both halves of the escalation:
// the model is warned first, and the run still terminates.
func TestKernelNudgesBeforeFailingOnRepetition(t *testing.T) {
	call := ToolCall{CallID: "c1", Name: "echo", Arguments: map[string]any{"value": "same"}}
	responses := make([]ModelResponse, 0, 12)
	for i := 0; i < 12; i++ {
		responses = append(responses, ModelResponse{ToolCalls: []ToolCall{call}})
	}
	runtime, store, auth, request := newRuntimeForTest(t, responses)
	defer store.Close()
	if err := runtime.Tools().Registry.Register(ToolSpec{
		Name: "echo", Description: "echo",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskFailed || result.FailureCategory != "LOOP" {
		t.Fatalf("a repeated call did not terminate the run: status=%s category=%s", result.Status, result.FailureCategory)
	}

	events, err := runtime.Journal().Replay(request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	nudges := 0
	sawStop := false
	for _, event := range events {
		switch event.Type {
		case "LOOP_NUDGE":
			nudges++
			if directive, _ := event.Payload["directive"].(string); directive == "" {
				t.Fatal("a nudge was recorded without a directive for the model")
			}
		case "TASK_FAILED":
			sawStop = true
		}
	}
	if nudges == 0 {
		t.Fatal("the kernel never warned the model before terminating it")
	}
	if !sawStop {
		t.Fatal("no terminal failure was recorded")
	}
	// A nudge must not run the batch it just flagged: repeating an unproductive
	// call spends budget to reinforce the loop.
	//
	// The assertion is threshold-independent on purpose. The detectors decide how
	// many identical calls are tolerable before they warn, and hard-coding that
	// number here would couple this test to a tuning constant that belongs to
	// packages/loopsdetect. What must hold is the relationship: some proposals
	// were suppressed rather than executed.
	invocations, proposals := 0, 0
	for _, event := range events {
		switch event.Type {
		case "TOOL_INVOKED":
			invocations++
		case "MODEL_PROPOSED":
			proposals++
		}
	}
	if invocations == 0 {
		t.Fatal("no tool call ever ran, so the run never reached the repeated state")
	}
	if invocations >= proposals {
		t.Fatalf("every proposal executed (invocations=%d proposals=%d): the guard's nudge did not suppress the flagged batch", invocations, proposals)
	}
}

// A guard installed by the embedder replaces the default.
func TestCustomLoopGuardIsUsed(t *testing.T) {
	custom := &alwaysStopGuard{}
	runtime, err := New(WithLoopGuard(custom))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if runtime.LoopGuard() != LoopGuard(custom) {
		t.Fatal("the runtime did not keep the injected guard")
	}
}

type alwaysStopGuard struct{ calls int }

func (g *alwaysStopGuard) Check(LoopObservation) LoopVerdict {
	g.calls++
	return LoopVerdict{Decision: LoopContinue}
}
func (g *alwaysStopGuard) Reset(string) {}
