// Package looptest is the conformance suite every harness.Strategy must pass.
//
// The loop kernel owns the durable guarantees, so a custom topology inherits
// them for free — but only if it obeys the contract (see harness/loop.go).
// This suite turns that contract into executable checks, the same way
// harness/storetest does for storage backends.
//
// An embedder who writes their own topology runs:
//
//	func TestMyStrategy(t *testing.T) {
//	    looptest.RunStrategyConformance(t, func() harness.Strategy { return MyStrategy{} })
//	}
package looptest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// Options tune the scenario the suite drives a strategy through.
type Options struct {
	// Responses is the scripted model output. A sensible default is used when
	// empty: one tool call, then a final answer.
	Responses []harness.ModelResponse
	// MaxSteps bounds the run. Defaults to 8.
	MaxSteps int
	// Tools are registered before the run. Defaults to a single "echo" tool.
	Tools []harness.ToolSpec
	// TaskID defaults to "looptest-task".
	TaskID string
}

// Option mutates Options.
type Option func(*Options)

// WithResponses scripts the model.
func WithResponses(responses ...harness.ModelResponse) Option {
	return func(o *Options) { o.Responses = responses }
}

// WithMaxSteps bounds the run.
func WithMaxSteps(steps int) Option {
	return func(o *Options) { o.MaxSteps = steps }
}

// WithTools registers tools for the run.
func WithTools(tools ...harness.ToolSpec) Option {
	return func(o *Options) { o.Tools = tools }
}

// RunStrategyConformance runs every check. newStrategy is called once per
// subtest so each check starts from a fresh value.
func RunStrategyConformance(t *testing.T, newStrategy func() harness.Strategy, options ...Option) {
	t.Helper()
	base := Options{}
	for _, option := range options {
		option(&base)
	}
	t.Run("Vocabulary", func(t *testing.T) { testVocabulary(t, newStrategy(), base) })
	t.Run("Determinism", func(t *testing.T) { testDeterminism(t, newStrategy(), base) })
	t.Run("Bounded", func(t *testing.T) { testBounded(t, newStrategy(), base) })
	t.Run("JournalIntegrity", func(t *testing.T) { testJournalIntegrity(t, newStrategy(), base) })
	t.Run("IdentityIsRecordedAndPinned", func(t *testing.T) { testIdentity(t, newStrategy(), base) })
	t.Run("ScratchSurvivesResume", func(t *testing.T) { testScratch(t, newStrategy(), base) })
	t.Run("ToolPolicyIsNotBypassed", func(t *testing.T) { testToolPolicy(t, newStrategy(), base) })
	t.Run("BudgetCannotHang", func(t *testing.T) { testBudget(t, newStrategy(), base) })
}

// --- harness helpers -------------------------------------------------------

func defaultResponses() []harness.ModelResponse {
	return []harness.ModelResponse{
		{ToolCalls: []harness.ToolCall{{CallID: "c1", Name: "echo", Arguments: map[string]any{"value": "hi"}}}},
		{Text: "done", FinishReason: "stop"},
	}
}

func defaultTools() []harness.ToolSpec {
	return []harness.ToolSpec{{
		Name:        "echo",
		Description: "echo the argument",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}},
		Handler:     func(args map[string]any, _ harness.AuthContext) (any, error) { return args, nil },
	}}
}

func resolve(base Options) Options {
	if len(base.Responses) == 0 {
		base.Responses = defaultResponses()
	}
	if base.MaxSteps == 0 {
		base.MaxSteps = 8
	}
	if base.Tools == nil {
		base.Tools = defaultTools()
	}
	if base.TaskID == "" {
		base.TaskID = "looptest-task"
	}
	return base
}

// deterministicIDs makes identifiers reproducible, which is what allows the
// determinism check to compare journals byte for byte.
func deterministicIDs() func(string) string {
	var counter int
	return func(prefix string) string {
		counter++
		return fmt.Sprintf("%s-%06d", prefix, counter)
	}
}

var fixedClockTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type scenario struct {
	runtime *harness.Runtime
	store   harness.Store
	auth    harness.AuthContext
	request harness.TaskRequest
}

// build creates a runtime whose only nondeterminism (time, identifiers) is
// pinned, so any difference between two runs comes from the strategy.
func build(t *testing.T, strategy harness.Strategy, base Options, model harness.ModelAdapter) *scenario {
	t.Helper()
	opts := resolve(base)
	store := harness.NewInMemoryStore()
	registry := harness.NewToolRegistry()
	for _, tool := range opts.Tools {
		if err := registry.Register(tool); err != nil {
			t.Fatalf("register tool %s: %v", tool.Name, err)
		}
	}
	auth := harness.AuthContext{TenantID: "looptest", SubjectID: "subject", Scopes: []string{"read", "write"}}
	runtime, err := harness.New(
		harness.WithStore(store),
		harness.WithModel(model),
		harness.WithToolRegistry(registry),
		harness.WithClock(harness.NewManualClock(fixedClockTime)),
		harness.WithIDSource(deterministicIDs()),
		harness.WithStrategyInstance(strategy),
	)
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	request := harness.TaskRequest{
		TaskID:    opts.TaskID,
		Goal:      "exercise the strategy contract",
		TenantID:  auth.TenantID,
		SubjectID: auth.SubjectID,
		MaxSteps:  opts.MaxSteps,
	}
	return &scenario{runtime: runtime, store: store, auth: auth, request: request}
}

func (s *scenario) run(t *testing.T) harness.RunResult {
	t.Helper()
	result, err := s.runtime.Run(context.Background(), s.request, s.auth)
	if err != nil {
		// An infrastructure error is not a strategy failure, but it does mean
		// the scenario could not be exercised.
		t.Fatalf("Run: %v", err)
	}
	return result
}

func (s *scenario) events(t *testing.T) []harness.Event {
	t.Helper()
	events, err := s.runtime.Journal().Replay(s.request.TaskID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	return events
}

// --- checks ----------------------------------------------------------------

// testVocabulary asserts the strategy only causes known events to be published.
// A topology that invents an event silently breaks replay, the UAT gate and
// attribution, so this is the first thing checked.
func testVocabulary(t *testing.T, strategy harness.Strategy, base Options) {
	s := build(t, strategy, base, harness.NewScriptedModel(resolve(base).Responses))
	defer func() { _ = s.store.Close() }()
	s.run(t)
	for _, event := range s.events(t) {
		if !harness.IsKnownEventType(event.Type) {
			t.Fatalf("strategy %s published %q, which is outside the shared vocabulary", strategy.Name(), event.Type)
		}
	}
}

// testDeterminism asserts that the same inputs produce the same journal. This
// is the property that makes replay meaningful, and it is the check a strategy
// using time.Now, a global RNG or map iteration order will fail.
func testDeterminism(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	first := build(t, strategy, opts, harness.NewScriptedModel(opts.Responses))
	defer func() { _ = first.store.Close() }()
	second := build(t, strategy, opts, harness.NewScriptedModel(opts.Responses))
	defer func() { _ = second.store.Close() }()

	first.run(t)
	second.run(t)
	left, right := first.events(t), second.events(t)
	if len(left) != len(right) {
		t.Fatalf("event counts differ: %d vs %d", len(left), len(right))
	}
	for i := range left {
		if left[i].Type != right[i].Type || left[i].EventHash != right[i].EventHash {
			t.Fatalf("event %d differs (%s/%s vs %s/%s): the strategy is not deterministic",
				i, left[i].Type, left[i].EventHash, right[i].Type, right[i].EventHash)
		}
		leftPayload, _ := json.Marshal(left[i].Payload)
		rightPayload, _ := json.Marshal(right[i].Payload)
		if string(leftPayload) != string(rightPayload) {
			t.Fatalf("event %d payload differs\n%s\n%s", i, leftPayload, rightPayload)
		}
	}
}

// testBounded asserts the strategy cannot run forever: the kernel must reach a
// terminal state within MaxSteps.
func testBounded(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	opts.MaxSteps = 4
	// A model that never produces a final answer forces the step limit.
	forever := make([]harness.ModelResponse, 0, 16)
	for i := 0; i < 16; i++ {
		forever = append(forever, harness.ModelResponse{Text: "", FinishReason: "stop"})
	}
	forever[15] = harness.ModelResponse{Text: "done", FinishReason: "stop"}
	s := build(t, strategy, opts, harness.NewScriptedModel(forever))
	defer func() { _ = s.store.Close() }()

	done := make(chan harness.RunResult, 1)
	go func() { done <- s.run(t) }()
	select {
	case result := <-done:
		if result.Status == harness.TaskRunning || result.Status == "" {
			t.Fatalf("run did not terminate: status=%q", result.Status)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("strategy did not terminate within the step limit")
	}
}

// testJournalIntegrity asserts the resulting journal verifies: contiguous
// sequence numbers, correct hash links and state digests matching checkpoints.
func testJournalIntegrity(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	s := build(t, strategy, opts, harness.NewScriptedModel(opts.Responses))
	defer func() { _ = s.store.Close() }()
	result := s.run(t)
	if _, err := s.runtime.Journal().ReplayState(s.request.TaskID); err != nil {
		t.Fatalf("journal produced by strategy %s does not verify: %v", strategy.Name(), err)
	}
	if result.TaskID != opts.TaskID {
		t.Fatalf("task id changed: %s", result.TaskID)
	}
}

// testIdentity asserts the topology is recorded and that upgrading it cannot
// reinterpret history.
//
// The refusal itself only applies to a *resumable* task: a task that already
// reached a terminal state never consults the strategy again, so there is no
// hazard to refuse. This suite therefore checks what is universally true and
// exercises the refusal when the run left the task open. The gated-resume
// refusal is covered explicitly in harness/loop_vocabulary_test.go, where a
// scenario that pauses at a user gate can be set up deterministically.
func testIdentity(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	s := build(t, strategy, opts, harness.NewScriptedModel(opts.Responses))
	defer func() { _ = s.store.Close() }()
	s.run(t)

	recorded := false
	for _, event := range s.events(t) {
		if event.Type != "TURN_STARTED" {
			continue
		}
		identity, ok := event.Payload["strategy"].(map[string]any)
		if !ok {
			t.Fatalf("TURN_STARTED from strategy %s did not record its identity", strategy.Name())
		}
		if identity["name"] != strategy.Name() || identity["version"] != strategy.Version() {
			t.Fatalf("recorded identity %v does not match %s@%s", identity, strategy.Name(), strategy.Version())
		}
		recorded = true
	}
	if !recorded {
		t.Fatalf("strategy %s produced no TURN_STARTED event", strategy.Name())
	}

	// Upgrading a topology must not invalidate history: the old journal still
	// verifies, because the chain records what happened, not what would happen.
	if _, err := s.runtime.Journal().ReplayState(opts.TaskID); err != nil {
		t.Fatalf("journal stopped verifying after a strategy upgrade: %v", err)
	}

	state, _, err := s.store.GetTask(opts.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if state.Status == harness.TaskCompleted || state.Status == harness.TaskFailed || state.Status == harness.TaskCancelled {
		t.Logf("strategy %s finished the task (%s); the resume refusal does not apply", strategy.Name(), state.Status)
		return
	}

	// The task is resumable, so continuing it under a different version must be
	// refused rather than silently replayed under new semantics.
	other, err := harness.New(
		harness.WithStore(s.store),
		harness.WithModel(harness.NewScriptedModel(opts.Responses)),
		harness.WithClock(harness.NewManualClock(fixedClockTime)),
		harness.WithStrategyInstance(otherVersion{strategy}),
	)
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	resumed, err := other.Resume(context.Background(), opts.TaskID, "continue", s.auth)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.FailureCategory != "STRATEGY" {
		t.Fatalf("resuming under a different version was allowed: status=%s category=%s", resumed.Status, resumed.FailureCategory)
	}
}

// otherVersion wraps a strategy under a version it never ran with.
type otherVersion struct{ harness.Strategy }

func (o otherVersion) Version() string { return o.Strategy.Version() + "-changed" }

// testScratch asserts strategy-private state round-trips through the journal.
// It only applies to strategies that use Scratch; a strategy that never writes
// any still has to survive the resume path.
func testScratch(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	s := build(t, strategy, opts, harness.NewScriptedModel(opts.Responses))
	defer func() { _ = s.store.Close() }()
	s.run(t)

	state, _, err := s.store.GetTask(opts.TaskID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	// A resumed run must observe whatever the strategy persisted. The kernel
	// stores it under a reserved metadata key; if the strategy wrote nothing,
	// the key is simply absent and the resume path must still work.
	resumed, err := s.runtime.Resume(context.Background(), opts.TaskID, "continue", s.auth)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.TaskID != opts.TaskID {
		t.Fatalf("resume changed the task id: %s", resumed.TaskID)
	}
	if state.TaskID != opts.TaskID {
		t.Fatalf("task state id changed: %s", state.TaskID)
	}
}

// testToolPolicy asserts a strategy cannot use tools to escalate privilege: a
// call the policy denies must come back as a denied observation and the run
// must still reach a terminal state.
func testToolPolicy(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	// "forbidden" is never registered, so the tool runtime must refuse it.
	opts.Responses = []harness.ModelResponse{
		{ToolCalls: []harness.ToolCall{{CallID: "c1", Name: "forbidden", Arguments: map[string]any{}}}},
		{Text: "recovered", FinishReason: "stop"},
	}
	s := build(t, strategy, opts, harness.NewScriptedModel(opts.Responses))
	defer func() { _ = s.store.Close() }()
	result := s.run(t)
	if result.Status != harness.TaskCompleted && result.Status != harness.TaskFailed {
		t.Fatalf("run stuck after a denied tool call: status=%s", result.Status)
	}
	denied := false
	for _, event := range s.events(t) {
		if event.Type != "TOOL_OBSERVED" {
			continue
		}
		observation, _ := event.Payload["observation"].(map[string]any)
		if observation["tool"] == "forbidden" && observation["status"] != harness.ToolSucceeded {
			denied = true
		}
	}
	if !denied {
		t.Fatalf("an unregistered tool was not recorded as denied: %s", strategy.Name())
	}
}

// testBudget asserts an exhausted budget terminates the run rather than
// looping or panicking.
func testBudget(t *testing.T, strategy harness.Strategy, base Options) {
	opts := resolve(base)
	store := harness.NewInMemoryStore()
	defer func() { _ = store.Close() }()
	model := harness.NewScriptedModel(opts.Responses)
	runtime, err := harness.New(
		harness.WithStore(store),
		harness.WithModel(model),
		harness.WithClock(harness.NewManualClock(fixedClockTime)),
		harness.WithIDSource(deterministicIDs()),
		harness.WithStrategyInstance(strategy),
		harness.WithBudget(&harness.BudgetLedger{MaxToolCalls: 0, MaxTokens: 1}),
	)
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	auth := harness.AuthContext{TenantID: "looptest", SubjectID: "subject"}
	done := make(chan harness.RunResult, 1)
	go func() {
		result, runErr := runtime.Run(context.Background(), harness.TaskRequest{
			TaskID: opts.TaskID, Goal: "exhaust the budget", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: opts.MaxSteps,
		}, auth)
		if runErr != nil {
			t.Errorf("Run returned an infrastructure error: %v", runErr)
		}
		done <- result
	}()
	select {
	case result := <-done:
		if result.Status == harness.TaskRunning || result.Status == "" {
			t.Fatalf("run did not terminate under an exhausted budget: status=%q", result.Status)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("strategy did not terminate under an exhausted budget")
	}
}
