package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file is the acceptance suite for design §3.5. The first test is the
// point of the whole item: it turns "streaming does not change persistence"
// from a promise into an assertion, by running the same task through the
// non-streaming path and through a full-fidelity streaming path and comparing
// the authoritative MODEL_PROPOSED payloads byte for byte.
//
// Fidelity note. The comparison holds only when the adapter's Stream carries
// everything its Complete would return. The fixture below does; the built-in
// ScriptedModel/ConversationModel deliberately do not (they stream text only),
// which is why the design asks for "a same-semantics streaming adapter" rather
// than reusing them. The one field no delta channel can reconstruct is
// ModelResponse.Raw; an adapter that sets it in Complete must send a `raw`
// stream event to stay byte-identical. Nothing else is normalised.

// --- deterministic fixtures -------------------------------------------------

func sequentialIDs() func(string) string {
	var counter atomic.Int64
	return func(prefix string) string { return fmt.Sprintf("%s-%d", prefix, counter.Add(1)) }
}

// streamTestRuntime builds a runtime whose journal is a pure function of its
// inputs: fixed clock, fixed id source, fresh equivalent collaborators.
func streamTestRuntime(t *testing.T, model ModelAdapter, extra ...Option) (*Runtime, Store, AuthContext, TaskRequest) {
	t.Helper()
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	registry := NewToolRegistry()
	if err := registry.Register(ToolSpec{
		Name:        "echo",
		Description: "echo the arguments back",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}, "required": []string{"value"}},
		Handler:     func(arguments map[string]any, _ AuthContext) (any, error) { return arguments, nil },
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	options := []Option{
		WithStore(store),
		WithModel(model),
		WithToolRegistry(registry),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
	}
	options = append(options, extra...)
	runtime, err := New(options...)
	if err != nil {
		t.Fatalf("runtime: %v", err)
	}
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	request := TaskRequest{TaskID: "stream-conformance", Goal: "produce a deterministic answer", TenantID: auth.TenantID, SubjectID: auth.SubjectID, ContextWindow: 2048, MaxSteps: 8}
	return runtime, store, auth, request
}

func streamConformanceScript() []ModelResponse {
	return []ModelResponse{
		{
			Text:         "reading the workspace ",
			ToolCalls:    []ToolCall{{CallID: "call-1", Name: "echo", Arguments: map[string]any{"value": "hi"}}},
			FinishReason: "tool_calls",
			Usage:        ModelUsage{InputTokens: 17, OutputTokens: 9, CachedInputTokens: 4, CostUSD: 0.00025},
		},
		{
			Text:         "the workspace is clean",
			FinishReason: "stop",
			Usage:        ModelUsage{InputTokens: 31, OutputTokens: 12},
		},
	}
}

// faithfulStreamModel streams exactly what its Complete would return: text in
// fragments, tool calls as argument fragments, usage and a terminal marker.
type faithfulStreamModel struct {
	inner     *ScriptedModel
	textChunk int
	argChunk  int
}

func newFaithfulStreamModel(responses []ModelResponse) *faithfulStreamModel {
	inner := NewScriptedModel(responses)
	inner.ModelName = "faithful-stream"
	return &faithfulStreamModel{inner: inner, textChunk: 3, argChunk: 4}
}

func (m *faithfulStreamModel) Name() string { return m.inner.Name() }

func (m *faithfulStreamModel) Complete(ctx context.Context, request ModelRequest) (ModelResponse, error) {
	return m.inner.Complete(ctx, request)
}

func (m *faithfulStreamModel) Stream(ctx context.Context, request ModelRequest) (<-chan StreamEvent, error) {
	response, err := m.inner.Complete(ctx, request)
	if err != nil {
		return nil, err
	}
	out := make(chan StreamEvent, 8)
	go func() {
		defer close(out)
		emit := func(event StreamEvent) bool {
			select {
			case out <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for _, fragment := range fragments(response.Text, m.textChunk) {
			if !emit(StreamEvent{Type: streamEventTextDelta, Data: map[string]any{"delta": fragment}}) {
				return
			}
		}
		for index, call := range response.ToolCalls {
			if !emit(StreamEvent{Type: streamEventToolCallDelta, Data: map[string]any{"tool_calls": []any{map[string]any{
				"index": index, "id": call.CallID, "function": map[string]any{"name": call.Name},
			}}}}) {
				return
			}
			for _, fragment := range fragments(marshalArguments(call.Arguments), m.argChunk) {
				if !emit(StreamEvent{Type: streamEventToolCallDelta, Data: map[string]any{"tool_calls": []any{map[string]any{
					"index": index, "function": map[string]any{"arguments": fragment},
				}}}}) {
					return
				}
			}
		}
		if !emit(StreamEvent{Type: streamEventUsage, Data: map[string]any{"usage": map[string]any{
			"input_tokens":        response.Usage.InputTokens,
			"output_tokens":       response.Usage.OutputTokens,
			"cached_input_tokens": response.Usage.CachedInputTokens,
			"cost_usd":            response.Usage.CostUSD,
		}}}) {
			return
		}
		emit(StreamEvent{Type: streamEventDone, Data: map[string]any{"finish_reason": response.FinishReason}})
	}()
	return out, nil
}

// fragments splits text into rune-aligned pieces so a reassembled string is
// byte-identical to the original, including multi-byte text.
func fragments(text string, size int) []string {
	if text == "" {
		return nil
	}
	if size <= 0 {
		size = 1
	}
	runes := []rune(text)
	var out []string
	for start := 0; start < len(runes); start += size {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, string(runes[start:end]))
	}
	return out
}

type recordingEmitter struct {
	mu     sync.Mutex
	chunks []Chunk
}

func (e *recordingEmitter) Emit(chunk Chunk) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.chunks = append(e.chunks, chunk)
}

func (e *recordingEmitter) snapshot() []Chunk {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Chunk(nil), e.chunks...)
}

func (e *recordingEmitter) text() string {
	var builder strings.Builder
	for _, chunk := range e.snapshot() {
		if chunk.Kind == ChunkText {
			builder.WriteString(chunk.Text)
		}
	}
	return builder.String()
}

// countingModel records which entry point the runtime chose.
type countingModel struct {
	inner         ModelAdapter
	completeCalls atomic.Int64
	streamCalls   atomic.Int64
}

func (m *countingModel) Name() string { return m.inner.Name() }

func (m *countingModel) Complete(ctx context.Context, request ModelRequest) (ModelResponse, error) {
	m.completeCalls.Add(1)
	return m.inner.Complete(ctx, request)
}

func (m *countingModel) Stream(ctx context.Context, request ModelRequest) (<-chan StreamEvent, error) {
	m.streamCalls.Add(1)
	return m.inner.Stream(ctx, request)
}

// noThanksEmitter declines observations: it exists to prove that an opt-in
// observer keeps the run on the Complete path.
type noThanksEmitter struct{ calls atomic.Int64 }

func (e *noThanksEmitter) Emit(Chunk) { e.calls.Add(1) }

func (e *noThanksEmitter) WantsChunks(string) bool { return false }

// --- helpers ----------------------------------------------------------------

func journalEvents(t *testing.T, runtime *Runtime, taskID string) []Event {
	t.Helper()
	events, err := runtime.Journal().Replay(taskID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return events
}

func modelProposedPayloads(t *testing.T, events []Event) []string {
	t.Helper()
	var payloads []string
	for _, event := range events {
		if event.Type != "MODEL_PROPOSED" {
			continue
		}
		raw, err := CanonicalJSON(event.Payload)
		if err != nil {
			t.Fatalf("canonical payload: %v", err)
		}
		payloads = append(payloads, string(raw))
	}
	return payloads
}

func modelProposedHashes(events []Event) []string {
	var hashes []string
	for _, event := range events {
		if event.Type == "MODEL_PROPOSED" {
			hashes = append(hashes, event.EventHash)
		}
	}
	return hashes
}

// --- tests ------------------------------------------------------------------

// TestStreamingAssemblesIdenticalResponse is the acceptance test of design
// §3.5: the streaming path is an observation channel, so the authoritative
// MODEL_PROPOSED payload and hash must not move by a single byte.
func TestStreamingAssemblesIdenticalResponse(t *testing.T) {
	// Path A: no emitter at all -> Complete.
	plainRuntime, plainStore, auth, request := streamTestRuntime(t, NewScriptedModel(streamConformanceScript()))
	defer plainStore.Close()
	plainResult, err := plainRuntime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("plain run: %v", err)
	}
	if plainResult.Status != TaskCompleted {
		t.Fatalf("plain status=%s category=%s", plainResult.Status, plainResult.FailureCategory)
	}

	// Path B: the same semantics, streamed, with an observer attached.
	recorder := &recordingEmitter{}
	streamRuntime, streamStore, _, _ := streamTestRuntime(t, newFaithfulStreamModel(streamConformanceScript()), WithEmitter(recorder))
	defer streamStore.Close()
	streamResult, err := streamRuntime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("streaming run: %v", err)
	}
	if streamResult.Status != TaskCompleted {
		t.Fatalf("streaming status=%s category=%s last_error=%v", streamResult.Status, streamResult.FailureCategory, streamResult.State.LastError)
	}

	plainEvents := journalEvents(t, plainRuntime, request.TaskID)
	streamEvents := journalEvents(t, streamRuntime, request.TaskID)

	plainPayloads := modelProposedPayloads(t, plainEvents)
	streamPayloads := modelProposedPayloads(t, streamEvents)
	if len(plainPayloads) != 2 || len(streamPayloads) != 2 {
		t.Fatalf("expected 2 MODEL_PROPOSED events, got plain=%d streaming=%d", len(plainPayloads), len(streamPayloads))
	}
	for i := range plainPayloads {
		if plainPayloads[i] != streamPayloads[i] {
			t.Fatalf("MODEL_PROPOSED[%d] payload differs\n plain: %s\nstream: %s", i, plainPayloads[i], streamPayloads[i])
		}
	}

	plainHashes := modelProposedHashes(plainEvents)
	streamHashes := modelProposedHashes(streamEvents)
	for i := range plainHashes {
		if plainHashes[i] != streamHashes[i] {
			t.Fatalf("MODEL_PROPOSED[%d] event_hash differs: plain=%s stream=%s", i, plainHashes[i], streamHashes[i])
		}
	}

	// The whole journal must match, not just the model events: the streaming
	// path may not perturb any durable fact at all.
	if len(plainEvents) != len(streamEvents) {
		t.Fatalf("journal length differs: plain=%d streaming=%d", len(plainEvents), len(streamEvents))
	}
	for i := range plainEvents {
		if plainEvents[i].Type != streamEvents[i].Type || plainEvents[i].EventHash != streamEvents[i].EventHash {
			t.Fatalf("event %d differs: plain=%s/%s streaming=%s/%s", i,
				plainEvents[i].Type, plainEvents[i].EventHash, streamEvents[i].Type, streamEvents[i].EventHash)
		}
	}

	// The observation channel really did carry the incremental text.
	wantText := "reading the workspace " + "the workspace is clean"
	if got := recorder.text(); got != wantText {
		t.Fatalf("streamed text=%q, want %q", got, wantText)
	}
}

// TestStreamPrefersCompleteWhenNoEmitter pins the first row of the failure
// matrix: with nobody watching, the runtime must take exactly the call it took
// before streaming existed.
func TestStreamPrefersCompleteWhenNoEmitter(t *testing.T) {
	counter := &countingModel{inner: NewScriptedModel([]ModelResponse{{Text: "done", FinishReason: "stop"}})}
	runtime, store, auth, request := streamTestRuntime(t, counter)
	defer store.Close()

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s", result.Status, result.FailureCategory)
	}
	if got := counter.streamCalls.Load(); got != 0 {
		t.Fatalf("Stream called %d times without an emitter", got)
	}
	if got := counter.completeCalls.Load(); got == 0 {
		t.Fatal("Complete was never called without an emitter")
	}
	for _, event := range journalEvents(t, runtime, request.TaskID) {
		if event.Type == "MODEL_STREAM_DEGRADED" {
			t.Fatal("no emitter, yet the journal records a degraded stream")
		}
	}

	// An observer that declines observations is the same as no observer: this
	// is what makes `?include=text` zero-overhead and byte-identical.
	decliner := &noThanksEmitter{}
	counter2 := &countingModel{inner: NewScriptedModel([]ModelResponse{{Text: "done", FinishReason: "stop"}})}
	gated, gatedStore, _, gatedRequest := streamTestRuntime(t, counter2, WithEmitter(decliner))
	defer gatedStore.Close()
	gatedRequest.TaskID = "stream-declined"
	if _, err := gated.Run(context.Background(), gatedRequest, auth); err != nil {
		t.Fatalf("gated run: %v", err)
	}
	if got := counter2.streamCalls.Load(); got != 0 {
		t.Fatalf("Stream called %d times for an observer that declined", got)
	}
	if got := decliner.calls.Load(); got != 0 {
		t.Fatalf("declined observer received %d chunks", got)
	}
}

// TestStreamMidFlightFailureDoesNotFallBack pins the third row of the failure
// matrix: once output reached the observer, regenerating it would duplicate
// what the user already saw, so the step fails instead.
func TestStreamMidFlightFailureDoesNotFallBack(t *testing.T) {
	model := &midFlightFailureModel{}
	recorder := &recordingEmitter{}
	runtime, store, auth, request := streamTestRuntime(t, model, WithEmitter(recorder))
	defer store.Close()

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.Status != TaskFailed || result.FailureCategory != "MODEL" {
		t.Fatalf("status=%s category=%s, want FAILED/MODEL", result.Status, result.FailureCategory)
	}
	if got := model.completeCalls.Load(); got != 0 {
		t.Fatalf("Complete was called %d times after a mid-flight stream failure", got)
	}
	if got := model.streamCalls.Load(); got != 1 {
		t.Fatalf("Stream called %d times, want 1", got)
	}
	if got := recorder.text(); got != "half a" {
		t.Fatalf("observer saw %q, want %q", got, "half a")
	}
	for _, event := range journalEvents(t, runtime, request.TaskID) {
		if event.Type == "MODEL_PROPOSED" {
			t.Fatal("a mid-flight failure must not commit a MODEL_PROPOSED event")
		}
		if event.Type == "MODEL_STREAM_DEGRADED" {
			t.Fatal("a mid-flight failure must not be recorded as a fallback")
		}
	}
}

// TestStreamDegradesWhenFailurePrecedesOutput pins the second row: a stream
// that dies before showing anything may safely fall back, and the fallback
// leaves an auditable trace.
func TestStreamDegradesWhenFailurePrecedesOutput(t *testing.T) {
	model := &earlyFailureModel{inner: NewScriptedModel([]ModelResponse{{Text: "recovered answer", FinishReason: "stop"}})}
	runtime, store, auth, request := streamTestRuntime(t, model, WithEmitter(&recordingEmitter{}))
	defer store.Close()

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.Status != TaskCompleted || result.FinalAnswer != "recovered answer" {
		t.Fatalf("status=%s answer=%q category=%s", result.Status, result.FinalAnswer, result.FailureCategory)
	}
	degraded := false
	proposed := 0
	for _, event := range journalEvents(t, runtime, request.TaskID) {
		switch event.Type {
		case "MODEL_STREAM_DEGRADED":
			degraded = true
			if reason, _ := event.Payload["reason"].(string); reason == "" {
				t.Fatal("MODEL_STREAM_DEGRADED carries no reason")
			}
		case "MODEL_PROPOSED":
			proposed++
		}
	}
	if !degraded {
		t.Fatal("a stream that failed before producing output did not record MODEL_STREAM_DEGRADED")
	}
	if proposed != 1 {
		t.Fatalf("proposed=%d, want 1", proposed)
	}
}

// TestStreamDegradesWhenToolCallsAreLostFromAnUnfaithfulStream guards the real
// hazard behind the fidelity contract: the built-in scripted/conversation
// adapters stream text only, so a tool-call proposal would assemble into
// "finish_reason=tool_calls with no tool call" — an empty proposal that would
// fail the step. The assembler must notice and let the runtime degrade to
// Complete instead of committing a truncated response.
func TestStreamDegradesWhenToolCallsAreLostFromAnUnfaithfulStream(t *testing.T) {
	model := &lossyToolCallModel{}
	runtime, store, auth, request := streamTestRuntime(t, model, WithEmitter(&recordingEmitter{}))
	defer store.Close()

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.Status != TaskCompleted || result.FinalAnswer != "final answer" {
		t.Fatalf("status=%s answer=%q category=%s", result.Status, result.FinalAnswer, result.FailureCategory)
	}

	degraded := 0
	toolCalls := 0
	for _, event := range journalEvents(t, runtime, request.TaskID) {
		switch event.Type {
		case "MODEL_STREAM_DEGRADED":
			degraded++
		case "MODEL_PROPOSED":
			response, _ := event.Payload["response"].(map[string]any)
			calls, _ := response["tool_calls"].([]any)
			toolCalls += len(calls)
		}
	}
	if degraded != 1 {
		t.Fatalf("MODEL_STREAM_DEGRADED count=%d, want 1 (the tool-call step)", degraded)
	}
	if toolCalls != 1 {
		t.Fatalf("committed tool calls=%d, want 1 (the degraded step must fall back to Complete)", toolCalls)
	}
}

// TestStreamNotSupportedIsSilent covers the last row of the failure matrix: an
// adapter that cannot stream is a capability gap, not a degradation.
func TestStreamNotSupportedIsSilent(t *testing.T) {
	inner := NewScriptedModel([]ModelResponse{{Text: "plain answer", FinishReason: "stop"}})
	model := &notSupportedModel{inner: inner}
	runtime, store, auth, request := streamTestRuntime(t, model, WithEmitter(&recordingEmitter{}))
	defer store.Close()

	result, err := runtime.Run(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if result.Status != TaskCompleted || result.FinalAnswer != "plain answer" {
		t.Fatalf("status=%s answer=%q", result.Status, result.FinalAnswer)
	}
	if got := model.completeCalls.Load(); got != 1 {
		t.Fatalf("Complete called %d times, want 1", got)
	}
	for _, event := range journalEvents(t, runtime, request.TaskID) {
		if event.Type == "MODEL_STREAM_DEGRADED" {
			t.Fatal("ErrNotSupported must not be recorded as a degradation")
		}
	}
}

// TestStreamDroppedFramesAreCounted proves the backpressure policy of §3.3: a
// consumer that stops reading loses frames, the run is not stalled, and the loss
// is reported rather than silent.
//
// The mechanism is asserted directly rather than through a full run. An earlier
// version drove a real run with an unread channel and relied on the model
// emitting more frames than the buffer before the run finished: that held
// without -race and failed with it, because the assertion was really about
// producer/consumer timing rather than about the drop policy. A one-frame
// emitter cannot be raced.
func TestStreamDroppedFramesAreCounted(t *testing.T) {
	// Half of §3.3: frames beyond the buffer are dropped and counted.
	out := make(chan Chunk, 1)
	emitter := newAsyncEmitter(out)
	for i := 0; i < 5; i++ {
		emitter.Emit(Chunk{Kind: ChunkText, Text: "token"})
	}
	if got := int(emitter.Dropped()); got != 4 {
		t.Fatalf("dropped=%d, want 4 (one frame fits, four do not)", got)
	}
	// Emitting must never block, which is the property that keeps a slow or
	// vanished consumer from stalling the agent loop.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			emitter.Emit(Chunk{Kind: ChunkText, Text: "more"})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Emit blocked on a full buffer")
	}

	// Other half of §3.3: the loss is reported, so dropped output is observable
	// instead of silent.
	payload := (&Runtime{}).turnCompletedPayload(withRunEmitter(context.Background(), emitter), map[string]any{"turn_id": "t"})
	want := int(emitter.Dropped())
	if got := payloadInt(payload["stream_dropped"]); got != want {
		t.Fatalf("TURN_COMPLETED stream_dropped=%d, want %d", got, want)
	}
	// An unobserved run must not gain the field, so its journal stays identical
	// to the pre-streaming one.
	clean := (&Runtime{}).turnCompletedPayload(context.Background(), map[string]any{"turn_id": "t"})
	if _, present := clean["stream_dropped"]; present {
		t.Fatal("an unobserved run recorded stream_dropped")
	}
}

// TestStreamEventOrderingAndClose pins the shape of the channel itself: events
// arrive in journal order, the task's events all arrive, and the run closes the
// channel with a terminal ChunkDone.
func TestStreamEventOrderingAndClose(t *testing.T) {
	model := newFaithfulStreamModel([]ModelResponse{{Text: "hello streamed world", FinishReason: "stop", Usage: ModelUsage{InputTokens: 3, OutputTokens: 4}}})
	runtime, store, auth, request := streamTestRuntime(t, model)
	defer store.Close()

	chunks, err := runtime.RunStream(context.Background(), request, auth)
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	var collected []Chunk
	deadline := time.After(10 * time.Second)
collect:
	for {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				break collect
			}
			collected = append(collected, chunk)
		case <-deadline:
			t.Fatal("RunStream did not close")
		}
	}

	if len(collected) == 0 {
		t.Fatal("no chunks")
	}
	if last := collected[len(collected)-1]; last.Kind != ChunkDone {
		t.Fatalf("last chunk=%s, want done", last.Kind)
	} else if last.Data["status"] != TaskCompleted {
		t.Fatalf("done status=%v, want %s", last.Data["status"], TaskCompleted)
	}

	var text strings.Builder
	sequences := []int{}
	for _, chunk := range collected {
		switch chunk.Kind {
		case ChunkText:
			text.WriteString(chunk.Text)
			if chunk.TaskID != request.TaskID {
				t.Fatalf("text chunk for task %q, want %q", chunk.TaskID, request.TaskID)
			}
		case ChunkEvent:
			if chunk.Event == nil {
				t.Fatal("ChunkEvent without an event")
			}
			if chunk.Sequence != chunk.Event.Sequence {
				t.Fatalf("chunk sequence %d != event sequence %d", chunk.Sequence, chunk.Event.Sequence)
			}
			sequences = append(sequences, chunk.Sequence)
		case ChunkDone:
		default:
			t.Fatalf("unexpected chunk kind %s", chunk.Kind)
		}
	}
	if got := text.String(); got != "hello streamed world" {
		t.Fatalf("streamed text=%q", got)
	}
	for i := 1; i < len(sequences); i++ {
		if sequences[i] <= sequences[i-1] {
			t.Fatalf("event sequences are not strictly increasing: %v", sequences)
		}
	}
	journal := journalEvents(t, runtime, request.TaskID)
	if len(sequences) != len(journal) {
		t.Fatalf("delivered %d events, journal has %d", len(sequences), len(journal))
	}
	for i, event := range journal {
		if sequences[i] != event.Sequence {
			t.Fatalf("delivered sequence %d at position %d, journal has %d", sequences[i], i, event.Sequence)
		}
	}
}

// TestRunStreamCancelledConsumerCannotBlockRun is the -race guard for §3.3: a
// consumer that cancels and stops reading must unwind the run and close the
// channel instead of parking the loop.
func TestRunStreamCancelledConsumerCannotBlockRun(t *testing.T) {
	runtime, store, auth, request := streamTestRuntime(t, &endlessStreamModel{})
	defer store.Close()

	ctx, cancel := context.WithCancel(context.Background())
	chunks, err := runtime.RunStream(ctx, request, auth)
	if err != nil {
		t.Fatalf("RunStream: %v", err)
	}
	select {
	case <-chunks:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream never produced a chunk")
	}
	cancel()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-chunks:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("RunStream did not close after cancellation")
		}
	}
}

// TestRunStreamRejectsInvalidIdentity keeps the setup error out of the
// goroutine: an unusable request fails synchronously, with a closed channel
// never created.
func TestRunStreamRejectsInvalidIdentity(t *testing.T) {
	runtime, store, auth, _ := streamTestRuntime(t, NewScriptedModel(nil))
	defer store.Close()
	if _, err := runtime.RunStream(context.Background(), TaskRequest{TaskID: "", Goal: ""}, auth); err == nil {
		t.Fatal("RunStream accepted a request without a task id and goal")
	}
}

// --- failure fixtures -------------------------------------------------------

// midFlightFailureModel shows text, then dies. It must never be re-completed.
type midFlightFailureModel struct {
	completeCalls atomic.Int64
	streamCalls   atomic.Int64
}

func (m *midFlightFailureModel) Name() string { return "mid-flight" }

func (m *midFlightFailureModel) Complete(context.Context, ModelRequest) (ModelResponse, error) {
	m.completeCalls.Add(1)
	return ModelResponse{Text: "regenerated", FinishReason: "stop"}, nil
}

func (m *midFlightFailureModel) Stream(ctx context.Context, _ ModelRequest) (<-chan StreamEvent, error) {
	m.streamCalls.Add(1)
	out := make(chan StreamEvent, 3)
	out <- StreamEvent{Type: streamEventTextDelta, Data: map[string]any{"delta": "half "}}
	out <- StreamEvent{Type: streamEventTextDelta, Data: map[string]any{"delta": "a"}}
	out <- StreamEvent{Type: streamEventError, Data: map[string]any{"error": "provider exploded"}}
	close(out)
	return out, nil
}

// earlyFailureModel fails before producing anything, so Complete is the safe
// and correct answer.
type earlyFailureModel struct {
	inner         ModelAdapter
	completeCalls atomic.Int64
	streamCalls   atomic.Int64
}

func (m *earlyFailureModel) Name() string { return "early-failure" }

func (m *earlyFailureModel) Complete(ctx context.Context, request ModelRequest) (ModelResponse, error) {
	m.completeCalls.Add(1)
	return m.inner.Complete(ctx, request)
}

func (m *earlyFailureModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	m.streamCalls.Add(1)
	return nil, errors.New("connection refused before the first byte")
}

// notSupportedModel declares the capability gap explicitly.
type notSupportedModel struct {
	inner         ModelAdapter
	completeCalls atomic.Int64
}

func (m *notSupportedModel) Name() string { return "no-streaming" }

func (m *notSupportedModel) Complete(ctx context.Context, request ModelRequest) (ModelResponse, error) {
	m.completeCalls.Add(1)
	return m.inner.Complete(ctx, request)
}

func (m *notSupportedModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, ErrNotSupported
}

// lossyToolCallModel mirrors the built-in ConversationModel/ScriptedModel: its
// Stream forwards text and a terminal marker but silently drops tool calls.
type lossyToolCallModel struct {
	completeCalls atomic.Int64
	streamCalls   atomic.Int64
}

func (m *lossyToolCallModel) Name() string { return "lossy-tool-call" }

// Complete is stateless, like the offline conversation adapter: the proposal is
// derived from the request, so a fallback after a failed stream returns the same
// answer instead of consuming a second scripted response.
func (m *lossyToolCallModel) Complete(_ context.Context, request ModelRequest) (ModelResponse, error) {
	m.completeCalls.Add(1)
	if !hasToolObservation(request.Messages, "echo") {
		return ModelResponse{
			ToolCalls:    []ToolCall{{CallID: "call-1", Name: "echo", Arguments: map[string]any{"value": "hi"}}},
			FinishReason: "tool_calls",
		}, nil
	}
	return ModelResponse{Text: "final answer", FinishReason: "stop"}, nil
}

func (m *lossyToolCallModel) Stream(ctx context.Context, request ModelRequest) (<-chan StreamEvent, error) {
	m.streamCalls.Add(1)
	response, err := m.Complete(ctx, request)
	if err != nil {
		return nil, err
	}
	out := make(chan StreamEvent, 8)
	go func() {
		defer close(out)
		emit := func(event StreamEvent) bool {
			select {
			case out <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for _, fragment := range fragments(response.Text, 4) {
			if !emit(StreamEvent{Type: streamEventTextDelta, Data: map[string]any{"delta": fragment}}) {
				return
			}
		}
		// Tool calls are dropped, exactly like the offline adapters.
		emit(StreamEvent{Type: streamEventDone, Data: map[string]any{"finish_reason": response.FinishReason}})
	}()
	return out, nil
}

// endlessStreamModel never terminates on its own; only cancellation stops it.
type endlessStreamModel struct{}

func (m *endlessStreamModel) Name() string { return "endless" }

func (m *endlessStreamModel) Complete(context.Context, ModelRequest) (ModelResponse, error) {
	return ModelResponse{Text: "unused", FinishReason: "stop"}, nil
}

func (m *endlessStreamModel) Stream(ctx context.Context, _ ModelRequest) (<-chan StreamEvent, error) {
	out := make(chan StreamEvent)
	go func() {
		defer close(out)
		for {
			select {
			case out <- StreamEvent{Type: streamEventTextDelta, Data: map[string]any{"delta": "x"}}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// --- small utilities --------------------------------------------------------

func waitForTerminalTask(t *testing.T, store Store, taskID string, timeout time.Duration) TaskState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		state, _, err := store.GetTask(taskID)
		if err == nil && (state.Status == TaskCompleted || state.Status == TaskFailed || state.Status == TaskCancelled) {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s never reached a terminal state", taskID)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func payloadInt(value any) int {
	switch number := value.(type) {
	case int:
		return number
	case int64:
		return int(number)
	case float64:
		return int(number)
	case json.Number:
		parsed, _ := number.Int64()
		return int(parsed)
	}
	return 0
}
