package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// Streaming is an OBSERVATION channel, never a source of truth (design §3,
// principle P5).
//
// Incremental text is ephemeral: it is never journaled. The authoritative
// output of a model step stays the MODEL_PROPOSED event carrying the assembled
// ModelResponse, so replay is unaffected by whether anybody watched the run.
// Everything in this file exists to make that statement true rather than
// aspirational:
//
//   - the assembler turns the provider's delta vocabulary back into exactly the
//     ModelResponse Complete would have returned, so the journal is identical;
//   - the emitter never blocks the loop (a slow consumer loses "animation",
//     counted, never the run);
//   - a stream that fails before producing anything degrades to Complete and
//     records why; one that fails mid-flight fails the step, because
//     regenerating output the user already saw would duplicate it.

// ChunkKind classifies one message on the observation channel.
type ChunkKind string

const (
	// ChunkText is one incremental model text delta. Ephemeral: never journaled.
	ChunkText ChunkKind = "text"
	// ChunkToolCall is a parsed tool-call delta.
	ChunkToolCall ChunkKind = "tool_call"
	// ChunkEvent is a committed journal event. It is authoritative and already
	// durable when it reaches an observer.
	ChunkEvent ChunkKind = "event"
	// ChunkStep is a step boundary.
	ChunkStep ChunkKind = "step"
	// ChunkSubagent is a subagent lifecycle observation (design §4).
	ChunkSubagent ChunkKind = "subagent"
	// ChunkDone is the last chunk of a run; the channel closes right after it.
	ChunkDone ChunkKind = "done"
)

// Chunk is one message on the observation channel. When Kind is ChunkEvent,
// Event is non-nil and already persisted; every other kind is an ephemeral
// observation that a replay will not reproduce.
type Chunk struct {
	Kind     ChunkKind
	TaskID   string
	Sequence int            // ChunkEvent only: the journal sequence.
	Text     string         // ChunkText only.
	Event    *Event         // ChunkEvent only.
	Data     map[string]any // everything else.
}

// Emitter receives streaming observations. An implementation MUST be safe for
// concurrent use and MUST NOT block the caller: the model loop calls it
// directly, so a blocking emitter would stall the agent (design §3.3).
type Emitter interface{ Emit(Chunk) }

// EmitFunc adapts an ordinary function to Emitter.
type EmitFunc func(Chunk)

func (f EmitFunc) Emit(c Chunk) {
	if f != nil {
		f(c)
	}
}

// ChunkDemander is an optional Emitter extension: an observer that can report
// whether it currently wants chunks for a task.
//
// The runtime asks before it takes the streaming path, because observation is
// only free when nobody is watching: an observer that says no leaves the run on
// exactly the Complete path it would have taken with no emitter at all. The
// HTTP hub uses this to make `?include=text` opt-in and zero-overhead, and to
// keep the journal byte-identical for clients that never asked for text.
type ChunkDemander interface {
	WantsChunks(taskID string) bool
}

// ErrNotSupported reports that a ModelAdapter cannot stream. The runtime treats
// it as an ordinary capability gap: it falls back to Complete silently, with no
// degradation event, because nothing went wrong.
var ErrNotSupported = errors.New("harness: model adapter does not support streaming")

// asyncEmitter makes Emit non-blocking: when the buffer is full the chunk is
// dropped and counted. Blocking the agent loop on a slow consumer is not
// acceptable; the authoritative text still arrives with MODEL_PROPOSED, so what
// is lost is the "process animation" — and the dropped count makes even that
// loss observable (design §3.3).
type asyncEmitter struct {
	out     chan Chunk
	dropped atomic.Int64
}

func newAsyncEmitter(out chan Chunk) *asyncEmitter { return &asyncEmitter{out: out} }

func (e *asyncEmitter) Emit(chunk Chunk) {
	if e == nil || e.out == nil {
		return
	}
	select {
	case e.out <- chunk:
	default:
		e.dropped.Add(1)
	}
}

// Dropped reports how many chunks this emitter had to discard.
func (e *asyncEmitter) Dropped() int64 {
	if e == nil {
		return 0
	}
	return e.dropped.Load()
}

// WithEmitter injects a runtime-level observation channel. The default is nil
// (no emitter, no streaming, exactly today's behaviour).
//
// The emitter is shared by every run of the runtime, so it must be
// concurrency-safe and must route by Chunk.TaskID: task-scoped channels must
// never be stored on the Runtime itself.
func WithEmitter(emitter Emitter) Option {
	return func(c *runtimeConfig) error {
		if emitter == nil {
			return errors.New("harness: WithEmitter requires a non-nil emitter")
		}
		c.emitter = emitter
		return nil
	}
}

// SetEmitter installs (or clears, with nil) the runtime-level observation
// channel. It is the imperative counterpart of WithEmitter, for embedders that
// build a runtime before they know where observations should go.
func (r *Runtime) SetEmitter(emitter Emitter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.emitter = emitter
}

// runEmitterKey carries the per-run emitter. RunStream puts it on the context
// so the kernel can reach it without the Runtime holding task-scoped state.
type runEmitterKey struct{}

func withRunEmitter(ctx context.Context, emitter Emitter) context.Context {
	return context.WithValue(ctx, runEmitterKey{}, emitter)
}

func runEmitterFromContext(ctx context.Context) *asyncEmitter {
	emitter, _ := ctx.Value(runEmitterKey{}).(*asyncEmitter)
	return emitter
}

// streamDropped reports how many observation chunks this run's own channel had
// to discard. It is zero unless the run was started with RunStream.
func (r *Runtime) streamDropped(ctx context.Context) int64 {
	return runEmitterFromContext(ctx).Dropped()
}

// turnCompletedPayload attaches the run's dropped-frame count to a
// TURN_COMPLETED payload (design §3.3). The field only appears when something
// was actually dropped, so an unobserved run keeps the byte-identical journal
// it produced before streaming existed.
func (r *Runtime) turnCompletedPayload(ctx context.Context, payload map[string]any) map[string]any {
	dropped := r.streamDropped(ctx)
	if dropped == 0 {
		return payload
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["stream_dropped"] = dropped
	return payload
}

// streamObservers collects the observers that want chunks for this run: the
// per-run emitter created by RunStream and the runtime-level emitter, if it is
// interested in this task.
func (r *Runtime) streamObservers(ctx context.Context, taskID string) []Emitter {
	observers := make([]Emitter, 0, 2)
	if emitter := runEmitterFromContext(ctx); emitter != nil {
		observers = append(observers, emitter)
	}
	// r.emitter is read while the run holds r.mu, which is what serializes it
	// against SetEmitter.
	if emitter := r.emitter; emitter != nil {
		if demander, ok := emitter.(ChunkDemander); !ok || demander.WantsChunks(taskID) {
			observers = append(observers, emitter)
		}
	}
	return observers
}

func emitChunk(observers []Emitter, chunk Chunk) {
	for _, observer := range observers {
		if observer != nil {
			observer.Emit(chunk)
		}
	}
}

// completeModel is the model call the kernel uses. Without an interested
// observer it is exactly Complete.
//
// The assembled response is what gets written to MODEL_PROPOSED, so the
// failure matrix of design §3.2 is enforced here:
//
//	no emitter            -> Complete (zero behaviour change)
//	stream fails first    -> Complete + MODEL_STREAM_DEGRADED (auditable)
//	stream fails mid-way  -> the step fails with category MODEL, no fallback
//	ErrNotSupported       -> Complete, silently
func (r *Runtime) completeModel(ctx context.Context, state *TaskState, request ModelRequest) (ModelResponse, error) {
	observers := r.streamObservers(ctx, request.TaskID)
	if len(observers) == 0 {
		return r.model.Complete(ctx, request)
	}
	response, deltas, err := r.streamModel(ctx, request, observers)
	switch {
	case err == nil:
		return response, nil
	case errors.Is(err, ErrNotSupported):
		// The adapter simply cannot stream. Nothing failed.
		return r.model.Complete(ctx, request)
	case deltas == 0:
		// Nothing reached the observer, so regenerating is not a duplication:
		// fall back and leave an auditable trace of the degradation.
		r.recordStreamDegraded(state, err)
		return r.model.Complete(ctx, request)
	default:
		// The observer already saw output. Re-running Complete would show the
		// user a second, different answer, so the step fails instead.
		return ModelResponse{}, fmt.Errorf("model stream failed after %d chunks: %w", deltas, err)
	}
}

func (r *Runtime) recordStreamDegraded(state *TaskState, cause error) {
	if state == nil {
		return
	}
	_ = r.commitEvent(state, "MODEL_STREAM_DEGRADED", map[string]any{
		"turn_id": state.CurrentTurnID,
		"step":    state.Step + 1,
		"reason":  cause.Error(),
	})
}

// streamModel drives the adapter's stream, forwards ephemeral deltas to the
// observers and assembles the authoritative response. The returned count is the
// number of content deltas produced, which is what distinguishes "failed before
// anyone saw anything" from "failed mid-flight".
func (r *Runtime) streamModel(ctx context.Context, request ModelRequest, observers []Emitter) (ModelResponse, int, error) {
	events, err := r.model.Stream(ctx, request)
	if err != nil {
		return ModelResponse{}, 0, err
	}
	if events == nil {
		// A nil channel with a nil error is a malformed adapter, not a stream.
		return ModelResponse{}, 0, ErrNotSupported
	}
	assembler := newStreamAssembler()
	for event := range events {
		switch event.Type {
		case streamEventTextDelta:
			if delta := streamTextDelta(event.Data); delta != "" {
				emitChunk(observers, Chunk{Kind: ChunkText, TaskID: request.TaskID, Text: delta})
			}
		case streamEventToolCallDelta:
			emitChunk(observers, Chunk{Kind: ChunkToolCall, TaskID: request.TaskID, Data: event.Data})
		}
		if err := assembler.apply(event); err != nil {
			return ModelResponse{}, assembler.deltas(), err
		}
	}
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, assembler.deltas(), err
	}
	if !assembler.terminal {
		// A stream that stops without a terminal marker is truncated, and a
		// truncated response must never become the authoritative one.
		return ModelResponse{}, assembler.deltas(), errors.New("model stream ended without a terminal event")
	}
	response, err := assembler.finish()
	if err != nil {
		return ModelResponse{}, assembler.deltas(), err
	}
	return response, assembler.deltas(), nil
}

// The delta vocabulary a ModelAdapter's Stream produces. It is deliberately
// identical to what the provider adapters already emit, and it is the single
// place the runtime parses it: the OpenAI-compatible adapter's provider-side
// extraction and this assembler share one contract instead of two copies of the
// concatenation logic.
const (
	streamEventTextDelta     = "text_delta"
	streamEventToolCallDelta = "tool_call_delta"
	streamEventUsage         = "usage"
	streamEventFinishReason  = "finish_reason"
	streamEventDone          = "done"
	streamEventRaw           = "raw"
	streamEventError         = "error"
)

// streamAssembler rebuilds a ModelResponse from deltas. It is the component
// that makes "streaming does not change the journal" checkable.
type streamAssembler struct {
	text         strings.Builder
	calls        []*assembledToolCall
	callOrder    []int
	usage        ModelUsage
	raw          map[string]any
	finishReason string
	terminal     bool
	deltaCount   int
}

type assembledToolCall struct {
	id        string
	name      string
	arguments strings.Builder
}

func newStreamAssembler() *streamAssembler { return &streamAssembler{} }

// deltas counts content increments (text and tool calls). Usage and terminal
// markers are bookkeeping, not output the user could have seen.
func (a *streamAssembler) deltas() int { return a.deltaCount }

func (a *streamAssembler) apply(event StreamEvent) error {
	switch event.Type {
	case streamEventTextDelta, "text":
		if delta := streamTextDelta(event.Data); delta != "" {
			a.text.WriteString(delta)
			a.deltaCount++
		}
	case streamEventToolCallDelta, "tool_calls":
		if a.applyToolCalls(event.Data["tool_calls"]) {
			a.deltaCount++
		}
	case streamEventUsage:
		a.applyUsage(event.Data["usage"])
	case streamEventFinishReason:
		if reason := stringValue(event.Data["finish_reason"]); reason != "" {
			a.finishReason = reason
		}
		a.terminal = true
	case streamEventDone:
		if reason := stringValue(event.Data["finish_reason"]); reason != "" {
			a.finishReason = reason
		}
		if usage, ok := event.Data["usage"]; ok {
			a.applyUsage(usage)
		}
		a.terminal = true
	case streamEventRaw:
		if raw, ok := event.Data["raw"].(map[string]any); ok {
			a.raw = raw
		}
	case streamEventError:
		return errors.New("model stream error: " + streamErrorText(event.Data))
	default:
		// Unknown event types are ignored so a provider can add fields without
		// breaking assembly; they never create model output.
	}
	return nil
}

func (a *streamAssembler) applyToolCalls(value any) bool {
	switch calls := value.(type) {
	case []any:
		return a.applyToolCallList(calls)
	case []map[string]any:
		converted := make([]any, 0, len(calls))
		for _, call := range calls {
			converted = append(converted, call)
		}
		return a.applyToolCallList(converted)
	case []ToolCall:
		for index, call := range calls {
			a.applyToolCall(index, call.CallID, call.Name, marshalArguments(call.Arguments))
		}
		return len(calls) > 0
	}
	return false
}

func (a *streamAssembler) applyToolCallList(calls []any) bool {
	applied := false
	for position, raw := range calls {
		switch call := raw.(type) {
		case map[string]any:
			index := position
			if value, ok := call["index"]; ok {
				index = intNumber(value)
			}
			function, _ := call["function"].(map[string]any)
			arguments := ""
			if text, ok := function["arguments"].(string); ok {
				arguments = text
			} else if text, ok := call["arguments"].(string); ok {
				arguments = text
			}
			a.applyToolCall(index, stringValue(call["id"]), stringValue(function["name"]), arguments)
			applied = true
		case ToolCall:
			a.applyToolCall(position, call.CallID, call.Name, marshalArguments(call.Arguments))
			applied = true
		}
	}
	return applied
}

// applyToolCall merges one delta into the call at index. Provider streams send
// the id and name once and then the JSON arguments in fragments, so everything
// is accumulated and parsed only at finish().
func (a *streamAssembler) applyToolCall(index int, id, name, arguments string) {
	if index < 0 {
		index = 0
	}
	for len(a.calls) <= index {
		a.calls = append(a.calls, nil)
	}
	if a.calls[index] == nil {
		a.calls[index] = &assembledToolCall{}
		a.callOrder = append(a.callOrder, index)
	}
	entry := a.calls[index]
	if id != "" {
		entry.id = id
	}
	if name != "" {
		entry.name = name
	}
	entry.arguments.WriteString(arguments)
}

func (a *streamAssembler) applyUsage(value any) {
	switch usage := value.(type) {
	case ModelUsage:
		a.usage = usage
	case map[string]any:
		a.usage.InputTokens = firstInt(usage, "input_tokens", "prompt_tokens")
		a.usage.OutputTokens = firstInt(usage, "output_tokens", "completion_tokens")
		a.usage.CachedInputTokens = firstInt(usage, "cached_input_tokens", "cached_tokens")
		a.usage.LatencyMS = floatNumber(usage["latency_ms"])
		a.usage.CostUSD = floatNumber(usage["cost_usd"])
	}
}

// finish materializes the assembled response. It is the only place arguments
// fragments are parsed, which mirrors Complete (one parse of the full JSON).
func (a *streamAssembler) finish() (ModelResponse, error) {
	response := ModelResponse{
		Text:         a.text.String(),
		FinishReason: a.finishReason,
		Usage:        a.usage,
		Raw:          a.raw,
	}
	for _, index := range a.callOrder {
		call := a.calls[index]
		arguments := map[string]any{}
		if raw := strings.TrimSpace(call.arguments.String()); raw != "" {
			if err := json.Unmarshal([]byte(raw), &arguments); err != nil {
				return ModelResponse{}, fmt.Errorf("model stream produced malformed tool arguments: %w", err)
			}
		}
		response.ToolCalls = append(response.ToolCalls, ToolCall{CallID: call.id, Name: call.name, Arguments: arguments})
	}
	// A terminal response that claims tool calls must carry them. When it does
	// not, the delta channel dropped them (an adapter whose Stream is not
	// faithful), and committing this response would turn a tool-call step into
	// "empty model response". Reporting an error instead lets completeModel
	// repair it: with no delta observed yet, the step degrades to Complete and
	// the loss is journaled rather than swallowed.
	if strings.EqualFold(response.FinishReason, "tool_calls") && len(response.ToolCalls) == 0 {
		return ModelResponse{}, errors.New("model stream reported finish_reason=tool_calls without any tool call delta")
	}
	return response, nil
}

func streamTextDelta(data map[string]any) string {
	if delta := stringValue(data["delta"]); delta != "" {
		return delta
	}
	if text := stringValue(data["text"]); text != "" {
		return text
	}
	// Some adapters carry the fragment under the provider's own key.
	if content := stringValue(data["content"]); content != "" {
		return content
	}
	return ""
}

func streamErrorText(data map[string]any) string {
	for _, key := range []string{"error", "message", "error_description"} {
		if text := stringValue(data[key]); text != "" {
			return text
		}
	}
	if err, ok := data["error"].(error); ok {
		return err.Error()
	}
	return "unknown stream error"
}

func marshalArguments(arguments map[string]any) string {
	if len(arguments) == 0 {
		return ""
	}
	raw, err := json.Marshal(arguments)
	if err != nil {
		return ""
	}
	return string(raw)
}

func firstInt(data map[string]any, keys ...string) int {
	for _, key := range keys {
		if value, ok := data[key]; ok {
			return intNumber(value)
		}
	}
	return 0
}

// --- RunStream --------------------------------------------------------------

const (
	// defaultStreamBuffer is the per-run observation buffer. It is deliberately
	// small: the buffer exists to absorb a burst, not to make RunStream a queue.
	defaultStreamBuffer = 64
	// streamPollInterval is how often RunStream catches up with the journal.
	// Events travel through the same emitter as deltas; the journal remains the
	// authoritative source, so a consumer that falls behind can recover with
	// EventsSince instead of relying on this channel.
	streamPollInterval = 25 * time.Millisecond
	// streamDoneGrace bounds how long the final ChunkDone may wait for a
	// consumer that stopped reading. A stuck consumer must not pin the closing
	// goroutine forever, and it must never affect the run itself.
	streamDoneGrace = 2 * time.Second
)

// RunStream is the embedder-facing entry point: it runs the task and returns a
// buffered channel of observations that is closed when the run ends. Cancelling
// ctx terminates the run and closes the channel.
//
// The chunks are observations only. The authoritative trajectory is still the
// journal, which is why a consumer that misses chunks can recover with
// EventsSince and why a slow consumer can never stall the agent.
func (r *Runtime) RunStream(ctx context.Context, request TaskRequest, auth AuthContext) (<-chan Chunk, error) {
	request = request.WithDefaults()
	if err := validateIdentity(request, auth); err != nil {
		return nil, err
	}
	buffer := r.streamBuffer
	if buffer <= 0 {
		buffer = defaultStreamBuffer
	}
	out := make(chan Chunk, buffer)
	emitter := newAsyncEmitter(out)
	runCtx := withRunEmitter(ctx, emitter)
	go r.driveStream(runCtx, request, auth, out, emitter)
	return out, nil
}

func (r *Runtime) driveStream(ctx context.Context, request TaskRequest, auth AuthContext, out chan Chunk, emitter *asyncEmitter) {
	defer close(out)

	follower := &journalFollower{store: r.store, emitter: emitter, taskID: request.TaskID}
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		follower.follow(ctx, stop)
	}()

	result, err := r.Run(ctx, request, auth)

	close(stop)
	<-stopped
	// The run is over, so one last pass guarantees the final events (including
	// the terminal ones) are offered to the consumer.
	follower.poll()

	done := Chunk{
		Kind:   ChunkDone,
		TaskID: request.TaskID,
		Data: map[string]any{
			"status":           result.Status,
			"failure_category": result.FailureCategory,
			// The run's total loss. It can exceed the stream_dropped committed
			// with the last TURN_COMPLETED, because chunks emitted after that
			// commit can still be dropped, but it never under-reports it.
			"stream_dropped": emitter.Dropped(),
		},
	}
	if err != nil {
		done.Data["error"] = err.Error()
	}
	select {
	case out <- done:
	case <-ctx.Done():
	case <-time.After(streamDoneGrace):
		// A consumer that never reads and never cancels loses the terminal
		// chunk; closing the channel still ends a well-behaved range loop.
	}
}

// journalFollower forwards committed events to the observation channel. It runs
// in its own goroutine so a slow consumer only delays observations, never the
// agent loop.
type journalFollower struct {
	store   CoreStore
	emitter *asyncEmitter
	taskID  string
	cursor  int
}

func (f *journalFollower) follow(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(streamPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !f.poll() {
				return
			}
		}
	}
}

func (f *journalFollower) poll() bool {
	events, err := f.store.EventsSince(f.taskID, f.cursor)
	if err != nil {
		return false
	}
	for _, event := range events {
		f.cursor = event.Sequence
		committed := event
		f.emitter.Emit(Chunk{
			Kind:     ChunkEvent,
			TaskID:   event.TaskID,
			Sequence: event.Sequence,
			Event:    &committed,
		})
	}
	return true
}
