package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// These tests cover design §3.4: `?include=text` is opt-in, token frames are
// ephemeral, and the authoritative event frames keep coming from the journal
// poll. The fixture below is a full-fidelity streaming adapter, so the
// assembled response the runtime records is the same one Complete would
// return; the run is held inside a gated tool so the test can subscribe before
// the streaming model step starts.

// --- fixture model ----------------------------------------------------------

type sseFixtureModel struct {
	mu            sync.Mutex
	responses     []harness.ModelResponse
	completeCalls int
	streamCalls   int
}

func (m *sseFixtureModel) Name() string { return "sse-fixture" }

func (m *sseFixtureModel) take() (harness.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.responses) == 0 {
		return harness.ModelResponse{}, errors.New("sse fixture exhausted")
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	if response.FinishReason == "" {
		response.FinishReason = "stop"
	}
	return response, nil
}

func (m *sseFixtureModel) counts() (complete, stream int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.completeCalls, m.streamCalls
}

func (m *sseFixtureModel) Complete(context.Context, harness.ModelRequest) (harness.ModelResponse, error) {
	m.mu.Lock()
	m.completeCalls++
	m.mu.Unlock()
	return m.take()
}

func (m *sseFixtureModel) Stream(ctx context.Context, _ harness.ModelRequest) (<-chan harness.StreamEvent, error) {
	m.mu.Lock()
	m.streamCalls++
	m.mu.Unlock()
	response, err := m.take()
	if err != nil {
		return nil, err
	}
	out := make(chan harness.StreamEvent, 8)
	go func() {
		defer close(out)
		emit := func(event harness.StreamEvent) bool {
			select {
			case out <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for _, fragment := range sseFragments(response.Text, 4) {
			if !emit(harness.StreamEvent{Type: "text_delta", Data: map[string]any{"delta": fragment}}) {
				return
			}
		}
		if !emit(harness.StreamEvent{Type: "usage", Data: map[string]any{"usage": map[string]any{
			"input_tokens":        response.Usage.InputTokens,
			"output_tokens":       response.Usage.OutputTokens,
			"cached_input_tokens": response.Usage.CachedInputTokens,
		}}}) {
			return
		}
		emit(harness.StreamEvent{Type: "done", Data: map[string]any{"finish_reason": response.FinishReason}})
	}()
	return out, nil
}

func sseFragments(text string, size int) []string {
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

// --- SSE parsing ------------------------------------------------------------

type sseFrame struct {
	id    string
	event string
	data  string
}

func parseSSEFrames(body string) []sseFrame {
	var frames []sseFrame
	current := sseFrame{}
	flush := func() {
		if current.event != "" || current.data != "" || current.id != "" {
			frames = append(frames, current)
		}
		current = sseFrame{}
	}
	for _, rawLine := range strings.Split(body, "\n") {
		line := strings.TrimRight(rawLine, "\r")
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "id: "):
			current.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			current.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			current.data = strings.TrimPrefix(line, "data: ")
		}
	}
	flush()
	return frames
}

func (f sseFrame) payload(t *testing.T) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(f.data), &payload); err != nil {
		t.Fatalf("frame %s data is not JSON: %q", f.event, f.data)
	}
	return payload
}

// --- scenario ---------------------------------------------------------------

const sseFinalAnswer = `{"status":"completed","artifact_id":"ART-HARNESS-DEMO","verified":true}`

var sseScenarioCounter atomic.Int64

type sseScenario struct {
	t           *testing.T
	store       harness.Store
	model       *sseFixtureModel
	release     chan struct{}
	toolEntered chan struct{}
	server      *httptest.Server
	taskID      string
	create      chan *http.Response
}

func newSSEScenario(t *testing.T) *sseScenario {
	t.Helper()
	store, err := harness.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	scenario := &sseScenario{
		t:           t,
		store:       store,
		release:     make(chan struct{}),
		toolEntered: make(chan struct{}),
		taskID:      fmt.Sprintf("sse-stream-%d", sseScenarioCounter.Add(1)),
	}
	scenario.model = &sseFixtureModel{responses: []harness.ModelResponse{
		{
			ToolCalls:    []harness.ToolCall{{CallID: "gate-1", Name: "gate_tool", Arguments: map[string]any{"value": "go"}}},
			FinishReason: "tool_calls",
			Usage:        harness.ModelUsage{InputTokens: 9, OutputTokens: 3},
		},
		{
			Text:         sseFinalAnswer,
			FinishReason: "stop",
			Usage:        harness.ModelUsage{InputTokens: 11, OutputTokens: 7, CachedInputTokens: 2},
		},
	}}

	var entered sync.Once
	factory := func(_ string, request harness.TaskRequest, auth harness.AuthContext) (*harness.DemoEnvironment, error) {
		env, err := harness.BuildDemoEnvironmentWithStore(store, request, auth)
		if err != nil {
			return nil, err
		}
		if err := env.Registry.Register(harness.ToolSpec{
			Name:        "gate_tool",
			Description: "blocks until the test releases it",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}},
			Handler: func(_ map[string]any, _ harness.AuthContext) (any, error) {
				entered.Do(func() { close(scenario.toolEntered) })
				select {
				case <-scenario.release:
					return map[string]any{"released": true}, nil
				case <-time.After(20 * time.Second):
					return nil, errors.New("gate timeout")
				}
			},
		}); err != nil {
			return nil, err
		}
		env.Runtime.SetModel(scenario.model)
		return env, nil
	}
	handler, err := New(Config{Store: store, SessionFactory: factory})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	scenario.server = httptest.NewServer(handler)
	t.Cleanup(scenario.server.Close)
	return scenario
}

// startRun issues the creating POST and waits until the run is parked inside
// the gated tool, which means every pre-streaming model step is already done.
func (s *sseScenario) startRun() {
	s.t.Helper()
	body, err := json.Marshal(map[string]any{"task_id": s.taskID, "goal": "stream a verified artifact"})
	if err != nil {
		s.t.Fatal(err)
	}
	s.create = make(chan *http.Response, 1)
	go func() {
		request, err := http.NewRequest(http.MethodPost, s.server.URL+"/api/harness/runs", bytes.NewReader(body))
		if err != nil {
			s.create <- nil
			return
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := s.server.Client().Do(request)
		if err != nil {
			s.create <- nil
			return
		}
		s.create <- response
	}()
	select {
	case <-s.toolEntered:
	case <-time.After(15 * time.Second):
		s.t.Fatal("the run never reached the gated tool")
	}
}

// openEvents subscribes with the SSE handler. It returns only after the handler
// has flushed its first frame, which happens after the hub subscription is in
// place: that is the handshake the token frames rely on.
func (s *sseScenario) openEvents(ctx context.Context, includeText bool) *http.Response {
	s.t.Helper()
	query := "/api/harness/runs/" + s.taskID + "/events?follow=1"
	if includeText {
		query += "&include=text"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.server.URL+query, nil)
	if err != nil {
		s.t.Fatal(err)
	}
	request.Header.Set("Accept", "text/event-stream")
	response, err := s.server.Client().Do(request)
	if err != nil {
		s.t.Fatalf("sse request: %v", err)
	}
	return response
}

func (s *sseScenario) waitCreate() map[string]any {
	s.t.Helper()
	select {
	case response := <-s.create:
		if response == nil {
			s.t.Fatal("create request failed")
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusAccepted {
			s.t.Fatalf("create status=%d body=%s", response.StatusCode, raw)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			s.t.Fatalf("create body: %v", err)
		}
		return payload
	case <-time.After(20 * time.Second):
		s.t.Fatal("create request did not finish")
	}
	return nil
}

// --- tests ------------------------------------------------------------------

func TestSSEIncludeTextYieldsTokenFrames(t *testing.T) {
	scenario := newSSEScenario(t)
	scenario.startRun()

	response := scenario.openEvents(context.Background(), true)
	body := make(chan string, 1)
	go func() {
		raw, _ := io.ReadAll(response.Body)
		response.Body.Close()
		body <- string(raw)
	}()

	close(scenario.release)
	scenario.waitCreate()

	var raw string
	select {
	case raw = <-body:
	case <-time.After(20 * time.Second):
		t.Fatal("SSE stream did not finish")
	}

	var tokens strings.Builder
	authoritative := map[string]int{}
	for _, frame := range parseSSEFrames(raw) {
		switch frame.event {
		case "token":
			payload := frame.payload(t)
			text, _ := payload["text"].(string)
			tokens.WriteString(text)
		case "stream_dropped":
			t.Fatalf("unexpected token loss for a fast client: %s", raw)
		default:
			authoritative[frame.event]++
			if frame.id == "" {
				t.Fatalf("authoritative frame %s has no journal id", frame.event)
			}
		}
	}

	// The ephemeral tokens must reconstruct exactly the authoritative text.
	if got := tokens.String(); got != sseFinalAnswer {
		t.Fatalf("token frames reassemble to %q, want %q", got, sseFinalAnswer)
	}
	for _, wanted := range []string{"MODEL_PROPOSED", "TASK_COMPLETED", "MODEL_REQUESTED"} {
		if authoritative[wanted] == 0 {
			t.Fatalf("authoritative event %s missing from the stream: %s", wanted, raw)
		}
	}
	if complete, stream := scenario.model.counts(); complete != 1 || stream != 1 {
		t.Fatalf("model calls: complete=%d stream=%d, want 1/1 (only the watched step streams)", complete, stream)
	}
}

func TestSSEDefaultHasNoTokenFrames(t *testing.T) {
	scenario := newSSEScenario(t)
	scenario.startRun()

	response := scenario.openEvents(context.Background(), false)
	body := make(chan string, 1)
	go func() {
		raw, _ := io.ReadAll(response.Body)
		response.Body.Close()
		body <- string(raw)
	}()

	close(scenario.release)
	created := scenario.waitCreate()
	if created["status"] != harness.TaskCompleted {
		t.Fatalf("create status=%v", created["status"])
	}

	var raw string
	select {
	case raw = <-body:
	case <-time.After(20 * time.Second):
		t.Fatal("SSE stream did not finish")
	}

	if strings.Contains(raw, "event: token") || strings.Contains(raw, "event: stream_dropped") {
		t.Fatalf("default SSE must not emit token frames: %s", raw)
	}
	if !strings.Contains(raw, "event: MODEL_PROPOSED") || !strings.Contains(raw, "event: TASK_COMPLETED") {
		t.Fatalf("default SSE lost its authoritative frames: %s", raw)
	}
	if complete, stream := scenario.model.counts(); stream != 0 {
		t.Fatalf("default SSE streamed anyway: complete=%d stream=%d", complete, stream)
	}
}

// TestStreamSubscriberCancelDoesNotBlockRun is the -race guard for a client
// that walks away mid-run: the hub must not stall or panic the session.
func TestStreamSubscriberCancelDoesNotBlockRun(t *testing.T) {
	scenario := newSSEScenario(t)
	scenario.startRun()

	ctx, cancel := context.WithCancel(context.Background())
	response := scenario.openEvents(ctx, true)
	cancel()
	response.Body.Close()

	close(scenario.release)
	created := scenario.waitCreate()
	if created["status"] != harness.TaskCompleted {
		t.Fatalf("run did not complete after the subscriber left: %v", created["status"])
	}
}

func TestStreamHubWantsChunksFollowsSubscribers(t *testing.T) {
	hub := newStreamHub()
	if hub.WantsChunks("task") {
		t.Fatal("hub wanted chunks with no subscriber: include=text would not be opt-in")
	}
	chunks, unsubscribe := hub.Subscribe("task", 2)
	if !hub.WantsChunks("task") {
		t.Fatal("hub did not want chunks with a subscriber")
	}
	hub.Publish("task", harness.Chunk{Kind: harness.ChunkText, TaskID: "task", Text: "hi"})
	select {
	case chunk := <-chunks:
		if chunk.Text != "hi" {
			t.Fatalf("chunk=%+v", chunk)
		}
	case <-time.After(time.Second):
		t.Fatal("published chunk never arrived")
	}
	unsubscribe()
	if hub.WantsChunks("task") {
		t.Fatal("hub kept wanting chunks after the subscriber left")
	}
	if _, open := <-chunks; open {
		t.Fatal("unsubscribe did not close the subscriber channel")
	}
}

func TestStreamHubDropsInsteadOfBlocking(t *testing.T) {
	hub := newStreamHub()
	subscription := hub.subscribe("task", 2)
	defer subscription.Close()

	// Publish must never block, however slow the consumer is.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			hub.Publish("task", harness.Chunk{Kind: harness.ChunkText, TaskID: "task", Text: "x"})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a slow subscriber")
	}
	if got := len(subscription.chunks); got != 2 {
		t.Fatalf("buffered chunks=%d, want 2", got)
	}
	if got := subscription.Dropped(); got != 98 {
		t.Fatalf("dropped=%d, want 98", got)
	}
	// Publishing after the subscriber left must be a no-op, never a send on a
	// closed channel.
	subscription.Close()
	for i := 0; i < 10; i++ {
		hub.Publish("task", harness.Chunk{Kind: harness.ChunkText, TaskID: "task", Text: "y"})
	}
}

func TestStreamDroppedFrameFormat(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeStreamDroppedFrame(recorder, recorder, 3)
	if got, want := recorder.Body.String(), "event: stream_dropped\ndata: {\"count\":3}\n\n"; got != want {
		t.Fatalf("frame=%q, want %q", got, want)
	}
	// A count of zero is not a frame.
	recorder = httptest.NewRecorder()
	writeStreamDroppedFrame(recorder, recorder, 0)
	if recorder.Body.Len() != 0 {
		t.Fatalf("zero loss emitted %q", recorder.Body.String())
	}
}

func TestIncludeTextIsOptIn(t *testing.T) {
	for _, testCase := range []struct {
		query string
		want  bool
	}{
		{"", false},
		{"?include=", false},
		{"?include=events", false},
		{"?follow=1", false},
		{"?include=text", true},
		{"?include=1", true},
		{"?include=true", true},
		{"?follow=1&include=text", true},
	} {
		request := httptest.NewRequest(http.MethodGet, "/events"+testCase.query, nil)
		if got := includeText(request); got != testCase.want {
			t.Fatalf("includeText(%q)=%v, want %v", testCase.query, got, testCase.want)
		}
	}
}
