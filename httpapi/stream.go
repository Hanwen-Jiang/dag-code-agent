package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// streamHub fans ephemeral observation chunks out to the SSE clients watching a
// task. It is the only cross-goroutine channel in the HTTP layer: the run
// executes in the session goroutine, the SSE write loop in the request
// goroutine, and every publish has to be safe (design §3.4).
//
// Delivery is deliberately lossy. A slow client must never stall a run, so each
// subscriber owns a buffered channel and a full buffer DROPS the chunk and
// counts it. What is lost is only ever "process animation": authoritative
// frames still come from the journal poll, so a reconnect or a dropped token
// never loses an event.
type streamHub struct {
	mu   sync.RWMutex
	subs map[string]map[*streamSubscriber]struct{}
}

// defaultSubscriberBuffer is the per-client token buffer.
const defaultSubscriberBuffer = 256

type streamSubscriber struct {
	ch      chan harness.Chunk
	dropped atomic.Int64
}

// streamSubscription is the handler-side view of a subscription: the channel to
// read plus the counters that make loss observable.
type streamSubscription struct {
	chunks <-chan harness.Chunk
	sub    *streamSubscriber
	cancel func()
}

// Dropped reports how many frames this subscriber missed so far.
func (s *streamSubscription) Dropped() int64 {
	if s == nil || s.sub == nil {
		return 0
	}
	return s.sub.dropped.Load()
}

// Close removes the subscription. It is idempotent and safe to call while
// Publish is running: removal happens under the write lock, so no send can race
// the channel close.
func (s *streamSubscription) Close() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
}

func newStreamHub() *streamHub {
	return &streamHub{subs: map[string]map[*streamSubscriber]struct{}{}}
}

// Publish delivers one chunk to every subscriber of the task. It never blocks:
// a subscriber whose buffer is full loses the chunk and counts it.
func (h *streamHub) Publish(taskID string, chunk harness.Chunk) {
	if h == nil || taskID == "" {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for subscriber := range h.subs[taskID] {
		select {
		case subscriber.ch <- chunk:
		default:
			subscriber.dropped.Add(1)
		}
	}
}

// Subscribe registers a subscriber for a task. The caller must call the
// returned cancel function when it stops reading.
func (h *streamHub) Subscribe(taskID string, buffer int) (<-chan harness.Chunk, func()) {
	subscription := h.subscribe(taskID, buffer)
	return subscription.chunks, subscription.Close
}

func (h *streamHub) subscribe(taskID string, buffer int) *streamSubscription {
	if buffer <= 0 {
		buffer = defaultSubscriberBuffer
	}
	subscriber := &streamSubscriber{ch: make(chan harness.Chunk, buffer)}
	h.mu.Lock()
	if h.subs == nil {
		h.subs = map[string]map[*streamSubscriber]struct{}{}
	}
	if h.subs[taskID] == nil {
		h.subs[taskID] = map[*streamSubscriber]struct{}{}
	}
	h.subs[taskID][subscriber] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if subscribers := h.subs[taskID]; subscribers != nil {
				delete(subscribers, subscriber)
				if len(subscribers) == 0 {
					delete(h.subs, taskID)
				}
			}
			// The write lock guarantees no Publish is mid-send: a send holds the
			// read lock, and a later Publish no longer sees this subscriber.
			close(subscriber.ch)
			h.mu.Unlock()
		})
	}
	return &streamSubscription{chunks: subscriber.ch, sub: subscriber, cancel: cancel}
}

// Subscribers reports how many clients are watching a task.
func (h *streamHub) Subscribers(taskID string) int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[taskID])
}

// WantsChunks implements harness.ChunkDemander. It is what makes `?include=text`
// opt-in: with no subscriber the runtime stays on the plain Complete path, so a
// session that never asked for text pays nothing and its journal is byte
// identical to a run without streaming.
func (h *streamHub) WantsChunks(taskID string) bool {
	return h.Subscribers(taskID) > 0
}

// Emit makes the hub itself the observer installed on a session runtime.
//
// It MUST be the hub and not a harness.EmitFunc wrapper: the runtime asks an
// observer whether it wants chunks (harness.ChunkDemander), and a bare function
// type cannot answer, which would silently turn every session into a streaming
// one. The compile-time assertions below pin that.
func (h *streamHub) Emit(chunk harness.Chunk) {
	h.Publish(chunk.TaskID, chunk)
}

var (
	_ harness.Emitter       = (*streamHub)(nil)
	_ harness.ChunkDemander = (*streamHub)(nil)
)

// withStreamObservations decorates a session factory so every environment it
// builds carries the hub as its runtime-level observation channel.
func (s *Server) withStreamObservations(factory func(string, harness.TaskRequest, harness.AuthContext) (*harness.DemoEnvironment, error)) func(string, harness.TaskRequest, harness.AuthContext) (*harness.DemoEnvironment, error) {
	if factory == nil || s.stream == nil {
		return factory
	}
	return func(mode string, request harness.TaskRequest, auth harness.AuthContext) (*harness.DemoEnvironment, error) {
		env, err := factory(mode, request, auth)
		if err != nil || env == nil || env.Runtime == nil {
			return env, err
		}
		env.Runtime.SetEmitter(s.stream)
		return env, nil
	}
}

// includeText reports whether the request opted into ephemeral token frames.
// It is off unless asked for (design §3.4), so the existing SSE contract is
// unchanged for every client that does not send it.
func includeText(r *http.Request) bool {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("include"))) {
	case "text", "1", "true":
		return true
	}
	return false
}

// writeStreamDroppedFrame tells a client how many token frames it missed. The
// loss is reported, never silent (design §3.3).
func writeStreamDroppedFrame(w http.ResponseWriter, flusher http.Flusher, count int64) {
	if count <= 0 {
		return
	}
	payload, err := json.Marshal(map[string]any{"count": count})
	if err != nil {
		return
	}
	if _, err := fmt.Fprintf(w, "event: stream_dropped\ndata: %s\n\n", payload); err != nil {
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
}
