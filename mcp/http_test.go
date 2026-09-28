package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// TestMCPHTTPTransport exercises the streamable HTTP transport (design 6.2)
// against a real HTTP server: the initialize/tools/list replies come back as
// JSON, the tools/call reply comes back as an SSE stream that first carries a
// progress notification, and the session header returned by the server is
// echoed on every later request.
func TestMCPHTTPTransport(t *testing.T) {
	const session = "session-abc"

	var (
		mu       sync.Mutex
		sessions []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read", http.StatusBadRequest)
			return
		}
		var msg rpcMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}

		mu.Lock()
		sessions = append(sessions, r.Header.Get(mcpSessionHeader))
		mu.Unlock()

		if msg.Method != "initialize" && r.Header.Get(mcpSessionHeader) != session {
			http.Error(w, "missing session header", http.StatusBadRequest)
			return
		}

		switch msg.Method {
		case "initialize":
			w.Header().Set(mcpSessionHeader, session)
			writeJSON(t, w, map[string]any{
				"jsonrpc": "2.0",
				"id":      json.RawMessage(msg.ID),
				"result": map[string]any{
					"protocolVersion": "2025-03-26", // lower than proposed: must be accepted
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "http-echo", "version": "9.9"},
				},
			})
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
		case "tools/list":
			writeJSON(t, w, map[string]any{
				"jsonrpc": "2.0",
				"id":      json.RawMessage(msg.ID),
				"result": map[string]any{
					"tools": []map[string]any{{
						"name":        "ping",
						"description": "Ping over HTTP.",
						"inputSchema": map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string"}}},
						"annotations": map[string]any{"readOnlyHint": true, "idempotentHint": true},
					}},
				},
			})
		case "tools/call":
			w.Header().Set("Content-Type", "text/event-stream")
			flusher, _ := w.(http.Flusher)
			progress, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"method":  "notifications/progress",
				"params":  map[string]any{"progressToken": "p1", "progress": 1, "total": 2},
			})
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", progress)
			if flusher != nil {
				flusher.Flush()
			}
			result, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      json.RawMessage(msg.ID),
				"result": map[string]any{
					"content":           []map[string]any{{"type": "text", "text": "pong"}},
					"structuredContent": map[string]any{"pong": true},
				},
			})
			fmt.Fprintf(w, "data: %s\n\n", result)
			if flusher != nil {
				flusher.Flush()
			}
		default:
			http.Error(w, "unknown method", http.StatusNotFound)
		}
	}))
	defer server.Close()

	progress := make(chan Progress, 8)
	client, err := Dial(context.Background(), ServerConfig{
		Name:       "http-echo",
		Transport:  &HTTPTransport{Endpoint: server.URL, Headers: map[string]string{"X-Test": "1"}},
		OnProgress: func(p Progress) { progress <- p },
		Logger:     &recordingLogger{},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	info, err := client.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if info.ProtocolVersion != "2025-03-26" {
		t.Fatalf("negotiated version = %q, want 2025-03-26", info.ProtocolVersion)
	}
	if info.Name != "http-echo" {
		t.Fatalf("server name = %q", info.Name)
	}

	specs, err := client.Tools(ctx)
	if err != nil {
		t.Fatalf("Tools: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "http-echo__ping" {
		t.Fatalf("tools = %+v", specs)
	}
	if specs[0].SideEffect != sideEffectNone || specs[0].IdempotencyRequired {
		t.Fatalf("annotation mapping = %q/%v", specs[0].SideEffect, specs[0].IdempotencyRequired)
	}

	result := client.Call(ctx, harness.ToolCall{CallID: "http-1", Name: "http-echo__ping", Arguments: map[string]any{"value": "hi"}}, harness.AuthContext{})
	if !result.OK() {
		t.Fatalf("call over HTTP = %+v", result)
	}
	if got := structured(t, result)["pong"]; got != true {
		t.Fatalf("structuredContent = %v", got)
	}

	select {
	case p := <-progress:
		if p.Progress != 1 {
			t.Fatalf("progress = %+v", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no progress notification delivered from the SSE stream")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sessions) < 4 {
		t.Fatalf("expected at least 4 requests, saw %d (%v)", len(sessions), sessions)
	}
	if sessions[0] != "" {
		t.Fatalf("initialize must not carry a session header, got %q", sessions[0])
	}
	for i, got := range sessions[1:] {
		if got != session {
			t.Fatalf("request %d carried session %q, want %q", i+2, got, session)
		}
	}
}

// TestMCPHTTPTransportErrors checks that transport-level HTTP failures are
// surfaced instead of being mistaken for tool results.
func TestMCPHTTPTransportErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprintln(w, `{"error":"upstream is down"}`)
	}))
	defer server.Close()

	client, err := Dial(context.Background(), ServerConfig{
		Name:      "http-echo",
		Transport: &HTTPTransport{Endpoint: server.URL},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	_, err = client.Initialize(context.Background())
	if err == nil {
		t.Fatal("expected Initialize to fail")
	}
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("error = %v, want the upstream status", err)
	}
}

// TestMCPHTTPTransportCloseUnblocksReceive proves Close stops a blocked
// reader rather than leaking it.
func TestMCPHTTPTransportCloseUnblocksReceive(t *testing.T) {
	transport := &HTTPTransport{Endpoint: "http://127.0.0.1:1"}
	transport.init()
	done := make(chan error, 1)
	go func() {
		_, err := transport.Receive(context.Background())
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := transport.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Receive returned no error after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receive stayed blocked after Close")
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Errorf("writing response: %v", err)
	}
}
