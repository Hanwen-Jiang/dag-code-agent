// Command mcp_echo_server is a minimal, real MCP server speaking
// newline-delimited JSON-RPC 2.0 over stdio.
//
// It exists so that mcp.Client can be tested end to end against an actual
// subprocess rather than a mock, and it doubles as the smallest readable
// example of the server side of the protocol. It implements:
//
//	initialize                 (protocolVersion / capabilities / serverInfo)
//	notifications/initialized  (ignored)
//	tools/list                 (six tools with schemas and annotations)
//	tools/call                 (see tools())
//	notifications/cancelled    (recorded; in-flight sleeps abort)
//
// Environment:
//
//	MCP_ECHO_PROTOCOL_VERSION  force the revision reported by initialize, so
//	                           tests can exercise version negotiation.
//
// The "probe_server_request" tool sends a sampling/createMessage request to
// the client and reports the client's reply, which is how the client's
// MethodNotFound behaviour for server-initiated requests is covered.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultProtocolVersion = "2025-06-18"

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type server struct {
	out     *bufio.Writer
	writeMu sync.Mutex

	mu           sync.Mutex
	cancelled    map[string]bool
	running      map[string]chan struct{}
	replies      map[string]chan message
	nextServerID int

	protocolVersion string
}

func main() {
	s := &server{
		out:             bufio.NewWriter(os.Stdout),
		cancelled:       map[string]bool{},
		running:         map[string]chan struct{}{},
		replies:         map[string]chan message{},
		protocolVersion: strings.TrimSpace(os.Getenv("MCP_ECHO_PROTOCOL_VERSION")),
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadBytes('\n')
		if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
			s.handleLine(trimmed)
		}
		if err != nil {
			return
		}
	}
}

func (s *server) handleLine(line []byte) {
	var msg message
	if err := json.Unmarshal(line, &msg); err != nil {
		return
	}
	switch {
	case msg.Method == "" && len(msg.ID) > 0:
		s.mu.Lock()
		wait := s.replies[keyOf(msg.ID)]
		s.mu.Unlock()
		if wait != nil {
			select {
			case wait <- msg:
			default:
			}
		}
	case msg.Method != "" && len(msg.ID) > 0:
		// Requests run concurrently, so a long call never blocks the read
		// loop from seeing notifications/cancelled. Notifications are handled
		// inline below, which is what makes cancellation observable in order.
		go s.dispatch(msg.ID, msg.Method, msg.Params)
	case msg.Method != "":
		s.handleNotification(msg.Method, msg.Params)
	}
}

func (s *server) handleNotification(method string, params json.RawMessage) {
	switch method {
	case "notifications/initialized":
		return
	case "notifications/cancelled":
		var payload struct {
			RequestID json.RawMessage `json:"requestId"`
			Reason    string          `json:"reason"`
		}
		if err := json.Unmarshal(params, &payload); err != nil {
			return
		}
		key := keyOf(payload.RequestID)
		s.mu.Lock()
		if s.cancelled[key] {
			s.mu.Unlock()
			return
		}
		s.cancelled[key] = true
		stop := s.running[key]
		s.mu.Unlock()
		if stop != nil {
			close(stop)
		}
	case "notifications/progress":
		return
	}
}

func (s *server) dispatch(id json.RawMessage, method string, params json.RawMessage) {
	key := keyOf(id)
	stop := make(chan struct{})
	s.mu.Lock()
	s.running[key] = stop
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.running, key)
		s.mu.Unlock()
	}()

	switch method {
	case "initialize":
		s.writeResult(id, map[string]any{
			"protocolVersion": s.negotiatedVersion(params),
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "mcp-echo-server", "version": "0.1.0"},
		})
	case "tools/list":
		s.writeResult(id, map[string]any{"tools": tools()})
	case "tools/call":
		var call struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
			Meta      map[string]any `json:"_meta"`
		}
		if err := json.Unmarshal(params, &call); err != nil {
			s.writeError(id, -32602, "invalid tools/call params: "+err.Error())
			return
		}
		result, rpcErr := s.callTool(call.Name, call.Arguments, stop, key)
		if rpcErr != nil {
			s.writeError(id, rpcErr.Code, rpcErr.Message)
			return
		}
		s.writeResult(id, result)
	default:
		s.writeError(id, -32601, "method not found: "+method)
	}
}

func (s *server) negotiatedVersion(params json.RawMessage) string {
	if s.protocolVersion != "" {
		return s.protocolVersion
	}
	var init struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	_ = json.Unmarshal(params, &init)
	if init.ProtocolVersion != "" {
		return init.ProtocolVersion
	}
	return defaultProtocolVersion
}

func (s *server) callTool(name string, arguments map[string]any, stop chan struct{}, key string) (map[string]any, *rpcError) {
	switch name {
	case "echo":
		text, ok := arguments["text"].(string)
		if !ok {
			return nil, &rpcError{Code: -32602, Message: "echo requires a string argument \"text\""}
		}
		return textResult(text, map[string]any{"echo": text}), nil

	case "add":
		a, aok := number(arguments["a"])
		b, bok := number(arguments["b"])
		if !aok || !bok {
			return nil, &rpcError{Code: -32602, Message: "add requires numeric arguments \"a\" and \"b\""}
		}
		sum := a + b
		return textResult(fmt.Sprintf("%g", sum), map[string]any{"sum": sum}), nil

	case "sleep":
		ms, ok := number(arguments["ms"])
		if !ok || ms < 0 {
			return nil, &rpcError{Code: -32602, Message: "sleep requires a non-negative numeric argument \"ms\""}
		}
		deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return errorResult("cancelled by client"), nil
			case <-ticker.C:
			}
			if !time.Now().Before(deadline) {
				return textResult("slept", map[string]any{"slept_ms": ms}), nil
			}
			elapsed := ms - time.Until(deadline).Seconds()*1000
			s.notify("notifications/progress", map[string]any{
				"progressToken": key,
				"progress":      math.Round(elapsed*1000) / 1000,
				"total":         ms,
				"message":       "sleeping",
			})
		}

	case "cancellations":
		s.mu.Lock()
		ids := make([]string, 0, len(s.cancelled))
		for id := range s.cancelled {
			ids = append(ids, id)
		}
		s.mu.Unlock()
		sort.Strings(ids)
		return textResult(strings.Join(ids, ","), map[string]any{"cancelled": ids}), nil

	case "fail":
		text, _ := arguments["message"].(string)
		if text == "" {
			text = "tool failed on purpose"
		}
		return map[string]any{
			"isError": true,
			"content": []map[string]any{{"type": "text", "text": text}},
		}, nil

	case "probe_server_request":
		return s.probeServerRequest(), nil

	default:
		return nil, &rpcError{Code: -32602, Message: "unknown tool: " + name}
	}
}

// probeServerRequest sends a server-initiated sampling/createMessage request
// and reports what the client answered.
func (s *server) probeServerRequest() map[string]any {
	s.mu.Lock()
	s.nextServerID++
	id := fmt.Sprintf("srv-probe-%d", s.nextServerID)
	rawID, err := json.Marshal(id)
	if err != nil {
		s.mu.Unlock()
		return errorResult("cannot encode probe id")
	}
	wait := make(chan message, 1)
	s.replies[keyOf(rawID)] = wait
	s.mu.Unlock()

	s.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(rawID),
		"method":  "sampling/createMessage",
		"params":  map[string]any{"maxTokens": 16, "messages": []any{}},
	})

	report := map[string]any{"request_id": id}
	select {
	case reply := <-wait:
		if reply.Error != nil {
			report["error_code"] = reply.Error.Code
			report["error_message"] = reply.Error.Message
		} else {
			report["result"] = json.RawMessage(reply.Result)
		}
	case <-time.After(5 * time.Second):
		report["error_message"] = "client did not answer the probe"
	}
	return textResult("probe complete", report)
}

func (s *server) notify(method string, params any) {
	s.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (s *server) writeResult(id json.RawMessage, result any) {
	s.write(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (s *server) writeError(id json.RawMessage, code int, text string) {
	s.write(map[string]any{"jsonrpc": "2.0", "id": id, "error": &rpcError{Code: code, Message: text}})
}

func (s *server) write(payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _ = s.out.Write(append(encoded, '\n'))
	_ = s.out.Flush()
}

func keyOf(id json.RawMessage) string { return strings.TrimSpace(string(id)) }

func number(value any) (float64, bool) {
	raw, ok := value.(float64)
	if !ok {
		return 0, false
	}
	return raw, true
}

func textResult(text string, structured map[string]any) map[string]any {
	result := map[string]any{"content": []map[string]any{{"type": "text", "text": text}}}
	if structured != nil {
		result["structuredContent"] = structured
	}
	return result
}

func errorResult(text string) map[string]any {
	return map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": text}}}
}

func objectSchema(properties map[string]any, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func tools() []map[string]any {
	return []map[string]any{
		{
			"name":        "echo",
			"description": "Echo text back.",
			"inputSchema": objectSchema(map[string]any{"text": map[string]any{"type": "string"}}, "text"),
			"annotations": map[string]any{
				"readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false,
			},
		},
		{
			"name":        "add",
			"description": "Add two numbers.",
			"inputSchema": objectSchema(map[string]any{
				"a": map[string]any{"type": "number"},
				"b": map[string]any{"type": "number"},
			}, "a", "b"),
			"annotations": map[string]any{
				"readOnlyHint": false, "destructiveHint": true, "idempotentHint": false,
			},
		},
		{
			// Deliberately unannotated: exercises the conservative UNKNOWN mapping.
			"name":        "sleep",
			"description": "Sleep for the given number of milliseconds, emitting progress.",
			"inputSchema": objectSchema(map[string]any{"ms": map[string]any{"type": "number"}}, "ms"),
		},
		{
			"name":        "cancellations",
			"description": "Report the JSON-RPC ids cancelled by the client.",
			"inputSchema": objectSchema(map[string]any{}),
			"annotations": map[string]any{"readOnlyHint": true},
		},
		{
			"name":        "fail",
			"description": "Return an isError result.",
			"inputSchema": objectSchema(map[string]any{"message": map[string]any{"type": "string"}}),
			"annotations": map[string]any{"readOnlyHint": true, "idempotentHint": true},
		},
		{
			"name":        "probe_server_request",
			"description": "Ask the client a sampling/createMessage question and report its answer.",
			"inputSchema": objectSchema(map[string]any{}),
			"annotations": map[string]any{"readOnlyHint": true, "idempotentHint": true},
		},
	}
}
