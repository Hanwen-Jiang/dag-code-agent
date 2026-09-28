package mcp

import (
	"encoding/json"
	"strconv"
	"strings"
)

// ProtocolVersion is the MCP revision this client proposes during
// initialization (design 6.3).
const ProtocolVersion = "2025-06-18"

// supportedProtocolVersions are the revisions this client accepts from a
// server. A server may answer with a lower revision than the one proposed;
// the client accepts it instead of erroring (MCP spec, version negotiation).
var supportedProtocolVersions = []string{"2024-11-05", "2025-03-26", "2025-06-18"}

const jsonrpcVersion = "2.0"

// JSON-RPC 2.0 error codes used by the client itself.
const (
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// rpcError is a JSON-RPC 2.0 error object.
type rpcError struct {
	Code    int64           `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// rpcMessage is any inbound JSON-RPC message: a response, a notification or a
// server-initiated request. The three are distinguished by Method/ID presence.
type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// outRequest is an outbound request. Client-generated ids are always integers.
type outRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// outNotification is an outbound notification (no id, no reply expected).
type outNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// outResult replies to a server-initiated request.
type outResult struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result"`
}

// outError replies to a server-initiated request with an error.
type outError struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Error   *rpcError       `json:"error"`
}

// idKey normalizes a JSON-RPC id so responses can be correlated with the
// pending request that produced them. Numbers and strings are both allowed by
// the spec; ids this client generates are always integers.
func idKey(raw json.RawMessage) (string, bool) {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return "", false
	}
	if text[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return "", false
		}
		return value, true
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err != nil {
		return "", false
	}
	if integer, err := strconv.ParseInt(number.String(), 10, 64); err == nil {
		return strconv.FormatInt(integer, 10), true
	}
	return number.String(), true
}
