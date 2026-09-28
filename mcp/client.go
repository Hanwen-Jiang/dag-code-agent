// Package mcp implements a real Model Context Protocol (MCP) client.
//
// It lives outside the embeddable core on purpose: the stdio transport
// executes external processes, and every embedder of `harness` must not be
// forced to accept a library that spawns children (design 6.1). The core
// defines only the harness.ToolSource seam; this package satisfies it, so MCP
// tools flow through the existing schema/scope/timeout/retry/idempotency/audit
// pipeline unchanged.
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// The client satisfies the core's external tool seam.
var _ harness.ToolSource = (*Client)(nil)

const (
	clientName    = "dag-code-agent"
	clientVersion = "0.1.0"
)

// Defaults. Timeouts are configurable through ServerConfig; the handshake has
// its own budget because a slow start-up must not consume the per-call budget
// (design 6.3).
const (
	defaultCallTimeout      = 30 * time.Second
	defaultHandshakeTimeout = 10 * time.Second
	// cancelNotifyTimeout bounds the notifications/cancelled write so a
	// cancellation returns promptly even when the transport is wedged.
	cancelNotifyTimeout = 2 * time.Second
	// closeWaitTimeout bounds how long Close waits for the reader goroutine
	// after the transport has been torn down.
	closeWaitTimeout = 5 * time.Second
	// receiveBuffer decouples the transport reader from the JSON-RPC
	// dispatcher.
	receiveBuffer = 64
	// maxListPages caps tools/list pagination so a server that keeps
	// returning a cursor cannot loop forever.
	maxListPages = 100
)

// Sentinel errors. Callers that only inspect harness.ToolResult should use the
// MCP_* error codes instead.
var (
	ErrClosed                     = errors.New("mcp: client is closed")
	ErrNotInitialized             = errors.New("mcp: client is not initialized")
	ErrUnsupportedProtocolVersion = errors.New("mcp: unsupported protocol version")
	ErrUnknownTool                = errors.New("mcp: unknown tool")
)

// Logger is the minimal logging seam (for example *log.Logger). Capability
// gaps such as unimplemented server-initiated requests are reported here
// rather than silently swallowed.
type Logger interface {
	Printf(format string, args ...any)
}

// Progress is an MCP notifications/progress payload (design 6.3). Section 3's
// streaming Chunk type does not exist in this revision of the core, so the
// client exposes the notification through ServerConfig.OnProgress; wiring it
// into the streaming channel is a one-line adapter once stream.go lands.
type Progress struct {
	ProgressToken any     `json:"progressToken,omitempty"`
	Progress      float64 `json:"progress"`
	Total         float64 `json:"total,omitempty"`
	Message       string  `json:"message,omitempty"`
}

// ServerInfo is the outcome of the initialize handshake.
type ServerInfo struct {
	Name            string         `json:"name,omitempty"`
	Version         string         `json:"version,omitempty"`
	ProtocolVersion string         `json:"protocol_version"`
	Capabilities    map[string]any `json:"capabilities,omitempty"`
}

// Transport is a bidirectional JSON-RPC message channel. Send writes one
// message, Receive returns one message. Implementations must be safe for one
// concurrent sender set plus one reader.
type Transport interface {
	Send(ctx context.Context, payload []byte) error
	Receive(ctx context.Context) ([]byte, error)
	Close() error
}

// ServerConfig describes one MCP server. The Transport field is the only thing
// that knows how bytes reach the server: configuration (where commands and
// environment variables come from) is a host concern and stays outside both
// the core and this package (design 6.5).
type ServerConfig struct {
	Name string
	// Transport is required.
	Transport Transport
	// Timeout bounds a single request (default 30s). It is an upper bound
	// for a hung server; the core pipeline's per-tool timeout normally
	// fires first.
	Timeout time.Duration
	// HandshakeTimeout bounds the initialize exchange (default 10s).
	HandshakeTimeout time.Duration
	// Logger is optional.
	Logger Logger
	// OnProgress is optional and is invoked from a dedicated goroutine for
	// every notifications/progress message. It must not assume ordering.
	OnProgress func(Progress)
}

// RPCError is a JSON-RPC 2.0 error returned by the server.
type RPCError struct {
	Code    int64
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string {
	if len(bytes.TrimSpace(e.Data)) > 0 && string(bytes.TrimSpace(e.Data)) != "null" {
		return fmt.Sprintf("mcp: server error %d: %s (data: %s)", e.Code, e.Message, e.Data)
	}
	return fmt.Sprintf("mcp: server error %d: %s", e.Code, e.Message)
}

// Client speaks MCP to exactly one server. A single reader goroutine owns the
// inbound side and dispatches responses to the goroutine waiting on each
// JSON-RPC id; writes are serialized by a writer lock.
type Client struct {
	cfg       ServerConfig
	name      string
	transport Transport
	timeout   time.Duration
	handshake time.Duration

	initMu sync.Mutex

	writeMu sync.Mutex

	mu          sync.Mutex
	nextID      int64
	pending     map[string]chan *rpcMessage
	closed      bool
	closeCause  error
	closeReturn error
	readerErr   error
	initialized bool
	listing     bool
	info        ServerInfo
	tools       map[string]rawTool

	readerCtx    context.Context
	cancelReader context.CancelFunc
	readerDone   chan struct{}
	closeOnce    sync.Once
}

// Dial builds a client for cfg and starts its reader goroutine. It does not
// perform the handshake: call Initialize before Tools or Call (design 6.2).
// A transport that implements Start(context.Context) error is started here so
// configuration and security failures (for example an executable outside the
// allowlist) surface from Dial rather than from the reader goroutine.
func Dial(ctx context.Context, cfg ServerConfig) (*Client, error) {
	name := strings.TrimSpace(cfg.Name)
	if name == "" {
		return nil, errors.New("mcp: ServerConfig.Name is required")
	}
	if strings.Contains(name, "__") {
		return nil, fmt.Errorf("mcp: server name %q must not contain %q", name, "__")
	}
	if cfg.Transport == nil {
		return nil, errors.New("mcp: ServerConfig.Transport is required")
	}
	c := &Client{
		cfg:        cfg,
		name:       name,
		transport:  cfg.Transport,
		timeout:    cfg.Timeout,
		handshake:  cfg.HandshakeTimeout,
		pending:    map[string]chan *rpcMessage{},
		tools:      map[string]rawTool{},
		readerDone: make(chan struct{}),
	}
	if c.timeout <= 0 {
		c.timeout = defaultCallTimeout
	}
	if c.handshake <= 0 {
		c.handshake = defaultHandshakeTimeout
	}
	if starter, ok := cfg.Transport.(interface {
		Start(context.Context) error
	}); ok {
		if err := starter.Start(orBackground(ctx)); err != nil {
			_ = cfg.Transport.Close()
			return nil, err
		}
	}
	c.readerCtx, c.cancelReader = context.WithCancel(context.Background())
	go c.readLoop()
	return c, nil
}

// Name is the tool namespace this client registers under.
func (c *Client) Name() string { return c.name }

// ProtocolVersion returns the revision negotiated with the server.
func (c *Client) ProtocolVersion() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.info.ProtocolVersion != "" {
		return c.info.ProtocolVersion
	}
	return ProtocolVersion
}

// Info returns the negotiated server information.
func (c *Client) Info() ServerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.info
}

// Initialize performs the MCP handshake: initialize, version negotiation, then
// the notifications/initialized notification. It is idempotent.
func (c *Client) Initialize(ctx context.Context) (ServerInfo, error) {
	c.initMu.Lock()
	defer c.initMu.Unlock()

	c.mu.Lock()
	if c.initialized {
		info := c.info
		c.mu.Unlock()
		return info, nil
	}
	closed := c.closed
	cause := c.closeCauseLocked()
	c.mu.Unlock()
	if closed {
		return ServerInfo{}, cause
	}

	callCtx, cancel := context.WithTimeout(orBackground(ctx), c.handshake)
	defer cancel()

	params := map[string]any{
		"protocolVersion": ProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    clientName,
			"version": clientVersion,
		},
	}
	raw, err := c.call(callCtx, "initialize", params)
	if err != nil {
		return ServerInfo{}, err
	}
	var result struct {
		ProtocolVersion string         `json:"protocolVersion"`
		Capabilities    map[string]any `json:"capabilities"`
		ServerInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"serverInfo"`
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &result); err != nil {
			return ServerInfo{}, fmt.Errorf("mcp: decode initialize result: %w", err)
		}
	}
	version, err := negotiateProtocolVersion(result.ProtocolVersion)
	if err != nil {
		return ServerInfo{}, err
	}
	if err := c.notify(callCtx, "notifications/initialized", nil); err != nil {
		return ServerInfo{}, fmt.Errorf("mcp: send notifications/initialized: %w", err)
	}
	info := ServerInfo{
		Name:            result.ServerInfo.Name,
		Version:         result.ServerInfo.Version,
		ProtocolVersion: version,
		Capabilities:    result.Capabilities,
	}
	c.mu.Lock()
	c.info = info
	c.initialized = true
	c.mu.Unlock()
	return info, nil
}

// Tools lists the server's tools as harness.ToolSpecs (name, schema and
// annotation mapping). It must be called after a successful Initialize.
func (c *Client) Tools(ctx context.Context) ([]harness.ToolSpec, error) {
	if err := c.requireInitialized(); err != nil {
		return nil, err
	}
	callCtx, cancel := c.callContext(orBackground(ctx))
	defer cancel()

	collected := map[string]rawTool{}
	cursor := ""
	for page := 0; page < maxListPages; page++ {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.call(callCtx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var result struct {
			Tools      []rawTool `json:"tools"`
			NextCursor string    `json:"nextCursor"`
		}
		if len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, &result); err != nil {
				return nil, fmt.Errorf("mcp: decode tools/list result: %w", err)
			}
		}
		for _, tool := range result.Tools {
			name := strings.TrimSpace(tool.Name)
			if name == "" {
				c.logf("ignoring a tool without a name")
				continue
			}
			tool.Name = name
			// Keyed by the qualified name, which is what Call receives.
			collected[qualifiedToolName(c.name, name)] = tool
		}
		cursor = strings.TrimSpace(result.NextCursor)
		if cursor == "" {
			break
		}
	}

	names := make([]string, 0, len(collected))
	for name := range collected {
		names = append(names, name)
	}
	sort.Strings(names)
	specs := make([]harness.ToolSpec, 0, len(names))
	for _, name := range names {
		specs = append(specs, mapTool(c.name, collected[name]))
	}
	c.mu.Lock()
	c.tools = collected
	c.listing = true
	c.mu.Unlock()
	return specs, nil
}

// Call invokes a tool on the server. It satisfies harness.ToolSource, so the
// core's ToolRuntime can drive it; the auth argument has no MCP equivalent and
// is not forwarded (MCP carries no caller identity in this version).
//
// Failure mapping (design 6.3): a JSON-RPC error becomes FAILED with the
// server's numeric code in ErrorCode; a CallToolResult with isError=true
// becomes FAILED with ErrorCode MCP_TOOL_ERROR. A cancelled or timed-out call
// becomes UNKNOWN, because the server may already have applied its side
// effect.
func (c *Client) Call(ctx context.Context, call harness.ToolCall, _ harness.AuthContext) harness.ToolResult {
	started := time.Now()
	result := harness.ToolResult{
		CallID:   call.CallID,
		Name:     call.Name,
		Status:   harness.ToolFailed,
		Attempts: 1,
	}
	finish := func() harness.ToolResult {
		result.LatencyMS = float64(time.Since(started).Microseconds()) / 1000
		return result
	}

	if err := c.requireInitialized(); err != nil {
		result.ErrorCode = "MCP_NOT_INITIALIZED"
		if errors.Is(err, ErrClosed) {
			result.ErrorCode = "MCP_CLOSED"
		}
		result.ErrorMessage = err.Error()
		return finish()
	}
	tool, listed, ok := c.lookupTool(call.Name)
	if !ok {
		if !listed {
			result.ErrorCode = "MCP_TOOLS_NOT_LISTED"
			result.ErrorMessage = "mcp: call Tools(ctx) before Call so the client knows the server's tool names"
			return finish()
		}
		result.ErrorCode = "MCP_UNKNOWN_TOOL"
		result.ErrorMessage = fmt.Sprintf("tool %q is not published by server %s", call.Name, c.name)
		return finish()
	}

	callCtx, cancel := c.callContext(orBackground(ctx))
	defer cancel()

	arguments := call.Arguments
	if arguments == nil {
		arguments = map[string]any{}
	}
	raw, err := c.call(callCtx, "tools/call", map[string]any{"name": tool.Name, "arguments": arguments})
	if err != nil {
		switch {
		case errors.Is(callCtx.Err(), context.DeadlineExceeded):
			result.Status = harness.ToolUnknown
			result.ErrorCode = "MCP_TIMEOUT"
			result.ErrorMessage = "mcp: call timed out after " + c.timeout.String()
		case errors.Is(callCtx.Err(), context.Canceled):
			result.Status = harness.ToolUnknown
			result.ErrorCode = "MCP_CANCELLED"
			result.ErrorMessage = "mcp: call cancelled by the caller"
		case errors.Is(err, ErrClosed):
			result.ErrorCode = "MCP_CLOSED"
			result.ErrorMessage = err.Error()
		default:
			var rpcErr *RPCError
			if errors.As(err, &rpcErr) {
				result.ErrorCode = strconv.FormatInt(rpcErr.Code, 10)
				result.ErrorMessage = rpcErr.Error()
			} else {
				result.ErrorCode = "MCP_TRANSPORT_ERROR"
				result.ErrorMessage = err.Error()
			}
		}
		return finish()
	}

	var decoded map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			result.ErrorCode = "MCP_INVALID_RESULT"
			result.ErrorMessage = "mcp: decode tools/call result: " + err.Error()
			return finish()
		}
	}
	if isError, _ := decoded["isError"].(bool); isError {
		result.ErrorCode = "MCP_TOOL_ERROR"
		result.ErrorMessage = toolErrorMessage(decoded)
		return finish()
	}
	result.Status = harness.ToolSucceeded
	// The whole CallToolResult is preserved losslessly; flattening content
	// here would silently drop structuredContent.
	result.Value = decoded
	return finish()
}

// Close tears the client down: in-flight requests wake up with MCP_CLOSED and
// the transport is asked to reclaim the server (the stdio transport kills the
// whole process group).
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		c.cancelReader()
		transportErr := c.transport.Close()

		c.mu.Lock()
		c.closeErrLocked(transportErr)
		c.mu.Unlock()

		select {
		case <-c.readerDone:
		case <-time.After(closeWaitTimeout):
			c.logf("reader goroutine did not stop within %s", closeWaitTimeout)
		}
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeReturn
}

// ---------------------------------------------------------------------------
// JSON-RPC plumbing
// ---------------------------------------------------------------------------

// call sends one request and waits for its correlated response. Concurrency is
// explicit: every caller holds its own pending channel and the single reader
// dispatches by id. On context cancellation the client emits
// notifications/cancelled and returns at once.
func (c *Client) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	ctx = orBackground(ctx)

	c.mu.Lock()
	if c.closed {
		cause := c.closeCauseLocked()
		c.mu.Unlock()
		return nil, cause
	}
	c.nextID++
	id := c.nextID
	key := strconv.FormatInt(id, 10)
	wait := make(chan *rpcMessage, 1)
	c.pending[key] = wait
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
	}()

	payload, err := json.Marshal(outRequest{JSONRPC: jsonrpcVersion, ID: id, Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("mcp: encode %s request: %w", method, err)
	}
	if err := c.send(ctx, payload); err != nil {
		return nil, err
	}

	select {
	case msg := <-wait:
		if msg.Error != nil {
			return nil, &RPCError{Code: msg.Error.Code, Message: msg.Error.Message, Data: msg.Error.Data}
		}
		return msg.Result, nil
	case <-ctx.Done():
		c.notifyCancelled(id, ctx.Err())
		return nil, ctx.Err()
	case <-c.readerDone:
		return nil, c.closedCause()
	}
}

// notify sends a JSON-RPC notification (no id, no response).
func (c *Client) notify(ctx context.Context, method string, params any) error {
	payload, err := json.Marshal(outNotification{JSONRPC: jsonrpcVersion, Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("mcp: encode %s notification: %w", method, err)
	}
	return c.send(ctx, payload)
}

// notifyCancelled tells the server to abandon a request. It is bounded by
// cancelNotifyTimeout so cancellation never blocks on a stalled transport.
func (c *Client) notifyCancelled(id int64, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), cancelNotifyTimeout)
	defer cancel()
	params := map[string]any{"requestId": id, "reason": cause.Error()}
	if err := c.notify(ctx, "notifications/cancelled", params); err != nil {
		c.logf("notifications/cancelled for request %d was not delivered: %v", id, err)
	}
}

func (c *Client) send(ctx context.Context, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return c.closedCause()
	}
	if err := c.transport.Send(ctx, payload); err != nil {
		if errors.Is(err, ErrClosed) {
			return c.closedCause()
		}
		return err
	}
	return nil
}

func (c *Client) readLoop() {
	defer close(c.readerDone)
	for {
		data, err := c.transport.Receive(c.readerCtx)
		if err != nil {
			c.recordReaderError(err)
			return
		}
		frame := bytes.TrimSpace(data)
		if len(frame) == 0 {
			continue
		}
		var msg rpcMessage
		if err := json.Unmarshal(frame, &msg); err != nil {
			c.logf("dropping malformed JSON-RPC frame: %v", err)
			continue
		}
		c.dispatch(&msg)
	}
}

func (c *Client) dispatch(msg *rpcMessage) {
	hasID := len(bytes.TrimSpace(msg.ID)) > 0
	switch {
	case msg.Method != "" && hasID:
		// Server-initiated request. This version implements none of them, so
		// answer honestly instead of pretending (design 6.3). Run it off the
		// reader goroutine so a slow transport cannot stall dispatch.
		go c.replyMethodNotFound(msg)
	case msg.Method != "":
		c.handleNotification(msg)
	case hasID:
		key, ok := idKey(msg.ID)
		if !ok {
			c.logf("dropping response with unusable id %s", msg.ID)
			return
		}
		c.mu.Lock()
		wait := c.pending[key]
		c.mu.Unlock()
		if wait == nil {
			c.logf("dropping response for unknown request id %s", key)
			return
		}
		select {
		case wait <- msg:
		default:
			c.logf("dropping duplicate response for request id %s", key)
		}
	default:
		c.logf("dropping JSON-RPC message with neither method nor id")
	}
}

func (c *Client) handleNotification(msg *rpcMessage) {
	if msg.Method != "notifications/progress" {
		c.logf("ignoring notification %q", msg.Method)
		return
	}
	var progress Progress
	if len(bytes.TrimSpace(msg.Params)) > 0 {
		if err := json.Unmarshal(msg.Params, &progress); err != nil {
			c.logf("dropping malformed notifications/progress: %v", err)
			return
		}
	}
	onProgress := c.cfg.OnProgress
	if onProgress == nil {
		return
	}
	// Prefer dropping an observation over blocking the single reader.
	go onProgress(progress)
}

func (c *Client) replyMethodNotFound(msg *rpcMessage) {
	c.logf("server-initiated request %q is not implemented by this client; replying MethodNotFound", msg.Method)
	payload, err := json.Marshal(outError{
		JSONRPC: jsonrpcVersion,
		ID:      msg.ID,
		Error:   &rpcError{Code: codeMethodNotFound, Message: "method not found: " + msg.Method},
	})
	if err != nil {
		c.logf("cannot encode MethodNotFound reply for %q: %v", msg.Method, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelNotifyTimeout)
	defer cancel()
	if err := c.send(ctx, payload); err != nil {
		c.logf("MethodNotFound reply for %q was not delivered: %v", msg.Method, err)
	}
}

func (c *Client) recordReaderError(err error) {
	c.mu.Lock()
	if c.readerErr == nil {
		c.readerErr = err
	}
	c.mu.Unlock()
}

// closeErrLocked records the teardown result; transportErr non-nil wins because
// it says why the server could not be reclaimed. The cause always wraps
// ErrClosed so callers can recognise a dead client with errors.Is.
func (c *Client) closeErrLocked(transportErr error) {
	if c.closeCause == nil {
		if transportErr != nil {
			c.closeCause = fmt.Errorf("%w: %v", ErrClosed, transportErr)
		} else {
			c.closeCause = ErrClosed
		}
	}
	if transportErr != nil {
		c.closeReturn = transportErr
	}
}

func (c *Client) closeCauseLocked() error {
	if c.closeCause != nil {
		return c.closeCause
	}
	if c.readerErr != nil {
		return fmt.Errorf("%w: %v", ErrClosed, c.readerErr)
	}
	return ErrClosed
}

// closedCause reports why the client stopped working.
func (c *Client) closedCause() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCauseLocked()
}

func (c *Client) requireInitialized() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return c.closeCauseLocked()
	}
	if !c.initialized {
		return ErrNotInitialized
	}
	return nil
}

func (c *Client) lookupTool(name string) (rawTool, bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tool, ok := c.tools[name]
	return tool, c.listing, ok
}

func (c *Client) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(orBackground(ctx))
	}
	return context.WithTimeout(orBackground(ctx), c.timeout)
}

func (c *Client) logf(format string, args ...any) {
	if c.cfg.Logger == nil {
		return
	}
	c.cfg.Logger.Printf("mcp[%s] "+format, append([]any{c.name}, args...)...)
}

// negotiateProtocolVersion accepts a server revision this client supports,
// including one lower than the proposed version, and rejects anything else
// (design 6.3).
func negotiateProtocolVersion(server string) (string, error) {
	version := strings.TrimSpace(server)
	if version == "" {
		return "", fmt.Errorf("%w: server returned an empty protocolVersion", ErrUnsupportedProtocolVersion)
	}
	for _, supported := range supportedProtocolVersions {
		if version == supported {
			return version, nil
		}
	}
	return "", fmt.Errorf("%w: server offered %q, client supports %s",
		ErrUnsupportedProtocolVersion, version, strings.Join(supportedProtocolVersions, ", "))
}

// toolErrorMessage extracts a human message from a CallToolResult.
func toolErrorMessage(result map[string]any) string {
	var parts []string
	if content, ok := result["content"].([]any); ok {
		for _, item := range content {
			entry, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := entry["text"].(string); ok && strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
		}
	}
	if len(parts) == 0 {
		return "mcp: tool reported isError=true"
	}
	return strings.Join(parts, "; ")
}

func orBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
