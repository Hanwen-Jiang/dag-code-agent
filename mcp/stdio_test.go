package mcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var (
	echoBuildOnce sync.Once
	echoBinary    string
	echoBuildErr  error
)

// echoServerBinary builds testdata/mcp_echo_server once per test binary, so
// every test talks to a real subprocess instead of a mock.
func echoServerBinary(t *testing.T) string {
	t.Helper()
	echoBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "mcp-echo-server")
		if err != nil {
			echoBuildErr = err
			return
		}
		binary := filepath.Join(dir, "mcp_echo_server")
		cmd := exec.Command("go", "build", "-o", binary, "./testdata/mcp_echo_server")
		cmd.Dir = ".." // the package directory's parent is the module root
		if out, err := cmd.CombinedOutput(); err != nil {
			echoBuildErr = fmt.Errorf("building testdata/mcp_echo_server: %v\n%s", err, out)
			return
		}
		echoBinary = binary
	})
	if echoBuildErr != nil {
		t.Fatalf("echo server: %v", echoBuildErr)
	}
	return echoBinary
}

// recordingLogger keeps client logs out of test output while still letting a
// test assert on them.
type recordingLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *recordingLogger) Printf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.buf, format+"\n", args...)
}

func (l *recordingLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// dialEcho starts a real mcp_echo_server subprocess and completes the
// handshake. mutate can adjust the config before Dial; env entries are passed
// to the child explicitly (the transport whitelists the environment).
func dialEcho(t *testing.T, mutate func(*ServerConfig), env ...string) *Client {
	t.Helper()
	binary := echoServerBinary(t)
	cfg := ServerConfig{
		Name: "echo",
		Transport: &StdioTransport{
			Command:      binary,
			AllowedRoots: []string{filepath.Dir(binary)},
			Env:          env,
		},
		Logger: &recordingLogger{},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return client
}

// mustListTools performs tools/list, which is what teaches the client the
// server's tool names (Call refuses names it has not seen).
func mustListTools(t *testing.T, client *Client) []harness.ToolSpec {
	t.Helper()
	specs, err := client.Tools(context.Background())
	if err != nil {
		t.Fatalf("Tools: %v", err)
	}
	return specs
}

func callTool(t *testing.T, client *Client, name string, arguments map[string]any) harness.ToolResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return client.Call(ctx, harness.ToolCall{CallID: "call-1", Name: name, Arguments: arguments}, harness.AuthContext{TenantID: "t", SubjectID: "s"})
}

func structured(t *testing.T, result harness.ToolResult) map[string]any {
	t.Helper()
	value, ok := result.Value.(map[string]any)
	if !ok {
		t.Fatalf("result value is %T, want the decoded CallToolResult: %+v", result.Value, result)
	}
	sc, _ := value["structuredContent"].(map[string]any)
	return sc
}

// ---------------------------------------------------------------------------
// 6.7 acceptance tests
// ---------------------------------------------------------------------------

// TestMCPStdioEndToEnd drives initialize -> tools/list -> tools/call against a
// real MCP server process over stdio.
func TestMCPStdioEndToEnd(t *testing.T) {
	client := dialEcho(t, nil)

	info := client.Info()
	if info.Name != "mcp-echo-server" {
		t.Fatalf("serverInfo.Name = %q", info.Name)
	}
	if info.ProtocolVersion != ProtocolVersion {
		t.Fatalf("protocolVersion = %q, want %q", info.ProtocolVersion, ProtocolVersion)
	}
	if client.ProtocolVersion() != ProtocolVersion {
		t.Fatalf("client.ProtocolVersion() = %q", client.ProtocolVersion())
	}

	specs, err := client.Tools(context.Background())
	if err != nil {
		t.Fatalf("Tools: %v", err)
	}
	if len(specs) != 6 {
		t.Fatalf("got %d tools, want 6: %+v", len(specs), specs)
	}
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, spec.Name)
	}
	for _, want := range []string{"echo__echo", "echo__add", "echo__sleep", "echo__cancellations", "echo__fail", "echo__probe_server_request"} {
		if !contains(names, want) {
			t.Fatalf("tool %q missing from %v", want, names)
		}
	}

	result := callTool(t, client, "echo__echo", map[string]any{"text": "hello mcp"})
	if !result.OK() {
		t.Fatalf("echo failed: %+v", result)
	}
	if got := structured(t, result)["echo"]; got != "hello mcp" {
		t.Fatalf("echo structuredContent = %v", got)
	}
	if result.Attempts != 1 || result.LatencyMS < 0 {
		t.Fatalf("unexpected result bookkeeping: %+v", result)
	}

	sum := callTool(t, client, "echo__add", map[string]any{"a": 2, "b": 3})
	if !sum.OK() {
		t.Fatalf("add failed: %+v", sum)
	}
	if got := structured(t, sum)["sum"]; got != float64(5) {
		t.Fatalf("add structuredContent = %v", got)
	}
}

// TestMCPToolEntersRegistry proves an MCP tool lands in the normal
// ToolRegistry and is then subject to the existing pipeline: schema validation,
// the ExecutionPolicy authorizer, and the real MCP round trip behind a
// core-controlled wrapper handler.
func TestMCPToolEntersRegistry(t *testing.T) {
	client := dialEcho(t, nil)
	registry := harness.NewToolRegistry()

	registered, err := registry.RegisterSource(context.Background(), client)
	if err != nil {
		t.Fatalf("RegisterSource: %v", err)
	}
	if registered != 6 {
		t.Fatalf("registered %d tools, want 6", registered)
	}
	spec, ok := registry.Get("echo__echo")
	if !ok {
		t.Fatal("echo__echo is not in the registry")
	}
	if spec.Handler == nil {
		t.Fatal("registered MCP tool has no core-controlled handler")
	}
	if spec.SideEffect != sideEffectNone || spec.IdempotencyRequired {
		t.Fatalf("echo__echo mapping = %q/%v, want NONE/false", spec.SideEffect, spec.IdempotencyRequired)
	}
	if len(registry.Schemas([]string{"echo__echo"})) != 1 {
		t.Fatal("echo__echo has no model-facing schema")
	}

	// The authorizer sees the qualified name and can refuse it.
	policy := harness.ExecutionPolicy{AllowedTools: []string{"echo__echo"}}
	runtime := harness.NewToolRuntimeWithPolicy(registry, nil, policy)
	auth := harness.AuthContext{TenantID: "t", SubjectID: "s"}

	denied := runtime.Execute(context.Background(), harness.ToolCall{
		CallID: "denied", Name: "echo__add", Arguments: map[string]any{"a": 1, "b": 1},
	}, auth)
	if denied.ErrorCode != "POLICY_DENIED" {
		t.Fatalf("denied call = %+v, want POLICY_DENIED", denied)
	}

	invalid := runtime.Execute(context.Background(), harness.ToolCall{
		CallID: "invalid", Name: "echo__echo", Arguments: map[string]any{},
	}, auth)
	if invalid.ErrorCode != "INVALID_ARGUMENTS" {
		t.Fatalf("schema validation result = %+v, want INVALID_ARGUMENTS", invalid)
	}

	allowed := runtime.Execute(context.Background(), harness.ToolCall{
		CallID: "allowed", Name: "echo__echo", Arguments: map[string]any{"text": "through the pipeline"},
	}, auth)
	if !allowed.OK() {
		t.Fatalf("allowed call = %+v", allowed)
	}
	if got := structured(t, allowed)["echo"]; got != "through the pipeline" {
		t.Fatalf("pipeline echoed %v", got)
	}
}

// TestMCPVersionNegotiation accepts a lower revision the server reports.
func TestMCPVersionNegotiation(t *testing.T) {
	client := dialEcho(t, nil, "MCP_ECHO_PROTOCOL_VERSION=2024-11-05")

	if got := client.ProtocolVersion(); got != "2024-11-05" {
		t.Fatalf("negotiated version = %q, want 2024-11-05", got)
	}
	if got := client.Info().ProtocolVersion; got != "2024-11-05" {
		t.Fatalf("Info().ProtocolVersion = %q", got)
	}
	// The client still works against the older revision.
	if _, err := client.Tools(context.Background()); err != nil {
		t.Fatalf("Tools after downgrade: %v", err)
	}
}

// TestMCPUnsupportedVersionRejected is the conservative counterpart: a revision
// this client does not know is refused instead of assumed compatible.
func TestMCPUnsupportedVersionRejected(t *testing.T) {
	binary := echoServerBinary(t)
	cfg := ServerConfig{
		Name: "echo",
		Transport: &StdioTransport{
			Command:      binary,
			AllowedRoots: []string{filepath.Dir(binary)},
			Env:          []string{"MCP_ECHO_PROTOCOL_VERSION=1999-01-01"},
		},
	}
	client, err := Dial(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, err := client.Initialize(context.Background()); !errors.Is(err, ErrUnsupportedProtocolVersion) {
		t.Fatalf("Initialize error = %v, want ErrUnsupportedProtocolVersion", err)
	}
}

// TestMCPCancellation cancels an in-flight call and proves the client both
// returned promptly and told the server via notifications/cancelled.
func TestMCPCancellation(t *testing.T) {
	progress := make(chan Progress, 32)
	client := dialEcho(t, func(cfg *ServerConfig) {
		cfg.OnProgress = func(p Progress) {
			select {
			case progress <- p:
			default:
			}
		}
	})

	mustListTools(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan harness.ToolResult, 1)
	go func() {
		done <- client.Call(ctx, harness.ToolCall{
			CallID: "cancel-me", Name: "echo__sleep", Arguments: map[string]any{"ms": 60000},
		}, harness.AuthContext{})
	}()

	select {
	case <-progress:
		// The call is genuinely in flight on the server.
	case <-time.After(5 * time.Second):
		t.Fatal("no notifications/progress arrived before cancellation")
	}

	start := time.Now()
	cancel()
	var result harness.ToolResult
	select {
	case result = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Call did not return promptly after ctx cancellation")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("cancellation took %s", elapsed)
	}
	if result.ErrorCode != "MCP_CANCELLED" {
		t.Fatalf("cancelled call = %+v, want MCP_CANCELLED", result)
	}
	if result.Status != harness.ToolUnknown {
		t.Fatalf("cancelled call status = %q, want UNKNOWN (the side effect outcome is unknown)", result.Status)
	}

	report := callTool(t, client, "echo__cancellations", map[string]any{})
	if !report.OK() {
		t.Fatalf("cancellations failed: %+v", report)
	}
	if cancelled, _ := structured(t, report)["cancelled"].([]any); len(cancelled) == 0 {
		t.Fatal("server never received notifications/cancelled")
	}
}

// TestMCPAllowedRoots enforces the stdio security surface (design 6.6).
func TestMCPAllowedRoots(t *testing.T) {
	binary := echoServerBinary(t)
	root := t.TempDir()

	t.Run("binary outside the allowlist", func(t *testing.T) {
		client, err := Dial(context.Background(), ServerConfig{
			Name:      "evil",
			Transport: &StdioTransport{Command: binary, AllowedRoots: []string{root}},
		})
		if err == nil {
			_ = client.Close()
			t.Fatal("a binary outside AllowedRoots was executed")
		}
		if !strings.Contains(err.Error(), "outside the allowed roots") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("bare command name", func(t *testing.T) {
		client, err := Dial(context.Background(), ServerConfig{
			Name:      "evil",
			Transport: &StdioTransport{Command: filepath.Base(binary), AllowedRoots: []string{root}},
		})
		if err == nil {
			_ = client.Close()
			t.Fatal("a bare $PATH command was accepted")
		}
		if !strings.Contains(err.Error(), "absolute path") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("empty allowlist", func(t *testing.T) {
		client, err := Dial(context.Background(), ServerConfig{
			Name:      "evil",
			Transport: &StdioTransport{Command: binary},
		})
		if err == nil {
			_ = client.Close()
			t.Fatal("an empty allowlist was treated as allow-all")
		}
	})

	t.Run("symlink escape", func(t *testing.T) {
		link := filepath.Join(root, "echo-link")
		if err := os.Symlink(binary, link); err != nil {
			t.Skipf("cannot create symlink: %v", err)
		}
		client, err := Dial(context.Background(), ServerConfig{
			Name:      "evil",
			Transport: &StdioTransport{Command: link, AllowedRoots: []string{root}},
		})
		if err == nil {
			_ = client.Close()
			t.Fatal("a symlink escaping the allowlist was executed")
		}
		if !strings.Contains(err.Error(), "outside the allowed roots") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// protocol behaviour (design 6.3 / 6.4)
// ---------------------------------------------------------------------------

// TestMCPToolSpecMapping pins the annotation mapping of design 6.4.
func TestMCPToolSpecMapping(t *testing.T) {
	client := dialEcho(t, nil)
	specs, err := client.Tools(context.Background())
	if err != nil {
		t.Fatalf("Tools: %v", err)
	}
	byName := map[string]harness.ToolSpec{}
	for _, spec := range specs {
		byName[spec.Name] = spec
	}

	cases := []struct {
		tool       string
		sideEffect string
		idempotent bool
	}{
		{"echo__echo", sideEffectNone, false},         // readOnlyHint + idempotentHint
		{"echo__add", sideEffectIrreversible, true},   // destructiveHint, not idempotent
		{"echo__sleep", sideEffectUnknown, true},      // no annotations at all
		{"echo__cancellations", sideEffectNone, true}, // readOnlyHint only
	}
	for _, tc := range cases {
		spec, ok := byName[tc.tool]
		if !ok {
			t.Fatalf("%s missing", tc.tool)
		}
		if spec.SideEffect != tc.sideEffect {
			t.Errorf("%s side effect = %q, want %q", tc.tool, spec.SideEffect, tc.sideEffect)
		}
		if spec.IdempotencyRequired != tc.idempotent {
			t.Errorf("%s IdempotencyRequired = %v, want %v", tc.tool, spec.IdempotencyRequired, tc.idempotent)
		}
		if spec.InputSchema == nil || spec.InputSchema["type"] != "object" {
			t.Errorf("%s lost its JSON Schema: %+v", tc.tool, spec.InputSchema)
		}
		if spec.Description == "" {
			t.Errorf("%s lost its description", tc.tool)
		}
	}

	// The MCP inputSchema is passed through untouched: the core validator
	// rejects a call that violates it.
	echo := byName["echo__echo"]
	properties, _ := echo.InputSchema["properties"].(map[string]any)
	if _, ok := properties["text"]; !ok {
		t.Fatalf("echo inputSchema lost its properties: %+v", echo.InputSchema)
	}
	required, _ := echo.InputSchema["required"].([]any)
	if len(required) != 1 || required[0] != "text" {
		t.Fatalf("echo inputSchema required = %v", echo.InputSchema["required"])
	}
}

// TestMCPErrorMapping covers JSON-RPC errors, isError results and unknown tools.
func TestMCPErrorMapping(t *testing.T) {
	client := dialEcho(t, nil)
	mustListTools(t, client)

	t.Run("jsonrpc error", func(t *testing.T) {
		result := callTool(t, client, "echo__add", map[string]any{"a": "not-a-number", "b": 1})
		if result.OK() {
			t.Fatalf("expected failure, got %+v", result)
		}
		if result.ErrorCode != "-32602" {
			t.Fatalf("ErrorCode = %q, want the server's numeric code -32602", result.ErrorCode)
		}
		if !strings.Contains(result.ErrorMessage, "numeric") {
			t.Fatalf("ErrorMessage = %q", result.ErrorMessage)
		}
	})

	t.Run("isError result", func(t *testing.T) {
		result := callTool(t, client, "echo__fail", map[string]any{"message": "boom"})
		if result.OK() {
			t.Fatalf("expected failure, got %+v", result)
		}
		if result.ErrorCode != "MCP_TOOL_ERROR" {
			t.Fatalf("ErrorCode = %q, want MCP_TOOL_ERROR", result.ErrorCode)
		}
		if !strings.Contains(result.ErrorMessage, "boom") {
			t.Fatalf("ErrorMessage = %q", result.ErrorMessage)
		}
	})

	t.Run("unknown tool", func(t *testing.T) {
		result := callTool(t, client, "echo__does_not_exist", map[string]any{})
		if result.ErrorCode != "MCP_UNKNOWN_TOOL" {
			t.Fatalf("ErrorCode = %q, want MCP_UNKNOWN_TOOL", result.ErrorCode)
		}
	})

	t.Run("tools require initialize", func(t *testing.T) {
		binary := echoServerBinary(t)
		raw, err := Dial(context.Background(), ServerConfig{
			Name: "echo",
			Transport: &StdioTransport{
				Command:      binary,
				AllowedRoots: []string{filepath.Dir(binary)},
			},
		})
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer func() { _ = raw.Close() }()
		if _, err := raw.Tools(context.Background()); !errors.Is(err, ErrNotInitialized) {
			t.Fatalf("Tools error = %v, want ErrNotInitialized", err)
		}
		if result := raw.Call(context.Background(), harness.ToolCall{Name: "echo__echo"}, harness.AuthContext{}); result.ErrorCode != "MCP_NOT_INITIALIZED" {
			t.Fatalf("Call before Initialize = %+v", result)
		}
	})
}

// TestMCPPerCallTimeout checks the per-call budget and that a timeout is
// reported as an unknown outcome rather than a plain failure.
func TestMCPPerCallTimeout(t *testing.T) {
	client := dialEcho(t, func(cfg *ServerConfig) {
		cfg.Timeout = 300 * time.Millisecond
	})
	mustListTools(t, client)
	start := time.Now()
	result := callTool(t, client, "echo__sleep", map[string]any{"ms": 30000})
	if result.ErrorCode != "MCP_TIMEOUT" {
		t.Fatalf("ErrorCode = %q, want MCP_TIMEOUT (%+v)", result.ErrorCode, result)
	}
	if result.Status != harness.ToolUnknown {
		t.Fatalf("Status = %q, want UNKNOWN", result.Status)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
}

// TestMCPCloseIsTerminal pins the Close contract (design 6.2): later calls fail
// with MCP_CLOSED, Close is idempotent, and an in-flight request is released
// instead of hanging forever.
func TestMCPCloseIsTerminal(t *testing.T) {
	client := dialEcho(t, nil)
	mustListTools(t, client)

	done := make(chan harness.ToolResult, 1)
	go func() {
		done <- client.Call(context.Background(), harness.ToolCall{
			CallID: "in-flight", Name: "echo__sleep", Arguments: map[string]any{"ms": 60000},
		}, harness.AuthContext{})
	}()
	time.Sleep(150 * time.Millisecond)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case result := <-done:
		if result.ErrorCode != "MCP_CLOSED" {
			t.Fatalf("in-flight call = %+v, want MCP_CLOSED", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an in-flight request was not released by Close")
	}

	if result := client.Call(context.Background(), harness.ToolCall{CallID: "after", Name: "echo__echo"}, harness.AuthContext{}); result.ErrorCode != "MCP_CLOSED" {
		t.Fatalf("call after Close = %+v, want MCP_CLOSED", result)
	}
}

// TestMCPServerInitiatedRequest checks that sampling/* requests from the server
// are answered with MethodNotFound instead of being ignored.
func TestMCPServerInitiatedRequest(t *testing.T) {
	logger := &recordingLogger{}
	client := dialEcho(t, func(cfg *ServerConfig) { cfg.Logger = logger })
	mustListTools(t, client)

	result := callTool(t, client, "echo__probe_server_request", map[string]any{})
	if !result.OK() {
		t.Fatalf("probe failed: %+v", result)
	}
	report := structured(t, result)
	if got := report["error_code"]; got != float64(codeMethodNotFound) {
		t.Fatalf("client answered sampling/createMessage with %v, want %d", got, codeMethodNotFound)
	}
	if !strings.Contains(logger.String(), "MethodNotFound") {
		t.Fatalf("capability gap was not logged: %s", logger.String())
	}
}

// TestMCPConcurrentCalls multiplexes several requests over the one reader.
func TestMCPConcurrentCalls(t *testing.T) {
	client := dialEcho(t, nil)
	mustListTools(t, client)

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			text := fmt.Sprintf("worker-%d", i)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			result := client.Call(ctx, harness.ToolCall{CallID: text, Name: "echo__echo", Arguments: map[string]any{"text": text}}, harness.AuthContext{})
			if !result.OK() {
				errs <- fmt.Sprintf("worker %d: %+v", i, result)
				return
			}
			value, _ := result.Value.(map[string]any)
			var got any
			if content, ok := value["structuredContent"].(map[string]any); ok {
				got = content["echo"]
			}
			if got != text {
				errs <- fmt.Sprintf("worker %d: echoed %v, want %s", i, got, text)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// ---------------------------------------------------------------------------
// harness.ToolSource seam
// ---------------------------------------------------------------------------

type fakeSource struct {
	name    string
	tools   []harness.ToolSpec
	replies map[string]any
}

func (f *fakeSource) Name() string { return f.name }

func (f *fakeSource) Tools(context.Context) ([]harness.ToolSpec, error) { return f.tools, nil }

func (f *fakeSource) Call(_ context.Context, call harness.ToolCall, _ harness.AuthContext) harness.ToolResult {
	return harness.ToolResult{CallID: call.CallID, Name: call.Name, Status: harness.ToolSucceeded, Value: f.replies[call.Name], Attempts: 1}
}

func (f *fakeSource) Close() error { return nil }

// TestRegisterSourceNamespaceAndValidation pins RegisterSource's contract: the
// "<source>__" prefix is applied exactly once and a tool whose schema is not a
// usable JSON Schema is rejected instead of poisoning the registry.
func TestRegisterSourceNamespaceAndValidation(t *testing.T) {
	source := &fakeSource{
		name: "srv",
		tools: []harness.ToolSpec{
			{Name: "plain", InputSchema: map[string]any{"type": "object"}},
			{Name: "srv__already", InputSchema: map[string]any{"type": "object"}},
			{Name: "broken", InputSchema: map[string]any{"type": func() {}}},
			{Name: "", InputSchema: map[string]any{"type": "object"}},
		},
		replies: map[string]any{"srv__plain": "plain-value"},
	}
	registry := harness.NewToolRegistry()
	registered, err := registry.RegisterSource(context.Background(), source)
	if err == nil {
		t.Fatal("expected an error reporting the rejected tools")
	}
	if registered != 2 {
		t.Fatalf("registered = %d, want 2 (err=%v)", registered, err)
	}
	if !strings.Contains(err.Error(), "srv__broken") {
		t.Fatalf("error does not name the rejected tool: %v", err)
	}
	if _, ok := registry.Get("srv__plain"); !ok {
		t.Fatal("srv__plain was not registered")
	}
	if _, ok := registry.Get("srv__srv__already"); ok {
		t.Fatal("the source prefix was applied twice")
	}
	if _, ok := registry.Get("srv__already"); !ok {
		t.Fatal("an already-prefixed name was not preserved")
	}

	// The wrapper handler delegates to ToolSource.Call through the pipeline.
	spec, _ := registry.Get("srv__plain")
	value, err := spec.Handler(map[string]any{}, harness.AuthContext{})
	if err != nil {
		t.Fatalf("wrapper handler: %v", err)
	}
	if value != "plain-value" {
		t.Fatalf("wrapper handler returned %v", value)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
