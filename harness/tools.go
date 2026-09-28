package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]ToolSpec
}

func NewToolRegistry() *ToolRegistry { return &ToolRegistry{tools: map[string]ToolSpec{}} }
func (r *ToolRegistry) Register(spec ToolSpec) error {
	if strings.TrimSpace(spec.Name) == "" || spec.Handler == nil {
		return errors.New("invalid tool spec")
	}
	if spec.InputSchema == nil {
		spec.InputSchema = map[string]any{"type": "object"}
	}
	if spec.TimeoutSeconds <= 0 {
		spec.TimeoutSeconds = 10
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[spec.Name] = spec
	return nil
}
func (r *ToolRegistry) Get(name string) (ToolSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.tools[name]
	return v, ok
}
func (r *ToolRegistry) List() []ToolSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ToolSpec, 0, len(r.tools))
	for _, v := range r.tools {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (r *ToolRegistry) Schemas(allowed []string) []map[string]any {
	return r.schemas(allowed, nil)
}

func (r *ToolRegistry) SchemasForAuth(allowed []string, auth AuthContext) []map[string]any {
	return r.schemas(allowed, &auth)
}

func (r *ToolRegistry) schemas(allowed []string, auth *AuthContext) []map[string]any {
	result := []map[string]any{}
	for _, spec := range r.List() {
		if len(allowed) > 0 && !ContainsString(allowed, spec.Name) {
			continue
		}
		if auth != nil {
			permitted := true
			for _, scope := range spec.RequiredScopes {
				if !auth.HasScope(scope) {
					permitted = false
					break
				}
			}
			if !permitted {
				continue
			}
		}
		result = append(result, spec.SchemaForModel())
	}
	return result
}

type ToolRuntime struct {
	Registry *ToolRegistry
	Store    IdempotencyStore
	Policy   *ExecutionPolicy
	// MaxParallel bounds a concurrent tool batch. Zero selects the default.
	MaxParallel int
}

func NewToolRuntime(registry *ToolRegistry, store IdempotencyStore) *ToolRuntime {
	return &ToolRuntime{Registry: registry, Store: store}
}

// defaultToolParallelism bounds concurrent tool execution when unset.
const defaultToolParallelism = 4

// parallelism returns the effective concurrency bound.
func (r *ToolRuntime) parallelism() int {
	if r == nil || r.MaxParallel <= 0 {
		return defaultToolParallelism
	}
	return r.MaxParallel
}

// specConcurrencySafe reports whether a tool may run alongside its siblings.
//
// Only an explicit declaration counts. The previous field, SideEffect, was
// never read by any decision — "IRREVERSIBLE" was the only value in the tree and
// nothing acted on it — so it cannot be trusted as an implicit safety signal.
func specConcurrencySafe(spec ToolSpec) bool {
	return spec.ConcurrencySafe
}

// batchCanRunConcurrently reports whether every call in a batch may run at once.
//
// A single unsafe member serializes the whole batch: running part of a batch in
// parallel and part after it would reorder effects relative to the model's
// request, and no test could tell that apart from a bug.
func (r *ToolRuntime) batchCanRunConcurrently(calls []ToolCall) bool {
	if r == nil || r.Registry == nil || len(calls) < 2 {
		return false
	}
	for _, call := range calls {
		spec, ok := r.Registry.Get(call.Name)
		if !ok || !specConcurrencySafe(spec) {
			return false
		}
	}
	return true
}

func NewToolRuntimeWithPolicy(registry *ToolRegistry, store IdempotencyStore, policy ExecutionPolicy) *ToolRuntime {
	return &ToolRuntime{Registry: registry, Store: store, Policy: &policy}
}

func validateArguments(value map[string]any, schema map[string]any) error {
	if schema == nil {
		return nil
	}
	return validateJSONSchemaValue(value, schema)
}

func (r *ToolRuntime) Execute(ctx context.Context, call ToolCall, auth AuthContext) ToolResult {
	started := time.Now()
	result := ToolResult{CallID: call.CallID, Name: call.Name, Status: ToolFailed, Attempts: 1}
	spec, ok := r.Registry.Get(call.Name)
	if !ok {
		result.ErrorCode = "TOOL_NOT_FOUND"
		result.ErrorMessage = "tool is not registered"
		return result
	}
	if r.Policy != nil {
		if err := r.Policy.AuthorizeTool(call.Name); err != nil {
			result.ErrorCode = "POLICY_DENIED"
			result.ErrorMessage = err.Error()
			return result
		}
	}
	for _, scope := range spec.RequiredScopes {
		if !auth.HasScope(scope) {
			result.ErrorCode = "PERMISSION_DENIED"
			result.ErrorMessage = "missing scope " + scope
			return result
		}
	}
	if err := validateArguments(call.Arguments, spec.InputSchema); err != nil {
		result.ErrorCode = "INVALID_ARGUMENTS"
		result.ErrorMessage = err.Error()
		return result
	}
	idemKey := stringValue(call.Arguments["idempotency_key"])
	scope := auth.TenantID + ":" + spec.Name
	if spec.IdempotencyRequired && idemKey == "" {
		result.ErrorCode = "IDEMPOTENCY_REQUIRED"
		result.ErrorMessage = "idempotency_key is required"
		return result
	}
	if strings.EqualFold(spec.SideEffect, "IRREVERSIBLE") && idemKey == "" {
		result.ErrorCode = "IDEMPOTENCY_REQUIRED"
		result.ErrorMessage = "irreversible tool requires idempotency_key"
		return result
	}
	if r.Store != nil && idemKey != "" {
		var prior ToolResult
		if found, err := r.Store.GetIdempotency(scope, idemKey, &prior); err == nil && found {
			prior.CallID = call.CallID
			prior.IdempotentReplay = true
			prior.LatencyMS = float64(time.Since(started).Microseconds()) / 1000
			return prior
		}
	}
	maxAttempts := spec.MaxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	timeout := time.Duration(spec.TimeoutSeconds * float64(time.Second))
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		result.Attempts = attempt
		done := make(chan struct{})
		var value any
		var callErr error
		go func() { value, callErr = spec.Handler(call.Arguments, auth); close(done) }()
		select {
		case <-done:
			if callErr == nil {
				result.Status = ToolSucceeded
				result.Value = value
				result.ErrorCode = ""
				result.ErrorMessage = ""
				result.LatencyMS = float64(time.Since(started).Microseconds()) / 1000
				if r.Store != nil && idemKey != "" {
					_, _ = r.Store.PutIdempotency(scope, idemKey, result)
				}
				return result
			}
			result.ErrorCode = "HANDLER_ERROR"
			result.ErrorMessage = callErr.Error()
		case <-time.After(timeout):
			result.Status = ToolUnknown
			result.ErrorCode = "TIMEOUT"
			result.ErrorMessage = "side effect outcome is unknown after timeout"
			result.LatencyMS = float64(time.Since(started).Microseconds()) / 1000
			if r.Store != nil && idemKey != "" {
				_, _ = r.Store.PutIdempotency(scope, idemKey, result)
			}
			return result
		}
		if attempt < maxAttempts {
			time.Sleep(time.Duration(attempt) * 20 * time.Millisecond)
		}
	}
	result.LatencyMS = float64(time.Since(started).Microseconds()) / 1000
	return result
}

type MCPServer struct {
	Runtime         *ToolRuntime
	ProtocolVersion string
}

func NewMCPServer(runtime *ToolRuntime) *MCPServer {
	return &MCPServer{Runtime: runtime, ProtocolVersion: "2025-06-18"}
}
func (s *MCPServer) Initialize(version string) (map[string]any, error) {
	if version != "" && version != s.ProtocolVersion {
		return nil, fmt.Errorf("PROTOCOL_VERSION_MISMATCH: server=%s client=%s", s.ProtocolVersion, version)
	}
	return map[string]any{"protocolVersion": s.ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "agent-harness", "version": "0.1"}}, nil
}
func (s *MCPServer) ListTools() []map[string]any {
	if s == nil || s.Runtime == nil {
		return nil
	}
	return s.Runtime.Registry.Schemas(nil)
}
func (s *MCPServer) CallTool(ctx context.Context, call ToolCall, auth AuthContext) ToolResult {
	return s.Runtime.Execute(ctx, call, auth)
}

type MCPClient struct {
	Server          *MCPServer
	Auth            AuthContext
	ProtocolVersion string
	initialized     bool
}

func NewMCPClient(server *MCPServer, auth AuthContext) *MCPClient {
	return &MCPClient{Server: server, Auth: auth, ProtocolVersion: "2025-06-18"}
}
func (c *MCPClient) Initialize() (map[string]any, error) {
	v, err := c.Server.Initialize(c.ProtocolVersion)
	if err == nil {
		c.initialized = true
	}
	return v, err
}
func (c *MCPClient) ListTools() ([]map[string]any, error) {
	if !c.initialized {
		if _, err := c.Initialize(); err != nil {
			return nil, err
		}
	}
	return c.Server.ListTools(), nil
}
func (c *MCPClient) Call(ctx context.Context, call ToolCall) ToolResult {
	if !c.initialized {
		if _, err := c.Initialize(); err != nil {
			return ToolResult{CallID: call.CallID, Name: call.Name, Status: ToolFailed, ErrorCode: "MCP_NOT_INITIALIZED", ErrorMessage: err.Error()}
		}
	}
	return c.Server.CallTool(ctx, call, c.Auth)
}
