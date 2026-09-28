package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"
)

// ToolSource is an external tool source (an MCP server, a plugin, a remote
// registry). The core depends only on this interface and knows nothing about
// any concrete transport: process spawning and HTTP live outside the
// embeddable closure (see docs/runtime-completion-design.md 6.1).
type ToolSource interface {
	Name() string
	Tools(ctx context.Context) ([]ToolSpec, error)
	Call(ctx context.Context, call ToolCall, auth AuthContext) ToolResult
	Close() error
}

// RegisterSource pulls the source's tools and registers them. It returns the
// number of tools that registered successfully (that is, whose specs passed
// schema validation). Tool names are prefixed with "<source>__" so tools from
// different sources cannot collide.
//
// RegisterSource never hands an external handler straight to the model: for
// every "<source>__<tool>" it registers a wrapper whose handler is controlled
// by the core pipeline and which delegates to ToolSource.Call. MCP tools
// therefore flow through the existing schema/scope/timeout/retry/idempotency/
// audit path with no pipeline changes.
func (r *ToolRegistry) RegisterSource(ctx context.Context, src ToolSource) (int, error) {
	if src == nil {
		return 0, errors.New("nil tool source")
	}
	if r == nil {
		return 0, errors.New("nil tool registry")
	}
	source := strings.TrimSpace(src.Name())
	if source == "" {
		return 0, errors.New("tool source has no name")
	}
	specs, err := src.Tools(ctx)
	if err != nil {
		return 0, fmt.Errorf("tool source %s: %w", source, err)
	}
	registered := 0
	var rejected []string
	for _, spec := range specs {
		if strings.TrimSpace(spec.Name) == "" {
			rejected = append(rejected, "(a tool with an empty name)")
			continue
		}
		spec = normalizeSourcedSpec(source, spec)
		inner := spec
		spec.Handler = func(arguments map[string]any, auth AuthContext) (any, error) {
			return callToolSource(src, inner.Name, arguments, auth)
		}
		if err := validateToolSchema(spec.InputSchema); err != nil {
			rejected = append(rejected, fmt.Sprintf("%s (%v)", spec.Name, err))
			continue
		}
		if err := r.Register(spec); err != nil {
			rejected = append(rejected, fmt.Sprintf("%s (%v)", spec.Name, err))
			continue
		}
		registered++
	}
	if len(rejected) > 0 {
		return registered, fmt.Errorf("tool source %s: %d of %d tools rejected: %s",
			source, len(rejected), len(specs), strings.Join(rejected, ", "))
	}
	return registered, nil
}

// normalizeSourcedSpec applies the "<source>__" namespace and the registry's
// own defaults. A source that already prefixes its names (mcp.Client does, see
// design 6.4) is left alone so the prefix is never applied twice.
func normalizeSourcedSpec(source string, spec ToolSpec) ToolSpec {
	name := strings.TrimSpace(spec.Name)
	if prefix := source + "__"; !strings.HasPrefix(name, prefix) {
		name = prefix + name
	}
	spec.Name = name
	if spec.InputSchema == nil {
		spec.InputSchema = map[string]any{"type": "object"}
	}
	return spec
}

// callToolSource adapts a ToolSource result to the ToolHandler contract.
// ToolHandler has no context, so the call runs on a background context; the
// pipeline's own per-tool timeout (ToolSpec.TimeoutSeconds) still governs it.
func callToolSource(src ToolSource, name string, arguments map[string]any, auth AuthContext) (any, error) {
	result := src.Call(context.Background(), ToolCall{Name: name, Arguments: arguments}, auth)
	if result.Status == ToolSucceeded {
		return result.Value, nil
	}
	code := result.ErrorCode
	if code == "" {
		code = "TOOL_SOURCE_FAILED"
	}
	message := result.ErrorMessage
	if message == "" {
		message = result.Status
	}
	return nil, fmt.Errorf("%s: %s", code, message)
}

// validateToolSchema compiles a tool's JSON Schema with the same validator the
// pipeline uses at call time, so a malformed schema is rejected at
// registration instead of failing on the first call.
func validateToolSchema(schema map[string]any) error {
	if schema == nil {
		return nil
	}
	payload, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("schema is not JSON-encodable: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("tool-source-schema.json", bytes.NewReader(payload)); err != nil {
		return fmt.Errorf("invalid JSON schema: %w", err)
	}
	if _, err := compiler.Compile("tool-source-schema.json"); err != nil {
		return fmt.Errorf("invalid JSON schema: %w", err)
	}
	return nil
}
