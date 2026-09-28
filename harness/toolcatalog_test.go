package harness

import (
	"context"
	"strings"
	"testing"
)

// Deferred tool loading exists to stop a large catalogue from crowding out the
// work. These tests pin both halves: the schemas actually go away, and the
// resolver actually gets them back.

func catalogRuntime(t *testing.T, toolCount int, threshold int) (*Runtime, *ContextBuilder, AuthContext) {
	t.Helper()
	registry := NewToolRegistry()
	for i := 0; i < toolCount; i++ {
		name := "workspace_tool_" + string(rune('a'+i))
		if err := registry.Register(ToolSpec{
			Name: name, Description: "does a workspace thing with files and directories",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{"type": "string"}, "depth": map[string]any{"type": "number"},
				},
			},
			Handler: func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := registry.Register(ToolSpec{
		Name: "read_file", Description: "read a file from the workspace", RequiredScopes: []string{"workspace:read"},
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel([]ModelResponse{{Text: "ok", FinishReason: "stop"}})),
		WithToolRegistry(registry), WithToolCatalogThreshold(threshold))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	builder := NewContextBuilder(nil, nil, registry, nil)
	builder.ToolCatalogThreshold = threshold
	return runtime, builder, AuthContext{TenantID: "t", SubjectID: "s", Scopes: []string{"read", "write", "workspace:read"}}
}

func compileContext(t *testing.T, builder *ContextBuilder, auth AuthContext) ContextPacket {
	t.Helper()
	packet, err := builder.BuildWithContext(context.Background(), TaskRequest{
		TaskID: "catalog", Goal: "read a file", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
	}, auth, TaskState{TaskID: "catalog", Status: TaskRunning}, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return packet
}

// Above the threshold, schemas are replaced by the catalogue plus the resolver.
func TestToolCatalogDefersSchemasAboveThreshold(t *testing.T) {
	_, builder, auth := catalogRuntime(t, 8, 3)
	packet := compileContext(t, builder, auth)

	if len(packet.Tools) >= 8 {
		t.Fatalf("schemas were not deferred: %d tool schemas compiled", len(packet.Tools))
	}
	names := map[string]bool{}
	for _, schema := range packet.Tools {
		function, _ := schema["function"].(map[string]any)
		names[stringValue(function["name"])] = true
	}
	if !names[ToolSearchToolName()] {
		t.Fatal("the deferred set does not include the resolver the catalogue points at")
	}
	// The catalogue must name every deferred tool, or the model cannot know it
	// exists.
	var note string
	for _, message := range packet.Messages {
		if content, ok := message["content"].(string); ok && strings.Contains(content, ToolSearchToolName()) {
			note = content
		}
	}
	if note == "" {
		t.Fatal("no catalogue note was injected")
	}
	if !strings.Contains(note, "read_file") {
		t.Fatalf("the catalogue omits a real tool:\n%s", note)
	}
	if !strings.Contains(note, "read a file from the workspace") {
		t.Fatalf("the catalogue entry carries no description:\n%s", note)
	}
}

// Below the threshold nothing changes: this is the compatibility guarantee.
func TestToolCatalogIsOptIn(t *testing.T) {
	_, builder, auth := catalogRuntime(t, 3, 0)
	packet := compileContext(t, builder, auth)
	if len(packet.Tools) != 4 {
		t.Fatalf("tools=%d, want 4 (deferral must be off at threshold 0)", len(packet.Tools))
	}
	for _, schema := range packet.Tools {
		function, _ := schema["function"].(map[string]any)
		if stringValue(function["name"]) == ToolSearchToolName() {
			t.Fatal("the resolver was registered even though deferral is disabled")
		}
	}
}

// The resolver must return the schema that was withheld.
func TestToolSearchResolvesWithheldSchema(t *testing.T) {
	runtime, _, auth := catalogRuntime(t, 8, 3)
	spec, ok := runtime.Tools().Registry.Get(ToolSearchToolName())
	if !ok {
		t.Fatal("the resolver is not registered when deferral is enabled")
	}
	result, err := spec.Handler(map[string]any{"query": "read_file"}, auth)
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	payload, _ := result.(map[string]any)
	tools, _ := payload["tools"].([]map[string]any)
	if len(tools) == 0 {
		t.Fatalf("the resolver returned nothing for a tool that exists: %#v", payload)
	}
	function, _ := tools[0]["function"].(map[string]any)
	if got := stringValue(function["name"]); got != "read_file" {
		t.Fatalf("resolver returned %q, want read_file (a name match must outrank a description match)", got)
	}
	// It must carry the full schema, or the model still cannot call the tool.
	parameters, _ := function["parameters"].(map[string]any)
	if parameters == nil || parameters["properties"] == nil {
		t.Fatal("the resolver returned a tool without its schema")
	}
}

// The resolver is a normal tool: it must not become a way around scope checks.
func TestToolSearchRespectsScopes(t *testing.T) {
	runtime, _, _ := catalogRuntime(t, 8, 3)
	spec, _ := runtime.Tools().Registry.Get(ToolSearchToolName())
	// An identity without workspace:read must not see read_file.
	result, err := spec.Handler(map[string]any{"query": "read_file"}, AuthContext{TenantID: "t", SubjectID: "s", Scopes: []string{"read"}})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	payload, _ := result.(map[string]any)
	tools, _ := payload["tools"].([]map[string]any)
	// Tools with no scope requirement are legitimately visible to everyone, so
	// the assertion is about the scoped one specifically.
	for _, tool := range tools {
		function, _ := tool["function"].(map[string]any)
		if stringValue(function["name"]) == "read_file" {
			t.Fatal("the resolver leaked read_file to a caller without workspace:read")
		}
	}
}

// Same query, same order: a replayed run must make the same tool choices.
func TestToolSearchOrderingIsStable(t *testing.T) {
	runtime, _, auth := catalogRuntime(t, 8, 3)
	spec, _ := runtime.Tools().Registry.Get(ToolSearchToolName())
	first, _ := spec.Handler(map[string]any{"query": "workspace"}, auth)
	second, _ := spec.Handler(map[string]any{"query": "workspace"}, auth)
	firstName := stringValue(first.(map[string]any)["names"])
	secondName := stringValue(second.(map[string]any)["names"])
	if firstName != secondName {
		t.Fatalf("resolver ordering is unstable: %v vs %v", firstName, secondName)
	}
}
