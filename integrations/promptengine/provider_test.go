package promptengine

import (
	"context"
	"testing"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	promptengine "github.com/Hanwen-Jiang/dag-code-agent/packages/prompt-engine"
)

func TestAdapterSatisfiesTheSeam(t *testing.T) {
	var _ harness.SystemPromptProvider = (*Provider)(nil)
}

func TestProviderWithoutEngineRendersNothing(t *testing.T) {
	request := harness.TaskRequest{TaskID: "t", Goal: "g"}
	var nilProvider *Provider
	if got, err := nilProvider.SystemPrompt(context.Background(), request, harness.AuthContext{}); err != nil || got != "" {
		t.Fatalf("nil provider returned %q, %v; want empty and no error", got, err)
	}
	empty := NewProvider(nil, "some-template")
	if got, err := empty.SystemPrompt(context.Background(), request, harness.AuthContext{}); err != nil || got != "" {
		t.Fatalf("provider without an engine returned %q, %v", got, err)
	}
	// An empty reference means "say nothing", which leaves the built-in prompt
	// in place rather than blanking it.
	noTemplate := NewProvider(promptengine.NewEngine(), "")
	if got, _ := noTemplate.SystemPrompt(context.Background(), request, harness.AuthContext{}); got != "" {
		t.Fatalf("provider with no template returned %q, want empty", got)
	}
}

// A registered template must render, and the task's fields must be available to
// it as data.
func TestProviderRendersARegisteredTemplate(t *testing.T) {
	engine := promptengine.NewEngine()
	if err := engine.Register("worker", "1", "Objective: {{.objective}} for {{.tenant_id}}", nil); err != nil {
		t.Fatalf("Register: %v", err)
	}
	provider := NewProvider(engine, "worker@1")
	rendered, err := provider.SystemPrompt(context.Background(), harness.TaskRequest{
		TaskID: "t", Goal: "review the schema", TenantID: "acme",
	}, harness.AuthContext{})
	if err != nil {
		t.Fatalf("SystemPrompt: %v", err)
	}
	if rendered == "" {
		t.Fatal("a registered template rendered nothing")
	}
	if !contains(rendered, "review the schema") || !contains(rendered, "acme") {
		t.Fatalf("the template did not receive the task as data: %q", rendered)
	}
}

// A missing template is an error the harness turns into a fallback, not a panic
// and not a silent empty prompt.
func TestProviderReportsAMissingTemplate(t *testing.T) {
	provider := NewProvider(promptengine.NewEngine(), "does-not-exist@1")
	if _, err := provider.SystemPrompt(context.Background(), harness.TaskRequest{TaskID: "t", Goal: "g"}, harness.AuthContext{}); err == nil {
		t.Fatal("a missing template reported no error, so the harness could not fall back knowingly")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
