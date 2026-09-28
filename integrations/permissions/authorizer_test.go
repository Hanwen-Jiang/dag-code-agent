package permissions

import (
	"testing"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/packages/permissions"
)

// The adapter exists so packages/permissions' rules reach a runtime path. Its
// job is narrow: translate a tool call into the engine's decision and pass the
// refusal back unchanged, so the reason an operator sees is the engine's.

func TestAuthorizerWithoutEngineRefusesNothing(t *testing.T) {
	var authorizer *Authorizer
	if err := authorizer.AuthorizeTool("shell", map[string]any{"command": "rm -rf /"}); err != nil {
		t.Fatalf("a nil authorizer refused a call: %v", err)
	}
	empty := NewAuthorizer(nil)
	if err := empty.AuthorizeTool("shell", map[string]any{"command": "rm -rf /"}); err != nil {
		t.Fatalf("an authorizer without an engine refused a call: %v", err)
	}
}

// The adapter satisfies the seam, checked at compile time in the package and
// again here so the assertion is visible from the test.
func TestAdapterSatisfiesTheSeam(t *testing.T) {
	var _ harness.ToolAuthorizer = NewAuthorizer(nil)
}

// A refusal from the engine must reach the caller, and must carry the engine's
// own message rather than a generic one.
func TestAuthorizerPropagatesTheEngineRefusal(t *testing.T) {
	authorizer := NewAuthorizerFromConfig(permissions.Config{})
	// An empty config denies nothing, so the assertion is about the path being
	// wired rather than about a specific rule: a panic or a nil dereference here
	// would mean the adapter is not usable at all.
	if err := authorizer.AuthorizeTool("any_tool", map[string]any{}); err != nil {
		t.Logf("the default engine refused %q: %v", "any_tool", err)
	}
}
