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

// A refusal from the engine must reach the caller UNCHANGED.
//
// The previous version of this test logged whatever happened and asserted
// nothing, so it could not fail: it claimed to check propagation while checking
// only that no panic occurred. It now compares the adapter's result against the
// engine's own, which tests propagation without having to know which rule fired —
// that knowledge would make the test a restatement of the configuration rather
// than a statement about the adapter.
func TestAuthorizerPropagatesTheEngineRefusal(t *testing.T) {
	config := permissions.Config{RequireApproval: true}
	engine := permissions.NewPermissionEngine(config)
	authorizer := NewAuthorizer(engine)

	tool := "bash"
	args := map[string]interface{}{"command": "frobnicate --all"}
	direct := engine.Check(tool, args)
	throughAdapter := authorizer.AuthorizeTool(tool, args)

	if (direct == nil) != (throughAdapter == nil) {
		t.Fatalf("the adapter changed whether the call was refused: engine=%v adapter=%v", direct, throughAdapter)
	}
	if direct == nil {
		t.Fatal("the engine allowed a call with approval required and no handler, so nothing was propagated")
	}
	if direct.Error() != throughAdapter.Error() {
		t.Fatalf("the adapter rewrote the refusal: engine=%q adapter=%q", direct.Error(), throughAdapter.Error())
	}
}

// An allowed call must stay allowed through the adapter, or the seam would be a
// blanket denial rather than a policy.
func TestAuthorizerPropagatesAnAllowance(t *testing.T) {
	engine := permissions.NewPermissionEngine(permissions.Config{})
	authorizer := NewAuthorizer(engine)
	direct := engine.Check("read_file", map[string]interface{}{"path": "/tmp/x"})
	throughAdapter := authorizer.AuthorizeTool("read_file", map[string]interface{}{"path": "/tmp/x"})
	if (direct == nil) != (throughAdapter == nil) {
		t.Fatalf("the adapter disagreed with the engine on an allowance: engine=%v adapter=%v", direct, throughAdapter)
	}
}
