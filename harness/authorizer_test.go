package harness

import (
	"context"
	"errors"
	"testing"
)

// The authorizer seam lets a richer rule engine narrow what the core's allowlist
// permits. It must not be able to widen it.

type denyingAuthorizer struct {
	denied map[string]bool
	calls  int
}

func (a *denyingAuthorizer) AuthorizeTool(name string, _ map[string]any) error {
	a.calls++
	if a.denied[name] {
		return errors.New("denied by rule")
	}
	return nil
}

func authorizerRuntime(t *testing.T, authorizer ToolAuthorizer, policy *ExecutionPolicy) (*Runtime, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	registry := NewToolRegistry()
	for _, name := range []string{"allowed_tool", "blocked_tool"} {
		if err := registry.Register(ToolSpec{
			Name: name, InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
			ConcurrencySafe: true, SideEffect: "NONE",
			Handler: func(args map[string]any, _ AuthContext) (any, error) { return map[string]any{"ran": true}, nil },
		}); err != nil {
			t.Fatal(err)
		}
	}
	// The policy arrives through WithTools, which is also the case where a
	// caller supplies their own ToolRuntime — the branch where an authorizer
	// would be silently dropped if it were only applied to the default one.
	runtime, err := New(WithStore(store), WithModel(NewScriptedModel(nil)),
		WithTools(&ToolRuntime{Registry: registry, Policy: policy}), WithToolAuthorizer(authorizer))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	return runtime, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

// A rule refusal must stop the call, and must be distinguishable from a policy
// refusal so an operator can tell which layer refused.
func TestAuthorizerCanRefuseAnAllowedTool(t *testing.T) {
	authorizer := &denyingAuthorizer{denied: map[string]bool{"blocked_tool": true}}
	policy := &ExecutionPolicy{AllowedTools: []string{"allowed_tool", "blocked_tool"}}
	runtime, auth := authorizerRuntime(t, authorizer, policy)

	result := runtime.Tools().Execute(context.Background(), ToolCall{CallID: "c", Name: "blocked_tool", Arguments: map[string]any{}}, auth)
	if result.Status != ToolFailed || result.ErrorCode != "DENIED" {
		t.Fatalf("status=%s code=%s, want FAILED/DENIED", result.Status, result.ErrorCode)
	}
	if authorizer.calls == 0 {
		t.Fatal("the authorizer was never consulted")
	}
}

// A tool the policy allows and the authorizer permits still runs: the seam must
// not become a second, stricter policy by accident.
func TestAuthorizerAllowsWhatItDoesNotDeny(t *testing.T) {
	authorizer := &denyingAuthorizer{denied: map[string]bool{}}
	policy := &ExecutionPolicy{AllowedTools: []string{"allowed_tool", "blocked_tool"}}
	runtime, auth := authorizerRuntime(t, authorizer, policy)

	result := runtime.Tools().Execute(context.Background(), ToolCall{CallID: "c", Name: "allowed_tool", Arguments: map[string]any{}}, auth)
	if result.Status != ToolSucceeded {
		t.Fatalf("status=%s code=%s error=%s", result.Status, result.ErrorCode, result.ErrorMessage)
	}
}

// The critical property: an authorizer cannot widen the allowlist. A seam that
// could would be a hole rather than a feature.
func TestAuthorizerCannotPermitWhatThePolicyRefuses(t *testing.T) {
	permissive := &denyingAuthorizer{denied: map[string]bool{}}
	policy := &ExecutionPolicy{AllowedTools: []string{"allowed_tool"}}
	runtime, auth := authorizerRuntime(t, permissive, policy)

	result := runtime.Tools().Execute(context.Background(), ToolCall{CallID: "c", Name: "blocked_tool", Arguments: map[string]any{}}, auth)
	if result.Status != ToolFailed {
		t.Fatalf("an authorizer permitted a tool the policy refuses: status=%s", result.Status)
	}
	if result.ErrorCode != "POLICY_DENIED" {
		t.Fatalf("code=%s, want POLICY_DENIED so the refusing layer is identifiable", result.ErrorCode)
	}
}

// An authorizer without a policy must still run. Nesting it inside the policy
// branch made that configuration silently do nothing.
func TestAuthorizerRunsWithoutAPolicy(t *testing.T) {
	authorizer := &denyingAuthorizer{denied: map[string]bool{"blocked_tool": true}}
	runtime, auth := authorizerRuntime(t, authorizer, nil)

	result := runtime.Tools().Execute(context.Background(), ToolCall{CallID: "c", Name: "blocked_tool", Arguments: map[string]any{}}, auth)
	if result.Status != ToolFailed || result.ErrorCode != "DENIED" {
		t.Fatalf("an authorizer without a policy did not run: status=%s code=%s", result.Status, result.ErrorCode)
	}
}
