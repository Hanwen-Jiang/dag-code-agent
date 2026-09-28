// Package permissions adapts packages/permissions to the harness authorization
// seam.
//
// It lives outside the core because packages/permissions carries sixteen
// third-party dependencies. The core keeps only the ToolAuthorizer interface, so
// an embedder who does not want a rule engine does not pay for one — and the
// core's dependency budget is unchanged (guard G7).
package permissions

import (
	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/packages/permissions"
)

// Authorizer refuses tool calls that the permission engine denies.
type Authorizer struct {
	engine *permissions.PermissionEngine
}

// NewAuthorizer wraps an engine. A nil engine refuses nothing, which is the
// same as not installing an authorizer.
func NewAuthorizer(engine *permissions.PermissionEngine) *Authorizer {
	return &Authorizer{engine: engine}
}

// NewAuthorizerFromConfig builds an engine from a config.
//
// The engine is constructed eagerly rather than lazily on first use: a
// misconfiguration should surface where it is set up, not in the middle of a
// task that is already running.
func NewAuthorizerFromConfig(config permissions.Config) *Authorizer {
	return NewAuthorizer(permissions.NewPermissionEngine(config))
}

// AuthorizeTool implements harness.ToolAuthorizer.
//
// It can only refuse. The harness runs this after its own allowlist and treats
// either refusal as final, so a rule here narrows what the policy permits and
// cannot widen it.
func (a *Authorizer) AuthorizeTool(name string, args map[string]any) error {
	if a == nil || a.engine == nil {
		return nil
	}
	return a.engine.Check(name, args)
}

// compile-time assertion that the adapter satisfies the seam.
var _ harness.ToolAuthorizer = (*Authorizer)(nil)
