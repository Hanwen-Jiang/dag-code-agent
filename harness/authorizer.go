package harness

// Tool authorization seam.
//
// ExecutionPolicy decides whether a tool is on the allowlist, by name and
// nothing else. That is the right core policy — it is cheap, it is the last line
// of defence, and it must not depend on a third-party engine — but it cannot
// express "rm -rf is denied even though a shell tool is allowed", which is what
// packages/permissions was written for. That package carries sixteen third-party
// dependencies, so the core cannot import it and its rules reached no runtime
// path.
//
// The seam is deliberately narrow. It has no identity parameter: the engine that
// motivated it is configured per tool rather than per caller, and a parameter
// that is accepted and then ignored reads as a promise — the mistake just fixed
// on Action.Model. Identity-aware authorization would be a deliberate extension
// with an engine that uses it.

// ToolAuthorizer may refuse a tool call that the policy allowed.
//
// It can only refuse. Both the policy and every authorizer must pass, so an
// authorizer can narrow what is permitted but cannot widen it: a seam that could
// bypass the allowlist would be a hole rather than a feature.
type ToolAuthorizer interface {
	AuthorizeTool(name string, args map[string]any) error
}

// WithToolAuthorizer installs an authorizer, applied after the policy.
func WithToolAuthorizer(authorizer ToolAuthorizer) Option {
	return func(c *runtimeConfig) error {
		c.toolAuthorizer = authorizer
		return nil
	}
}
