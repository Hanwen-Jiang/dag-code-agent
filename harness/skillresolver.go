package harness

import "errors"

// errNilSkillResolver rejects a nil resolver at option time rather than
// dereferencing it during context compilation.
var errNilSkillResolver = errors.New("harness: WithSkillResolver requires a non-nil resolver")

// Skill resolution seam.
//
// The kernel previously resolved skills through *SkillRegistry: a 96-line map
// with lexical overlap scoring. packages/skills holds the real thing — pattern
// mining, versioning, approval, metrics, feedback-driven learning and a
// constraint-aware selector — and no runtime path reached it.
//
// The interface below is deliberately the same shape as *SkillRegistry.Resolve,
// so the existing registry already satisfies it: this is a widening of the type,
// not a new mechanism, and nothing that worked before stops working.

// SkillResolver selects the skills that should shape a task's context.
//
// Implementations must be safe for concurrent use. Returning an empty slice is
// valid and means "no skill applies", which is different from an error: a
// resolver that cannot answer should not fail the run.
type SkillResolver interface {
	Resolve(goal string, auth AuthContext) []SkillManifest
}

// compile-time assertion: the built-in registry is one valid resolver.
var _ SkillResolver = (*SkillRegistry)(nil)

// WithSkillResolver replaces the context compiler's skill source.
func WithSkillResolver(resolver SkillResolver) Option {
	return func(c *runtimeConfig) error {
		if resolver == nil {
			return errNilSkillResolver
		}
		c.skillResolver = resolver
		return nil
	}
}
