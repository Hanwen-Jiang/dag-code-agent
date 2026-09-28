package harness

import (
	"context"
	"strings"
)

// System prompt seam.
//
// The context compiler hardcoded its system prompt: one sentence, same for every
// task, every tenant and every deployment. An embedder who needs a different
// one — a domain preamble, a style guide, a versioned prompt they want to A/B —
// had to fork the compiler to change a string.
//
// packages/prompt-engine was written for exactly that, with versioned templates
// and rendering, and reached no runtime path. It carries third-party
// dependencies, so like the other engines it is adapted from outside the core.
//
// What the seam deliberately does NOT replace: the developer message stating that
// model output is a proposal and the runtime commits state and effects. That is
// the control plane's contract with itself, not a style choice, and an embedder
// who removed it would be weakening the guarantee the whole runtime rests on.

// SystemPromptProvider supplies the system prompt for a task.
//
// The returned string becomes the first stable message. An empty string means
// "use the built-in prompt", which keeps a provider that has nothing to say from
// producing an empty system message.
//
// Implementations must not block for long: this runs during context compilation
// on the task's own path.
type SystemPromptProvider interface {
	SystemPrompt(ctx context.Context, request TaskRequest, auth AuthContext) (string, error)
}

// WithSystemPromptProvider installs a system prompt provider.
func WithSystemPromptProvider(provider SystemPromptProvider) Option {
	return func(c *runtimeConfig) error {
		c.systemPrompt = provider
		return nil
	}
}

// defaultSystemPrompt is used when no provider is installed, and whenever one
// fails or returns nothing.
const defaultSystemPrompt = "You are a controlled Agent Harness worker."

// systemPromptFor resolves the prompt, falling back rather than failing.
//
// A provider is an enhancement, not a dependency: a template store that is
// unreachable should degrade the wording, not prevent the task from running.
// The failure is not silent — it is reported on the packet so an operator can
// see which prompt was actually used.
func (b *ContextBuilder) systemPromptFor(ctx context.Context, request TaskRequest, auth AuthContext) (string, bool) {
	if b == nil || b.SystemPrompt == nil {
		return defaultSystemPrompt, false
	}
	rendered, err := b.SystemPrompt.SystemPrompt(ctx, request, auth)
	if err != nil || strings.TrimSpace(rendered) == "" {
		return defaultSystemPrompt, err != nil
	}
	return rendered, false
}
