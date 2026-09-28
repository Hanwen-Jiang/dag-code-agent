// Package promptengine adapts packages/prompt-engine to the harness system
// prompt seam.
//
// It lives outside the core because packages/prompt-engine carries third-party
// dependencies. The core keeps only the SystemPromptProvider interface, so the
// dependency budget is unchanged (guard G7).
package promptengine

import (
	"context"
	"fmt"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	promptengine "github.com/Hanwen-Jiang/dag-code-agent/packages/prompt-engine"
)

// Provider renders the system prompt from a versioned template.
type Provider struct {
	engine *promptengine.Engine
	// Template is the reference to render, e.g. "worker-system" or
	// "worker-system@2". An empty reference renders nothing, which leaves the
	// built-in prompt in place.
	Template string
}

// NewProvider wraps an engine.
func NewProvider(engine *promptengine.Engine, templateRef string) *Provider {
	return &Provider{engine: engine, Template: templateRef}
}

// NewProviderFromManager wraps a manager's engine, which is where templates are
// registered and versioned.
func NewProviderFromManager(manager *promptengine.PromptManager, templateRef string) *Provider {
	if manager == nil {
		return &Provider{Template: templateRef}
	}
	return &Provider{engine: manager.Engine(), Template: templateRef}
}

// SystemPrompt implements harness.SystemPromptProvider.
//
// The task is rendered as data rather than interpolated by the caller, so a goal
// containing template syntax is data and cannot become part of the template. That
// matters: the goal is untrusted input.
func (p *Provider) SystemPrompt(ctx context.Context, request harness.TaskRequest, _ harness.AuthContext) (string, error) {
	if p == nil || p.engine == nil || p.Template == "" {
		return "", nil
	}
	rendered, err := p.engine.Render(ctx, p.Template, map[string]any{
		"goal":       request.TaskID,
		"objective":  request.Goal,
		"task_id":    request.TaskID,
		"tenant_id":  request.TenantID,
		"subject_id": request.SubjectID,
		"constraints": func() []string {
			return request.Constraints
		}(),
	})
	if err != nil {
		return "", fmt.Errorf("promptengine: render %q: %w", p.Template, err)
	}
	return rendered, nil
}

// compile-time assertion that the adapter satisfies the seam.
var _ harness.SystemPromptProvider = (*Provider)(nil)
