// Package hooks adapts packages/hooks to the harness lifecycle seam.
//
// It lives outside the core because packages/hooks carries third-party
// dependencies. The core keeps only the HookBus interface, so the dependency
// budget is unchanged (guard G7) and an embedder who does not want script hooks
// does not pay for one.
package hooks

import (
	"context"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	dshhooks "github.com/Hanwen-Jiang/dag-code-agent/packages/hooks"
)

// Bus forwards lifecycle events to a hooks manager.
type Bus struct {
	manager *dshhooks.Manager
}

// NewBus wraps a manager. A nil manager delivers nothing, which is the same as
// not installing a bus.
func NewBus(manager *dshhooks.Manager) *Bus {
	return &Bus{manager: manager}
}

// NewBusFromConfigFile builds a manager from a hooks configuration file and
// wraps it.
//
// The configuration is loaded eagerly so a malformed file surfaces where the bus
// is constructed rather than on the first event, which would be mid-task.
func NewBusFromConfigFile(path string) (*Bus, error) {
	manager := dshhooks.NewHookManager()
	if err := manager.LoadConfig(path); err != nil {
		return nil, err
	}
	return NewBus(manager), nil
}

// Publish implements harness.HookBus.
//
// Event kinds are mapped onto the four hook points the manager supports; a kind
// it has no point for is dropped rather than guessed at, because delivering an
// event under the wrong hook point would run the wrong script.
func (b *Bus) Publish(_ context.Context, event harness.LifecycleEvent) error {
	if b == nil || b.manager == nil {
		return nil
	}
	payload := &dshhooks.EventPayload{
		Event:     string(event.Kind),
		ToolName:  event.ToolName,
		ToolInput: event.ToolArgs,
		SessionID: event.TaskID,
		Timestamp: event.At,
		Metadata: map[string]interface{}{
			"tenant_id":  event.TenantID,
			"subject_id": event.SubjectID,
			"step":       event.Step,
			"status":     event.Status,
			"error_code": event.ErrorCode,
		},
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now().UTC()
	}
	switch event.Kind {
	case harness.LifecycleTaskStarted:
		return b.manager.NotifySessionStart(event.TaskID)
	case harness.LifecycleTaskFinished:
		return b.manager.NotifyStop(event.TaskID)
	case harness.LifecycleToolUsed:
		b.manager.RunPostToolUse(payload)
		return nil
	default:
		// An unmapped kind is dropped deliberately rather than routed to a
		// default hook point.
		return nil
	}
}

// compile-time assertion that the adapter satisfies the seam.
var _ harness.HookBus = (*Bus)(nil)
