package harness

import (
	"context"
	"time"
)

// Lifecycle hook seam.
//
// The journal is the durable record, and it is the right source of truth — but it
// is not a notification mechanism. An embedder that wants to alert on a failed
// task, count runs in a metrics system or hand a tool invocation to a policy
// script has to poll the store, which couples their latency to their polling
// interval and their correctness to their cursor handling.
//
// packages/hooks was written for exactly that and reached no runtime path. It
// carries third-party dependencies, so like the authorization engine it is
// adapted from outside the core rather than imported by it.
//
// Two rules, both of which the tests pin:
//
//   - A hook failure never fails the run. Notification is an enhancement, not a
//     dependency; a broken alerting script must not stop the work it was meant to
//     observe. Failures are reported to the runtime logger instead.
//   - Events describe what happened; they do not decide it. A hook cannot veto a
//     tool call — that is the authorizer's job, where a refusal is deliberate and
//     accounted for. Mixing the two would make a notification script into an
//     undeclared policy.

// LifecycleEventKind names a point in a task's life.
type LifecycleEventKind string

const (
	// LifecycleTaskStarted is published once a run has begun.
	LifecycleTaskStarted LifecycleEventKind = "task_started"
	// LifecycleTaskFinished is published when a run reaches a terminal state.
	LifecycleTaskFinished LifecycleEventKind = "task_finished"
	// LifecycleToolUsed is published after a tool call has been resolved.
	LifecycleToolUsed LifecycleEventKind = "tool_used"
)

// LifecycleEvent is what a hook bus receives.
//
// It carries identifiers and outcomes rather than a snapshot of the task: a bus
// that wanted the full state could read the journal, and copying it here would
// make every event as expensive as the largest task.
type LifecycleEvent struct {
	Kind      LifecycleEventKind
	TaskID    string
	TenantID  string
	SubjectID string
	Step      int
	// Status is the task or tool status at the point of publication.
	Status string
	// ToolName and ToolArgs are set for LifecycleToolUsed.
	ToolName string
	ToolArgs map[string]any
	// ErrorCode is set when the event reports a failure.
	ErrorCode string
	At        time.Time
}

// HookBus receives lifecycle events.
//
// Implementations must not block for long: Publish is called on the run's own
// path, so a slow bus delays the work it is observing. Implementations that do
// real I/O should queue and return.
type HookBus interface {
	Publish(ctx context.Context, event LifecycleEvent) error
}

// WithHookBus installs a lifecycle hook bus.
func WithHookBus(bus HookBus) Option {
	return func(c *runtimeConfig) error {
		c.hookBus = bus
		return nil
	}
}

// publishHook delivers an event on a best-effort basis.
//
// It never returns an error and never fails the run: a notification that cannot
// be delivered is a monitoring problem, and turning it into a task failure would
// let a broken alerting script stop the work. The failure is logged so it is not
// silent either.
func (r *Runtime) publishHook(ctx context.Context, event LifecycleEvent) {
	bus := r.hookBus
	if bus == nil {
		return
	}
	if event.At.IsZero() {
		event.At = clockOrSystem(r.clock).Now()
	}
	if err := bus.Publish(ctx, event); err != nil && r.logger != nil {
		r.logger.Debug("lifecycle hook delivery failed",
			"kind", string(event.Kind), "task_id", event.TaskID, "err", err)
	}
}
