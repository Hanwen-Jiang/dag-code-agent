package harness

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Watchdog: breaking runs that stop making progress.
//
// Cancellation and RunHandle let a CALLER stop a task. Neither helps when
// nothing is watching: a model that never returns, or a step that hangs, leaves
// a task holding the runtime lock indefinitely with no terminal state and no one
// to notice.
//
// The watchdog is deliberately a policy on the runtime rather than a scheduler
// layered on top, because the thing it must interrupt is the run itself, and
// only the run can be told to stop.
//
// Wall-clock, not the injected Clock: a watchdog exists to bound real elapsed
// time. Driving it from a FixedClock would mean a stalled task is never noticed
// in tests that fix the clock, which is exactly when a stall is easiest to miss.

// WatchdogPolicy bounds how long a task may run and how long it may say nothing.
type WatchdogPolicy struct {
	// MaxDuration cancels a task that runs longer than this. Zero disables it.
	MaxDuration time.Duration
	// StallTimeout cancels a task that produces no new journal events for this
	// long. Zero disables it.
	//
	// Progress is measured in events rather than in time since the last step,
	// because a single legitimate step — a large model call — can take minutes,
	// while a run that has genuinely stopped writing has stopped.
	StallTimeout time.Duration
	// CheckInterval is how often the policy is evaluated. Zero selects the
	// default, and a floor is applied so a misconfigured value cannot spin.
	CheckInterval time.Duration
}

const (
	defaultWatchdogInterval = 5 * time.Second
	minWatchdogInterval     = 10 * time.Millisecond
)

// WithWatchdog installs a watchdog for live runs. It is off by default: an
// embedder's definition of "too long" is theirs to choose, and a default
// deadline that killed healthy long-running work would be worse than none.
func WithWatchdog(policy WatchdogPolicy) Option {
	return func(c *runtimeConfig) error {
		if policy.MaxDuration < 0 || policy.StallTimeout < 0 {
			return errors.New("harness: WithWatchdog durations must not be negative")
		}
		c.watchdog = policy
		return nil
	}
}

// ErrWatchdogTimeout reports a task cancelled for exceeding its wall-clock bound.
var ErrWatchdogTimeout = errors.New("watchdog: task exceeded its time budget")

// ErrWatchdogStalled reports a task cancelled for producing no progress.
var ErrWatchdogStalled = errors.New("watchdog: task made no progress")

// startWatchdog begins supervising a live run, if a policy is configured.
//
// It returns immediately; the supervision goroutine ends when the run does.
func (r *Runtime) startWatchdog(runCtx context.Context, cancel context.CancelCauseFunc, taskID string) {
	policy := r.watchdog
	if policy.MaxDuration <= 0 && policy.StallTimeout <= 0 {
		return
	}
	interval := policy.CheckInterval
	if interval <= 0 {
		interval = defaultWatchdogInterval
	}
	if interval < minWatchdogInterval {
		interval = minWatchdogInterval
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		started := time.Now()
		lastCount, lastChange := -1, time.Now()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
			if policy.MaxDuration > 0 && time.Since(started) > policy.MaxDuration {
				cancel(fmt.Errorf("%w (limit %s)", ErrWatchdogTimeout, policy.MaxDuration))
				return
			}
			if policy.StallTimeout <= 0 {
				continue
			}
			count, err := r.eventCount(taskID)
			if err != nil {
				// A store that cannot be read is not evidence of a stall, and
				// cancelling on it would turn a transient read error into a
				// killed task.
				continue
			}
			if count != lastCount {
				lastCount, lastChange = count, time.Now()
				continue
			}
			if time.Since(lastChange) > policy.StallTimeout {
				cancel(fmt.Errorf("%w (no new events for %s)", ErrWatchdogStalled, policy.StallTimeout))
				return
			}
		}
	}()
}

// eventCount reports how many events a task has recorded.
func (r *Runtime) eventCount(taskID string) (int, error) {
	events, err := r.store.Events(taskID)
	if err != nil {
		return 0, err
	}
	return len(events), nil
}
