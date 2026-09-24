package harness

import (
	"sync"
	"time"
)

// Clock is the time source for every durable decision in the control plane.
//
// Embedders inject a clock so that lease expiry, idempotency deadlines,
// checkpoint timestamps and event ordering can be tested deterministically
// instead of with time.Sleep. The zero value is not usable; use SystemClock
// (the default) or FixedClock/SteppedClock in tests.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// SystemClock returns the default wall-clock source.
func SystemClock() Clock { return systemClock{} }

type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

// FixedClock returns a clock that never advances. It is useful for tests that
// only care about timestamp shape, not ordering.
func FixedClock(at time.Time) Clock { return fixedClock{at: at} }

type steppedClock struct {
	mu    sync.Mutex
	at    time.Time
	step  time.Duration
	calls int
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The first reading is the base time; each subsequent reading advances by
	// step so that a test can drive lease/idempotency expiry by calling Now.
	if c.calls > 0 {
		c.at = c.at.Add(c.step)
	}
	c.calls++
	return c.at
}

// SteppedClock returns a clock that advances by step on each reading after the
// first. A test can therefore cross a lease TTL with a known number of reads.
func SteppedClock(at time.Time, step time.Duration) Clock {
	return &steppedClock{at: at, step: step}
}

// Advance moves a stepped clock forward without consuming a reading. It is a
// no-op for clocks that do not support manual movement.
func Advance(clock Clock, d time.Duration) {
	if stepped, ok := clock.(*steppedClock); ok {
		stepped.mu.Lock()
		defer stepped.mu.Unlock()
		stepped.at = stepped.at.Add(d)
	}
}

// utcNow formats a clock reading as the canonical RFC3339Nano UTC timestamp
// used by the event journal, checkpoints and artifacts.
func utcNow(clock Clock) string {
	if clock == nil {
		clock = SystemClock()
	}
	return clock.Now().UTC().Format(time.RFC3339Nano)
}

// clockOrSystem normalizes a possibly-nil clock.
func clockOrSystem(clock Clock) Clock {
	if clock == nil {
		return SystemClock()
	}
	return clock
}
