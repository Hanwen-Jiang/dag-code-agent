package harness

import (
	"context"
	"errors"
	"sync"
)

// RunHandle lets an embedder start a task without blocking on it.
//
// Runtime.Run holds the runtime lock for the whole duration of a task, so a host
// that calls it directly either blocks a request thread for minutes or invents
// its own goroutine and bookkeeping. A handle makes that bookkeeping part of the
// API: start, observe, wait, cancel.
//
// It deliberately owns no state that the journal does not already hold. Status
// reads the store, cancellation goes through the same registry Cancel uses, and
// the terminal result is whatever the run produced — so a handle cannot drift
// from the durable truth.

// ErrRunHandleClosed reports a wait on a handle whose result was already taken
// in a way that cannot be repeated.
var ErrRunHandleClosed = errors.New("harness: run handle is closed")

// RunHandle is a live or finished task started with Start.
type RunHandle struct {
	// TaskID is the durable task id, so a host can persist it and resume later.
	TaskID string

	runtime *Runtime
	auth    AuthContext
	done    chan struct{}

	mu      sync.Mutex
	result  RunResult
	err     error
	settled bool
}

// Start begins a task and returns immediately.
//
// The context governs the run exactly as it does for Run: cancelling it cancels
// the task.
func (r *Runtime) Start(ctx context.Context, request TaskRequest, auth AuthContext) *RunHandle {
	handle := &RunHandle{TaskID: request.TaskID, runtime: r, auth: auth, done: make(chan struct{})}
	go func() {
		result, err := r.Run(ctx, request, auth)
		handle.mu.Lock()
		handle.result, handle.err, handle.settled = result, err, true
		handle.mu.Unlock()
		close(handle.done)
	}()
	return handle
}

// Done is closed when the task reaches a terminal state.
func (h *RunHandle) Done() <-chan struct{} {
	if h == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return h.done
}

// Wait blocks until the task settles or the caller's context expires.
//
// The caller's context is separate from the run's: giving up on waiting is not
// the same decision as cancelling the task, and conflating them would make a
// timeout silently kill work.
func (h *RunHandle) Wait(ctx context.Context) (RunResult, error) {
	if h == nil {
		return RunResult{}, ErrRunHandleClosed
	}
	select {
	case <-h.done:
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.result, h.err
	case <-ctx.Done():
		return RunResult{}, ctx.Err()
	}
}

// Settled reports whether the task has finished, without blocking.
func (h *RunHandle) Settled() bool {
	if h == nil {
		return true
	}
	select {
	case <-h.done:
		return true
	default:
		return false
	}
}

// Cancel asks the task to stop.
//
// It goes through Runtime.Cancel, so a handle has no privileged path: the
// request is durable and goes through the same identity check as any other
// caller's.
func (h *RunHandle) Cancel(ctx context.Context) (RunResult, error) {
	if h == nil {
		return RunResult{}, ErrRunHandleClosed
	}
	return h.runtime.Cancel(h.TaskID, h.auth)
}

// Status reads the durable task state.
//
// It reads the store rather than any cached value, so it reports what is
// actually recorded — including a task this handle did not start, such as one
// resumed by another process.
func (h *RunHandle) Status() (TaskState, error) {
	if h == nil {
		return TaskState{}, ErrRunHandleClosed
	}
	state, _, err := h.runtime.store.GetTask(h.TaskID)
	return state, err
}

// Running reports whether the runtime currently has a live run for this task.
func (h *RunHandle) Running() bool {
	if h == nil {
		return false
	}
	return h.runtime.runCancelFunc(h.TaskID) != nil
}
