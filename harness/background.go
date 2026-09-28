package harness

import (
	"context"
	"sort"
)

// Background agents: tasks whose lifetime is not the caller's.
//
// Start ties a run to the caller's context, which is right for a request and
// wrong for work that must survive one — a long investigation, a scheduled
// sweep, a task the host wants to poll rather than await. Cancelling the
// caller's context would otherwise kill work nobody asked to stop.
//
// Two things make that safe rather than merely convenient:
//
//   - The runtime owns the context, not context.Background(). A task detached
//     from everything would outlive Close and leak.
//   - The handle is registered with the runtime, so a host can enumerate what is
//     running instead of tracking it in parallel bookkeeping that can drift.

// StartBackground begins a task detached from the caller's context.
//
// The request's context is not consulted: only Runtime.Close, an explicit
// Cancel, or the watchdog stops a background task.
func (r *Runtime) StartBackground(request TaskRequest, auth AuthContext) (*RunHandle, error) {
	if r == nil {
		return nil, ErrRunHandleClosed
	}
	if err := validateIdentity(request, auth); err != nil {
		return nil, err
	}
	ctx := r.baseCtx
	if ctx == nil {
		// A runtime built without New (tests, or the deprecated constructor)
		// still has to give the task a context that Close can reach.
		ctx = context.Background()
	}
	handle := r.startWithContext(ctx, request, auth)
	r.trackBackground(handle)
	return handle, nil
}

// startWithContext is Start with an explicit context, so the background path
// does not have to duplicate the goroutine and result plumbing.
func (r *Runtime) startWithContext(ctx context.Context, request TaskRequest, auth AuthContext) *RunHandle {
	handle := &RunHandle{TaskID: request.TaskID, runtime: r, auth: auth, done: make(chan struct{})}
	go func() {
		result, err := r.Run(ctx, request, auth)
		handle.mu.Lock()
		handle.result, handle.err, handle.settled = result, err, true
		handle.mu.Unlock()
		close(handle.done)
		r.untrackBackground(request.TaskID)
	}()
	return handle
}

func (r *Runtime) trackBackground(handle *RunHandle) {
	r.backgroundMu.Lock()
	defer r.backgroundMu.Unlock()
	if r.background == nil {
		r.background = map[string]*RunHandle{}
	}
	r.background[handle.TaskID] = handle
}

func (r *Runtime) untrackBackground(taskID string) {
	r.backgroundMu.Lock()
	defer r.backgroundMu.Unlock()
	delete(r.background, taskID)
}

// BackgroundTasks lists the background tasks that have not settled, ordered by
// task id so the result is stable.
//
// A settled task removes itself: the map is a view of what is running, not a
// history. The durable record is the journal.
func (r *Runtime) BackgroundTasks() []string {
	r.backgroundMu.Lock()
	defer r.backgroundMu.Unlock()
	ids := make([]string, 0, len(r.background))
	for id := range r.background {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// ResumableTasks lists durable tasks that are not terminal.
//
// This is what makes background work survive a restart. A task whose process
// died mid-run is left non-terminal in the store, and without a way to find it
// the work would be silently abandoned — durable only in the sense that nobody
// cleaned it up. Resuming one is Runtime.Run with the same task id.
func (r *Runtime) ResumableTasks(tenantID string) ([]string, error) {
	entries, err := r.store.ListTasks()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		state, _, err := r.store.GetTask(entry.TaskID)
		if err != nil {
			continue
		}
		if tenantID != "" && state.TenantID != tenantID {
			continue
		}
		switch state.Status {
		case TaskCompleted, TaskFailed, TaskCancelled:
			continue
		}
		ids = append(ids, state.TaskID)
	}
	sort.Strings(ids)
	return ids, nil
}
