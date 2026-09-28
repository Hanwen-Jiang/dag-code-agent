package harness

import (
	"errors"
	"strings"
	"sync"
)

// Loop detection seam.
//
// The kernel used to decide this with a bare counter: hash each tool call, count
// how many times that fingerprint had been seen for the task, and fail the run
// once the count passed three. That can only ever kill the task — it has no way
// to warn the model and let it change strategy, which is the cheaper outcome in
// almost every case.
//
// The seam below keeps the termination guarantee while adding the warning. The
// default implementation is backed by packages/loopsdetect, whose eleven
// detectors and nudge escalation were implemented and tested but reached by no
// runtime path.

// LoopDecision is what the guard wants the kernel to do.
type LoopDecision int

const (
	// LoopContinue means nothing suspicious was found.
	LoopContinue LoopDecision = iota
	// LoopNudge means: do not run this batch, tell the model why, and let it
	// try a different approach. The kernel injects Directive as an observation.
	LoopNudge
	// LoopForceStop means: terminate the run with category LOOP.
	LoopForceStop
)

func (d LoopDecision) String() string {
	switch d {
	case LoopContinue:
		return "continue"
	case LoopNudge:
		return "nudge"
	case LoopForceStop:
		return "force_stop"
	default:
		return "unknown"
	}
}

// LoopObservation is what the guard sees: the model's most recent proposal plus
// whatever ran, so a guard can reason about repetition and lack of progress.
type LoopObservation struct {
	TaskID      string
	Step        int
	ToolCalls   []ToolCall
	ToolResults []ToolResult
	Text        string
}

// LoopVerdict is the guard's answer, carrying the evidence a Nudge needs to be
// actionable for the model.
type LoopVerdict struct {
	Decision LoopDecision
	Message  string
	Evidence []string
}

// Directive is the text injected into the context on a Nudge. It always names a
// concrete alternative, because "you are looping" alone tends to produce another
// identical call.
func (v LoopVerdict) Directive() string {
	var b strings.Builder
	b.WriteString("Loop guard: the previous attempt repeated a pattern that is not making progress.")
	if v.Message != "" {
		b.WriteString(" ")
		b.WriteString(v.Message)
	}
	if len(v.Evidence) > 0 {
		b.WriteString(" Evidence: ")
		b.WriteString(strings.Join(v.Evidence, "; "))
	}
	b.WriteString(" Change strategy: use different arguments, a different tool, or produce the final answer with what you already have.")
	return b.String()
}

// LoopGuard decides whether a run is stuck. Implementations must be safe for
// concurrent use: several tasks share one runtime.
type LoopGuard interface {
	// Check inspects the observation and returns a verdict.
	Check(observation LoopObservation) LoopVerdict
	// Reset clears per-task state, so a fresh task cannot inherit another
	// task's history.
	Reset(taskID string)
}

// WithLoopGuard replaces the default detector-backed guard.
func WithLoopGuard(guard LoopGuard) Option {
	return func(c *runtimeConfig) error {
		if guard == nil {
			return errNilLoopGuard
		}
		c.loopGuard = guard
		return nil
	}
}

// detectorLoopGuard adapts packages/loopsdetect to the seam.
//
// One manager per task is required, not one shared: the manager holds nudge
// counts and detector windows, so sharing it would let one task's repetition
// push another task over the escalation threshold.
type detectorLoopGuard struct {
	mu       sync.Mutex
	managers map[string]*loopManager
	history  int
}

// loopManager is the package-level detector manager plus the rolling call
// history its detectors inspect. The detectors reason over a window of calls,
// so the caller has to supply one.
type loopManager struct {
	manager *detectorManager
	recent  []detectorToolCall
}

// newDetectorLoopGuard builds the default guard.
func newDetectorLoopGuard() *detectorLoopGuard {
	return &detectorLoopGuard{managers: map[string]*loopManager{}, history: loopHistoryLimit}
}

// maxLoopSlack is the extra iterations allowed beyond the configured budgets.
//
// A legitimate iteration may consume nothing directly: a loop-guard nudge skips
// its batch after Step has already advanced. The slack absorbs those while still
// catching a strategy that consumes nothing at all.
const maxLoopSlack = 64

// loopHistoryLimit bounds the rolling window handed to the detectors.
const loopHistoryLimit = 64

func (g *detectorLoopGuard) managerFor(taskID string) *loopManager {
	if existing, ok := g.managers[taskID]; ok {
		return existing
	}
	created := &loopManager{manager: newDetectorManager()}
	g.managers[taskID] = created
	return created
}

func (g *detectorLoopGuard) Check(observation LoopObservation) LoopVerdict {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.managerFor(observation.TaskID)

	calls := make([]detectorToolCall, 0, len(observation.ToolCalls))
	for i, call := range observation.ToolCalls {
		entry := detectorToolCall{Name: call.Name, Input: call.Arguments, Iteration: observation.Step}
		if i < len(observation.ToolResults) {
			result := observation.ToolResults[i]
			entry.Status = detectorStatus(result.Status)
			entry.Output = detectorOutput(result.Value)
		}
		calls = append(calls, entry)
	}
	// The detectors inspect a window, so the current batch is appended to the
	// task's recent history rather than checked in isolation.
	state.recent = append(state.recent, calls...)
	if len(state.recent) > g.history {
		state.recent = state.recent[len(state.recent)-g.history:]
	}
	result := state.manager.Check(state.recent)
	if result == nil {
		return LoopVerdict{Decision: LoopContinue}
	}
	return LoopVerdict{
		Decision: detectorDecision(result.Decision),
		Message:  result.Message,
		Evidence: result.Evidence,
	}
}

func (g *detectorLoopGuard) Reset(taskID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.managers, taskID)
}

// detectorDecision maps the detector vocabulary onto the kernel's.
func detectorDecision(decision detectorDecisionKind) LoopDecision {
	switch decision {
	case detectorNudge:
		return LoopNudge
	case detectorForceStop:
		return LoopForceStop
	default:
		return LoopContinue
	}
}

// detectorStatus maps a harness tool status onto the detector vocabulary.
func detectorStatus(status string) string {
	switch status {
	case ToolSucceeded:
		return "success"
	case ToolFailed:
		return "error"
	default:
		// An unknown outcome is reported as a timeout-shaped failure rather than
		// a success: a detector must not be told a call worked when the kernel
		// could not establish that.
		return "timeout"
	}
}

// detectorOutput normalises a tool value into the map the detectors inspect.
func detectorOutput(value any) map[string]any {
	if value == nil {
		return nil
	}
	if asMap, ok := value.(map[string]any); ok {
		return asMap
	}
	return map[string]any{"value": value}
}

// errNilLoopGuard rejects a nil guard at option time rather than panicking in
// the middle of a run.
var errNilLoopGuard = errors.New("harness: WithLoopGuard requires a non-nil guard")

// compile-time assertion
var _ LoopGuard = (*detectorLoopGuard)(nil)
