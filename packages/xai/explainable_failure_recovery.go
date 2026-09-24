package xai

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// FailureClass is a deterministic category of agent failure.
type FailureClass string

const (
	ClassTransient     FailureClass = "transient"     // network blips, timeouts, 5xx
	ClassDeterministic FailureClass = "deterministic" // bugs, invalid input, permissions
	ClassCapability    FailureClass = "capability"    // task exceeds model/tool capability
	ClassLoop          FailureClass = "loop"          // stuck repeating without progress
	ClassUnknown       FailureClass = "unknown"
)

// BackoffSchedule is a deterministic exponential backoff: base * factor^(attempt-1),
// capped.
type BackoffSchedule struct {
	BaseMS float64
	Factor float64
	MaxMS  float64
}

// DefaultBackoff is 100ms doubling up to 5s.
var DefaultBackoff = BackoffSchedule{BaseMS: 100, Factor: 2, MaxMS: 5000}

// Delay returns the wait before attempt n (1-based).
func (s *BackoffSchedule) Delay(attempt int) float64 {
	if attempt <= 0 {
		attempt = 1
	}
	d := s.BaseMS
	for i := 1; i < attempt; i++ {
		d *= s.Factor
		if d > s.MaxMS {
			return s.MaxMS
		}
	}
	if d > s.MaxMS {
		return s.MaxMS
	}
	return d
}

// RecoveryAction is the concrete action to take.
type RecoveryAction string

const (
	ActionRetryWithBackoff  RecoveryAction = "retry_with_backoff"
	ActionDegradeCapability RecoveryAction = "degrade_capability"
	ActionFallbackModel     RecoveryAction = "fallback_model"
	ActionEscalateHuman     RecoveryAction = "escalate_to_human"
)

// FailureContext describes the failure at decision time.
type FailureContext struct {
	ErrorMessage  string
	ToolName      string
	Iteration     int
	Attempt       int // how many times this same operation was already tried
	RetriesLeft   int // retry budget remaining
	HasFallback   bool
	BudgetRemains bool
}

// RecoveryDecision is the selected strategy plus a deterministic explanation.
type RecoveryDecision struct {
	Class       FailureClass   `json:"class"`
	Action      RecoveryAction `json:"action"`
	AtMost      int            `json:"at_most"` // for retry: remaining attempts
	BackoffMS   []float64      `json:"backoff_ms,omitempty"`
	Explanation string         `json:"explanation"`
}

// FailureRecoveryPolicy deterministically classifies failures and selects
// recovery strategies (retry with backoff / degrade capability / fallback
// model / escalate to human). It also learns from observed recovery outcomes:
// for each (class, action) pair it tracks successes/total so that preference
// tie-breaks favor actions that historically worked.
type FailureRecoveryPolicy struct {
	mu      sync.Mutex
	backoff BackoffSchedule
	// outcomes[class][action] = [successes, total]
	outcomes map[FailureClass]map[RecoveryAction][2]int
}

// NewFailureRecoveryPolicy creates a policy with the default backoff schedule.
func NewFailureRecoveryPolicy() *FailureRecoveryPolicy {
	return &FailureRecoveryPolicy{
		backoff:  DefaultBackoff,
		outcomes: make(map[FailureClass]map[RecoveryAction][2]int),
	}
}

// SetBackoff overrides the backoff schedule.
func (p *FailureRecoveryPolicy) SetBackoff(s BackoffSchedule) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.BaseMS > 0 && s.Factor >= 1 {
		p.backoff = s
	}
}

// ClassifyFailure maps an error message to a deterministic failure class.
func ClassifyFailure(msg string) FailureClass {
	m := strings.ToLower(msg)
	switch {
	case strings.Contains(m, "loop") || strings.Contains(m, "repeated") || strings.Contains(m, "no progress"):
		return ClassLoop
	case strings.Contains(m, "permission") || strings.Contains(m, "denied") ||
		strings.Contains(m, "invalid") || strings.Contains(m, "syntax") ||
		strings.Contains(m, "parse") || strings.Contains(m, "not found") ||
		strings.Contains(m, "type error") || strings.Contains(m, "nil pointer") ||
		strings.Contains(m, "division by zero") || strings.Contains(m, "assert"):
		return ClassDeterministic
	case strings.Contains(m, "timeout") || strings.Contains(m, "timed out") ||
		strings.Contains(m, "unavailable") || strings.Contains(m, "connection") ||
		strings.Contains(m, "rate limit") || strings.Contains(m, "503") ||
		strings.Contains(m, "502") || strings.Contains(m, "econnreset") ||
		strings.Contains(m, "deadline exceeded") || strings.Contains(m, "temporarily"):
		return ClassTransient
	case strings.Contains(m, "capability") || strings.Contains(m, "too complex") ||
		strings.Contains(m, "cannot") || strings.Contains(m, "unsupported") ||
		strings.Contains(m, "context overflow") || strings.Contains(m, "too many tokens"):
		return ClassCapability
	default:
		return ClassUnknown
	}
}

// Decide selects the recovery strategy for a failure. Deterministic rules:
//
//   - loop: never retry the same path (it would repeat); degrade capability.
//   - deterministic: re-running the same inputs fails identically, so degrade
//     capability (single confirmation retry only on the very first attempt).
//   - transient: retry with backoff while retry budget and budget remain;
//     otherwise fallback model, else escalate to a human.
//   - capability: fallback model if one exists, else degrade the task.
//   - unknown: one exploratory retry if this is the first attempt, then
//     fallback, then escalate.
func (p *FailureRecoveryPolicy) Decide(ctx FailureContext) RecoveryDecision {
	p.mu.Lock()
	defer p.mu.Unlock()

	class := ClassifyFailure(ctx.ErrorMessage)
	backoff := p.backoff

	decide := func(action RecoveryAction, explanation string) RecoveryDecision {
		return RecoveryDecision{Class: class, Action: action, Explanation: explanation}
	}

	// withRetry fills in the attempt count and the concrete backoff delays.
	withRetry := func(d RecoveryDecision, attempts int) RecoveryDecision {
		if attempts < 0 {
			attempts = 0
		}
		d.AtMost = attempts
		d.BackoffMS = make([]float64, 0, attempts)
		for a := ctx.Attempt + 1; a <= ctx.Attempt+attempts; a++ {
			d.BackoffMS = append(d.BackoffMS, backoff.Delay(a))
		}
		return d
	}

	switch class {
	case ClassLoop:
		return decide(ActionDegradeCapability,
			fmt.Sprintf("failure looks like a loop (%q); retrying the same path would repeat it, so degrade capability (shorter step budget / simpler plan)", ctx.ErrorMessage))

	case ClassDeterministic:
		if ctx.Attempt == 0 && ctx.RetriesLeft > 0 && ctx.BudgetRemains {
			return withRetry(decide(ActionRetryWithBackoff,
				fmt.Sprintf("deterministic fault (%q) on first attempt: single retry to confirm it is reproducible, then degrade", ctx.ErrorMessage)), 1)
		}
		return decide(ActionDegradeCapability,
			fmt.Sprintf("deterministic fault (%q): re-running the same inputs will fail identically, so degrade capability or change approach", ctx.ErrorMessage))

	case ClassTransient:
		if ctx.RetriesLeft > 0 && ctx.BudgetRemains {
			return withRetry(decide(ActionRetryWithBackoff,
				fmt.Sprintf("transient fault (%q): retry with exponential backoff (%d attempt(s) left in budget)", ctx.ErrorMessage, ctx.RetriesLeft)), ctx.RetriesLeft)
		}
		if ctx.HasFallback {
			return decide(ActionFallbackModel,
				fmt.Sprintf("transient fault (%q) with no retry budget: switch to fallback provider", ctx.ErrorMessage))
		}
		return decide(ActionEscalateHuman,
			fmt.Sprintf("transient fault (%q) persists and no fallback exists: escalate to a human", ctx.ErrorMessage))

	case ClassCapability:
		if ctx.HasFallback {
			return decide(ActionFallbackModel,
				fmt.Sprintf("capability gap (%q): route to a fallback model with more capability", ctx.ErrorMessage))
		}
		return decide(ActionDegradeCapability,
			fmt.Sprintf("capability gap (%q) and no fallback: degrade the task (split it or reduce scope)", ctx.ErrorMessage))

	default: // unknown
		if ctx.Attempt == 0 && ctx.RetriesLeft > 0 && ctx.BudgetRemains {
			return withRetry(decide(ActionRetryWithBackoff,
				fmt.Sprintf("unknown failure (%q): one exploratory retry with backoff before escalating", ctx.ErrorMessage)), 1)
		}
		if ctx.HasFallback {
			return decide(ActionFallbackModel,
				fmt.Sprintf("unknown failure (%q): try a fallback model", ctx.ErrorMessage))
		}
		return decide(ActionEscalateHuman,
			fmt.Sprintf("unknown failure (%q) with no recovery path: escalate to a human", ctx.ErrorMessage))
	}
}

// Observe records the outcome of a recovery so future preference calls can
// favor historically successful actions.
func (p *FailureRecoveryPolicy) Observe(class FailureClass, action RecoveryAction, success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.outcomes[class]
	if !ok {
		m = make(map[RecoveryAction][2]int)
		p.outcomes[class] = m
	}
	v := m[action]
	v[1]++
	if success {
		v[0]++
	}
	m[action] = v
}

// actionRank is the static priority (lower = tried first) used by
// LearnedPreference when no observations exist yet.
var actionRank = map[RecoveryAction]int{
	ActionRetryWithBackoff:  0,
	ActionDegradeCapability: 1,
	ActionFallbackModel:     2,
	ActionEscalateHuman:     3,
}

// LearnedPreference ranks candidate actions for a class using observed success
// rates (actions with observations first, best rate first), falling back to
// the static priority for actions without data.
func (p *FailureRecoveryPolicy) LearnedPreference(class FailureClass, candidates ...RecoveryAction) []RecoveryAction {
	p.mu.Lock()
	defer p.mu.Unlock()

	type ranked struct {
		action RecoveryAction
		rate   float64
		total  int
		rank   int
	}
	var rs []ranked
	for _, a := range candidates {
		r, ok := actionRank[a]
		if !ok {
			r = 99
		}
		item := ranked{action: a, rank: r}
		if m, ok := p.outcomes[class]; ok {
			if v, ok := m[a]; ok && v[1] > 0 {
				item.rate = float64(v[0]) / float64(v[1])
				item.total = v[1]
			}
		}
		rs = append(rs, item)
	}
	sort.SliceStable(rs, func(i, j int) bool {
		if (rs[i].total > 0) != (rs[j].total > 0) {
			return rs[i].total > 0
		}
		if rs[i].rate != rs[j].rate {
			return rs[i].rate > rs[j].rate
		}
		return rs[i].rank < rs[j].rank
	})
	out := make([]RecoveryAction, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.action)
	}
	return out
}

// SuccessRate reports the observed success rate for a (class, action) pair.
// The boolean is false when no observations exist yet.
func (p *FailureRecoveryPolicy) SuccessRate(class FailureClass, action RecoveryAction) (float64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.outcomes[class]
	if !ok {
		return 0, false
	}
	v, ok := m[action]
	if !ok || v[1] == 0 {
		return 0, false
	}
	return float64(v[0]) / float64(v[1]), true
}
