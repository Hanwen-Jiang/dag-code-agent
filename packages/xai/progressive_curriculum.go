package xai

import (
	"fmt"
	"sort"
	"sync"
)

// ComplexityLevel is the curriculum difficulty scale (1..5).
type ComplexityLevel int

const (
	CurBeginner     ComplexityLevel = 1
	CurIntermediate ComplexityLevel = 2
	CurAdvanced     ComplexityLevel = 3
	CurExpert       ComplexityLevel = 4
	CurProficient   ComplexityLevel = 5
)

// LearnerState is the state machine state for one learner in one domain.
type LearnerState string

const (
	StateAssessment  LearnerState = "assessment" // probing level with low-difficulty tasks
	StateFoundation  LearnerState = "foundation"
	StateDevelopment LearnerState = "development"
	StateMastery     LearnerState = "mastery"
	StatePlateau     LearnerState = "plateau" // stalled: failed promotions repeatedly
)

// CurriculumLearner is the per-(learner, domain) progress record.
type CurriculumLearner struct {
	LearnerID     string
	Domain        string
	State         LearnerState
	Level         float64 // effective difficulty, [MinLevel, MaxLevel]
	SuccessStreak int
	FailStreak    int
	TotalAttempts int
	TotalSuccess  int
	// ConsecLowWindows counts consecutive windows at/below the demotion
	// threshold; drives the plateau transition.
	ConsecLowWindows int

	// successWindow/levelWindow are the last `Window` attempts (internal).
	successWindow []bool
	levelWindow   []ComplexityLevel
}

// CurriculumConfig tunes the promotion/demotion machine.
type CurriculumConfig struct {
	// Window is the number of most recent attempts considered.
	Window int
	// PromoteSuccessRate: >= this success rate over the window promotes.
	PromoteSuccessRate float64
	// DemoteSuccessRate: <= this success rate over the window demotes.
	DemoteSuccessRate float64
	MaxLevel          float64
	MinLevel          float64
	// StepSize is how far the effective level moves per promotion/demotion.
	StepSize float64
	// PlateauAfter counts consecutive low windows before a learner is
	// classified as plateaued instead of simply demoted.
	PlateauAfter int
}

// DefaultCurriculumConfig returns the standard curriculum settings.
func DefaultCurriculumConfig() CurriculumConfig {
	return CurriculumConfig{
		Window:             5,
		PromoteSuccessRate: 0.8,
		DemoteSuccessRate:  0.4,
		MaxLevel:           5,
		MinLevel:           1,
		StepSize:           1,
		PlateauAfter:       2,
	}
}

// TaskDifficulty is a gateable unit of curriculum work.
type TaskDifficulty struct {
	ID         string
	Level      ComplexityLevel // stage required to attempt
	Difficulty float64         // fine-grained difficulty [1,5]
}

// CurriculumEvent is the transition report for one recorded attempt.
type CurriculumEvent struct {
	LearnerID   string
	Domain      string
	Before      LearnerState
	After       LearnerState
	Promoted    bool
	Demoted     bool
	LevelBefore float64
	LevelAfter  float64
	Reason      string
}

// ProgressiveCurriculum is a stage machine: learners move between
// assessment -> foundation -> development -> mastery (with a plateau state
// for stalled learners) based on their recent success rate, with task
// difficulty gating. All transitions are deterministic.
type ProgressiveCurriculum struct {
	mu       sync.RWMutex
	config   CurriculumConfig
	stages   map[LearnerState]ComplexityLevel
	learners map[string]*CurriculumLearner
}

// NewProgressiveCurriculum builds the curriculum with the given config.
func NewProgressiveCurriculum(cfg CurriculumConfig) *ProgressiveCurriculum {
	if cfg.Window <= 0 || cfg.StepSize <= 0 {
		cfg = DefaultCurriculumConfig()
	}
	if cfg.MaxLevel <= cfg.MinLevel {
		cfg.MaxLevel, cfg.MinLevel = 5, 1
	}
	if cfg.PlateauAfter <= 0 {
		cfg.PlateauAfter = 2
	}
	return &ProgressiveCurriculum{
		config: cfg,
		stages: map[LearnerState]ComplexityLevel{
			StateAssessment:  CurBeginner,
			StateFoundation:  CurIntermediate,
			StateDevelopment: CurAdvanced,
			StateMastery:     CurExpert,
			StatePlateau:     CurBeginner,
		},
		learners: make(map[string]*CurriculumLearner),
	}
}

func learnerKey(learnerID, domain string) string {
	return learnerID + "@" + domain
}

// Stage returns the configured stage level for a learner in a domain; unseen
// learners are created in the assessment state.
func (c *ProgressiveCurriculum) Stage(learnerID, domain string) ComplexityLevel {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.learnerLocked(learnerID, domain)
	return c.stages[l.State]
}

func (c *ProgressiveCurriculum) learnerLocked(learnerID, domain string) *CurriculumLearner {
	key := learnerKey(learnerID, domain)
	l, ok := c.learners[key]
	if !ok {
		l = &CurriculumLearner{
			LearnerID:     learnerID,
			Domain:        domain,
			State:         StateAssessment,
			Level:         c.config.MinLevel,
			successWindow: make([]bool, 0, c.config.Window),
			levelWindow:   make([]ComplexityLevel, 0, c.config.Window),
		}
		c.learners[key] = l
	}
	return l
}

// GateTask reports whether a learner may attempt a task: the task's level must
// not exceed the learner's current stage. Plateaued learners may only attempt
// tasks at or below their stage. Unseen learners may attempt beginner-stage
// tasks during onboarding.
func (c *ProgressiveCurriculum) GateTask(learnerID, domain string, task TaskDifficulty) (bool, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := learnerKey(learnerID, domain)
	l, ok := c.learners[key]
	if !ok {
		if task.Level > CurBeginner {
			return false, "new learner may only start at beginner tasks"
		}
		return true, "new learner onboarding at beginner stage"
	}
	stage := c.stages[l.State]
	if task.Level > stage {
		if l.State == StatePlateau {
			return false, fmt.Sprintf("plateaued learner: task level %d above stage %d", task.Level, stage)
		}
		return false, fmt.Sprintf("task level %d above current stage %d (%s)", task.Level, stage, l.State)
	}
	return true, fmt.Sprintf("within current stage %d (%s)", stage, l.State)
}

// RecordAttempt feeds one attempt outcome into the state machine. Deterministic
// rules over the sliding window of the most recent `Window` attempts:
//
//   - success rate >= PromoteSuccessRate AND at least one window success on a
//     task at or above the current stage -> promote (next state,
//     level + StepSize, capped at MaxLevel).
//   - success rate <= DemoteSuccessRate -> demote (previous state,
//     level - StepSize, floored at MinLevel). After PlateauAfter consecutive
//     low windows, the learner enters plateau instead.
//   - otherwise the state is held.
func (c *ProgressiveCurriculum) RecordAttempt(learnerID, domain string, taskLevel ComplexityLevel, success bool) CurriculumEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.learnerLocked(learnerID, domain)
	ev := CurriculumEvent{
		LearnerID:   learnerID,
		Domain:      domain,
		Before:      l.State,
		LevelBefore: l.Level,
	}

	l.TotalAttempts++
	if success {
		l.TotalSuccess++
		l.SuccessStreak++
		l.FailStreak = 0
	} else {
		l.FailStreak++
		l.SuccessStreak = 0
	}

	rate, gated := c.windowStats(l, taskLevel, success)

	// A healthy window clears the consecutive-low-window streak.
	if rate > c.config.DemoteSuccessRate {
		l.ConsecLowWindows = 0
	}

	switch {
	case rate >= c.config.PromoteSuccessRate && gated && l.State != StateMastery:
		l.State = nextStage(l.State)
		l.Level = minF(c.config.MaxLevel, l.Level+c.config.StepSize)
		ev.Promoted = true
		ev.Reason = fmt.Sprintf("success rate %.2f over last %d attempts with stage-gated success -> promote to %s",
			rate, c.config.Window, l.State)
	case rate <= c.config.DemoteSuccessRate:
		// This window is at/below the demotion threshold.
		l.ConsecLowWindows++
		if l.ConsecLowWindows >= c.config.PlateauAfter {
			// Repeatedly stalled: classify as plateaued (from any state).
			l.State = StatePlateau
			l.ConsecLowWindows = 0
			ev.Reason = fmt.Sprintf("success rate %.2f for %d consecutive low windows -> plateau",
				rate, c.config.PlateauAfter)
		} else if l.State != StateAssessment {
			l.State = prevStage(l.State)
			l.Level = maxF(c.config.MinLevel, l.Level-c.config.StepSize)
			ev.Demoted = true
			ev.Reason = fmt.Sprintf("success rate %.2f -> demote to %s (low window %d/%d)",
				rate, l.State, l.ConsecLowWindows, c.config.PlateauAfter)
		} else {
			ev.Reason = fmt.Sprintf("success rate %.2f at difficulty floor; hold assessment (low window %d/%d)",
				rate, l.ConsecLowWindows, c.config.PlateauAfter)
		}
	default:
		ev.Reason = fmt.Sprintf("success rate %.2f within [%.2f, %.2f]: hold %s",
			rate, c.config.DemoteSuccessRate, c.config.PromoteSuccessRate, l.State)
	}

	ev.After = l.State
	ev.LevelAfter = l.Level
	return ev
}

// windowStats records the attempt in the learner's ring buffers and returns
// (rate over window, promotion-gate satisfied). The gate requires at least
// one success within the window on a task at or above the learner's current
// stage — i.e. demonstrated ability at that difficulty, not just lucky easy
// wins.
func (c *ProgressiveCurriculum) windowStats(l *CurriculumLearner, taskLevel ComplexityLevel, success bool) (float64, bool) {
	l.successWindow = append(l.successWindow, success)
	l.levelWindow = append(l.levelWindow, taskLevel)
	if len(l.successWindow) > c.config.Window {
		l.successWindow = l.successWindow[len(l.successWindow)-c.config.Window:]
		l.levelWindow = l.levelWindow[len(l.levelWindow)-c.config.Window:]
	}
	succ := 0
	for _, s := range l.successWindow {
		if s {
			succ++
		}
	}
	rate := float64(succ) / float64(len(l.successWindow))

	stage := c.stages[l.State]
	gated := false
	for i, s := range l.successWindow {
		if s && l.levelWindow[i] >= stage {
			gated = true
			break
		}
	}
	return rate, gated
}

func nextStage(s LearnerState) LearnerState {
	switch s {
	case StateAssessment:
		return StateFoundation
	case StateFoundation:
		return StateDevelopment
	case StateDevelopment:
		return StateMastery
	default:
		return s
	}
}

func prevStage(s LearnerState) LearnerState {
	switch s {
	case StateMastery:
		return StateDevelopment
	case StateDevelopment:
		return StateFoundation
	case StateFoundation, StatePlateau:
		return StateAssessment
	default:
		return s
	}
}

// State returns the current state and level for a learner in a domain.
func (c *ProgressiveCurriculum) State(learnerID, domain string) (LearnerState, float64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	key := learnerKey(learnerID, domain)
	if l, ok := c.learners[key]; ok {
		return l.State, l.Level
	}
	return StateAssessment, c.config.MinLevel
}

// Leaderboard ranks learners in a domain by (success rate, level), descending.
func (c *ProgressiveCurriculum) Leaderboard(domain string) []CurriculumLearner {
	c.mu.RLock()
	defer c.mu.RUnlock()
	type row struct {
		l    *CurriculumLearner
		rate float64
	}
	var rows []row
	for _, l := range c.learners {
		if l.Domain != domain {
			continue
		}
		rate := 0.0
		if l.TotalAttempts > 0 {
			rate = float64(l.TotalSuccess) / float64(l.TotalAttempts)
		}
		rows = append(rows, row{l: l, rate: rate})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].rate != rows[j].rate {
			return rows[i].rate > rows[j].rate
		}
		if rows[i].l.Level != rows[j].l.Level {
			return rows[i].l.Level > rows[j].l.Level
		}
		return rows[i].l.LearnerID < rows[j].l.LearnerID
	})
	out := make([]CurriculumLearner, 0, len(rows))
	for _, r := range rows {
		out = append(out, *r.l)
	}
	return out
}
