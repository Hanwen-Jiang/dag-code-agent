// Package loopsdetect implements loop detection algorithms to prevent infinite
// execution cycles in long-running agent tasks.
//
// Each detector returns one of three decisions:
//
//	Continue  - normal operation, nothing detected
//	Nudge     - warn the LLM and give it a chance to adjust strategy
//	ForceStop - inject a final message and terminate without tools
//
// Escalation: if a Nudge is produced 3 consecutive manager checks in a row,
// the manager automatically escalates to ForceStop.
package loopsdetect

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Detector is the loop detection contract.
type Detector interface {
	Name() string
	Check(calls []ToolCall) Decision
}

// Decision represents the action taken by a detector.
type Decision int

const (
	Continue Decision = iota // normal operation
	Nudge                    // warn and give the LLM a chance to adjust
	ForceStop                // terminate immediately
)

func (d Decision) String() string {
	switch d {
	case Continue:
		return "continue"
	case Nudge:
		return "nudge"
	case ForceStop:
		return "force_stop"
	default:
		return "unknown"
	}
}

// MaxDecision returns the higher-priority decision of two.
func MaxDecision(d1, d2 Decision) Decision {
	if d2 > d1 {
		return d2
	}
	return d1
}

// AllDecisions returns the highest-priority decision from a list.
func AllDecisions(decisions []Decision) Decision {
	max := Continue
	for _, d := range decisions {
		if d > max {
			max = d
		}
	}
	return max
}

// DetectorResult combines a decision with detailed information.
type DetectorResult struct {
	DetectorName string
	Decision     Decision
	Message      string
	Evidence     []string // evidence supporting this decision
	Thresholds   ThresholdConfig
}

// ToolCall represents a tool invocation record.
type ToolCall struct {
	Name         string
	Input        map[string]interface{}
	Output       map[string]interface{}
	Status       string // success, error, timeout
	Timestamp    time.Time
	Iteration    int
	ExecutionMs  float64
	ParentCallID string // for call hierarchy tracking
}

// ThresholdConfig holds configurable thresholds for the detectors.
type ThresholdConfig struct {
	ConsecutiveSameCall int // consecutive identical calls before Nudge
	SlidingWindowSize   int // size of the analyzed window
	SearchEscalation    int // consecutive searches before warning
	SleepFrequency      int // sleep calls in the window before warning
	NoProgressLimit     int // repeats of a single tool before warning
}

// DefaultThresholds returns production-tested default thresholds.
func DefaultThresholds() ThresholdConfig {
	return ThresholdConfig{
		ConsecutiveSameCall: 2, // trigger on 2 consecutive duplicates
		SlidingWindowSize:   20,
		SearchEscalation:    3,
		SleepFrequency:      2,
		NoProgressLimit:     8,
	}
}

// ============================================
// DETECTOR 1: ConsecutiveDuplicate
// Same tool + identical arguments called N times in a row.
// ============================================
type ConsecutiveDuplicateDetector struct {
	consecutive int
}

func NewConsecutiveDuplicateDetector(consecutive int) *ConsecutiveDuplicateDetector {
	if consecutive < 2 {
		consecutive = 2
	}
	return &ConsecutiveDuplicateDetector{consecutive: consecutive}
}

func (d *ConsecutiveDuplicateDetector) Name() string { return "ConsecutiveDuplicate" }

func (d *ConsecutiveDuplicateDetector) Check(calls []ToolCall) Decision {
	if len(calls) < d.consecutive {
		return Continue
	}
	run := 1
	for i := len(calls) - 1; i > 0; i-- {
		if callFingerprint(calls[i]) == callFingerprint(calls[i-1]) {
			run++
			if run >= d.consecutive {
				return Nudge
			}
		} else {
			run = 1
		}
	}
	return Continue
}

// ============================================
// DETECTOR 2: ExactDuplicate
// Same tool + identical arguments appearing minCount times in a sliding window.
// ============================================
type ExactDuplicateDetector struct {
	windowSize int
	minCount   int
}

func NewExactDuplicateDetector(windowSize, minCount int) *ExactDuplicateDetector {
	return &ExactDuplicateDetector{windowSize: windowSize, minCount: minCount}
}

func (d *ExactDuplicateDetector) Name() string { return "ExactDuplicate" }

func (d *ExactDuplicateDetector) Check(calls []ToolCall) Decision {
	window := getSlidingWindow(calls, d.windowSize)
	if len(window) < d.minCount {
		return Continue
	}
	countMap := make(map[string]int)
	for _, call := range window {
		countMap[callFingerprint(call)]++
	}
	for _, count := range countMap {
		if count >= d.minCount {
			return Nudge
		}
	}
	return Continue
}

// ============================================
// DETECTOR 3: SameToolError
// The same tool erroring minErrors consecutive times.
// ============================================
type SameToolErrorDetector struct {
	minErrors int
}

func NewSameToolErrorDetector(minErrors int) *SameToolErrorDetector {
	return &SameToolErrorDetector{minErrors: minErrors}
}

func (d *SameToolErrorDetector) Name() string { return "SameToolError" }

func (d *SameToolErrorDetector) Check(calls []ToolCall) Decision {
	if len(calls) < d.minErrors {
		return Continue
	}
	run := 0
	lastTool := ""
	for i := len(calls) - 1; i >= 0; i-- {
		call := calls[i]
		if !isErrorStatus(call.Status) {
			break
		}
		if lastTool == "" {
			lastTool = call.Name
		}
		if call.Name != lastTool {
			break
		}
		run++
		if run >= d.minErrors {
			return Nudge
		}
	}
	return Continue
}

func isErrorStatus(status string) bool {
	switch status {
	case "error", "failed", "timeout":
		return true
	}
	return false
}

// ============================================
// DETECTOR 4: FamilyNoProgress
// Same tool family called many times on similar inputs in the window.
// ============================================
type FamilyNoProgressDetector struct {
	toolFamilies map[string]string
	windowSize   int
}

// NewFamilyNoProgressDetector creates the detector with the built-in families.
func NewFamilyNoProgressDetector() *FamilyNoProgressDetector {
	return &FamilyNoProgressDetector{
		windowSize: 10,
		toolFamilies: map[string]string{
			"bash":         "shell",
			"shell_exec":   "shell",
			"git_operations": "version_control",
			"git_commit":   "version_control",
			"git_push":     "version_control",
			"web_search":   "information_gathering",
			"http_request": "information_gathering",
			"code_edit":    "development",
			"file_write":   "development",
		},
	}
}

func (d *FamilyNoProgressDetector) Name() string { return "FamilyNoProgress" }

func (d *FamilyNoProgressDetector) Check(calls []ToolCall) Decision {
	window := getSlidingWindow(calls, d.windowSize)
	familyCounts := make(map[string]int)
	for _, call := range window {
		if family := d.toolFamilies[call.Name]; family != "" {
			familyCounts[family]++
		}
	}
	// Thresholds: 7+ = ForceStop, 5+ = Nudge.
	for _, count := range familyCounts {
		if count >= 7 {
			return ForceStop
		}
	}
	for _, count := range familyCounts {
		if count >= 5 {
			return Nudge
		}
	}
	return Continue
}

// ============================================
// DETECTOR 5: SearchEscalation
// Excessive consecutive searching.
// ============================================
type SearchEscalationDetector struct {
	searchTools []string
	warningTh   int
	stopTh      int
}

func NewSearchEscalationDetector(warningTh, stopTh int) *SearchEscalationDetector {
	return &SearchEscalationDetector{
		searchTools:   []string{"search", "http_get", "web_lookup", "web_search"},
		warningTh:     warningTh,
		stopTh:        stopTh,
	}
}

func (d *SearchEscalationDetector) Name() string { return "SearchEscalation" }

func (d *SearchEscalationDetector) Check(calls []ToolCall) Decision {
	window := getSlidingWindow(calls, 10)
	consecutiveSearches := 0
	for i := len(window) - 1; i >= 0; i-- {
		if d.isSearchTool(window[i].Name) {
			consecutiveSearches++
			continue
		}
		break
	}
	if consecutiveSearches >= d.stopTh {
		return ForceStop
	}
	if consecutiveSearches >= d.warningTh {
		return Nudge
	}
	return Continue
}

func (d *SearchEscalationDetector) isSearchTool(name string) bool {
	for _, tool := range d.searchTools {
		if strings.Contains(name, tool) {
			return true
		}
	}
	return false
}

// ============================================
// DETECTOR 6: NoProgress
// Any single tool repeated limit+ times in the window (broad safety net).
// ============================================
type NoProgressDetector struct {
	limit int
}

func NewNoProgressDetector(limit int) *NoProgressDetector { return &NoProgressDetector{limit: limit} }

func (d *NoProgressDetector) Name() string { return "NoProgress" }

func (d *NoProgressDetector) Check(calls []ToolCall) Decision {
	window := getSlidingWindow(calls, d.limit*2)
	counts := make(map[string]int)
	for _, call := range window {
		counts[call.Name]++
	}
	for _, count := range counts {
		if count >= d.limit {
			return Nudge
		}
	}
	return Continue
}

// ============================================
// DETECTOR 7: ToolModeSwitch
// A visual capture tool called right after a successful GUI operation.
// ============================================
type ToolModeSwitchDetector struct {
	guiTools    map[string]bool
	visualTools map[string]bool
}

func NewToolModeSwitchDetector() *ToolModeSwitchDetector {
	return &ToolModeSwitchDetector{
		guiTools: map[string]bool{
			"click_element": true,
			"type_text":     true,
			"select_option": true,
		},
		visualTools: map[string]bool{
			"take_screenshot": true,
			"capture_region":  true,
		},
	}
}

func (d *ToolModeSwitchDetector) Name() string { return "ToolModeSwitch" }

func (d *ToolModeSwitchDetector) Check(calls []ToolCall) Decision {
	if len(calls) < 2 {
		return Continue
	}
	secondLast, last := calls[len(calls)-2], calls[len(calls)-1]
	if secondLast.Status == "success" && d.guiTools[secondLast.Name] && d.visualTools[last.Name] {
		return Nudge
	}
	return Continue
}

// ============================================
// DETECTOR 8: SuccessAfterError
// Same tool recovers (success) right after failing with the same input.
// Repeated flapping indicates instability.
// ============================================
type SuccessAfterErrorDetector struct {
	// FlapLimit is how many error->success flaps in the window trigger a Nudge.
	FlapLimit int
}

func NewSuccessAfterErrorDetector() *SuccessAfterErrorDetector {
	return &SuccessAfterErrorDetector{FlapLimit: 3}
}

func (d *SuccessAfterErrorDetector) Name() string { return "SuccessAfterError" }

func (d *SuccessAfterErrorDetector) Check(calls []ToolCall) Decision {
	if len(calls) < 2 {
		return Continue
	}
	window := getSlidingWindow(calls, 10)
	flaps := 0
	for i := 1; i < len(window); i++ {
		prev, cur := window[i-1], window[i]
		if prev.Name != cur.Name {
			continue
		}
		if isErrorStatus(prev.Status) && cur.Status == "success" {
			flaps++
		}
	}
	if d.FlapLimit > 0 && flaps >= d.FlapLimit {
		return Nudge
	}
	return Continue
}

// ============================================
// DETECTOR 9: Sleep
// Sleep calls in shell commands (waiting on state that may never change).
// ============================================
type SleepDetector struct {
	warningTh int
	stopTh    int
}

func NewSleepDetector(warningTh, stopTh int) *SleepDetector {
	return &SleepDetector{warningTh: warningTh, stopTh: stopTh}
}

func (d *SleepDetector) Name() string { return "Sleep" }

func (d *SleepDetector) Check(calls []ToolCall) Decision {
	window := getSlidingWindow(calls, 10)
	sleepCount := 0
	for _, call := range window {
		if call.Name != "bash" && call.Name != "shell" {
			continue
		}
		if cmd, ok := call.Input["command"].(string); ok && strings.Contains(cmd, "sleep ") {
			sleepCount++
		}
	}
	if sleepCount >= d.stopTh {
		return ForceStop
	}
	if sleepCount >= d.warningTh {
		return Nudge
	}
	return Continue
}

// ============================================
// DETECTOR 10: NgramRepetition
// The same n-gram of tool names repeats repeatedly in the sequence
// (e.g. search->edit->search->edit->search->edit). This catches loops that
// alternate between tools and therefore evade exact-duplicate detectors.
// ============================================
type NgramRepetitionDetector struct {
	windowSize int
	ngram      int // n-gram length
	minRepeats int // repeats of the same n-gram before a Nudge
}

// NewNgramRepetitionDetector creates the detector with defaults: window 20,
// n-gram length 2, 4 repeats.
func NewNgramRepetitionDetector(windowSize, ngram, minRepeats int) *NgramRepetitionDetector {
	if ngram < 2 {
		ngram = 2
	}
	if minRepeats < 2 {
		minRepeats = 2
	}
	return &NgramRepetitionDetector{windowSize: windowSize, ngram: ngram, minRepeats: minRepeats}
}

func (d *NgramRepetitionDetector) Name() string { return "NgramRepetition" }

func (d *NgramRepetitionDetector) Check(calls []ToolCall) Decision {
	window := getSlidingWindow(calls, d.windowSize)
	if len(window) < d.ngram*d.minRepeats {
		return Continue
	}
	names := make([]string, len(window))
	for i, c := range window {
		names[i] = c.Name
	}
	counts := make(map[string]int)
	for i := 0; i+d.ngram <= len(names); i++ {
		gram := strings.Join(names[i:i+d.ngram], "→")
		counts[gram]++
	}
	for gram, count := range counts {
		_ = gram
		if count >= d.minRepeats {
			// Stronger signal: an overlapping consecutive repeat (A B A B A B).
			if d.hasConsecutiveRun(names) {
				return ForceStop
			}
			return Nudge
		}
	}
	return Continue
}

// hasConsecutiveRun reports whether the sequence contains the same n-gram
// repeated back-to-back (ABABAB), a tight loop signature.
func (d *NgramRepetitionDetector) hasConsecutiveRun(names []string) bool {
	if len(names) < d.ngram*3 {
		return false
	}
	for i := 0; i+d.ngram*3 <= len(names); i++ {
		gram := names[i : i+d.ngram]
		ok := true
		for r := 1; r < 3; r++ {
			for j, name := range gram {
				if names[i+d.ngram*r+j] != name {
					ok = false
					break
				}
			}
			if !ok {
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// ============================================
// DETECTOR 11: ProgressStagnation
// Consecutive calls whose outputs are all identical: the agent is not making
// progress even though it keeps acting.
// ============================================
type ProgressStagnationDetector struct {
	limit int // consecutive identical outputs before a Nudge
}

// NewProgressStagnationDetector creates the detector; limit defaults to 3.
func NewProgressStagnationDetector(limit int) *ProgressStagnationDetector {
	if limit < 2 {
		limit = 3
	}
	return &ProgressStagnationDetector{limit: limit}
}

func (d *ProgressStagnationDetector) Name() string { return "ProgressStagnation" }

func (d *ProgressStagnationDetector) Check(calls []ToolCall) Decision {
	if len(calls) < d.limit {
		return Continue
	}
	run := 1
	for i := len(calls) - 1; i > 0; i-- {
		if outputFingerprint(calls[i]) == outputFingerprint(calls[i-1]) &&
			outputFingerprint(calls[i]) != "" {
			run++
			if run >= d.limit {
				return Nudge
			}
		} else {
			break
		}
	}
	return Continue
}

// ============================
// Utility functions
// ============================

func getSlidingWindow(calls []ToolCall, size int) []ToolCall {
	if size <= 0 || len(calls) <= size {
		return calls
	}
	return calls[len(calls)-size:]
}

// callFingerprint builds a deterministic fingerprint from tool name and
// arguments (keys sorted so map ordering never matters).
func callFingerprint(call ToolCall) string {
	parts := make([]string, 0, 1+len(call.Input))
	parts = append(parts, call.Name)
	keys := make([]string, 0, len(call.Input))
	for k := range call.Input {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, call.Input[k]))
	}
	return strings.Join(parts, "|")
}

// outputFingerprint builds a deterministic fingerprint of a call's output.
// Empty outputs fingerprint to "" so detectors can ignore them.
func outputFingerprint(call ToolCall) string {
	if len(call.Output) == 0 {
		return ""
	}
	keys := make([]string, 0, len(call.Output))
	for k := range call.Output {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, call.Output[k]))
	}
	return strings.Join(parts, "|")
}

// LoopDetectorManager orchestrates all detectors.
type LoopDetectorManager struct {
	detectors  []Detector
	nudgeCount int // consecutive Nudge results; 3+ escalates to ForceStop
	thresholds ThresholdConfig
}

// NewLoopDetectorManager builds the manager with the full default detector set.
func NewLoopDetectorManager() *LoopDetectorManager {
	thresholds := DefaultThresholds()
	return &LoopDetectorManager{
		thresholds: thresholds,
		detectors: []Detector{
			NewConsecutiveDuplicateDetector(thresholds.ConsecutiveSameCall),
			NewExactDuplicateDetector(thresholds.SlidingWindowSize, 3),
			NewSameToolErrorDetector(4),
			NewFamilyNoProgressDetector(),
			NewSearchEscalationDetector(thresholds.SearchEscalation, 5),
			NewNoProgressDetector(thresholds.NoProgressLimit),
			NewToolModeSwitchDetector(),
			NewSuccessAfterErrorDetector(),
			NewSleepDetector(thresholds.SleepFrequency, 4),
			NewNgramRepetitionDetector(thresholds.SlidingWindowSize, 2, 4),
			NewProgressStagnationDetector(3),
		},
	}
}

// AddDetector appends a custom detector to the manager.
func (m *LoopDetectorManager) AddDetector(detector Detector) {
	m.detectors = append(m.detectors, detector)
}

// Check runs all detectors over the call history and returns the combined
// decision with evidence. Repeated Nudge outcomes escalate to ForceStop
// after 3 consecutive checks.
func (m *LoopDetectorManager) Check(calls []ToolCall) *DetectorResult {
	allDecisions := make([]Decision, 0, len(m.detectors))
	evidence := make([]string, 0)

	for _, det := range m.detectors {
		decision := det.Check(calls)
		allDecisions = append(allDecisions, decision)
		if decision == Nudge || decision == ForceStop {
			evidence = append(evidence, fmt.Sprintf("%s: %s", det.Name(), decision.String()))
		}
	}

	maxDecision := AllDecisions(allDecisions)

	switch maxDecision {
	case Nudge:
		m.nudgeCount++
		if m.nudgeCount >= 3 {
			maxDecision = ForceStop
			evidence = append(evidence, "escalated_from_repeated_nudges")
		}
	case Continue:
		m.nudgeCount = 0
	case ForceStop:
		// leave nudgeCount; a ForceStop already terminates the loop
	}

	msg := ""
	if maxDecision != Continue {
		msg = fmt.Sprintf("loop detected: %s (evidence: %s)", maxDecision, strings.Join(evidence, "; "))
	}

	return &DetectorResult{
		DetectorName: "LoopDetectorManager",
		Decision:     maxDecision,
		Message:      msg,
		Evidence:     evidence,
		Thresholds:   m.thresholds,
	}
}

// ResetNudgeCount resets the consecutive-nudge counter (call after the LLM
// acknowledges a Nudge and changes strategy).
func (m *LoopDetectorManager) ResetNudgeCount() { m.nudgeCount = 0 }

// IncrementNudgeCount manually bumps the nudge counter (e.g. when a Nudge
// message was already sent out-of-band).
func (m *LoopDetectorManager) IncrementNudgeCount() { m.nudgeCount++ }

// NudgeCount exposes the current consecutive-nudge counter (for tests and UIs).
func (m *LoopDetectorManager) NudgeCount() int { return m.nudgeCount }
