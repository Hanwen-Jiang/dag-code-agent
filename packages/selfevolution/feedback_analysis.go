package selfevolution

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// FeedbackSignal is one piece of user or system feedback.
type FeedbackSignal struct {
	FeedbackID string
	TaskID     string
	Source     string // "user", "system", ...
	Rating     int    // 1 (worst) .. 5 (best)
	Comment    string
	Outcome    string // "success", "failure", ...
	CreatedAt  time.Time
}

// FeedbackConfig configures a FeedbackAnalyzer.
type FeedbackConfig struct {
	// HistoryWindowSize caps how many recent signals are retained in memory.
	HistoryWindowSize int
	// CorrelationPeriod is the sliding time window (relative to the newest
	// signal) considered by Analyze. <= 0 disables the time window and only
	// the size cap applies.
	CorrelationPeriod time.Duration
	// MinSamples is the number of in-window signals before a report is
	// considered actionable (the report is still produced below this, just
	// flagged in its recommendations).
	MinSamples int
	Thresholds FeedbackThresholds
}

// FeedbackThresholds are the actionable thresholds used by Analyze.
type FeedbackThresholds struct {
	// MinFeedbackRate is the minimum average number of feedback signals per
	// distinct task below which a coverage recommendation is emitted.
	MinFeedbackRate float64
	// SentimentThresh is a normalized sentiment threshold in [0,1] (rating 1
	// -> 0, rating 5 -> 1). Signals with normalized sentiment at or below it
	// count as negative.
	SentimentThresh float64
	// FailureRateThresh: the fraction of signals reporting a failure outcome
	// above which a failure investigation recommendation is emitted.
	FailureRateThresh float64
}

// DefaultFeedbackConfig returns standard feedback-analyzer settings.
func DefaultFeedbackConfig() FeedbackConfig {
	return FeedbackConfig{
		HistoryWindowSize: 1000,
		CorrelationPeriod: 24 * time.Hour,
		MinSamples:        20,
		Thresholds: FeedbackThresholds{
			MinFeedbackRate:   0.5,
			SentimentThresh:   0.25, // ratings 1-2 are negative
			FailureRateThresh: 0.2,
		},
	}
}

// FeedbackReport is the output of Analyze.
type FeedbackReport struct {
	Samples         int
	AvgRating       float64
	NegativeRate    float64
	TopIssues       []string
	Recommendations []string
}

// issueKeywords is the closed vocabulary used for issue clustering. Each
// keyword maps to a canonical issue label; occurrences are counted across
// negative-feedback comments.
var issueKeywords = map[string]string{
	"slow":       "slowness",
	"slowness":   "slowness",
	"latency":    "slowness",
	"timeout":    "timeouts",
	"timed":      "timeouts",
	"crash":      "crashes",
	"crashed":    "crashes",
	"bug":        "bugs",
	"bugs":       "bugs",
	"error":      "errors",
	"errors":     "errors",
	"fail":       "failures",
	"failed":     "failures",
	"failure":    "failures",
	"wrong":      "incorrect_results",
	"incorrect":  "incorrect_results",
	"hallucinat": "incorrect_results",
	"miss":       "missed_points",
	"missing":    "missed_points",
	"incomplete": "incomplete_answers",
	"partial":    "incomplete_answers",
	"confus":     "confusing_output",
	"unclear":    "confusing_output",
	"format":     "formatting",
	"broken":     "broken_output",
	"cost":       "cost",
	"expensive":  "cost",
	"price":      "cost",
	"privacy":    "privacy_concerns",
	"leak":       "privacy_concerns",
}

// FeedbackAnalyzer collects feedback signals and analyzes them over a
// sliding window. All methods are safe for concurrent use. The analyzer is
// fully deterministic: it never reads the wall clock — the "now" for the
// sliding time window is the newest signal's timestamp.
type FeedbackAnalyzer struct {
	mu      sync.Mutex
	config  FeedbackConfig
	signals []FeedbackSignal // ring buffer, oldest first
}

// NewFeedbackAnalyzer creates an analyzer with the given config.
func NewFeedbackAnalyzer(cfg FeedbackConfig) *FeedbackAnalyzer {
	if cfg.HistoryWindowSize <= 0 {
		cfg.HistoryWindowSize = DefaultFeedbackConfig().HistoryWindowSize
	}
	return &FeedbackAnalyzer{
		config:  cfg,
		signals: make([]FeedbackSignal, 0, cfg.HistoryWindowSize),
	}
}

// Record appends a feedback signal to the history, evicting the oldest beyond
// the configured window size.
func (f *FeedbackAnalyzer) Record(signal FeedbackSignal) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals = append(f.signals, signal)
	if len(f.signals) > f.config.HistoryWindowSize {
		f.signals = f.signals[len(f.signals)-f.config.HistoryWindowSize:]
	}
}

// normalizeRating maps a 1..5 rating to [0,1]. Out-of-range ratings are
// clamped.
func normalizeRating(r int) float64 {
	if r < 1 {
		r = 1
	}
	if r > 5 {
		r = 5
	}
	return float64(r-1) / 4.0
}

// isFailureOutcome reports whether an outcome string denotes a failure.
func isFailureOutcome(outcome string) bool {
	o := strings.ToLower(outcome)
	return o == "failure" || o == "failed" || o == "error"
}

// tokenize lowercases text into alphanumeric tokens.
func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
}

// inWindow returns the signals within the sliding window: the most recent
// HistoryWindowSize entries, further restricted to those within
// CorrelationPeriod of the newest signal's timestamp (when configured).
func (f *FeedbackAnalyzer) inWindow() []FeedbackSignal {
	start := 0
	if len(f.signals) > f.config.HistoryWindowSize {
		start = len(f.signals) - f.config.HistoryWindowSize
	}
	window := f.signals[start:]

	if f.config.CorrelationPeriod <= 0 {
		return window
	}
	if len(window) == 0 {
		return window
	}
	newest := window[len(window)-1].CreatedAt
	cutoff := newest.Add(-f.config.CorrelationPeriod)
	// Find the first index at or after the cutoff (signals are appended in
	// record order; assume monotonic non-decreasing timestamps, which the
	// sliding-window contract requires).
	lo := 0
	for lo < len(window) && window[lo].CreatedAt.Before(cutoff) {
		lo++
	}
	return window[lo:]
}

// Analyze computes the feedback report over the sliding window.
//
// Real logic:
//   - Samples / AvgRating / NegativeRate come from the in-window signals.
//   - Negative = normalized sentiment <= Thresholds.SentimentThresh.
//   - TopIssues: issue-keyword frequency across negative comments (top 5,
//     ties broken alphabetically).
//   - Recommendations: one deterministic line per triggered threshold
//     (low coverage, high negative rate, high failure rate), plus a
//     minimum-samples notice when the window is too small to act on.
func (f *FeedbackAnalyzer) Analyze() *FeedbackReport {
	f.mu.Lock()
	defer f.mu.Unlock()

	report := &FeedbackReport{
		TopIssues:       []string{},
		Recommendations: []string{},
	}

	window := f.inWindow()
	report.Samples = len(window)
	if len(window) == 0 {
		report.Recommendations = append(report.Recommendations, "no feedback in window; collect more signals before analyzing")
		return report
	}

	// Ratings and negativity.
	var ratingSum float64
	negative := 0
	failures := 0
	taskSet := make(map[string]bool)
	issueCounts := make(map[string]int)

	for _, s := range window {
		ratingSum += float64(clampRating(s.Rating))
		if normalizeRating(s.Rating) <= f.config.Thresholds.SentimentThresh {
			negative++
			for _, tok := range tokenize(s.Comment) {
				if label, ok := issueKeywords[tok]; ok {
					issueCounts[label]++
				}
			}
		}
		if isFailureOutcome(s.Outcome) {
			failures++
		}
		if s.TaskID != "" {
			taskSet[s.TaskID] = true
		}
	}

	report.AvgRating = ratingSum / float64(len(window))
	report.NegativeRate = float64(negative) / float64(len(window))
	failureRate := float64(failures) / float64(len(window))
	feedbackPerTask := 0.0
	if len(taskSet) > 0 {
		feedbackPerTask = float64(len(window)) / float64(len(taskSet))
	}

	// Top issues by frequency, ties alphabetical.
	type kv struct {
		issue string
		count int
	}
	pairs := make([]kv, 0, len(issueCounts))
	for issue, c := range issueCounts {
		pairs = append(pairs, kv{issue, c})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count != pairs[j].count {
			return pairs[i].count > pairs[j].count
		}
		return pairs[i].issue < pairs[j].issue
	})
	top := 5
	if top > len(pairs) {
		top = len(pairs)
	}
	for i := 0; i < top; i++ {
		report.TopIssues = append(report.TopIssues, fmt.Sprintf("%s (%d)", pairs[i].issue, pairs[i].count))
	}

	// Recommendations from thresholds (deterministic, in fixed order).
	if len(window) < f.config.MinSamples {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("only %d/%d minimum samples in window; treat findings as preliminary", len(window), f.config.MinSamples))
	}
	if len(taskSet) > 0 && feedbackPerTask < f.config.Thresholds.MinFeedbackRate {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("low feedback coverage: %.2f signals per task < %.2f minimum; increase feedback sampling",
				feedbackPerTask, f.config.Thresholds.MinFeedbackRate))
	}
	if report.NegativeRate >= 0.5 && negative > 0 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("high negative sentiment: %.0f%% of feedback is negative; address top issues first: %s",
				report.NegativeRate*100, strings.Join(report.TopIssues, ", ")))
	}
	if failureRate > f.config.Thresholds.FailureRateThresh {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("failure rate %.0f%% exceeds threshold %.0f%%; investigate failure categories",
				failureRate*100, f.config.Thresholds.FailureRateThresh*100))
	}
	if len(report.Recommendations) == 0 {
		report.Recommendations = append(report.Recommendations, "feedback health is within thresholds; no action required")
	}

	return report
}

func clampRating(r int) int {
	if r < 1 {
		return 1
	}
	if r > 5 {
		return 5
	}
	return r
}
