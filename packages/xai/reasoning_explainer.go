package xai

import (
	"fmt"
	"strings"
)

// Supported explainer languages.
const (
	LanguageEnglish = "en"
	LanguageChinese = "zh-CN"
)

// ExplainerConfig configures a ReasoningExplainer.
type ExplainerConfig struct {
	// Language selects the narrative language: "en" or "zh-CN".
	// Any other value falls back to English.
	Language string
	// MaxSectionEvents caps how many events are narrated in the timeline.
	MaxSectionEvents int
	// IncludeEvidence appends a data-backed evidence section.
	IncludeEvidence bool
}

// DefaultExplainerConfig returns the standard explainer configuration.
func DefaultExplainerConfig() ExplainerConfig {
	return ExplainerConfig{
		Language:         LanguageEnglish,
		MaxSectionEvents: 8,
		IncludeEvidence:  true,
	}
}

// ReasoningSegment narrates one execution step.
type ReasoningSegment struct {
	Iteration int     `json:"iteration"`
	Action    string  `json:"action"`
	Outcome   string  `json:"outcome"`
	LatencyMS float64 `json:"latency_ms,omitempty"`
}

// NextStep is a concrete, deterministic recommendation derived from the trace.
type NextStep struct {
	Priority int    `json:"priority"`
	Action   string `json:"action"`
	Reason   string `json:"reason"`
}

// Explanation is a deterministic natural-language narrative of a trace:
// what ran, in what order, where the time went, what failed and the most
// likely cause, plus suggested next steps. All numbers are computed from the
// trace data; templates are only used for phrasing.
type Explanation struct {
	TaskID     string             `json:"task_id"`
	Language   string             `json:"language"`
	Success    bool               `json:"success"`
	Narrative  string             `json:"narrative"`
	Segments   []ReasoningSegment `json:"segments"`
	TopFailure *FailureInsight    `json:"top_failure,omitempty"`
	NextSteps  []NextStep         `json:"next_steps"`
	Evidence   []EvidenceItem     `json:"evidence,omitempty"`
}

// FailureInsight states the most likely cause of the trace's failures.
type FailureInsight struct {
	Cause       string  `json:"cause"`
	ToolName    string  `json:"tool_name,omitempty"`
	Iteration   int     `json:"iteration"`
	Occurrences int     `json:"occurrences"`
	Severity    string  `json:"severity"`
	Confidence  float64 `json:"confidence"`
	Message     string  `json:"message"`
}

// EvidenceItem is a raw data point backing the narrative.
type EvidenceItem struct {
	Kind  string  `json:"kind"` // "latency", "error", "tool_usage", "duration"
	Label string  `json:"label"`
	Value float64 `json:"value"`
	Unit  string  `json:"unit,omitempty"`
}

// ReasoningExplainer turns a Trace into an Explanation. It is fully
// deterministic and language-aware (English and zh-CN).
type ReasoningExplainer struct {
	config ExplainerConfig
}

// NewReasoningExplainer creates an explainer with the given config.
func NewReasoningExplainer(cfg ExplainerConfig) *ReasoningExplainer {
	if cfg.Language == "" {
		cfg = DefaultExplainerConfig()
	}
	if cfg.MaxSectionEvents <= 0 {
		cfg.MaxSectionEvents = DefaultExplainerConfig().MaxSectionEvents
	}
	return &ReasoningExplainer{config: cfg}
}

type langPack struct {
	ranEvents       func(n int) string
	noEvents        func() string
	statusCompleted func() string
	statusFailed    func() string
	statusOther     func(s string) string
	totalTime       func(ms float64) string
	slowest         func(tool string, ms float64) string
	toolUsage       func(tool string, calls int, ms float64) string
	iterations      func(a, b int) string
	noErrors        func() string
	errorLine       func(tool string, iter int, msg string) string
	causeLine       func(cause, tool string, count int) string
	stepRetry       func(tool string) string
	stepInput       func(tool string) string
	stepEscalate    func() string
	stepBottleneck  func(tool string) string
	stepNone        func() string
}

func enPack() langPack {
	return langPack{
		ranEvents:       func(n int) string { return fmt.Sprintf("The task ran %d execution event(s).", n) },
		noEvents:        func() string { return "The task recorded no execution events." },
		statusCompleted: func() string { return "It finished successfully." },
		statusFailed:    func() string { return "It failed." },
		statusOther:     func(s string) string { return fmt.Sprintf("Its final status was %q.", s) },
		totalTime:       func(ms float64) string { return fmt.Sprintf("Total traced time was %.0f ms", ms) },
		slowest: func(tool string, ms float64) string {
			return fmt.Sprintf("The slowest event took %.0f ms%s.", ms, toolSuffix(tool))
		},
		toolUsage: func(tool string, calls int, ms float64) string {
			return fmt.Sprintf("Tool %s was called %d time(s), taking %.0f ms in total.", tool, calls, ms)
		},
		iterations: func(a, b int) string { return fmt.Sprintf("Iterations covered: %d-%d.", a, b) },
		noErrors:   func() string { return "No errors were recorded." },
		errorLine: func(tool string, iter int, msg string) string {
			return fmt.Sprintf("Iteration %d: %sfailure — %s.", iter, toolPrefix(tool), truncateRuneLimit(msg, 80))
		},
		causeLine: func(cause, tool string, count int) string {
			return fmt.Sprintf("Most likely cause: %s (seen %d time%s%s).", cause, count, pluralS(count), toolSuffix(tool))
		},
		stepRetry: func(tool string) string {
			return fmt.Sprintf("Retry %s with backoff; the failure looks transient.", tool)
		},
		stepInput: func(tool string) string {
			return fmt.Sprintf("Validate the input passed to %s before re-running.", tool)
		},
		stepEscalate: func() string { return "Escalate to a human: permission issues cannot be fixed automatically." },
		stepBottleneck: func(tool string) string {
			return fmt.Sprintf("Profile %s to cut latency: it dominates the traced time.", tool)
		},
		stepNone: func() string { return "No corrective action needed; monitor for regressions." },
	}
}

func zhPack() langPack {
	return langPack{
		ranEvents:       func(n int) string { return fmt.Sprintf("任务共执行了 %d 个事件。", n) },
		noEvents:        func() string { return "任务没有记录任何执行事件。" },
		statusCompleted: func() string { return "任务成功完成。" },
		statusFailed:    func() string { return "任务失败了。" },
		statusOther:     func(s string) string { return fmt.Sprintf("最终状态为 %q。", s) },
		totalTime:       func(ms float64) string { return fmt.Sprintf("总耗时 %.0f 毫秒", ms) },
		slowest: func(tool string, ms float64) string {
			return fmt.Sprintf("最慢的事件耗时 %.0f 毫秒%s。", ms, toolSuffixZh(tool))
		},
		toolUsage: func(tool string, calls int, ms float64) string {
			return fmt.Sprintf("工具 %s 被调用 %d 次，累计耗时 %.0f 毫秒。", tool, calls, ms)
		},
		iterations: func(a, b int) string { return fmt.Sprintf("覆盖迭代：%d-%d。", a, b) },
		noErrors:   func() string { return "未记录到错误。" },
		errorLine: func(tool string, iter int, msg string) string {
			return fmt.Sprintf("迭代 %d：%s失败 — %s。", iter, toolPrefixZh(tool), truncateRuneLimit(msg, 80))
		},
		causeLine: func(cause, tool string, count int) string {
			return fmt.Sprintf("最可能的原因：%s（出现 %d 次%s）。", cause, count, toolSuffixZh(tool))
		},
		stepRetry: func(tool string) string {
			return fmt.Sprintf("对 %s 进行退避重试，该失败看起来是暂时性的。", tool)
		},
		stepInput: func(tool string) string {
			return fmt.Sprintf("重新运行前，先校验传给 %s 的输入。", tool)
		},
		stepEscalate: func() string { return "升级给人工处理：权限问题无法自动修复。" },
		stepBottleneck: func(tool string) string {
			return fmt.Sprintf("剖析 %s 以降低延迟：它占据了绝大部分时间。", tool)
		},
		stepNone: func() string { return "无需纠正操作；持续监控即可。" },
	}
}

func toolPrefix(tool string) string {
	if tool == "" {
		return ""
	}
	return "tool " + tool + ": "
}

func toolPrefixZh(tool string) string {
	if tool == "" {
		return ""
	}
	return "工具 " + tool + "："
}

func toolSuffixZh(tool string) string {
	if tool == "" {
		return ""
	}
	return "（" + tool + "）"
}

func pluralS(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// Generate builds the explanation for a trace.
func (e *ReasoningExplainer) Generate(trace *Trace) *Explanation {
	lang := e.config.Language
	if !strings.EqualFold(lang, LanguageChinese) {
		lang = LanguageEnglish
	}
	pack := enPack()
	if strings.EqualFold(lang, LanguageChinese) {
		pack = zhPack()
	}

	if trace == nil {
		return &Explanation{
			TaskID:    "unknown",
			Language:  lang,
			Narrative: pack.noEvents(),
		}
	}

	exp := &Explanation{
		TaskID:   trace.TaskID,
		Language: lang,
		Success:  trace.Status == TraceCompleted,
	}

	// Segments: what ran, in what order.
	maxSeg := e.config.MaxSectionEvents
	for i, ev := range trace.Events {
		if i >= maxSeg {
			break
		}
		exp.Segments = append(exp.Segments, ReasoningSegment{
			Iteration: ev.Iteration,
			Action:    actionForEvent(ev),
			Outcome:   outcomeForEvent(ev),
			LatencyMS: ev.LatencyMS,
		})
	}

	// Where the time went.
	analysis := NewAttributionAnalyzer(DefaultAttributionConfig()).Analyze(trace)
	exp.TopFailure = e.topFailure(trace, analysis)

	// Suggested next steps.
	exp.NextSteps = e.nextSteps(trace, analysis)

	// Evidence: raw numbers behind the narrative.
	if e.config.IncludeEvidence {
		exp.Evidence = e.collectEvidence(trace, analysis)
	}

	exp.Narrative = e.buildNarrative(trace, analysis, pack)
	return exp
}

func actionForEvent(ev TraceEvent) string {
	switch {
	case ev.Error != "":
		if ev.ToolName != "" {
			return fmt.Sprintf("executed tool %s and it failed", ev.ToolName)
		}
		return "encountered a failure"
	case ev.ToolName != "":
		return fmt.Sprintf("executed tool %s", ev.ToolName)
	case ev.Type == "llm_call" || ev.Type == "llm":
		return "made an LLM call"
	case ev.Type != "":
		return ev.Type
	default:
		return "executed a step"
	}
}

func outcomeForEvent(ev TraceEvent) string {
	if ev.Error != "" {
		return truncateRuneLimit(ev.Error, 80)
	}
	if ev.Output != "" {
		return truncateRuneLimit(ev.Output, 80)
	}
	if ev.LatencyMS > 0 {
		return fmt.Sprintf("completed in %.0f ms", ev.LatencyMS)
	}
	return "completed"
}

// topFailure derives the most likely failure cause from error/tool
// correlation: the cause label with the most occurrences wins (ties broken by
// first appearance), attributed to the tool seen most often with that cause.
func (e *ReasoningExplainer) topFailure(trace *Trace, analysis *AttributionReport) *FailureInsight {
	if analysis.ErrorCount == 0 {
		return nil
	}
	type causeAgg struct {
		count    int
		tools    map[string]int
		seenAt   int
		severity string
	}
	byCause := make(map[string]*causeAgg)
	var order []string
	for i, ev := range trace.Events {
		if ev.Error == "" {
			continue
		}
		cause := classifyErrorCause(ev)
		agg, ok := byCause[cause]
		if !ok {
			agg = &causeAgg{tools: make(map[string]int), seenAt: i}
			byCause[cause] = agg
			order = append(order, cause)
		}
		agg.count++
		if ev.ToolName != "" {
			agg.tools[ev.ToolName]++
		}
		agg.severity = severityForError(ev)
	}

	bestCause := order[0]
	bestAgg := byCause[bestCause]
	for _, cause := range order[1:] {
		if byCause[cause].count > bestAgg.count {
			bestCause = cause
			bestAgg = byCause[cause]
		}
	}

	bestTool := ""
	bestCount := 0
	for tool, n := range bestAgg.tools {
		if n > bestCount || (n == bestCount && tool < bestTool) {
			bestTool = tool
			bestCount = n
		}
	}

	// The root error (first in the causal chain) carries the headline message.
	root := trace.ErrorEvents()[0]

	// Confidence: share of all errors sharing the dominant cause, damped by
	// how evenly causes are split.
	occurrences := bestAgg.count
	conf := float64(occurrences) / float64(analysis.ErrorCount)
	if analysis.ErrorCount > 1 {
		conf *= 0.5 + 0.5*float64(occurrences)/float64(analysis.ErrorCount)
	}

	return &FailureInsight{
		Cause:       bestCause,
		ToolName:    bestTool,
		Iteration:   root.Iteration,
		Occurrences: occurrences,
		Severity:    bestAgg.severity,
		Confidence:  conf,
		Message:     root.Error,
	}
}

func (e *ReasoningExplainer) nextSteps(trace *Trace, analysis *AttributionReport) []NextStep {
	var steps []NextStep
	failure := e.topFailure(trace, analysis)
	if failure != nil {
		switch failure.Cause {
		case "transient_infrastructure":
			steps = append(steps, NextStep{Priority: 1, Action: "retry_with_backoff", Reason: enPack().stepRetry(orElse(failure.ToolName, "the failing step"))})
		case "invalid_input":
			steps = append(steps, NextStep{Priority: 1, Action: "validate_input", Reason: enPack().stepInput(orElse(failure.ToolName, "the failing tool"))})
		case "permission_denied":
			steps = append(steps, NextStep{Priority: 1, Action: "escalate_to_human", Reason: enPack().stepEscalate()})
		default:
			steps = append(steps, NextStep{Priority: 1, Action: "inspect_root_error", Reason: enPack().stepInput(orElse(failure.ToolName, "the failing tool"))})
		}
	}
	if len(analysis.SlowTools) > 0 && analysis.SlowTools[0].Share >= 0.5 {
		steps = append(steps, NextStep{Priority: 3, Action: "profile_hot_tool", Reason: enPack().stepBottleneck(analysis.SlowTools[0].Tool)})
	}
	if len(steps) == 0 {
		steps = append(steps, NextStep{Priority: 1, Action: "none", Reason: enPack().stepNone()})
	}
	return steps
}

func orElse(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func (e *ReasoningExplainer) collectEvidence(trace *Trace, analysis *AttributionReport) []EvidenceItem {
	items := make([]EvidenceItem, 0, 4)
	items = append(items, EvidenceItem{
		Kind: "duration", Label: "total traced time",
		Value: trace.TotalTimeMS(), Unit: "ms",
	})
	if s := trace.SlowestEvent(); s.LatencyMS > 0 {
		items = append(items, EvidenceItem{
			Kind: "latency", Label: fmt.Sprintf("slowest event (iteration %d)", s.Iteration),
			Value: s.LatencyMS, Unit: "ms",
		})
	}
	for _, st := range analysis.SlowTools {
		items = append(items, EvidenceItem{
			Kind: "tool_usage", Label: fmt.Sprintf("tool %s", st.Tool),
			Value: float64(st.Calls), Unit: "calls",
		})
	}
	items = append(items, EvidenceItem{
		Kind: "error", Label: "error events",
		Value: float64(analysis.ErrorCount), Unit: "count",
	})
	return items
}

func (e *ReasoningExplainer) buildNarrative(trace *Trace, analysis *AttributionReport, pack langPack) string {
	if trace == nil || len(trace.Events) == 0 {
		return pack.noEvents()
	}
	var b strings.Builder

	// What ran, in what order.
	fmt.Fprintf(&b, "%s ", pack.ranEvents(len(trace.Events)))
	switch trace.Status {
	case TraceCompleted:
		b.WriteString(pack.statusCompleted())
	case TraceFailed, TraceTimedOut:
		b.WriteString(pack.statusFailed())
	default:
		b.WriteString(pack.statusOther(string(trace.Status)))
	}

	// Time summary.
	total := trace.TotalTimeMS()
	slowest := trace.SlowestEvent()
	if total > 0 {
		fmt.Fprintf(&b, " %s.", pack.totalTime(total))
	}
	if slowest.LatencyMS > 0 {
		b.WriteString(" " + pack.slowest(slowest.ToolName, slowest.LatencyMS))
	}

	// Where the time went, tool by tool (top 3).
	for i, st := range analysis.SlowTools {
		if i >= 3 {
			break
		}
		fmt.Fprintf(&b, " %s", pack.toolUsage(st.Tool, st.Calls, st.TotalLatencyMS))
	}
	if first, last := trace.Events[0].Iteration, trace.Events[len(trace.Events)-1].Iteration; last >= first {
		fmt.Fprintf(&b, " %s", pack.iterations(first, last))
	}

	// What failed and the most likely cause.
	b.WriteString("\n")
	if analysis.ErrorCount == 0 {
		b.WriteString(pack.noErrors() + " ")
	} else {
		failure := e.topFailure(trace, analysis)
		if failure != nil {
			fmt.Fprintf(&b, "%s ", pack.causeLine(failure.Cause, failure.ToolName, failure.Occurrences))
		}
		for i, ev := range trace.ErrorEvents() {
			if i >= 3 {
				fmt.Fprintf(&b, "(+%d more)", analysis.ErrorCount-3)
				break
			}
			fmt.Fprintf(&b, "%s ", pack.errorLine(ev.ToolName, ev.Iteration, ev.Error))
		}
	}

	// Suggested next steps.
	b.WriteString("\n")
	for i, step := range e.nextSteps(trace, analysis) {
		fmt.Fprintf(&b, "%d. %s", i+1, step.Reason)
		if i < len(e.nextSteps(trace, analysis))-1 {
			b.WriteString(" ")
		}
	}
	b.WriteString("\n")

	return strings.TrimSpace(b.String()) + "\n"
}
