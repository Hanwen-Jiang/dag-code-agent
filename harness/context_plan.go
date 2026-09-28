package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Context lifecycle.
//
// The compiler used to handle an over-budget context by amputating it: keep the
// stable messages and the last one, discard everything between. That lost every
// tool result and every earlier turn, which is why a multi-turn session felt
// like it had no memory of the conversation, and it recorded nothing about what
// it had thrown away.
//
// This file replaces that with a staged pipeline whose decisions are explicit
// and journaled. Two rules are non-negotiable:
//
//  1. Compaction only touches the NON-STABLE suffix. The pinned prefix
//     (system, developer, constraints and the caller's stable messages) must be
//     byte-identical before and after, or the prompt-cache key changes and the
//     cache goes permanently cold. See TestStablePrefixSurvivesCompaction.
//  2. Every decision is recorded in a ContextPlan that travels with the
//     CONTEXT_COMPILED event, so "what the model was shown" is auditable
//     instead of something a reader has to re-derive.

// SegmentKind classifies one addressable piece of the model context.
type SegmentKind string

const (
	SegmentSystem     SegmentKind = "system"
	SegmentConstraint SegmentKind = "constraint"
	SegmentTurn       SegmentKind = "turn"
	SegmentToolResult SegmentKind = "tool_result"
	SegmentSummary    SegmentKind = "summary"
)

// ContextSegment is one independently manageable piece of context.
type ContextSegment struct {
	// ID is deterministic for a given compilation, so a plan can name segments
	// and a test can assert on them without depending on pointer identity.
	ID string
	// Index is the segment's position in the original message list.
	Index int
	Kind  SegmentKind
	// Stable marks the pinned prefix: never summarized, spilled or dropped.
	Stable bool
	// Tokens is the estimated token cost of Content.
	Tokens int
	// Content is the message payload as it will be handed to the provider.
	Content map[string]any
}

// SpillRef points at a segment whose bulk was moved to artifact storage.
type SpillRef struct {
	SegmentID  string `json:"segment_id"`
	ArtifactID string `json:"artifact_id"`
	SHA256     string `json:"sha256"`
	Tokens     int    `json:"tokens"`
	Head       string `json:"head,omitempty"`
	Tail       string `json:"tail,omitempty"`
}

// compactRef is the shape carried in the plan payload: enough to audit the
// spill without duplicating the spilled text into the journal.
func (r SpillRef) compactRef() map[string]any {
	return map[string]any{
		"segment_id":  r.SegmentID,
		"artifact_id": r.ArtifactID,
		"sha256":      r.SHA256,
		"tokens":      r.Tokens,
	}
}

// SpillStore moves bulk context into durable storage and can read it back.
type SpillStore interface {
	Spill(ctx context.Context, auth AuthContext, segment ContextSegment) (SpillRef, error)
	Rehydrate(ctx context.Context, auth AuthContext, ref SpillRef) (ContextSegment, error)
}

// Compactor turns a run of segments into one summarizing segment.
type Compactor interface {
	Compact(ctx context.Context, segments []ContextSegment, targetTokens int) (ContextSegment, error)
}

// ContextPlan is the record of one compilation decision.
type ContextPlan struct {
	// Compacted is false when the context already fit and nothing was touched.
	Compacted  bool       `json:"compacted"`
	Kept       []string   `json:"kept,omitempty"`
	Summarized []string   `json:"summarized,omitempty"`
	Spilled    []SpillRef `json:"spilled,omitempty"`
	Dropped    []string   `json:"dropped,omitempty"`
	Tokens     int        `json:"tokens"`
	Budget     int        `json:"budget"`
}

// Payload is the bounded projection recorded in the journal: ids and counts,
// never the spilled text itself.
func (p ContextPlan) Payload() map[string]any {
	spilled := make([]map[string]any, 0, len(p.Spilled))
	for _, ref := range p.Spilled {
		spilled = append(spilled, ref.compactRef())
	}
	return map[string]any{
		"compacted":  p.Compacted,
		"kept":       p.Kept,
		"summarized": p.Summarized,
		"spilled":    spilled,
		"dropped":    p.Dropped,
		"tokens":     p.Tokens,
		"budget":     p.Budget,
	}
}

// ErrSpillUnavailable reports a spill attempt without a configured store.
var ErrSpillUnavailable = errors.New("SPILL_STORE_UNAVAILABLE")

// artifactSpillStore stores segments as content-addressed artifacts, reusing
// the same durable, idempotent, tenant-scoped projection that tool side effects
// already use.
type artifactSpillStore struct {
	artifacts ArtifactStore
}

func (s artifactSpillStore) Spill(_ context.Context, auth AuthContext, segment ContextSegment) (SpillRef, error) {
	if s.artifacts == nil {
		return SpillRef{}, ErrSpillUnavailable
	}
	payload := map[string]any{
		"segment_id": segment.ID,
		"kind":       string(segment.Kind),
		"content":    segment.Content,
	}
	record, err := s.artifacts.PutArtifact(auth, spillArtifactID(segment.ID), payload)
	if err != nil {
		return SpillRef{}, err
	}
	text := providerText(segment.Content)
	head, tail := headTail(text, spillHeadRunes, spillTailRunes)
	return SpillRef{
		SegmentID:  segment.ID,
		ArtifactID: record.ArtifactID,
		SHA256:     record.SHA256,
		Tokens:     segment.Tokens,
		Head:       head,
		Tail:       tail,
	}, nil
}

func (s artifactSpillStore) Rehydrate(_ context.Context, auth AuthContext, ref SpillRef) (ContextSegment, error) {
	if s.artifacts == nil {
		return ContextSegment{}, ErrSpillUnavailable
	}
	record, ok, err := s.artifacts.GetArtifact(auth, ref.ArtifactID)
	if err != nil {
		return ContextSegment{}, err
	}
	if !ok {
		return ContextSegment{}, fmt.Errorf("%w: artifact %s", ErrNotFound, ref.ArtifactID)
	}
	content, _ := record.Content["content"].(map[string]any)
	return ContextSegment{ID: ref.SegmentID, Content: content, Tokens: ref.Tokens}, nil
}

// spillArtifactID derives a stable artifact id from the segment, so a retried
// compilation spills to the same content-addressed slot instead of leaking a
// new artifact per attempt.
func spillArtifactID(segmentID string) string {
	return "CTX-" + segmentID
}

const (
	spillHeadRunes = 400
	spillTailRunes = 200
	// spillThresholdTokens is the size above which a single tool result is
	// moved out of the prompt. Below it, spilling costs a round trip for no
	// real saving.
	spillThresholdTokens = 512
	// summaryRunesPerSegment bounds the deterministic default summarizer.
	summaryRunesPerSegment = 160
	// runesPerToken mirrors EstimateTokens' byte/4 heuristic closely enough to
	// budget the summary against the same unit the pipeline measures in.
	runesPerToken = 4
)

func headTail(text string, head, tail int) (string, string) {
	runes := []rune(text)
	if len(runes) <= head+tail {
		return text, ""
	}
	return string(runes[:head]), string(runes[len(runes)-tail:])
}

// extractiveCompactor is the default summarizer. It is deterministic on
// purpose: a model-backed compactor would make two executions of the same
// journaled state produce different prompts, which breaks the property that a
// replayed run is reproducible. Embedders can still inject a model-backed
// Compactor, and the design doc records that trade-off.
type extractiveCompactor struct{}

func (extractiveCompactor) Compact(_ context.Context, segments []ContextSegment, targetTokens int) (ContextSegment, error) {
	if len(segments) == 0 {
		return ContextSegment{}, nil
	}
	// The summary must itself fit. Ignoring the target produced a summary that
	// the drop level then evicted wholesale, which is indistinguishable from
	// the amputation this pipeline replaced. Half the remaining budget is spent
	// on the summary, leaving room for the turn the model is answering.
	perSegment := summaryRunesPerSegment
	if targetTokens > 0 {
		allowedRunes := (targetTokens / 2) * runesPerToken
		if allowedRunes < len(segments) {
			allowedRunes = len(segments)
		}
		if budgeted := allowedRunes / len(segments); budgeted < perSegment {
			perSegment = budgeted
		}
	}
	if perSegment < 1 {
		perSegment = 1
	}
	lines := make([]string, 0, len(segments))
	ids := make([]string, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.ID)
		text := strings.TrimSpace(providerText(segment.Content))
		runes := []rune(text)
		if len(runes) > perSegment {
			text = string(runes[:perSegment]) + "…"
		}
		role := stringValue(segment.Content["role"])
		lines = append(lines, fmt.Sprintf("- (%s) %s", role, text))
	}
	sort.Strings(ids)
	return ContextSegment{
		ID:   "summary:" + strings.Join(ids, ","),
		Kind: SegmentSummary,
		Content: map[string]any{
			"role": "observation_summary",
			"content": fmt.Sprintf("Earlier context was summarized (%d segments):\n%s",
				len(segments), strings.Join(lines, "\n")),
		},
	}, nil
}

// planContext runs the staged pipeline against a message list.
//
// The pipeline is ordered so that the cheapest and most reversible step is
// tried first, and each level only runs when the previous one was not enough:
//
//  1. pin the stable prefix           (never touched)
//  2. summarize older turns           (lossy, recoverable from the journal)
//  3. spill oversized tool results    (recoverable via read_artifact)
//  4. drop the lowest priority tail   (recorded as dropped)
func planContext(ctx context.Context, builder *ContextBuilder, auth AuthContext, messages []map[string]any, budget int) ([]map[string]any, ContextPlan, error) {
	segments := segmentMessages(messages)
	plan := ContextPlan{Budget: budget, Tokens: EstimateTokens(messages)}
	plan.Kept = segmentIDs(segments)
	if budget > 0 && plan.Tokens <= budget {
		return messages, plan, nil
	}
	plan.Compacted = true

	pinned, rest := splitPinned(segments)
	working := append([]ContextSegment{}, rest...)

	// Level 2: summarize older turns, keeping the most recent turn verbatim so
	// the model still sees what it was just asked.
	turns, keepTail := splitSummarizable(working)
	if len(turns) > 0 {
		compactor := builder.Compactor
		if compactor == nil {
			compactor = extractiveCompactor{}
		}
		summary, err := compactor.Compact(ctx, turns, budget)
		if err == nil && strings.TrimSpace(providerText(summary.Content)) != "" {
			working = rebuildAfterSummary(working, turns, summary, keepTail)
			plan.Summarized = segmentIDs(turns)
		}
	}

	// Level 3: spill oversized, spillable segments.
	if builder.Spill != nil {
		for i := range working {
			if working[i].Stable || working[i].Tokens < spillThresholdTokens {
				continue
			}
			ref, err := builder.Spill.Spill(ctx, auth, working[i])
			if err != nil {
				// A spill failure must not fail the run: fall through to the
				// drop level, which is always available.
				continue
			}
			working[i].Content = spilledMessage(working[i].Content, ref)
			working[i].Tokens = EstimateTokens(working[i].Content)
			plan.Spilled = append(plan.Spilled, ref)
		}
	}

	// Level 4: drop the lowest priority segments until the estimate fits.
	sort.SliceStable(working, func(i, j int) bool { return segmentPriority(working[i]) > segmentPriority(working[j]) })
	all := append(append([]ContextSegment{}, pinned...), working...)
	for {
		rebuilt := flattenSegments(all)
		plan.Tokens = EstimateTokens(rebuilt)
		if budget <= 0 || plan.Tokens <= budget || len(working) == 0 {
			plan.Kept = segmentIDs(all)
			return rebuilt, plan, nil
		}
		// Drop the last (lowest priority) segment that is not pinned.
		victim := working[len(working)-1]
		working = working[:len(working)-1]
		all = append(append([]ContextSegment{}, pinned...), working...)
		plan.Dropped = append(plan.Dropped, victim.ID)
	}
}

// segmentMessages classifies each message so the pipeline can reason about it.
func segmentMessages(messages []map[string]any) []ContextSegment {
	segments := make([]ContextSegment, 0, len(messages))
	for index, message := range messages {
		kind := SegmentTurn
		role := stringValue(message["role"])
		switch {
		case role == "system" || role == "developer":
			kind = SegmentSystem
		case role == "constraint":
			kind = SegmentConstraint
		case role == "observation" || role == "previous_result":
			kind = SegmentToolResult
		}
		segments = append(segments, ContextSegment{
			ID:      fmt.Sprintf("%s:%d", kind, index),
			Index:   index,
			Kind:    kind,
			Stable:  isStableMessage(message, role),
			Tokens:  EstimateTokens(message),
			Content: message,
		})
	}
	return segments
}

// pinnedMarker marks a message the pipeline may never evict. It is an internal
// compiler hint: the provider projection only reads role and content, so the
// marker never reaches the model or the journal.
const pinnedMarker = "_pinned"

// isStableMessage reports whether a message belongs to the pinned prefix.
//
// Constraints count as pinned because they are hard invariants the model must
// keep seeing; dropping them would silently change the task's contract.
func isStableMessage(message map[string]any, role string) bool {
	if pinned, ok := message[pinnedMarker].(bool); ok && pinned {
		return true
	}
	if stable, ok := message["stable"].(bool); ok && stable {
		return true
	}
	switch role {
	case "system", "developer", "constraint":
		return true
	default:
		return false
	}
}

// splitPinned separates the pinned prefix from the compactable remainder. Only
// the leading run counts as pinned: a constraint that appears after compactable
// content cannot be hoisted without changing message order, and order matters
// to the provider.
func splitPinned(segments []ContextSegment) ([]ContextSegment, []ContextSegment) {
	cut := 0
	for cut < len(segments) && segments[cut].Stable {
		cut++
	}
	return segments[:cut], segments[cut:]
}

// splitSummarizable returns the turns eligible for summarization plus the
// trailing messages that must stay verbatim (the latest turn and, if present,
// a trailing system reminder).
func splitSummarizable(segments []ContextSegment) ([]ContextSegment, []ContextSegment) {
	lastTurn := -1
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Kind == SegmentTurn {
			lastTurn = i
			break
		}
	}
	if lastTurn <= 0 {
		return nil, segments
	}
	// Everything before the final turn is eligible; the final turn stays.
	return segments[:lastTurn], segments[lastTurn:]
}

func rebuildAfterSummary(working []ContextSegment, summarized []ContextSegment, summary ContextSegment, keepTail []ContextSegment) []ContextSegment {
	out := make([]ContextSegment, 0, len(working))
	summary.Index = -1
	summary.Tokens = EstimateTokens(summary.Content)
	out = append(out, summary)
	out = append(out, keepTail...)
	return out
}

// spilledMessage replaces an oversized payload with a bounded, actionable view.
func spilledMessage(original map[string]any, ref SpillRef) map[string]any {
	replacement := map[string]any{}
	for key, value := range original {
		replacement[key] = value
	}
	replacement["content"] = fmt.Sprintf(
		"[spilled %d tokens -> artifact %s (sha256 %s)]\n%s\n… %d tokens omitted; call read_artifact with artifact_id=%q to fetch the full value …\n%s",
		ref.Tokens, ref.ArtifactID, ref.SHA256, ref.Head, ref.Tokens, ref.ArtifactID, ref.Tail)
	replacement["spilled_artifact_id"] = ref.ArtifactID
	replacement["spilled_sha256"] = ref.SHA256
	return replacement
}

// segmentPriority orders segments for the drop level. Higher survives longer.
func segmentPriority(segment ContextSegment) int {
	if segment.Stable {
		return 100
	}
	switch segment.Kind {
	case SegmentConstraint:
		return 90
	case SegmentSummary:
		return 70
	case SegmentTurn:
		// Everything before the final turn was summarized, so any surviving
		// turn is the one the model is answering: it must outrank the summary.
		return 80
	case SegmentToolResult:
		return 40
	default:
		return 20
	}
}

func segmentIDs(segments []ContextSegment) []string {
	ids := make([]string, 0, len(segments))
	for _, segment := range segments {
		ids = append(ids, segment.ID)
	}
	return ids
}

// flattenSegments restores message order. Order is significant to the provider,
// so the pinned prefix and the surviving segments are re-emitted by their
// original index rather than in priority order.
func flattenSegments(segments []ContextSegment) []map[string]any {
	ordered := append([]ContextSegment{}, segments...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Index < ordered[j].Index })
	out := make([]map[string]any, 0, len(ordered))
	for _, segment := range ordered {
		out = append(out, segment.Content)
	}
	return out
}

var _ Compactor = extractiveCompactor{}
var _ SpillStore = artifactSpillStore{}
