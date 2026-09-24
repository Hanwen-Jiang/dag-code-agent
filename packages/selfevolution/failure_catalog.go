package selfevolution

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// FailureRecord is one observed failure, keyed by a stable signature so
// repeated occurrences of the same root cause aggregate into one entry.
type FailureRecord struct {
	Signature string // stable fingerprint (category + normalized detail)
	Category  string // e.g. "permission_denied", "loop_detected"
	TaskID    string // the task that last produced this failure
	Details   string // human-readable description
	Count     int    // total occurrences
	FirstSeen time.Time
	LastSeen  time.Time
	// Recovery is the last recovery strategy that resolved this failure,
	// learned online; empty until a successful recovery is recorded.
	Recovery string
}

// recoveryPlaybook maps a failure category to an ordered list of recovery
// strategies. It is the deterministic baseline used before the catalog has
// learned a category-specific fix.
var recoveryPlaybook = map[string][]string{
	"permission_denied": {"request_scope_elevation", "degrade_to_readonly", "escalate_to_human"},
	"loop_detected":     {"inject_break_hint", "reduce_step_budget", "fallback_model"},
	"tool_failure":      {"retry_with_backoff", "swap_tool", "skip_step"},
	"context_overflow":  {"compact_context", "drop_older_observations", "split_task"},
	"timeout":           {"extend_deadline", "reduce_output_tokens", "fallback_model"},
	"validation_failed": {"repair_output", "replan", "escalate_to_human"},
}

// defaultRecovery is returned when a category has no playbook entry.
const defaultRecovery = "escalate_to_human"

// FailureCatalog learns from observed failures and suggests recovery actions.
// It is thread-safe and deterministic (no randomness).
type FailureCatalog struct {
	mu    sync.RWMutex
	bySig map[string]*FailureRecord
	byCat map[string]*FailureRecord // category -> most recent record
}

// NewFailureCatalog creates an empty catalog.
func NewFailureCatalog() *FailureCatalog {
	return &FailureCatalog{
		bySig: make(map[string]*FailureRecord),
		byCat: make(map[string]*FailureRecord),
	}
}

// Record observes a failure, aggregating by signature. The LastSeen timestamp
// is supplied by the caller (tests use a fixed clock for determinism); a zero
// LastSeen is filled with the first-seen time so the record is always valid.
func (c *FailureCatalog) Record(rec FailureRecord) *FailureRecord {
	if rec.Signature == "" {
		rec.Signature = signatureFor(rec.Category, rec.Details)
	}
	if rec.LastSeen.IsZero() {
		rec.LastSeen = rec.FirstSeen
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.bySig[rec.Signature]; ok {
		existing.Count++
		existing.LastSeen = rec.LastSeen
		if rec.TaskID != "" {
			existing.TaskID = rec.TaskID
		}
		if rec.Details != "" {
			existing.Details = rec.Details
		}
		if rec.Recovery != "" {
			existing.Recovery = rec.Recovery
		}
		c.byCat[existing.Category] = existing
		return existing
	}

	created := rec
	created.FirstSeen = rec.LastSeen
	created.Count = 1
	c.bySig[rec.Signature] = &created
	c.byCat[rec.Category] = &created
	return &created
}

// MarkRecovered records that a failure signature was resolved by a given
// recovery strategy, so future SuggestRecovery calls for the category prefer
// the strategy that actually worked.
func (c *FailureCatalog) MarkRecovered(signature, recovery string) {
	if signature == "" || recovery == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if rec, ok := c.bySig[signature]; ok {
		rec.Recovery = recovery
		c.byCat[rec.Category] = rec
	}
}

// Lookup returns the record for a signature, or nil.
func (c *FailureCatalog) Lookup(signature string) *FailureRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec, ok := c.bySig[signature]
	if !ok {
		return nil
	}
	out := *rec
	return &out
}

// List returns all records, sorted by signature for deterministic output.
func (c *FailureCatalog) List() []FailureRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]FailureRecord, 0, len(c.bySig))
	for _, rec := range c.bySig {
		out = append(out, *rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Signature < out[j].Signature })
	return out
}

// SuggestRecovery returns the best recovery strategy for a category:
//  1. a strategy learned from a past successful recovery (most recent record
//     for the category that has a non-empty Recovery),
//  2. otherwise the first (highest-priority) strategy in the category's
//     playbook,
//  3. otherwise the default escalation.
func (c *FailureCatalog) SuggestRecovery(category string) string {
	c.mu.RLock()
	rec := c.byCat[category]
	c.mu.RUnlock()
	if rec != nil && rec.Recovery != "" {
		return rec.Recovery
	}
	if playbook, ok := recoveryPlaybook[category]; ok && len(playbook) > 0 {
		return playbook[0]
	}
	return defaultRecovery
}

// Category returns the most frequent categories, descending by total count,
// for dashboards and improvement reports.
func (c *FailureCatalog) Category() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	counts := make(map[string]int)
	for _, rec := range c.bySig {
		counts[rec.Category] += rec.Count
	}
	type kv struct {
		cat   string
		count int
	}
	pairs := make([]kv, 0, len(counts))
	for cat, n := range counts {
		pairs = append(pairs, kv{cat, n})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count != pairs[j].count {
			return pairs[i].count > pairs[j].count
		}
		return pairs[i].cat < pairs[j].cat
	})
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.cat)
	}
	return out
}

// signatureFor builds a stable fingerprint from a category and the salient
// tokens of a detail string (deterministic, order-insensitive).
func signatureFor(category, details string) string {
	tokens := strings.FieldsFunc(strings.ToLower(details), func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	})
	sort.Strings(tokens)
	return fmt.Sprintf("%s:%s", category, strings.Join(tokens, ","))
}
