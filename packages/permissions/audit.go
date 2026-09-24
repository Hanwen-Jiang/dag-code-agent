package permissions

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// AuditEntry is one record in the permission audit trail.
type AuditEntry struct {
	Timestamp time.Time   `json:"timestamp"`
	Tool      string      `json:"tool"`
	Command   string      `json:"command"`
	Verdict   Verdict     `json:"verdict"`
	Code      string      `json:"code"`
	Layer     int         `json:"layer"`
	Reason    string      `json:"reason"`
	Redacted  bool        `json:"redacted"`
	Duration  time.Duration `json:"duration_ns"`
}

// AuditLog is a thread-safe, optionally bounded ring of permission decisions.
type AuditLog struct {
	mu      sync.Mutex
	entries []AuditEntry
	cap     int // 0 = unbounded
}

// NewAuditLog creates an audit log; cap <= 0 keeps every entry.
func NewAuditLog(cap int) *AuditLog {
	return &AuditLog{cap: cap}
}

func (l *AuditLog) record(now time.Time, tool, cmd string, res *CheckResult, redacted bool) {
	e := AuditEntry{
		Timestamp: now,
		Tool:      tool,
		Command:   truncateForAudit(cmd),
		Verdict:   res.Verdict,
		Code:      res.Code,
		Layer:     res.Layer,
		Reason:    res.Reason,
		Redacted:  redacted,
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
	if l.cap > 0 && len(l.entries) > l.cap {
		l.entries = l.entries[len(l.entries)-l.cap:]
	}
}

// Entries returns a copy of the audit trail in order.
func (l *AuditLog) Entries() []AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AuditEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// Len returns the number of retained entries.
func (l *AuditLog) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Reset clears the audit trail.
func (l *AuditLog) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = nil
}

// Export returns the audit trail as pretty-printed JSON for reporting.
func (l *AuditLog) Export() (string, error) {
	data, err := json.MarshalIndent(l.Entries(), "", "  ")
	if err != nil {
		return "", fmt.Errorf("audit export failed: %w", err)
	}
	return string(data), nil
}

func truncateForAudit(s string) string {
	const maxLen = 500
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "...(truncated)"
}
