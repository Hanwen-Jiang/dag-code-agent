// Package permissions implements a layered permission engine for production-grade tool execution.
// It is the security boundary that prevents unauthorized or dangerous tool execution.
//
// Check layers (evaluated in order, first denial wins):
//
//  1. Tool allowlist          - unknown tools are denied by default
//  2. Command pattern rules   - hard-block regexes (immutable), config deny list,
//     composite command decomposition (sub-commands checked independently),
//     auto-allow list for read-only safe commands
//  3. Path scope              - file paths are canonicalized and must stay inside
//     allowed base paths; sensitive patterns (ssh keys, *.pem, ...) are denied
//  4. Secret redaction        - arguments matching secret patterns are redacted in
//     place before anything downstream sees them
//  5. Rate limit              - token-bucket style sliding window over the whole engine
//  6. Approval                - surviving calls require user approval via
//     ApprovalHandler; default-deny when no handler is configured
//
// Every decision is appended to an in-memory audit trail (AuditLog).
package permissions

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Verdict is the outcome category of a permission check.
type Verdict string

const (
	// VerdictAllow means the tool call may proceed.
	VerdictAllow Verdict = "allow"
	// VerdictDeny means the tool call is rejected.
	VerdictDeny Verdict = "deny"
)

// CheckResult is a verdict with the reason and the layer that produced it.
type CheckResult struct {
	Verdict  Verdict
	Code     string // stable machine-readable code, e.g. "HARD_BLOCKED"
	Layer    int    // 1..6, see package docs
	Reason   string
	Redacted bool // true if secret patterns were scrubbed from the arguments
}

// ApprovalHandler defines the user approval workflow for calls that pass every
// automated layer but are not on the auto-allow list.
type ApprovalHandler interface {
	RequestApproval(toolName string, input map[string]interface{}) error
}

// denyEntry is a configurable deny-list pattern.
type denyEntry struct {
	pattern     *regexp.Regexp
	description string
	category    string // "command", "path", "network"
}

// rateLimiter is a sliding-window rate limiter shared by the engine.
type rateLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	calls  []time.Time
	nowFn  func() time.Time
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, nowFn: time.Now}
}

// Allow reports whether a call at time t fits inside the window, and records it.
func (r *rateLimiter) Allow(t time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := t.Add(-r.window)
	kept := r.calls[:0]
	for _, c := range r.calls {
		if c.After(cutoff) {
			kept = append(kept, c)
		}
	}
	if r.limit > 0 && len(kept) >= r.limit {
		r.calls = kept
		return false
	}
	r.calls = append(kept, t)
	return true
}

// PermissionEngine manages multi-layer permission checks.
type PermissionEngine struct {
	config        Config
	hardBlocked   []*regexp.Regexp
	denyList      []*denyEntry
	allowPatterns []*regexp.Regexp
	secretPatterns []*regexp.Regexp
	toolSet       map[string]bool

	approval    ApprovalHandler
	rateLimiter *rateLimiter
	audit       *AuditLog
	logger      Logger

	nowFn func() time.Time

	mu sync.RWMutex
}

// NewPermissionEngine creates a new permission engine with the given
// configuration. Use DefaultConfig() for production defaults. A nil-safe
// default (NullLogger) is used for logging.
func NewPermissionEngine(cfg Config) *PermissionEngine {
	return newEngine(cfg, NullLogger{})
}

// NewPermissionEngineWithLogger creates an engine with an explicit Logger.
func NewPermissionEngineWithLogger(cfg Config, logger Logger) *PermissionEngine {
	return newEngine(cfg, logger)
}

func newEngine(cfg Config, logger Logger) *PermissionEngine {
	if logger == nil {
		logger = NullLogger{}
	}
	hardBlocked := make([]*regexp.Regexp, 0, len(cfg.HardBlockedPatterns))
	for _, p := range cfg.HardBlockedPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			logger.Warn("skipping invalid hard-block pattern", "pattern", p, "err", err.Error())
			continue
		}
		hardBlocked = append(hardBlocked, re)
	}

	denyList := make([]*denyEntry, 0, len(cfg.DeniedCommandPatterns))
	for i, p := range cfg.DeniedCommandPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			logger.Warn("skipping invalid deny pattern", "pattern", p, "err", err.Error())
			continue
		}
		desc := ""
		if i < len(cfg.DenyDescriptions) {
			desc = cfg.DenyDescriptions[i]
		}
		denyList = append(denyList, &denyEntry{pattern: re, description: desc, category: "command"})
	}

	allowPatterns := make([]*regexp.Regexp, 0, len(cfg.AllowedCommandPatterns))
	for _, p := range cfg.AllowedCommandPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			logger.Warn("skipping invalid allow pattern", "pattern", p, "err", err.Error())
			continue
		}
		allowPatterns = append(allowPatterns, re)
	}

	secretPatterns := make([]*regexp.Regexp, 0, len(cfg.SecretPatterns))
	for _, p := range cfg.SecretPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			logger.Warn("skipping invalid secret pattern", "pattern", p, "err", err.Error())
			continue
		}
		secretPatterns = append(secretPatterns, re)
	}

	toolSet := make(map[string]bool, len(cfg.ToolAllowlist))
	for _, t := range cfg.ToolAllowlist {
		toolSet[strings.ToLower(t)] = true
	}

	win := cfg.RateLimit.WindowDuration()

	return &PermissionEngine{
		config:         cfg,
		hardBlocked:    hardBlocked,
		denyList:       denyList,
		allowPatterns:  allowPatterns,
		secretPatterns: secretPatterns,
		toolSet:        toolSet,
		rateLimiter:    newRateLimiter(cfg.RateLimit.MaxCalls, win),
		audit:          NewAuditLog(0),
		logger:         logger,
		nowFn:          time.Now,
	}
}

// SetApprovalHandler installs the user approval workflow (layer 6).
func (e *PermissionEngine) SetApprovalHandler(h ApprovalHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.approval = h
}

// SetClock overrides the clock used for rate limiting and audit timestamps.
// Intended for deterministic tests; pass nil to restore time.Now.
func (e *PermissionEngine) SetClock(fn func() time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if fn == nil {
		fn = time.Now
	}
	e.nowFn = fn
	e.rateLimiter.nowFn = fn
}

// Audit exposes the engine's audit trail.
func (e *PermissionEngine) Audit() *AuditLog { return e.audit }

// LoadConfig reads permission configuration from a YAML file and replaces the
// engine's configurable layers (allowlist, deny list, path policy, rate limit,
// secret patterns). The immutable hard-block layer is taken from Config.
func (e *PermissionEngine) LoadConfig(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	newEngine_ := newEngine(cfg, e.logger)
	e.mu.Lock()
	e.denyList = newEngine_.denyList
	e.allowPatterns = newEngine_.allowPatterns
	e.secretPatterns = newEngine_.secretPatterns
	e.toolSet = newEngine_.toolSet
	e.config = cfg
	e.rateLimiter = newEngine_.rateLimiter
	e.rateLimiter.nowFn = e.nowFn // keep injected clock (test override)
	e.mu.Unlock()
	return nil
}

// Check validates whether a tool call should be permitted. It returns nil when
// the call is allowed and a *PermissionError describing the denial (with the
// layer, code and reason) otherwise.
func (e *PermissionEngine) Check(toolName string, input map[string]interface{}) error {
	_, err := e.CheckWithVerdict(toolName, input)
	return err
}

// CheckWithVerdict runs the full layered check and always returns the
// CheckResult describing the outcome. The error is non-nil exactly when the
// verdict is VerdictDeny.
func (e *PermissionEngine) CheckWithVerdict(toolName string, input map[string]interface{}) (*CheckResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := e.nowFn()
	cmdStr := e.stringifyInput(toolName, input)

	deny := func(code string, layer int, reason string, redacted bool) (*CheckResult, error) {
		res := &CheckResult{Verdict: VerdictDeny, Code: code, Layer: layer, Reason: reason, Redacted: redacted}
		e.audit.record(now, toolName, cmdStr, res, redacted)
		e.logger.Warn("permission denied", "tool", toolName, "code", code, "layer", layer, "reason", reason)
		return res, &PermissionError{
			Code:      code,
			Message:   fmt.Sprintf("%s %s: %s", toolName, code, reason),
			Layer:     layer,
			Reason:    reason,
			CanOverride: code == "APPROVAL_DENIED",
		}
	}
	allow := func(code string, layer int, reason string, redacted bool) *CheckResult {
		res := &CheckResult{Verdict: VerdictAllow, Code: code, Layer: layer, Reason: reason, Redacted: redacted}
		e.audit.record(now, toolName, cmdStr, res, redacted)
		return res
	}

	// ========== LAYER 1: Tool allowlist (default deny) ==========
	if len(e.toolSet) > 0 && !e.toolSet[strings.ToLower(toolName)] {
		return deny("TOOL_NOT_ALLOWED", 1,
			fmt.Sprintf("tool %q is not in the allowlist", toolName), false)
	}

	// ========== LAYER 2: Command pattern rules (regex) ==========
	// 2a. Immutable hard-block list.
	for _, re := range e.hardBlocked {
		if re.MatchString(cmdStr) {
			return deny("HARD_BLOCKED", 2,
				fmt.Sprintf("matches immutable hard-block pattern %q", re.String()), false)
		}
	}
	// 2b. Configured deny list.
	for _, entry := range e.denyList {
		if entry.pattern.MatchString(cmdStr) {
			desc := entry.description
			if desc == "" {
				desc = entry.pattern.String()
			}
			return deny("CONFIG_DENIED", 2, fmt.Sprintf("matches deny pattern: %s", desc), false)
		}
	}
	// 2c. Composite command decomposition: each sub-command is checked
	// independently so "ls && rm -rf /" cannot hide behind the safe prefix.
	subCommands := decomposeComposite(cmdStr)
	if len(subCommands) > 1 {
		for _, sub := range subCommands {
			if err := e.checkCommandPatterns(toolName, sub); err != nil {
				return deny("COMPOSITE_DENIED", 2,
					fmt.Sprintf("sub-command %q rejected: %v", sub, err), false)
			}
		}
	}

	// ========== LAYER 3: Path scope ==========
	if err := e.checkPathScope(input); err != nil {
		return deny("PATH_DENIED", 3, err.Error(), false)
	}

	// ========== LAYER 4: Secret redaction ==========
	redacted, scrubbed := e.redactSecrets(input)

	// ========== LAYER 5: Rate limit ==========
	if e.config.RateLimit.Enabled {
		if !e.rateLimiter.Allow(now) {
			return deny("RATE_LIMITED", 5,
				fmt.Sprintf("more than %d calls in %s window", e.config.RateLimit.MaxCalls, e.config.RateLimit.Window), redacted)
		}
	}

	// ========== LAYER 6: Auto-allow / user approval ==========
	// Auto-allow only applies to simple (non-composite) commands.
	if len(subCommands) <= 1 && e.matchesAllowPattern(cmdStr) {
		return allow("AUTO_ALLOWED", 6, "matches safe auto-allow pattern", redacted), nil
	}

	if h := e.approval; h != nil {
		if err := h.RequestApproval(toolName, scrubbed); err != nil {
			return deny("APPROVAL_DENIED", 6, fmt.Sprintf("user did not approve: %v", err), redacted)
		}
		return allow("USER_APPROVED", 6, "approved interactively", redacted), nil
	}

	if !e.config.RequireApproval {
		return allow("NO_APPROVAL_REQUIRED", 6, "approval not required by config", redacted), nil
	}

	return deny("DEFAULT_DENY", 6, "call is not auto-allowed and no approval handler is configured (default-deny)", redacted)
}

// checkCommandPatterns runs the hard-block + deny-list regexes for a single
// (already decomposed) command.
func (e *PermissionEngine) checkCommandPatterns(toolName, cmd string) error {
	for _, re := range e.hardBlocked {
		if re.MatchString(cmd) {
			return fmt.Errorf("hard-blocked: matches %q", re.String())
		}
	}
	for _, entry := range e.denyList {
		if entry.pattern.MatchString(cmd) {
			return fmt.Errorf("denied by config: %s", entry.description)
		}
	}
	return nil
}

// CheckUnwrapped validates a plain command string without recursion into
// composite decomposition (used for sub-commands).
func (e *PermissionEngine) CheckUnwrapped(toolName string, cmdStr string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.checkCommandPatterns(toolName, cmdStr)
}

// pathArgKeys are argument keys that carry filesystem paths.
var pathArgKeys = []string{"path", "file_path", "filepath", "target", "dir", "directory"}

// checkPathScope validates every path-like argument against the path policy.
func (e *PermissionEngine) checkPathScope(input map[string]interface{}) error {
	seen := make(map[string]bool)
	for _, key := range pathArgKeys {
		v, ok := input[key]
		if !ok {
			continue
		}
		p, ok := v.(string)
		if !ok || p == "" {
			continue
		}
		if seen[p] {
			continue
		}
		seen[p] = true
		if err := e.CheckPath(p, "read"); err != nil {
			return err
		}
	}
	return nil
}

// CheckPath validates a file path against the path security policy:
// canonicalization (symlink/traversal resistance), sensitive patterns, and
// the allowed base path boundary.
func (e *PermissionEngine) CheckPath(path string, accessMode string) error {
	expanded := expandHome(path)

	cleaned := filepath.Clean(expanded)
	// Reject relative traversal remnants after cleaning.
	if strings.Contains(cleaned, "..") {
		return &PathError{
			Code:      "PATH_TRAVERSAL",
			Message:   fmt.Sprintf("path traversal detected in %q", path),
			Path:      path,
			Operation: accessMode,
		}
	}

	// Resolve symlinks when the file exists; fall back to lexical cleaning.
	canonical := cleaned
	if resolved, err := filepath.EvalSymlinks(expanded); err == nil {
		canonical = resolved
	}

	for _, pattern := range e.config.PathPolicy.SensitivePatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			continue
		}
		if re.MatchString(canonical) {
			return &PathError{
				Code:      "SENSITIVE_PATH",
				Message:   fmt.Sprintf("accessing sensitive file pattern: %s", canonical),
				Path:      path,
				Operation: accessMode,
			}
		}
	}

	bases := e.config.PathPolicy.AllowedBasePaths
	if len(bases) > 0 {
		abs, err := filepath.Abs(canonical)
		if err != nil {
			return &PathError{
				Code:      "PATH_CANONICALIZE_FAILED",
				Message:   fmt.Sprintf("failed to canonicalize path %s: %v", path, err),
				Path:      path,
				Operation: accessMode,
			}
		}
		inside := false
		for _, base := range bases {
			b := expandHome(base)
			ab, err := filepath.Abs(b)
			if err != nil {
				continue
			}
			if abs == ab || strings.HasPrefix(abs, ab+string(filepath.Separator)) {
				inside = true
				break
			}
		}
		if !inside {
			return &PathError{
				Code:      "PATH_OUTSIDE_ALLOWED_REGION",
				Message:   fmt.Sprintf("%s outside allowed base paths: %v", abs, bases),
				Path:      path,
				Operation: accessMode,
			}
		}
	}

	return nil
}

// redactSecrets scans all string arguments for secret patterns and replaces
// the matches with [REDACTED]. It returns whether anything was redacted and a
// sanitized copy of the input (never mutating the caller's map).
func (e *PermissionEngine) redactSecrets(input map[string]interface{}) (bool, map[string]interface{}) {
	if len(e.secretPatterns) == 0 {
		return false, input
	}
	if len(input) == 0 {
		return false, input
	}
	redacted := false
	out := make(map[string]interface{}, len(input))
	for k, v := range input {
		nv, changed := redactValue(e.secretPatterns, v)
		if changed {
			redacted = true
		}
		out[k] = nv
	}
	return redacted, out
}

// redactValue recursively scrubs secret patterns from strings, maps and slices.
func redactValue(patterns []*regexp.Regexp, v interface{}) (interface{}, bool) {
	switch val := v.(type) {
	case string:
		return redactString(patterns, val)
	case map[string]interface{}:
		changed := false
		out := make(map[string]interface{}, len(val))
		for k, iv := range val {
			nv, c := redactValue(patterns, iv)
			if c {
				changed = true
			}
			out[k] = nv
		}
		return out, changed
	case []interface{}:
		changed := false
		out := make([]interface{}, len(val))
		for i, iv := range val {
			nv, c := redactValue(patterns, iv)
			if c {
				changed = true
			}
			out[i] = nv
		}
		return out, changed
	default:
		return v, false
	}
}

func redactString(patterns []*regexp.Regexp, s string) (string, bool) {
	changed := false
	for _, re := range patterns {
		if re.MatchString(s) {
			s = re.ReplaceAllString(s, "[REDACTED]")
			changed = true
		}
	}
	return s, changed
}

// matchesAllowPattern reports whether cmd matches any auto-allow pattern.
func (e *PermissionEngine) matchesAllowPattern(cmd string) bool {
	for _, re := range e.allowPatterns {
		if re.MatchString(cmd) {
			return true
		}
	}
	return false
}

// expandHome rewrites a leading "~" or "~/..." with the user home directory.
func expandHome(p string) string {
	if p == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
		return p
	}
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

// stringifyInput converts tool arguments to a deterministic string for
// pattern analysis. encoding/json sorts map keys, so output is stable.
func (e *PermissionEngine) stringifyInput(toolName string, input map[string]interface{}) string {
	if len(input) == 0 {
		return ""
	}
	switch strings.ToLower(toolName) {
	case "bash", "shell":
		if cmd, ok := input["command"].(string); ok {
			return cmd
		}
	case "file_write", "edit_file":
		if content, ok := input["content"].(string); ok {
			return content
		}
	case "http_request":
		if url, ok := input["url"].(string); ok {
			return url
		}
	}
	b, err := json.Marshal(input)
	if err != nil {
		return fmt.Sprintf("%v", input)
	}
	return string(b)
}

// decomposeComposite splits a command on shell operators (&& || ; | > < ` $( )
// and returns the trimmed non-empty parts. A single-command string returns a
// one-element slice; an empty string returns nil.
// compositeOpRe splits on shell operators: || && ; | > < $( ${ and backtick.
var compositeOpRe = regexp.MustCompile(`(\|\||&&|;|\||>|<|\$\(|\$\{|\` + "`" + `)`)

func decomposeComposite(cmd string) []string {
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return nil
	}
	parts := compositeOpRe.Split(cmd, -1)
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	if len(result) == 0 {
		return []string{cmd}
	}
	return result
}

// stableKeys returns sorted argument keys for deterministic output.
func stableKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
