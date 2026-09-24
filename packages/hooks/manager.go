// Package hooks implements a YAML-configurable hook engine with four event
// points (PreToolUse, PostToolUse, SessionStart, Stop). Hook commands are
// executed via os/exec with a timeout, receive the event as a JSON payload on
// stdin, and decide via the exit-code protocol:
//
//	exit 0  = allow (optionally with a JSON decision object on stdout)
//	exit 2  = deny (stderr is returned to the caller as the denial reason)
//	other   = warning (non-blocking; recorded and execution continues)
//
// For extra safety, only binaries whose canonical path lies inside an
// explicit allowlist of root directories may be used as hooks (no bare PATH
// lookups, no symlink escapes out of the allowed roots).
package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// EventType represents the four hook event points.
type EventType string

const (
	// PreToolUse fires after the permission check, before execution; it can block.
	PreToolUse EventType = "PreToolUse"
	// PostToolUse fires after execution completes (non-blocking).
	PostToolUse EventType = "PostToolUse"
	// SessionStart fires when a session begins.
	SessionStart EventType = "SessionStart"
	// Stop fires when a session ends.
	Stop EventType = "Stop"
)

// HookConfig represents a single hook configuration.
type HookConfig struct {
	Matcher  string `yaml:"matcher"`  // regex matching the tool name; empty = all tools
	Command  string `yaml:"command"`  // command to execute (path; arguments allowed)
	TimeoutMs int64  `yaml:"timeout_ms"` // optional per-hook timeout; 0 = manager default
}

// EventConfigs maps event types to their hooks.
type EventConfigs map[EventType][]HookConfig

// ConfigFile represents the YAML config structure (.dag/hooks.yaml).
type ConfigFile struct {
	Hooks EventConfigs `yaml:"hooks"`
}

// EventPayload is the JSON protocol for hook scripts.
type EventPayload struct {
	Event        string                 `json:"event"`
	ToolName     string                 `json:"tool_name,omitempty"`
	ToolInput    map[string]interface{} `json:"tool_input,omitempty"`
	ToolResponse interface{}            `json:"tool_response,omitempty"`
	SessionID    string                 `json:"session_id"`
	Timestamp    time.Time              `json:"timestamp"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// ValidationResult represents a hook's decision.
type ValidationResult struct {
	Allowed   bool     // true = allow execution
	DenyMsg   string   // denial reason returned to the caller when denied
	Warnings  []string // non-blocking hook warnings (timeout, non-zero exit)
}

// HookError wraps common hook errors.
type HookError struct {
	Code    string
	Message string
	Err     error
}

func (e *HookError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return fmt.Sprintf("%s (%s)", e.Message, e.Code)
}

func (e *HookError) Unwrap() error { return e.Err }

// Manager handles hook execution and lifecycle.
type Manager struct {
	mu           sync.RWMutex
	configPath   string
	config       ConfigFile
	matchers     map[EventType][]compiledHook
	timeout      time.Duration
	maxOutput    int64
	allowedRoots []string // binary allowlist (canonical prefixes)
}

type compiledHook struct {
	config    HookConfig
	matcher   *regexp.Regexp // nil = matches all tools
	resolved  resolvedCommand
}

type resolvedCommand struct {
	binary string // absolute canonical binary path
	args   []string
}

// NewHookManager creates a hook manager with default settings:
// 10s timeout, 10KB output cap, allowlist = current directory + home.
func NewHookManager() *Manager {
	m := &Manager{
		timeout:   10 * time.Second,
		maxOutput: 10 * 1024,
		matchers:  make(map[EventType][]compiledHook),
	}
	cwd, err := os.Getwd()
	if err == nil {
		m.allowedRoots = []string{cwd}
	}
	if home, err := os.UserHomeDir(); err == nil {
		m.allowedRoots = append(m.allowedRoots, home)
	}
	return m
}

// SetTimeout overrides the default per-hook execution timeout.
func (m *Manager) SetTimeout(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d > 0 {
		m.timeout = d
	}
}

// SetAllowedRoots replaces the binary allowlist. Hook commands may only
// reference binaries whose canonical path is inside one of these roots.
// Paths are resolved to canonical form.
func (m *Manager) SetAllowedRoots(roots []string) error {
	canonical := make([]string, 0, len(roots))
	for _, r := range roots {
		abs, err := filepath.Abs(r)
		if err != nil {
			return &HookError{Code: "PATH_ERROR", Message: "cannot resolve allowed root", Err: err}
		}
		if c, err := filepath.EvalSymlinks(abs); err == nil {
			abs = c
		}
		canonical = append(canonical, abs)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allowedRoots = canonical
	return nil
}

// LoadConfig loads and parses the hook configuration file, validating every
// hook's binary against the allowlist and every matcher as a regex.
func (m *Manager) LoadConfig(path string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return &HookError{Code: "PATH_ERROR", Message: "cannot resolve absolute path", Err: err}
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		return &HookError{Code: "FILE_NOT_FOUND", Message: "configuration file not found", Err: err}
	}
	if fileInfo.IsDir() {
		return &HookError{Code: "NOT_FILE", Message: "configuration path is a directory"}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return &HookError{Code: "READ_ERROR", Message: "cannot read configuration file", Err: err}
	}

	var cfg ConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return &HookError{Code: "PARSE_ERROR", Message: "invalid YAML syntax", Err: err}
	}

	compiled := make(map[EventType][]compiledHook)
	configDir := filepath.Dir(path)
	for eventType, hooks := range cfg.Hooks {
		for _, hook := range hooks {
			ch, err := m.compileHook(hook, configDir)
			if err != nil {
				return &HookError{
					Code:    "INVALID_HOOK",
					Message: fmt.Sprintf("invalid hook for %s (matcher %q): %v", eventType, hook.Matcher, err),
				}
			}
			compiled[eventType] = append(compiled[eventType], ch)
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.configPath = path
	m.config = cfg
	m.matchers = compiled
	return nil
}

// compileHook validates and resolves one hook configuration.
func (m *Manager) compileHook(hook HookConfig, configDir string) (compiledHook, error) {
	if strings.TrimSpace(hook.Command) == "" {
		return compiledHook{}, errors.New("empty command")
	}
	var matcher *regexp.Regexp
	if hook.Matcher != "" {
		re, err := regexp.Compile(hook.Matcher)
		if err != nil {
			return compiledHook{}, fmt.Errorf("invalid matcher regex: %w", err)
		}
		matcher = re
	}
	resolved, err := m.resolveCommand(hook.Command, configDir)
	if err != nil {
		return compiledHook{}, err
	}
	return compiledHook{config: hook, matcher: matcher, resolved: resolved}, nil
}

// resolveCommand parses the command line, resolves the binary to a canonical
// absolute path, and enforces the binary allowlist.
func (m *Manager) resolveCommand(command, configDir string) (resolvedCommand, error) {
	fields := strings.Fields(command)
	binPath := fields[0]

	abs := binPath
	if !filepath.IsAbs(abs) {
		// Relative paths resolve against the config directory first, then cwd.
		if joined := filepath.Join(configDir, abs); fileExists(joined) {
			abs = joined
		} else if cwd, err := os.Getwd(); err == nil {
			abs = filepath.Join(cwd, abs)
		} else {
			abs = filepath.Join(configDir, abs)
		}
	}
	abs = filepath.Clean(abs)

	// Symlinks must not escape the allowlist.
	canonical, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return resolvedCommand{}, fmt.Errorf("binary not found: %s", binPath)
	}

	m.mu.RLock()
	roots := append([]string(nil), m.allowedRoots...)
	m.mu.RUnlock()
	if !insideAnyRoot(canonical, roots) {
		return resolvedCommand{}, fmt.Errorf("binary %s is outside the allowed roots %v (allowlist)", canonical, roots)
	}

	if !fileExists(canonical) {
		return resolvedCommand{}, fmt.Errorf("binary not found: %s", binPath)
	}

	return resolvedCommand{binary: canonical, args: fields[1:]}, nil
}

func insideAnyRoot(canonical string, roots []string) bool {
	for _, root := range roots {
		if canonical == root || strings.HasPrefix(canonical, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// ValidatePreToolUse runs all matching PreToolUse hooks and returns the
// combined decision. The first denying hook (exit 2 or JSON deny) wins.
// Non-blocking warnings (timeout, other non-zero exits) are accumulated.
func (m *Manager) ValidatePreToolUse(eventPayload *EventPayload) (*ValidationResult, error) {
	result, err := m.RunHooks(PreToolUse, eventPayload)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RunHooks executes all hooks registered for an event type and returns the
// combined validation result. For PreToolUse a deny aborts at the first
// denial; for the other event types hooks run to completion.
func (m *Manager) RunHooks(eventType EventType, eventPayload *EventPayload) (*ValidationResult, error) {
	hooks := m.getMatchingHooks(eventType, eventPayload.ToolName)
	result := &ValidationResult{Allowed: true}
	for _, hook := range hooks {
		hookResult, err := m.executeHook(&hook, eventPayload)
		if err != nil {
			// Warnings never block execution.
			result.Warnings = append(result.Warnings, err.Error())
			continue
		}
		if !hookResult.Allowed {
			result.Allowed = false
			result.DenyMsg = hookResult.DenyMsg
			return result, nil
		}
	}
	return result, nil
}

// RunPostToolUse runs PostToolUse hooks after execution (non-blocking:
// hooks run in the background and failures are ignored).
func (m *Manager) RunPostToolUse(eventPayload *EventPayload) {
	go func() {
		_, _ = m.RunHooks(PostToolUse, eventPayload)
	}()
}

// RunPostToolUseSync is the synchronous variant, useful in tests and in
// contexts that need to observe hook completion.
func (m *Manager) RunPostToolUseSync(eventPayload *EventPayload) (*ValidationResult, error) {
	return m.RunHooks(PostToolUse, eventPayload)
}

// NotifySessionStart triggers SessionStart hooks.
func (m *Manager) NotifySessionStart(sessionID string) error {
	return m.notifyLifecycle(SessionStart, sessionID)
}

// NotifyStop triggers Stop hooks.
func (m *Manager) NotifyStop(sessionID string) error {
	return m.notifyLifecycle(Stop, sessionID)
}

func (m *Manager) notifyLifecycle(eventType EventType, sessionID string) error {
	payload := &EventPayload{
		Event:     string(eventType),
		SessionID: sessionID,
		Timestamp: time.Now(),
	}
	result, err := m.RunHooks(eventType, payload)
	if err != nil {
		return err
	}
	if len(result.Warnings) > 0 {
		return &HookError{Code: "LIFECYCLE_HOOK_WARNING", Message: "lifecycle hook warning", Err: errors.New(strings.Join(result.Warnings, "; "))}
	}
	return nil
}

// getMatchingHooks returns hooks whose matcher matches the given tool name.
func (m *Manager) getMatchingHooks(eventType EventType, toolName string) []compiledHook {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var matched []compiledHook
	for _, hook := range m.matchers[eventType] {
		if hook.matcher == nil {
			matched = append(matched, hook)
			continue
		}
		if hook.matcher.MatchString(toolName) {
			matched = append(matched, hook)
		}
	}
	return matched
}

// executeHook executes a single hook script with a timeout and returns its
// decision.
func (m *Manager) executeHook(hook *compiledHook, payload *EventPayload) (*ValidationResult, error) {
	m.mu.RLock()
	timeout := m.timeout
	m.mu.RUnlock()
	if hook.config.TimeoutMs > 0 {
		timeout = time.Duration(hook.config.TimeoutMs) * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return nil, &HookError{Code: "JSON_ENCODE_ERROR", Message: "failed to marshal event payload", Err: err}
	}

	cmd := exec.CommandContext(ctx, hook.resolved.binary, hook.resolved.args...)
	// Run the hook in its own process group so a timeout kills the whole
	// tree (shell + children), not just the top-level process; otherwise a
	// child holding the output pipes would block Wait until it exits.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = timeout
	cmd.Stdin = bytes.NewReader(jsonData)
	var stdout, stderr bytes.Buffer
	stdoutW := &MaxBytesWriter{W: &stdout, Max: m.maxOutput}
	stderrW := &MaxBytesWriter{W: &stderr, Max: m.maxOutput}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	runErr := cmd.Run()

	// Timeout or process launch failure: non-blocking warning.
	if runErr != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return &ValidationResult{Allowed: true}, &HookError{Code: "TIMEOUT", Message: "hook execution timed out"}
		}
		var exitErr *exec.ExitError
		if !errors.As(runErr, &exitErr) {
			return &ValidationResult{Allowed: true}, &HookError{Code: "EXEC_ERROR", Message: "failed to execute hook", Err: runErr}
		}
		switch exitErr.ExitCode() {
		case 2:
			return &ValidationResult{Allowed: false, DenyMsg: strings.TrimSpace(stderr.String())}, nil
		case 0:
			// Unreachable: exit 0 does not produce an error.
		default:
			return &ValidationResult{Allowed: true}, &HookError{
				Code:    "HOOK_WARNING",
				Message: fmt.Sprintf("hook returned non-zero exit code %d", exitErr.ExitCode()),
			}
		}
	}

	// Exit 0: allow, but honor the optional JSON decision protocol on stdout.
	if out := strings.TrimSpace(stdout.String()); out != "" {
		var decision struct {
			Decision string `json:"decision"`
			Reason   string `json:"reason"`
		}
		if err := json.Unmarshal([]byte(out), &decision); err == nil {
			switch decision.Decision {
			case "deny":
				reason := decision.Reason
				if reason == "" {
					reason = "hook denied the operation"
				}
				return &ValidationResult{Allowed: false, DenyMsg: reason}, nil
			case "approve":
				return &ValidationResult{Allowed: true}, nil
			}
		}
	}
	return &ValidationResult{Allowed: true}, nil
}

// MaxBytesWriter prevents writing more than N bytes; excess is discarded and
// the underlying writer receives io.EOF signaling truncation.
type MaxBytesWriter struct {
	W       io.Writer
	Max     int64
	Written int64
}

func (m *MaxBytesWriter) Write(p []byte) (int, error) {
	remaining := m.Max - m.Written
	if remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := m.W.Write(p)
	m.Written += int64(n)
	return n, err
}

// Helper functions for creating hook payloads.

// NewPreToolUsePayload creates a PreToolUse event payload.
func NewPreToolUsePayload(toolName string, input map[string]interface{}, sessionID string) *EventPayload {
	return &EventPayload{
		Event:      string(PreToolUse),
		ToolName:   toolName,
		ToolInput:  input,
		SessionID:  sessionID,
		Timestamp:  time.Now(),
	}
}

// NewPostToolUsePayload creates a PostToolUse event payload.
func NewPostToolUsePayload(toolName string, input, response map[string]interface{}, sessionID string) *EventPayload {
	return &EventPayload{
		Event:        string(PostToolUse),
		ToolName:     toolName,
		ToolInput:    input,
		ToolResponse: response,
		SessionID:    sessionID,
		Timestamp:    time.Now(),
	}
}

// NewSessionStartPayload creates a SessionStart event payload.
func NewSessionStartPayload(sessionID string) *EventPayload {
	return &EventPayload{
		Event:     string(SessionStart),
		SessionID: sessionID,
		Timestamp: time.Now(),
	}
}

// NewStopPayload creates a Stop event payload.
func NewStopPayload(sessionID string) *EventPayload {
	return &EventPayload{
		Event:     string(Stop),
		SessionID: sessionID,
		Timestamp: time.Now(),
	}
}
