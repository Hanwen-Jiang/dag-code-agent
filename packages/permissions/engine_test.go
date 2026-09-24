package permissions

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// testConfig returns a deterministic baseline: approval not required,
// rate limit off, no tool allowlist. Tests override what they exercise.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.RequireApproval = false
	cfg.RateLimit.Enabled = false
	return cfg
}

func mustCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected denial with code %s, got nil error", want)
	}
	var pe *PermissionError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PermissionError, got %T: %v", err, err)
	}
	if pe.Code != want {
		t.Fatalf("expected code %s, got %s (reason: %s)", want, pe.Code, pe.Reason)
	}
}

func TestHardBlockedCommand(t *testing.T) {
	e := NewPermissionEngine(testConfig())
	err := e.Check("bash", map[string]interface{}{"command": "rm -rf /"})
	mustCode(t, err, "HARD_BLOCKED")

	err = e.Check("bash", map[string]interface{}{"command": "dd if=/dev/zero of=/dev/sda"})
	mustCode(t, err, "HARD_BLOCKED")

	// Harmless command passes.
	if err := e.Check("bash", map[string]interface{}{"command": "ls -la"}); err != nil {
		t.Fatalf("expected allow for ls, got: %v", err)
	}
}

func TestCompositeDecomposition(t *testing.T) {
	cfg := testConfig()
	// Anchored pattern: only the sub-command (not the full string) matches.
	cfg.HardBlockedPatterns = []string{`^rm\s+-rf\s+/`}
	e := NewPermissionEngine(cfg)

	err := e.Check("bash", map[string]interface{}{"command": "ls && rm -rf /"})
	mustCode(t, err, "COMPOSITE_DENIED")

	// Decompose helper sanity.
	parts := decomposeComposite("a && b; c | d > e")
	if len(parts) != 5 {
		t.Fatalf("expected 5 parts, got %d: %v", len(parts), parts)
	}
}

func TestConfigDenyList(t *testing.T) {
	e := NewPermissionEngine(testConfig())
	err := e.Check("bash", map[string]interface{}{"command": "git push --force origin main"})
	mustCode(t, err, "CONFIG_DENIED")

	// Plain push is not denied by config (and is not auto-allowed, but
	// approval is disabled in testConfig, so it passes).
	if err := e.Check("bash", map[string]interface{}{"command": "git push origin main"}); err != nil {
		t.Fatalf("plain push should pass, got: %v", err)
	}
}

func TestToolAllowlist(t *testing.T) {
	cfg := testConfig()
	cfg.ToolAllowlist = []string{"bash", "file_read"}
	e := NewPermissionEngine(cfg)

	if err := e.Check("bash", map[string]interface{}{"command": "ls"}); err != nil {
		t.Fatalf("bash should be allowed, got: %v", err)
	}
	err := e.Check("curl", map[string]interface{}{"url": "http://example.com"})
	mustCode(t, err, "TOOL_NOT_ALLOWED")
}

func TestPathScope(t *testing.T) {
	e := NewPermissionEngine(testConfig())

	// /tmp is in the default allowed base paths; a file there passes.
	if err := e.Check("bash", map[string]interface{}{
		"command": "cat data.txt",
		"path":    "/tmp/data.txt",
	}); err != nil {
		t.Fatalf("path under /tmp should pass, got: %v", err)
	}

	// Path traversal out of /tmp.
	err := e.Check("bash", map[string]interface{}{
		"command": "cat evil",
		"path":    "/tmp/../../etc/passwd",
	})
	if err == nil {
		t.Fatal("traversal path should be denied")
	}

	// Sensitive pattern inside an allowed base.
	err = e.Check("bash", map[string]interface{}{
		"command": "cat key",
		"path":    "/tmp/id_rsa",
	})
	if err == nil {
		t.Fatal("sensitive path should be denied")
	}
}

func TestSecretRedaction(t *testing.T) {
	cfg := testConfig()
	cfg.RequireApproval = true // force through the approval handler

	var seenArgs map[string]interface{}
	e := NewPermissionEngine(cfg)
	e.SetApprovalHandler(approvalFunc(func(tool string, input map[string]interface{}) error {
		seenArgs = input
		return nil
	}))

	err := e.Check("bash", map[string]interface{}{
		"command": "deploy --api-key=sk-abcdefghij1234567890",
	})
	if err != nil {
		t.Fatalf("approved call should pass, got: %v", err)
	}
	if seenArgs == nil {
		t.Fatal("approval handler was not called")
	}
	cmd, _ := seenArgs["command"].(string)
	if strings.Contains(cmd, "sk-abcdefghij1234567890") {
		t.Fatalf("secret leaked to approval handler: %q", cmd)
	}
	if !strings.Contains(cmd, "[REDACTED]") {
		t.Fatalf("expected [REDACTED] marker in %q", cmd)
	}
}

func TestDefaultDenyWithoutApproval(t *testing.T) {
	cfg := testConfig()
	cfg.RequireApproval = true
	e := NewPermissionEngine(cfg)
	// Not auto-allowed, no handler installed.
	err := e.Check("bash", map[string]interface{}{"command": "frobnicate --all"})
	mustCode(t, err, "DEFAULT_DENY")
}

func TestApprovalDenied(t *testing.T) {
	cfg := testConfig()
	cfg.RequireApproval = true
	e := NewPermissionEngine(cfg)
	e.SetApprovalHandler(approvalFunc(func(tool string, input map[string]interface{}) error {
		return errors.New("user said no")
	}))
	err := e.Check("bash", map[string]interface{}{"command": "frobnicate --all"})
	mustCode(t, err, "APPROVAL_DENIED")
}

func TestAutoAllowShortCircuitsApproval(t *testing.T) {
	cfg := testConfig()
	cfg.RequireApproval = true
	e := NewPermissionEngine(cfg)
	calls := 0
	e.SetApprovalHandler(approvalFunc(func(tool string, input map[string]interface{}) error {
		calls++
		return nil
	}))
	if err := e.Check("bash", map[string]interface{}{"command": "git status"}); err != nil {
		t.Fatalf("git status should auto-allow, got: %v", err)
	}
	if calls != 0 {
		t.Fatalf("auto-allowed command must not hit the approval handler, calls=%d", calls)
	}
}

func TestRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.RateLimit = RateLimitConfig{Enabled: true, MaxCalls: 3, Window: "1m"}
	e := NewPermissionEngine(cfg)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.SetClock(func() time.Time { return base })

	args := map[string]interface{}{"command": "ls"}
	for i := 0; i < 3; i++ {
		if err := e.Check("bash", args); err != nil {
			t.Fatalf("call %d should pass, got: %v", i+1, err)
		}
	}
	// Fourth call within the same window is limited.
	err := e.Check("bash", args)
	mustCode(t, err, "RATE_LIMITED")

	// After the window slides past, calls are allowed again.
	e.SetClock(func() time.Time { return base.Add(2 * time.Minute) })
	if err := e.Check("bash", args); err != nil {
		t.Fatalf("call after window should pass, got: %v", err)
	}
}

func TestAuditTrail(t *testing.T) {
	e := NewPermissionEngine(testConfig())
	_ = e.Check("bash", map[string]interface{}{"command": "ls"})          // allowed (auto)
	_ = e.Check("bash", map[string]interface{}{"command": "rm -rf /"}) // denied

	entries := e.Audit().Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 audit entries, got %d", len(entries))
	}
	if entries[0].Verdict != VerdictAllow || entries[0].Code != "AUTO_ALLOWED" {
		t.Fatalf("entry 0 = %+v", entries[0])
	}
	if entries[1].Verdict != VerdictDeny || entries[1].Code != "HARD_BLOCKED" {
		t.Fatalf("entry 1 = %+v", entries[1])
	}

	jsonOut, err := e.Audit().Export()
	if err != nil {
		t.Fatalf("audit export failed: %v", err)
	}
	if !strings.Contains(jsonOut, "HARD_BLOCKED") {
		t.Fatalf("audit JSON missing denied code: %s", jsonOut)
	}
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/perm.yaml"
	yaml := fmt.Sprintf(`
tool_allowlist:
  - bash
denied_command_patterns:
  - ^forbidden
deny_descriptions:
  - explicitly forbidden in test
allowed_command_patterns:
  - ^okcmd
rate_limit:
  enabled: true
  max_calls: 2
  window: 5s
`)
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	e := NewPermissionEngine(cfg)
	if err := e.LoadConfig(path); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if err := e.Check("bash", map[string]interface{}{"command": "okcmd --fast"}); err != nil {
		t.Fatalf("okcmd should be auto-allowed after LoadConfig: %v", err)
	}
	err := e.Check("bash", map[string]interface{}{"command": "forbidden thing"})
	mustCode(t, err, "CONFIG_DENIED")
	err = e.Check("curl", map[string]interface{}{"url": "x"})
	mustCode(t, err, "TOOL_NOT_ALLOWED")
}

// approvalFunc adapts a function to ApprovalHandler.
type approvalFunc func(tool string, input map[string]interface{}) error

func (f approvalFunc) RequestApproval(tool string, input map[string]interface{}) error {
	return f(tool, input)
}
