package hooks

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeScript creates an executable hook script in dir and returns its path.
func writeScript(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// loadManager builds a manager whose allowlist root is dir and loads a YAML
// config in dir that references hooks by filename (relative to config dir).
func loadManager(t *testing.T, dir, yamlContent string) (*Manager, string) {
	t.Helper()
	m := NewHookManager()
	if err := m.SetAllowedRoots([]string{dir}); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "hooks.yaml")
	if err := os.WriteFile(cfgPath, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.LoadConfig(cfgPath); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return m, cfgPath
}

func TestPreToolUseDenyExit2(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "deny.sh", `#!/bin/bash
echo "forbidden by policy" >&2
exit 2
`)
	cfg := `
hooks:
  PreToolUse:
    - matcher: "bash"
      command: "./deny.sh"
`
	m, _ := loadManager(t, dir, cfg)

	payload := NewPreToolUsePayload("bash", map[string]interface{}{"command": "rm x"}, "sess-1")
	result, err := m.ValidatePreToolUse(payload)
	if err != nil {
		t.Fatalf("ValidatePreToolUse: %v", err)
	}
	if result.Allowed {
		t.Fatal("expected denial")
	}
	if !strings.Contains(result.DenyMsg, "forbidden by policy") {
		t.Fatalf("deny msg = %q", result.DenyMsg)
	}

	// A non-matching tool passes without invoking the hook.
	payload2 := NewPreToolUsePayload("file_read", map[string]interface{}{"path": "/tmp/a"}, "sess-1")
	res2, err := m.ValidatePreToolUse(payload2)
	if err != nil || !res2.Allowed {
		t.Fatalf("file_read should be allowed: %v %v", res2, err)
	}
}

func TestPreToolUseAllowAndPayloadDelivery(t *testing.T) {
	dir := t.TempDir()
	outFile := filepath.Join(dir, "received.json")
	writeScript(t, dir, "spy.sh", `#!/bin/bash
cat > "` + outFile + `"
exit 0
`)
	cfg := `
hooks:
  PreToolUse:
    - matcher: ".*"
      command: "./spy.sh"
`
	m, _ := loadManager(t, dir, cfg)

	payload := NewPreToolUsePayload("bash", map[string]interface{}{"command": "echo hi"}, "sess-42")
	result, err := m.ValidatePreToolUse(payload)
	if err != nil || !result.Allowed {
		t.Fatalf("expected allow: %v %v", result, err)
	}
	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("hook did not receive payload: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, `"event":"PreToolUse"`) {
		t.Fatalf("payload missing event: %s", s)
	}
	if !strings.Contains(s, `"tool_name":"bash"`) {
		t.Fatalf("payload missing tool_name: %s", s)
	}
	if !strings.Contains(s, `"session_id":"sess-42"`) {
		t.Fatalf("payload missing session_id: %s", s)
	}
	if !strings.Contains(s, `"command":"echo hi"`) {
		t.Fatalf("payload missing tool_input: %s", s)
	}
}

func TestPreToolUseJSONDecisionDeny(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "json-deny.sh", `#!/bin/bash
cat >/dev/null
echo '{"decision":"deny","reason":"custom json denial"}'
exit 0
`)
	cfg := `
hooks:
  PreToolUse:
    - matcher: ""
      command: "./json-deny.sh"
`
	m, _ := loadManager(t, dir, cfg)

	result, err := m.ValidatePreToolUse(NewPreToolUsePayload("bash", nil, "s"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Allowed {
		t.Fatal("expected JSON-decision denial")
	}
	if result.DenyMsg != "custom json denial" {
		t.Fatalf("deny reason = %q", result.DenyMsg)
	}
}

func TestNonZeroExitIsWarning(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "warn.sh", `#!/bin/bash
echo "oops" >&2
exit 3
`)
	cfg := `
hooks:
  PreToolUse:
    - matcher: ""
      command: "./warn.sh"
`
	m, _ := loadManager(t, dir, cfg)

	result, err := m.ValidatePreToolUse(NewPreToolUsePayload("bash", nil, "s"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Allowed {
		t.Fatal("exit 3 must not block")
	}
	if len(result.Warnings) == 0 || !strings.Contains(result.Warnings[0], "3") {
		t.Fatalf("expected warning about exit 3, got %v", result.Warnings)
	}
}

func TestHookTimeout(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "slow.sh", `#!/bin/bash
sleep 5
exit 0
`)
	cfg := `
hooks:
  PreToolUse:
    - matcher: ""
      command: "./slow.sh"
      timeout_ms: 200
`
	m, _ := loadManager(t, dir, cfg)

	start := time.Now()
	result, err := m.ValidatePreToolUse(NewPreToolUsePayload("bash", nil, "s"))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Allowed {
		t.Fatal("timeout must not block")
	}
	if len(result.Warnings) == 0 || !strings.Contains(result.Warnings[0], "TIMEOUT") {
		t.Fatalf("expected timeout warning, got %v", result.Warnings)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("hook was not killed at timeout (elapsed %v)", elapsed)
	}
}

func TestAllowlistRejectsExternalBinary(t *testing.T) {
	dir := t.TempDir()
	// /bin/echo is outside the temp dir => must be rejected at LoadConfig.
	cfg := `
hooks:
  PreToolUse:
    - matcher: ""
      command: "/bin/echo"
`
	m := NewHookManager()
	if err := m.SetAllowedRoots([]string{dir}); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "hooks.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.LoadConfig(cfgPath); err == nil {
		t.Fatal("expected LoadConfig to reject a binary outside the allowlist")
	} else if !strings.Contains(err.Error(), "allowlist") && !strings.Contains(err.Error(), "INVALID_HOOK") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBareCommandRejected(t *testing.T) {
	dir := t.TempDir()
	// A bare name resolves via PATH against the config dir; there is no
	// "nonsense-binary" there, so resolution must fail.
	cfg := `
hooks:
  PreToolUse:
    - matcher: ""
      command: "nonsense-binary"
`
	m := NewHookManager()
	if err := m.SetAllowedRoots([]string{dir}); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "hooks.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.LoadConfig(cfgPath); err == nil {
		t.Fatal("expected LoadConfig to reject an unresolvable bare command")
	}
}

func TestSessionLifecycle(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "lifecycle.log")
	writeScript(t, dir, "log.sh", `#!/bin/bash
cat | grep -o '"event":"[A-Za-z]*"' >> "` + marker + `"
exit 0
`)
	cfg := `
hooks:
  SessionStart:
    - command: "./log.sh"
  Stop:
    - command: "./log.sh"
`
	m, _ := loadManager(t, dir, cfg)

	if err := m.NotifySessionStart("sess-lc"); err != nil {
		t.Fatalf("NotifySessionStart: %v", err)
	}
	if err := m.NotifyStop("sess-lc"); err != nil {
		t.Fatalf("NotifyStop: %v", err)
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("lifecycle hook did not run: %v", err)
	}
	if !strings.Contains(string(data), "SessionStart") || !strings.Contains(string(data), "Stop") {
		t.Fatalf("lifecycle events missing: %s", data)
	}
}

func TestPostToolUseNonBlocking(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "post.log")
	writeScript(t, dir, "post.sh", `#!/bin/bash
cat >/dev/null
echo done >> "` + marker + `"
`)
	cfg := `
hooks:
  PostToolUse:
    - matcher: "bash"
      command: "./post.sh"
`
	m, _ := loadManager(t, dir, cfg)

	m.RunPostToolUse(NewPostToolUsePayload("bash", map[string]interface{}{"command": "x"}, map[string]interface{}{"ok": true}, "s"))

	// The async hook should finish quickly; poll briefly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(marker); err == nil {
			if strings.Contains(string(data), "done") {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("post-tool hook did not complete")
}

func TestMultipleHooksFirstDenyWins(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "allow.sh", "#!/bin/bash\nexit 0\n")
	writeScript(t, dir, "deny2.sh", "#!/bin/bash\necho second says no >&2\nexit 2\n")
	cfg := `
hooks:
  PreToolUse:
    - matcher: ""
      command: "./allow.sh"
    - matcher: ""
      command: "./deny2.sh"
`
	m, _ := loadManager(t, dir, cfg)

	result, err := m.ValidatePreToolUse(NewPreToolUsePayload("bash", nil, "s"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Allowed {
		t.Fatal("a denying hook must block even if an earlier hook allowed")
	}
	if !strings.Contains(result.DenyMsg, "second says no") {
		t.Fatalf("deny msg = %q", result.DenyMsg)
	}
}

func TestMaxBytesWriterTruncation(t *testing.T) {
	var buf strings.Builder
	w := &MaxBytesWriter{W: &buf, Max: 5}
	if _, err := w.Write([]byte("1234567890")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if buf.String() != "12345" {
		t.Fatalf("truncation: %q", buf.String())
	}
	if w.Written != 5 {
		t.Fatalf("written = %d", w.Written)
	}
	if n, err := w.Write([]byte("abc")); n != 0 || err == nil {
		t.Fatalf("post-max write: n=%d err=%v", n, err)
	}
}
