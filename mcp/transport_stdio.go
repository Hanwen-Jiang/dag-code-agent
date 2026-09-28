package mcp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The design of `harness` deliberately keeps process spawning out of the core
// closure; the whole point of this file is that it lives in `mcp` instead.
const (
	defaultMaxMessageBytes = 16 << 20 // 16 MiB per newline-delimited frame
	terminateGrace         = 3 * time.Second
	killGrace              = 2 * time.Second
	stderrTailBytes        = 8 << 10
	stdioWriteBuffer       = 64
)

// StdioTransport executes an MCP server and speaks newline-delimited JSON-RPC
// over its stdin/stdout (design 6.2).
//
// Executing a local binary is the same risk class packages/hooks already
// handles, and the mitigations are the ones proven there (design 6.6):
//
//   - Command must be an absolute path; there is no bare $PATH lookup ever.
//   - The canonical path (symlinks resolved) must live inside AllowedRoots, so
//     a symlink cannot escape the allowlist. An empty AllowedRoots refuses
//     everything rather than defaulting to something permissive.
//   - The child receives a whitelisted environment, never all of
//     os.Environ(); only Env entries explicitly supplied by the host are
//     passed through on top.
//   - The child runs in its own process group and Close kills the whole
//     group, so no orphan survives.
type StdioTransport struct {
	Command      string
	Args         []string
	Env          []string
	AllowedRoots []string

	mu       sync.Mutex
	started  bool
	startErr error
	conn     *stdioConn
}

// Start validates the allowlist and spawns the server. It is idempotent and is
// called by Dial when the transport implements it.
func (t *StdioTransport) Start(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started {
		return t.startErr
	}
	t.started = true
	ctx = orBackground(ctx)
	if err := ctx.Err(); err != nil {
		t.startErr = err
		return err
	}
	t.startErr = t.start()
	return t.startErr
}

func (t *StdioTransport) start() error {
	binary, err := resolveExecutable(t.Command, t.AllowedRoots)
	if err != nil {
		return err
	}

	cmd := exec.Command(binary, t.Args...)
	cmd.Env = buildEnv(t.Env)
	// Own process group: the group kill below reaches grandchildren too.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("mcp: server stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("mcp: server stdout pipe: %w", err)
	}
	stderr := &tailBuffer{limit: stderrTailBytes}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("mcp: start %s: %w", binary, err)
	}
	conn := &stdioConn{
		cmd:      cmd,
		stdin:    stdin,
		stdout:   bufio.NewReaderSize(stdout, 64<<10),
		stderr:   stderr,
		frames:   make(chan []byte, stdioWriteBuffer),
		done:     make(chan struct{}),
		readDone: make(chan struct{}),
		waitDone: make(chan struct{}),
	}
	t.conn = conn
	go conn.pump()
	go conn.reap()
	return nil
}

// Send writes one newline-delimited JSON-RPC message.
func (t *StdioTransport) Send(ctx context.Context, payload []byte) error {
	conn, err := t.connection(ctx)
	if err != nil {
		return err
	}
	frame := bytes.TrimSpace(payload)
	if len(frame) == 0 {
		return nil
	}
	if len(frame) > defaultMaxMessageBytes {
		return fmt.Errorf("mcp: outgoing frame exceeds %d bytes", defaultMaxMessageBytes)
	}
	if _, err := conn.write(append(append([]byte(nil), frame...), '\n')); err != nil {
		return err
	}
	return nil
}

// Receive returns the next frame from the server's stdout.
func (t *StdioTransport) Receive(ctx context.Context) ([]byte, error) {
	conn, err := t.connection(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case frame, ok := <-conn.frames:
		if !ok {
			return nil, conn.failure()
		}
		return frame, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-conn.done:
		return nil, ErrClosed
	}
}

// Close terminates the server's whole process group and waits for it to be
// reaped (SIGTERM, then SIGKILL after a grace period).
func (t *StdioTransport) Close() error {
	t.mu.Lock()
	conn := t.conn
	t.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.shutdown()
}

func (t *StdioTransport) connection(ctx context.Context) (*stdioConn, error) {
	if err := t.Start(ctx); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn == nil {
		return nil, errors.New("mcp: stdio transport is not started")
	}
	return t.conn, nil
}

// resolveExecutable enforces the allowlist: absolute path, canonical path,
// inside one of the canonicalised roots.
func resolveExecutable(command string, allowedRoots []string) (string, error) {
	raw := strings.TrimSpace(command)
	if raw == "" {
		return "", errors.New("mcp: Command is empty")
	}
	if !filepath.IsAbs(raw) {
		return "", fmt.Errorf("mcp: Command %q must be an absolute path; bare $PATH lookup is refused", raw)
	}
	if len(allowedRoots) == 0 {
		return "", errors.New("mcp: no AllowedRoots configured; refusing to execute anything")
	}
	// Symlinks must not escape the allowlist, so compare canonical paths.
	canonical, err := filepath.EvalSymlinks(filepath.Clean(raw))
	if err != nil {
		return "", fmt.Errorf("mcp: cannot resolve Command %q: %w", raw, err)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", fmt.Errorf("mcp: cannot stat Command %q: %w", canonical, err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("mcp: Command %q is a directory", canonical)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return "", fmt.Errorf("mcp: Command %q is not executable", canonical)
	}
	roots := canonicalRoots(allowedRoots)
	if !insideAnyRoot(canonical, roots) {
		return "", fmt.Errorf("mcp: binary %s is outside the allowed roots %v (allowlist)", canonical, roots)
	}
	return canonical, nil
}

func canonicalRoots(roots []string) []string {
	canonical := make([]string, 0, len(roots))
	for _, root := range roots {
		abs, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		}
		canonical = append(canonical, filepath.Clean(abs))
	}
	return canonical
}

func insideAnyRoot(canonical string, roots []string) bool {
	for _, root := range roots {
		if canonical == root || strings.HasPrefix(canonical, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// inheritedEnvAllowlist is the whitelist of parent variables a server may
// inherit. Loader-injection variables (LD_PRELOAD, DYLD_INSERT_LIBRARIES and
// friends) are absent by construction, so they cannot leak into the child.
var inheritedEnvAllowlist = []string{
	"PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TZ",
	"USER", "LOGNAME", "SHELL", "TERM",
	"SystemRoot", "COMSPEC", "PATHEXT", "WINDIR",
}

// buildEnv returns the child environment: the whitelist inherited from this
// process, then the host-provided entries (which win on conflict).
func buildEnv(extra []string) []string {
	merged := map[string]string{}
	var order []string
	add := func(entry string) {
		if entry == "" || strings.ContainsRune(entry, 0) {
			return
		}
		key, _, ok := strings.Cut(entry, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return
		}
		canonical := strings.ToUpper(key)
		if _, seen := merged[canonical]; !seen {
			order = append(order, canonical)
		}
		merged[canonical] = entry
	}
	for _, key := range inheritedEnvAllowlist {
		if value, ok := os.LookupEnv(key); ok {
			add(key + "=" + value)
		}
	}
	for _, entry := range extra {
		add(entry)
	}
	env := make([]string, 0, len(order))
	for _, key := range order {
		env = append(env, merged[key])
	}
	return env
}

// stdioConn owns one server process and its pipes.
type stdioConn struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr *tailBuffer

	frames   chan []byte
	done     chan struct{}
	readDone chan struct{}
	waitDone chan struct{}

	writeMu sync.Mutex

	mu        sync.Mutex
	readErr   error
	waitErr   error
	closeErr  error
	closeOnce sync.Once
}

func (c *stdioConn) write(frame []byte) (int, error) {
	select {
	case <-c.done:
		return 0, ErrClosed
	default:
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	written, err := c.stdin.Write(frame)
	if err != nil {
		if errors.Is(err, os.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE) {
			return written, fmt.Errorf("%w: %v", ErrClosed, err)
		}
		return written, fmt.Errorf("mcp: write to server stdin: %w", err)
	}
	return written, nil
}

// pump is the only reader of stdout; every frame is handed to Receive through
// a buffered channel.
func (c *stdioConn) pump() {
	defer close(c.readDone)
	defer close(c.frames)
	for {
		frame, err := readFrame(c.stdout)
		if err != nil {
			c.mu.Lock()
			if c.readErr == nil {
				c.readErr = err
			}
			c.mu.Unlock()
			return
		}
		if len(bytes.TrimSpace(frame)) == 0 {
			continue
		}
		select {
		case c.frames <- frame:
		case <-c.done:
			return
		}
	}
}

// reap waits for the reader to finish and then reaps the child, so Wait never
// races with an in-flight read of the same pipe.
func (c *stdioConn) reap() {
	<-c.readDone
	err := c.cmd.Wait()
	c.mu.Lock()
	c.waitErr = err
	c.mu.Unlock()
	close(c.waitDone)
}

func (c *stdioConn) shutdown() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.stdin.Close()
		_ = c.signal(syscall.SIGTERM)
		if !waitFor(c.waitDone, terminateGrace) {
			_ = c.signal(syscall.SIGKILL)
			if !waitFor(c.waitDone, killGrace) {
				c.closeErr = errors.New("mcp: server process group did not exit after SIGKILL")
				return
			}
		}
	})
	return c.closeErr
}

func (c *stdioConn) signal(sig syscall.Signal) error {
	if c.cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-c.cmd.Process.Pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// failure explains why the frame stream ended.
func (c *stdioConn) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.readErr != nil && errors.Is(c.readErr, io.EOF):
		return fmt.Errorf("%w: server closed its stdout", ErrClosed)
	case c.readErr != nil:
		return fmt.Errorf("mcp: read from server stdout: %w", c.readErr)
	case c.waitErr != nil:
		return fmt.Errorf("%w: server exited: %v (stderr: %s)", ErrClosed, c.waitErr, c.stderr.String())
	default:
		return ErrClosed
	}
}

// readFrame reads one newline-delimited frame with a hard size cap.
func readFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		if len(frame)+len(chunk) > defaultMaxMessageBytes {
			return nil, fmt.Errorf("mcp: JSON-RPC frame exceeds %d bytes", defaultMaxMessageBytes)
		}
		frame = append(frame, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return frame, nil
	}
}

func waitFor(ch chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	}
}

// tailBuffer keeps the last limit bytes written to it, for diagnostics.
type tailBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.limit {
		b.buf = b.buf[len(b.buf)-b.limit:]
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.buf))
}
