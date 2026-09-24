// Package wasi: Backend adapter.
//
// Backend implements the parent sandbox.Sandbox interface on top of the
// WasiExecutor. It maps the generic SandboxConfig/ExecuteInput onto the
// WASI-specific types and translates result statuses back.
package wasi

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"agent-harness/packages/sandbox"
)

// Backend is a sandbox.Sandbox backed by WASI + wasmtime.
type Backend struct {
	name  string
	mu    sync.RWMutex
	exec  *WasiExecutor
	cfg   WasiSandboxConfig
	ready bool
}

// NewBackend creates a new WASI-based sandbox backend with default
// configuration.
func NewBackend() *Backend {
	return &Backend{
		name: "wasi",
		cfg:  DefaultConfig(),
	}
}

// NewBackendWithConfig creates a WASI backend with an explicit config.
func NewBackendWithConfig(cfg WasiSandboxConfig) *Backend {
	return &Backend{name: "wasi", cfg: cfg}
}

// Name returns the backend name.
func (b *Backend) Name() string {
	return b.name
}

// Initialize prepares the sandbox environment (creates the shared wasmtime
// engine). It is idempotent.
func (b *Backend) Initialize(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ready {
		return nil
	}
	exec, err := NewWasiExecutor(b.cfg)
	if err != nil {
		return fmt.Errorf("wasi backend: %w", err)
	}
	b.exec = exec
	b.ready = true
	return nil
}

// Execute runs the provided input within isolation constraints. Only WASM
// bytecode is supported; other file types are rejected.
func (b *Backend) Execute(ctx context.Context, config sandbox.SandboxConfig, input sandbox.ExecuteInput) (*sandbox.ExecResult, error) {
	b.mu.RLock()
	exec, ready := b.exec, b.ready
	b.mu.RUnlock()
	if !ready || exec == nil {
		return nil, fmt.Errorf("wasi backend: not initialized, call Initialize first")
	}

	if input.FileType != "" && input.FileType != sandbox.FileTypeWASM {
		return nil, fmt.Errorf("wasi backend: unsupported file type %q (only wasm is supported)", input.FileType)
	}
	if len(input.Code) == 0 {
		return nil, fmt.Errorf("wasi backend: no WASM bytes provided")
	}

	// Honor a tighter per-call timeout when provided.
	if config.Timeout > 0 && config.Timeout < b.cfg.Timeout {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}
	wasIn := ExecuteInput{
		WasmBytes: input.Code,
		Args:      input.Args,
		Stdin:     input.InputData,
	}
	if input.Deadline != nil {
		timeout := time.Until(*input.Deadline)
		if timeout > 0 && timeout < b.cfg.Timeout {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}
	out, err := exec.Execute(ctx, wasIn)
	if err != nil {
		return nil, err
	}
	return convertResult(out), nil
}

// Cleanup releases resources held by this backend.
func (b *Backend) Cleanup() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.exec != nil {
		b.exec.Close()
		b.exec = nil
	}
	b.ready = false
}

// HealthCheck verifies the backend is operational.
func (b *Backend) HealthCheck(ctx context.Context) error {
	b.mu.RLock()
	exec, ready := b.exec, b.ready
	b.mu.RUnlock()
	if !ready {
		return fmt.Errorf("wasi backend: not initialized")
	}
	if err := exec.Initialize(ctx); err != nil {
		return err
	}
	// A fresh scratch dir confirms the host temp area is writable.
	d, err := os.MkdirTemp("", "wasi-health-*")
	if err != nil {
		return fmt.Errorf("wasi health: cannot create temp dir: %w", err)
	}
	os.RemoveAll(d)
	return nil
}

// IsolationLevel returns the isolation level this backend provides.
func (b *Backend) IsolationLevel() sandbox.IsolationLevel {
	return sandbox.WasmIsolation
}

// convertResult maps a WASI ExecResult onto the generic sandbox result.
func convertResult(r *ExecResult) *sandbox.ExecResult {
	status := sandbox.ExecFailed
	switch r.Status {
	case ExecSuccess:
		status = sandbox.ExecSucceeded
	case ExecTimedOut:
		status = sandbox.ExecTimedOut
	case ExecMemoryLimit:
		status = sandbox.ExecMemoryLimit
	case ExecFuelExhausted:
		status = sandbox.ExecFailed
	case ExecUnknown:
		status = sandbox.ExecUnknown
	default:
		status = sandbox.ExecFailed
	}
	return &sandbox.ExecResult{
		Status:        status,
		ExitCode:      r.ExitCode,
		Stdout:        r.Stdout,
		Stderr:        r.Stderr,
		ElapsedTimeMS: r.ElapsedMS,
		ErrorCode:     r.ErrorCode,
		ErrorMessage:  r.ErrorMessage,
	}
}
