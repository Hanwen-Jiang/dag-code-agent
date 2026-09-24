package wasi_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/sandbox"
	"github.com/Hanwen-Jiang/dag-code-agent/packages/sandbox/wasi"
)

func loadWasm(t *testing.T) []byte {
	t.Helper()
	d, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		p := filepath.Join(d, "testdata", "hello.wasm")
		if b, err := os.ReadFile(p); err == nil {
			return b
		}
		parent := filepath.Dir(d)
		if parent == d {
			break
		}
		d = parent
	}
	t.Fatal("testdata/hello.wasm not found")
	return nil
}

func TestBackendLifecycleAndExecute(t *testing.T) {
	b := wasi.NewBackend()
	if b.Name() != "wasi" {
		t.Fatalf("name = %q, want wasi", b.Name())
	}

	// Execute before Initialize must fail cleanly.
	if _, err := b.Execute(context.Background(), sandbox.SandboxConfig{}, sandbox.ExecuteInput{
		FileType: sandbox.FileTypeWASM,
	}); err == nil {
		t.Fatal("expected error before Initialize")
	}

	cfg := wasi.DefaultConfig()
	cfg.FuelLimit = 100_000_000
	cfg.Timeout = 5 * time.Second
	b = wasi.NewBackendWithConfig(cfg)

	if err := b.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	// Idempotent.
	if err := b.Initialize(context.Background()); err != nil {
		t.Fatalf("second Initialize: %v", err)
	}
	if err := b.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if b.IsolationLevel() != sandbox.WasmIsolation {
		t.Fatalf("isolation = %q, want wasm", b.IsolationLevel())
	}

	input := sandbox.ExecuteInput{
		Code:     loadWasm(t),
		FileType: sandbox.FileTypeWASM,
	}
	res, err := b.Execute(context.Background(), sandbox.SandboxConfig{}, input)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != sandbox.ExecSucceeded {
		t.Fatalf("status = %q (%s: %s), want succeeded", res.Status, res.ErrorCode, res.ErrorMessage)
	}
	if string(res.Stdout) != "hello from wasi\n" {
		t.Fatalf("stdout = %q", res.Stdout)
	}

	b.Cleanup()
	b.Cleanup() // idempotent
	// After Cleanup the executor is gone.
	if _, err := b.Execute(context.Background(), sandbox.SandboxConfig{}, input); err == nil {
		t.Fatal("expected error after Cleanup")
	}
}

func TestBackendRejectsNonWASM(t *testing.T) {
	b := wasi.NewBackendWithConfig(wasi.DefaultConfig())
	if err := b.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	defer b.Cleanup()

	if _, err := b.Execute(context.Background(), sandbox.SandboxConfig{}, sandbox.ExecuteInput{
		FileType: sandbox.FileTypePython,
		Code:     []byte("print('hi')"),
	}); err == nil {
		t.Fatal("expected rejection of non-WASM input with code bytes")
	}
}
