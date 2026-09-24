package wasi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v15"
)

// loadTestWasm returns the bytes of the repository's testdata/hello.wasm
// (a real compiled WASI module that prints "hello from wasi\n" and exits 0).
func loadTestWasm(t *testing.T) []byte {
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
	t.Fatal("testdata/hello.wasm not found by walking up from package dir")
	return nil
}

// newTestExecutor returns an executor with limits suitable for tests.
func newTestExecutor(t *testing.T) *WasiExecutor {
	t.Helper()
	ex, err := NewWasiExecutor(WasiSandboxConfig{
		FuelLimit: 100_000_000,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWasiExecutor: %v", err)
	}
	t.Cleanup(ex.Close)
	return ex
}

func TestExecuteHelloWasm(t *testing.T) {
	ex := newTestExecutor(t)
	wasmBytes := loadTestWasm(t)

	res, err := ex.Execute(context.Background(), ExecuteInput{
		WasmBytes: wasmBytes,
		Args:      []string{"arg1"},
		Env:       map[string]string{"GREETING": "hi"},
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecSuccess {
		t.Fatalf("status = %q (%s: %s), want success", res.Status, res.ErrorCode, res.ErrorMessage)
	}
	if res.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", res.ExitCode)
	}
	if got, want := string(res.Stdout), "hello from wasi\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if res.FuelConsumed == 0 {
		t.Fatal("expected fuel to be consumed")
	}
	if res.ElapsedMS < 0 {
		t.Fatalf("elapsed = %v, want >= 0", res.ElapsedMS)
	}
}

func TestExecuteNonZeroExit(t *testing.T) {
	ex := newTestExecutor(t)
	wat := `(module
		(import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
		(memory (export "memory") 1)
		(func (export "_start")
			(i32.const 3)
			call $proc_exit
		)
	)`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}

	res, err := ex.Execute(context.Background(), ExecuteInput{WasmBytes: wasmBytes})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecFailed {
		t.Fatalf("status = %q (%s), want failed", res.Status, res.ErrorCode)
	}
	if res.ExitCode != 3 {
		t.Fatalf("exit code = %d, want 3", res.ExitCode)
	}
}

func TestExecuteStdinEcho(t *testing.T) {
	ex := newTestExecutor(t)
	// Reads fd 0 until EOF into a buffer, then writes the same bytes to fd 1.
	wat := `(module
		(import "wasi_snapshot_preview1" "fd_read" (func $fd_read (param i32 i32 i32 i32) (result i32)))
		(import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
		(import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
		(memory (export "memory") 1)
		(func (export "_start")
			(local $total i32)
			(local $nread i32)
			(block $done
				(loop $again
					(i32.store (i32.const 0x10) (i32.add (i32.const 0x200) (local.get $total)))
					(i32.store (i32.const 0x14) (i32.const 64))
					(i32.const 0) (i32.const 0x10) (i32.const 1) (i32.const 0x100)
					(drop (call $fd_read))
					(local.set $nread (i32.load (i32.const 0x100)))
					(local.set $total (i32.add (local.get $total) (local.get $nread)))
					(i32.eqz (local.get $nread)) (br_if $done)
					(br $again)
				)
			)
			(i32.store (i32.const 0x20) (i32.const 0x200))
			(i32.store (i32.const 0x24) (local.get $total))
			(i32.const 1) (i32.const 0x20) (i32.const 1) (i32.const 0)
			(drop (call $fd_write))
			(i32.const 0)
			call $proc_exit
		)
	)`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}

	res, err := ex.Execute(context.Background(), ExecuteInput{
		WasmBytes: wasmBytes,
		Stdin:     []byte("ping from host\n"),
	})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecSuccess {
		t.Fatalf("status = %q (%s: %s), want success", res.Status, res.ErrorCode, res.ErrorMessage)
	}
	if got, want := string(res.Stdout), "ping from host\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestExecuteFuelExhaustion(t *testing.T) {
	ex, err := NewWasiExecutor(WasiSandboxConfig{
		FuelLimit: 1_000, // tiny budget
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWasiExecutor: %v", err)
	}
	defer ex.Close()

	// Infinite loop; fuel must stop it long before the wall-clock timeout.
	wat := `(module
		(memory (export "memory") 1)
		(func (export "_start")
			(loop $top
				(br $top)
			)
		)
	)`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}

	res, err := ex.Execute(context.Background(), ExecuteInput{WasmBytes: wasmBytes})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecFuelExhausted {
		t.Fatalf("status = %q (code=%s msg=%s), want fuel_exhausted", res.Status, res.ErrorCode, res.ErrorMessage)
	}
}

func TestExecuteTimeout(t *testing.T) {
	// No fuel cap: only epoch interruption can stop an infinite loop.
	ex, err := NewWasiExecutor(WasiSandboxConfig{
		FuelLimit: 0,
		Timeout:   300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWasiExecutor: %v", err)
	}
	defer ex.Close()

	wat := `(module
		(memory (export "memory") 1)
		(func (export "_start")
			(loop $top
				(br $top)
			)
		)
	)`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}

	start := time.Now()
	res, err := ex.Execute(context.Background(), ExecuteInput{WasmBytes: wasmBytes})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecTimedOut {
		t.Fatalf("status = %q (code=%s msg=%s), want timed_out", res.Status, res.ErrorCode, res.ErrorMessage)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("timeout took %v, want < 3s", elapsed)
	}
}

func TestExecuteContextCancelled(t *testing.T) {
	ex, err := NewWasiExecutor(WasiSandboxConfig{
		FuelLimit: 0,
		Timeout:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWasiExecutor: %v", err)
	}
	defer ex.Close()

	wat := `(module
		(memory (export "memory") 1)
		(func (export "_start")
			(loop $top
				(br $top)
			)
		)
	)`
	wasmBytes, err := wasmtime.Wat2Wasm(wat)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	res, err := ex.Execute(ctx, ExecuteInput{WasmBytes: wasmBytes})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecTimedOut {
		t.Fatalf("status = %q (code=%s msg=%s), want timed_out", res.Status, res.ErrorCode, res.ErrorMessage)
	}
}

func TestExecuteEmptyInput(t *testing.T) {
	ex := newTestExecutor(t)
	res, err := ex.Execute(context.Background(), ExecuteInput{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecFailed || res.ErrorCode != "INVALID_INPUT" {
		t.Fatalf("status=%q code=%q, want failed/INVALID_INPUT", res.Status, res.ErrorCode)
	}
}

func TestExecuteInvalidMagic(t *testing.T) {
	ex := newTestExecutor(t)
	res, err := ex.Execute(context.Background(), ExecuteInput{WasmBytes: []byte("not a wasm module")})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecFailed || res.ErrorCode != "INVALID_FORMAT" {
		t.Fatalf("status=%q code=%q, want failed/INVALID_FORMAT", res.Status, res.ErrorCode)
	}
}

func TestExecuteNoStartExport(t *testing.T) {
	ex := newTestExecutor(t)
	// A valid module with a memory export but no _start.
	wasmBytes, err := wasmtime.Wat2Wasm(`(module (memory (export "memory") 1))`)
	if err != nil {
		t.Fatalf("Wat2Wasm: %v", err)
	}
	res, err := ex.Execute(context.Background(), ExecuteInput{WasmBytes: wasmBytes})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if res.Status != ExecFailed || res.ErrorCode != "NO_ENTRY" {
		t.Fatalf("status=%q code=%q, want failed/NO_ENTRY", res.Status, res.ErrorCode)
	}
}

func TestConfigValidation(t *testing.T) {
	_, err := NewWasiExecutor(WasiSandboxConfig{
		FuelLimit:    100,
		Timeout:      time.Second,
		AllowedPaths: []string{"relative/path"},
	})
	if err == nil {
		t.Fatal("expected error for relative AllowedPaths")
	}
}
