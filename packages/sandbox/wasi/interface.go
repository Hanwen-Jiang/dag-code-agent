// Package wasi provides WebAssembly System Interface (WASI preview1) sandboxing
// for secure, fast code execution, built on the official wasmtime-go runtime.
//
// Security model
//   - Capability isolation: WASI preview1 exposes no network and no host
//     environment leakage (unless explicitly configured); only preopened host
//     paths are reachable from the guest.
//   - Resource limits: instruction fuel caps CPU work; epoch interruption
//     caps wall-clock time; the Wasm stack is bounded by the runtime.
//   - I/O capture: stdout/stderr are redirected to per-execution files that
//     are removed afterwards; stdin is an in-memory pipe.
//
// Determinism: identical inputs produce identical outputs and exit codes.
package wasi

import (
	"time"
)

// IsolationLevel names the isolation backend a deployment may select.
type IsolationLevel string

const (
	// WasmIsolation runs code in a WASI/wasmtime sandbox (this package).
	WasmIsolation IsolationLevel = "wasm"
	// MicroVMIsolation is reserved for Firecracker-style microVMs.
	MicroVMIsolation IsolationLevel = "microvm"
	// ContainerIsolation is reserved for Docker/container runtimes.
	ContainerIsolation IsolationLevel = "container"
	// ProcessIsolation runs in the same process with OS limits (least safe).
	ProcessIsolation IsolationLevel = "process"
)

// WasiSandboxConfig defines resource and isolation limits for the executor.
type WasiSandboxConfig struct {
	// MemoryLimitMB is the intended peak linear-memory budget in MiB. The
	// wasmtime v15 Go bindings do not expose a linear-memory cap directly, so
	// this is advisory: CPU work is bounded by FuelLimit and the Wasm stack
	// is bounded by the runtime default.
	MemoryLimitMB uint64

	// FuelLimit is the instruction quota per execution (0 disables fuel
	// metering, which is unsafe for untrusted code).
	FuelLimit uint64

	// Timeout bounds wall-clock execution via epoch interruption.
	Timeout time.Duration

	// AllowedPaths are host directories preopened into the sandbox guest
	// filesystem. Only absolute paths are accepted.
	AllowedPaths []string

	// ReadOnlyFS is advisory documentation of intent; WASI guest processes
	// can always create files in preopened directories.
	ReadOnlyFS bool

	// AllowNetwork is ignored: WASI preview1 has no network syscalls.
	AllowNetwork bool

	// AllowEnvAccess controls whether the host environment is inherited.
	// When false, only per-execution Env and ExtraEnv are visible to the
	// guest.
	AllowEnvAccess bool

	// ExtraEnv are additional environment variables set for the guest.
	ExtraEnv map[string]string
}

// DefaultConfig returns a safe default configuration.
func DefaultConfig() WasiSandboxConfig {
	return WasiSandboxConfig{
		MemoryLimitMB:  256,
		FuelLimit:      1_000_000_000,
		Timeout:        30 * time.Second,
		AllowedPaths:   []string{},
		ReadOnlyFS:     true,
		AllowNetwork:   false,
		AllowEnvAccess: false,
	}
}

// ExecutionStatus classifies the outcome of a sandboxed run.
type ExecutionStatus string

const (
	// ExecSuccess: the module completed normally (exit code recorded).
	ExecSuccess ExecutionStatus = "success"
	// ExecFailed: the module trapped for some other reason.
	ExecFailed ExecutionStatus = "failed"
	// ExecTimedOut: wall-clock deadline exceeded (side effects may be partial).
	ExecTimedOut ExecutionStatus = "timed_out"
	// ExecFuelExhausted: the instruction quota was consumed.
	ExecFuelExhausted ExecutionStatus = "fuel_exhausted"
	// ExecMemoryLimit: out-of-bounds memory access or allocation failure.
	ExecMemoryLimit ExecutionStatus = "memory_exceeded"
	// ExecUnknown: the outcome is ambiguous (e.g. context cancelled before
	// the trap was observed).
	ExecUnknown ExecutionStatus = "unknown"
)

// ExecuteInput is one unit of code to run inside the sandbox.
type ExecuteInput struct {
	// WasmBytes is the compiled WASM module (must start with the magic
	// header \0asm).
	WasmBytes []byte

	// Args are the command-line arguments passed via WASI argv.
	Args []string

	// Stdin is delivered to the guest's standard input (fd 0).
	Stdin []byte

	// Env are extra environment variables for this execution, always allowed
	// (host-env inheritance is gated by WasiSandboxConfig).
	Env map[string]string

	// Cwd is a fresh, per-execution guest working directory preopened as
	// "/"; it is created in the host temp dir and removed afterwards.
	Cwd string
}

// ExecResult is the observed outcome of a sandboxed execution.
type ExecResult struct {
	// Status classifies the run (see ExecutionStatus).
	Status ExecutionStatus

	// ExitCode is the WASI proc_exit code (0 on clean exit).
	ExitCode int

	// Stdout / Stderr are the captured streams.
	Stdout []byte
	Stderr []byte

	// ElapsedMS is wall-clock time for the call.
	ElapsedMS float64

	// FuelConsumed is the instruction quota consumed (0 when metering off).
	FuelConsumed uint64

	// ErrorCode / ErrorMessage carry machine/human diagnostics on failure.
	ErrorCode    string
	ErrorMessage string
}
