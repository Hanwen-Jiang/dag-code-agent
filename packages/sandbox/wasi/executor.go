package wasi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v15"
)

// maxWasmSize caps module size to avoid pathological allocations.
const maxWasmSize = 50 * 1024 * 1024

// WasiExecutor runs WASM modules inside a WASI sandbox. It is safe for
// concurrent use: executions are serialized on a mutex (the wasmtime store is
// single-threaded), so one process can safely serve many callers.
type WasiExecutor struct {
	config  WasiSandboxConfig
	engine  *wasmtime.Engine
	mu      sync.Mutex
	closed  bool
}

// NewWasiExecutor builds an executor and its shared engine. It fails on
// misconfiguration (e.g. relative AllowedPaths) so callers fail fast at
// startup rather than per-execution.
//
// FuelLimit == 0 disables fuel metering and relies on the epoch timeout alone
// (which is always active) to bound execution. A positive FuelLimit enables
// instruction metering with that budget. DefaultConfig() ships a safe budget.
func NewWasiExecutor(config WasiSandboxConfig) (*WasiExecutor, error) {
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	for _, p := range config.AllowedPaths {
		if !filepath.IsAbs(p) {
			return nil, fmt.Errorf("wasi: AllowedPaths must be absolute, got %q", p)
		}
	}

	wcfg := wasmtime.NewConfig()
	// Wasm stack bounds guest recursion; 8 MiB is the wasmtime default.
	wcfg.SetMaxWasmStack(8 * 1024 * 1024)
	// Fuel metering caps CPU instructions when a budget is configured. When
	// FuelLimit is 0 it is deliberately left off so the epoch timeout alone
	// bounds execution (fuel has a small implicit default otherwise).
	if config.FuelLimit > 0 {
		wcfg.SetConsumeFuel(true)
	}
	// Epoch interruption caps wall-clock time (see the per-execution ticker).
	wcfg.SetEpochInterruption(true)

	engine := wasmtime.NewEngineWithConfig(wcfg)
	return &WasiExecutor{config: config, engine: engine}, nil
}

// Initialize prepares the executor. NewWasiExecutor already did the work; this
// exists for interface compatibility and re-checks configuration.
func (e *WasiExecutor) Initialize(ctx context.Context) error {
	if e == nil || e.engine == nil {
		return errors.New("wasi: executor not initialized")
	}
	return nil
}

// Execute runs input inside the sandbox and returns the captured result. It
// never panics; failures are reported through ExecResult. The caller's context
// and WasiSandboxConfig.Timeout both bound execution.
func (e *WasiExecutor) Execute(ctx context.Context, input ExecuteInput) (*ExecResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	start := time.Now()
	res := &ExecResult{}

	// --- validate module -------------------------------------------------
	if len(input.WasmBytes) == 0 {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "INVALID_INPUT", "no WASM bytes provided"
		return res, nil
	}
	if len(input.WasmBytes) > maxWasmSize {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "MODULE_TOO_LARGE",
			fmt.Sprintf("WASM module exceeds %d bytes", maxWasmSize)
		return res, nil
	}
	if len(input.WasmBytes) < 4 || !bytesHasMagic(input.WasmBytes) {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "INVALID_FORMAT", "missing WASM magic header"
		return res, nil
	}

	// --- effective context (caller ctx AND configured timeout) ------------
	if e.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.config.Timeout)
		defer cancel()
	}

	// --- scratch dir: guest cwd is "/" -> host temp dir, preopened --------
	workDir, err := os.MkdirTemp("", "wasi-exec-*")
	if err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "ISOLATION_FAILED", err.Error()
		return res, nil
	}
	defer os.RemoveAll(workDir)

	// --- capture files ----------------------------------------------------
	outFile, err := os.CreateTemp(workDir, "out-*")
	if err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "IO_FAILED", err.Error()
		return res, nil
	}
	outPath := outFile.Name()
	outFile.Close()
	defer os.Remove(outPath)

	errFile, err := os.CreateTemp(workDir, "err-*")
	if err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "IO_FAILED", err.Error()
		return res, nil
	}
	errPath := errFile.Name()
	errFile.Close()
	defer os.Remove(errPath)

	stdinPath := ""
	if len(input.Stdin) > 0 {
		inFile, err := os.CreateTemp(workDir, "in-*")
		if err != nil {
			res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "IO_FAILED", err.Error()
			return res, nil
		}
		if _, err := inFile.Write(input.Stdin); err != nil {
			inFile.Close()
			res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "IO_FAILED", err.Error()
			return res, nil
		}
		inFile.Close()
		stdinPath = inFile.Name()
		defer os.Remove(stdinPath)
	}

	// --- WASI config ------------------------------------------------------
	argv := []string{"sandbox"}
	argv = append(argv, input.Args...)
	wasiCfg := wasmtime.NewWasiConfig()
	wasiCfg.SetArgv(argv)
	wasiCfg.SetStdoutFile(outPath)
	wasiCfg.SetStderrFile(errPath)
	if stdinPath != "" {
		if err := wasiCfg.SetStdinFile(stdinPath); err != nil {
			res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "IO_FAILED", err.Error()
			return res, nil
		}
	}

	// Environment: only what the operator explicitly allows.
	envKeys, envVals := e.envFor(input)
	wasiCfg.SetEnv(envKeys, envVals)
	if e.config.AllowEnvAccess {
		wasiCfg.InheritEnv()
	}

	// Preopens: guest "/" -> per-execution workdir, plus operator paths.
	if err := wasiCfg.PreopenDir(workDir, "/"); err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "ISOLATION_FAILED", err.Error()
		return res, nil
	}
	for _, p := range e.config.AllowedPaths {
		abs, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		if st, err := os.Stat(abs); err != nil || !st.IsDir() {
			continue // skip non-existent paths, never fail the execution
		}
		guestPath := "/" + strings.TrimPrefix(abs, "/")
		if err := wasiCfg.PreopenDir(abs, guestPath); err != nil {
			// A preopen failure is a policy violation, not a crash.
			res.Status, res.ErrorCode = ExecFailed, "PREOPEN_FAILED"
			res.ErrorMessage = fmt.Sprintf("preopen %s: %v", abs, err)
			return res, nil
		}
	}

	// --- compile + link + instantiate -------------------------------------
	module, err := wasmtime.NewModule(e.engine, input.WasmBytes)
	if err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "MODULE_COMPILE", err.Error()
		return res, nil
	}

	store := wasmtime.NewStore(e.engine)
	// Fuel: instruction budget (already enabled in engine config).
	if e.config.FuelLimit > 0 {
		if err := store.SetFuel(e.config.FuelLimit); err != nil {
			res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "FUEL_CONFIG", err.Error()
			return res, nil
		}
	}
	// Epoch deadline: one epoch of grace; the ticker below advances the
	// engine epoch until it exceeds this deadline, forcing an interrupt.
	store.SetEpochDeadline(1)
	store.SetWasi(wasiCfg)

	linker := wasmtime.NewLinker(e.engine)
	if err := linker.DefineWasi(); err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "WASI_LINK", err.Error()
		return res, nil
	}

	instance, err := linker.Instantiate(store, module)
	if err != nil {
		res.Status, res.ErrorCode, res.ErrorMessage = classifyErr(ExecFailed, "INSTANTIATE", err)
		return res, nil
	}

	// --- timeout enforcement: advance the engine epoch while we run ------
	// Capture a local engine reference so the goroutine never reads the
	// e.engine field (which Close may clear) without holding e.mu.
	eng := e.engine
	stopTicker := make(chan struct{})
	go func() {
		ticker := time.NewTicker(e.config.Timeout)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				eng.IncrementEpoch() // deadline(1) is now behind current epoch
			case <-stopTicker:
				return
			case <-ctx.Done():
				eng.IncrementEpoch()
				return
			}
		}
	}()

	// --- run ----------------------------------------------------------------
	fn := instance.GetFunc(store, "_start")
	if fn == nil {
		close(stopTicker)
		res.Status, res.ErrorCode, res.ErrorMessage = ExecFailed, "NO_ENTRY", "module does not export _start"
		return res, nil
	}

	_, callErr := fn.Call(store)

	close(stopTicker)
	res.ElapsedMS = float64(time.Since(start).Nanoseconds()) / 1e6

	if e.config.FuelLimit > 0 {
		if left, err := store.GetFuel(); err == nil {
			res.FuelConsumed = e.config.FuelLimit - left
		}
	}

	res.Stdout, _ = os.ReadFile(outPath)
	res.Stderr, _ = os.ReadFile(errPath)

	switch {
	case callErr == nil:
		res.Status, res.ExitCode = ExecSuccess, 0
	case isExitStatus(callErr):
		code := exitCodeOf(callErr)
		if code == 0 {
			res.Status, res.ExitCode = ExecSuccess, 0
		} else {
			res.Status, res.ExitCode = ExecFailed, int(code)
			res.ErrorCode, res.ErrorMessage = "WASI_EXIT", fmt.Sprintf("exited with status %d", code)
		}
	default:
		res.Status, res.ErrorCode, res.ErrorMessage = classifyErr(ExecFailed, "WASM_TRAP", callErr)
	}

	return res, nil
}

// envFor merges operator ExtraEnv with per-execution Env into ordered slices.
func (e *WasiExecutor) envFor(input ExecuteInput) ([]string, []string) {
	merged := make(map[string]string)
	for k, v := range e.config.ExtraEnv {
		merged[k] = v
	}
	for k, v := range input.Env {
		merged[k] = v
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	vals := make([]string, len(keys))
	for i, k := range keys {
		vals[i] = merged[k]
	}
	return keys, vals
}

// Close releases the shared engine. The underlying wasmtime engine is
// process-scoped and freed by the runtime finalizer; Close exists so callers
// can release the reference deterministically.
func (e *WasiExecutor) Close() {
	if e != nil {
		e.mu.Lock()
		e.engine = nil
		e.mu.Unlock()
	}
}

// --- helpers ----------------------------------------------------------------

// bytesHasMagic checks the \0asm magic header.
func bytesHasMagic(b []byte) bool {
	return len(b) >= 4 && b[0] == 0x00 && b[1] == 0x61 && b[2] == 0x73 && b[3] == 0x6d
}

// classifyErr turns a wasmtime error into a (status, code, message) triple.
// It keeps the original default status unless it recognises a fuel,
// interruption, or memory trap.
func classifyErr(fallback ExecutionStatus, code string, err error) (ExecutionStatus, string, string) {
	if err == nil {
		return fallback, code, ""
	}
	msg := err.Error()
	// wasmtime renders trap causes in the error string; match on stable
	// substrings so we stay compatible across wasmtime versions.
	if containsFold(msg, "out of fuel") || containsFold(msg, "fuel") {
		return ExecFuelExhausted, "FUEL_EXHAUSTED", msg
	}
	if containsFold(msg, "interrupt") {
		return ExecTimedOut, "TIMEOUT", msg
	}
	if containsFold(msg, "out of bounds") || containsFold(msg, "memory") {
		return ExecMemoryLimit, "MEMORY_LIMIT", msg
	}
	return fallback, code, msg
}

// isExitStatus reports whether err is a wasmtime error carrying a WASI exit
// status (raised when the guest calls proc_exit).
func isExitStatus(err error) bool {
	var we *wasmtime.Error
	if errors.As(err, &we) {
		_, ok := we.ExitStatus()
		return ok
	}
	return false
}

// exitCodeOf extracts the WASI exit code from a wasmtime error (0 if none).
func exitCodeOf(err error) int32 {
	var we *wasmtime.Error
	if errors.As(err, &we) {
		code, ok := we.ExitStatus()
		if ok {
			return code
		}
	}
	return 0
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}
