// Package sandbox provides a unified interface for secure code execution.
// It supports multiple isolation strategies: WASI, Firecracker microVMs, and gVisor.
// This is critical for running untrusted tool code without compromising the host.
package sandbox

import (
	"context"
	"time"
)

// IsolationLevel defines the strength of isolation.
type IsolationLevel string

const (
	// ProcessIsolation - Same process, limited by OS namespaces (weakest)
	ProcessIsolation IsolationLevel = "process"
	// ContainerIsolation - Separate container with cgroups (medium)
	ContainerIsolation IsolationLevel = "container"
	// MicroVMIsolation - Firecracker microVM (strong)
	MicroVMIsolation IsolationLevel = "microvm"
	// WasmIsolation - WebAssembly sandbox with WASI (balanced)
	WasmIsolation IsolationLevel = "wasm"
)

// SandboxConfig defines execution constraints and permissions.
type SandboxConfig struct {
	// Isolation level to use
	Isolation IsolationLevel
	
	// Resource limits
	CPUShares    int64 // CPU weight (relative share)
	MemoryBytes  uint64 // Memory limit in bytes
	DiskBytes    uint64 // Disk quota in bytes
	NetworkLimit *BandwidthLimit // Network bandwidth cap
	
	// Execution timeout
	Timeout time.Duration
	
	// Filesystem access
	ReadOnlyRoot bool          // Mount root as read-only
	WritePaths   []string      // Allowed write paths (absolute)
	AllowedPaths []string      // Paths explicitly allowed for read
	
	// Capabilities to enable/disable
	AllowSyscalls []string // Explicit syscall allowlist (empty=all allowed)
	DisallowSyscalls []string // Explicit syscall denylist
	
	// Environment variables to inject
	Env map[string]string
	
	// Working directory inside sandbox
	WorkingDir string
	
	// Security: Seccomp profile name (Linux) or AppArmor profile
	SecurityProfile string
}

// BandwidthLimit defines network bandwidth constraints.
type BandwidthLimit struct {
	RateBPS  uint64 // Bits per second (0 = unlimited)
	BurstBPS uint64 // Burst allowance in bits
}

// ExecResult represents the outcome of a sandboxed execution.
type ExecResult struct {
	// Execution status
	Status     ExecutionStatus
	ExitCode   int
	
	// Output streams
	Stdout []byte
	Stderr []byte
	
	// Timing
	ElapsedTimeMS   float64
	CPUTimeMS       float64
	
	// Resource usage
	MemoryPeakBytes   uint64
	DiskBytesWritten  uint64
	NetworkBytesOut   uint64
	
	// Security events
	SecurityViolations []SecurityViolation
	
	// Error details
	ErrorCode    string
	ErrorMessage string
	
	// Artifact reference (if any files produced)
	OutputArtifacts []ArtifactReference
}

// ExecutionStatus indicates the result type.
type ExecutionStatus string

const (
	// ExecSucceeded - Normal completion
	ExecSucceeded ExecutionStatus = "succeeded"
	// ExecFailed - Non-zero exit or error
	ExecFailed ExecutionStatus = "failed"
	// ExecTimedOut - Exceeded timeout
	ExecTimedOut ExecutionStatus = "timed_out"
	// ExecMemoryLimit - Exceeded memory limit
	ExecMemoryLimit ExecutionStatus = "memory_exceeded"
	// ExecSecurityViolation - Policy violation detected
	ExecSecurityViolation ExecutionStatus = "security_violation"
	// ExecUnknown - Timeout may have left uncertain side effects
	ExecUnknown ExecutionStatus = "unknown"
)

// SecurityViolation records a policy breach during execution.
type SecurityViolation struct {
	Timestamp   time.Time
	ViolationType string // e.g., "syscall_blocked", "path_escape_attempt"
	Details     string // Human-readable description
	Blocked     bool   // Whether the action was blocked
}

// ArtifactReference points to output produced by the sandbox.
type ArtifactReference struct {
	PathInSandbox string // Path inside sandbox
	PathOnHost    string // Path extracted on host (if allowed)
	SizeBytes     uint64
	ContentType   string
	ChecksumSHA256 string // Integrity verification
}

// Sandbox is the interface all isolation backends must implement.
type Sandbox interface {
	// Name returns the backend name.
	Name() string
	
	// Initialize prepares the sandbox environment (e.g., pull WASM runtime, spin up VM manager).
	Initialize(ctx context.Context) error
	
	// Execute runs the provided code within isolation constraints.
	// The code can be source code (Go/Python/etc.) or bytecode (WASM).
	Execute(ctx context.Context, config SandboxConfig, input ExecuteInput) (*ExecResult, error)
	
	// Cleanup releases resources held by this sandbox instance.
	Cleanup()
	
	// HealthCheck verifies the sandbox is operational.
	HealthCheck(ctx context.Context) error
}

// ExecuteInput provides code and arguments for execution.
type ExecuteInput struct {
	// Code is the executable content.
	// For WASM: byte slice of .wasm file
	// For interpreted languages: source code string
	// For compiled binaries: byte slice of ELF/WASM binary
	Code []byte
	
	// FileType indicates the format.
	FileType FileType
	
	// Arguments command-line args
	Args []string
	
	// InputData stdin content
	InputData []byte
	
	// Deadline override per-execution
	Deadline *time.Time
	
	// SessionID for correlation across multiple executions
	SessionID string
}

// FileType indicates the code format.
type FileType string

const (
	FileTypeWASM   FileType = "wasm"
	FileTypeGo     FileType = "go"
	FileTypePython FileType = "python"
	FileTypeJavaScript FileType = "javascript"
	FileTypeBinary FileType = "binary"
)

// BackendRegistry manages sandbox backend instances.
type BackendRegistry struct {
	backends map[string]Sandbox
	defaultBackend string
}

func NewBackendRegistry() *BackendRegistry {
	return &BackendRegistry{
		backends: make(map[string]Sandbox),
	}
}

func (r *BackendRegistry) Register(name string, sb Sandbox) error {
	if _, exists := r.backends[name]; exists {
		return &SandboxError{Code: "BACKEND_ALREADY_REGISTERED", Message: "backend " + name + " already registered"}
	}
	r.backends[name] = sb
	if r.defaultBackend == "" {
		r.defaultBackend = name
	}
	return nil
}

func (r *BackendRegistry) Get(name string) (Sandbox, error) {
	if name == "" {
		name = r.defaultBackend
	}
	sb, ok := r.backends[name]
	if !ok {
		return nil, &SandboxError{Code: "BACKEND_NOT_FOUND", Message: "sandbox backend " + name + " not found"}
	}
	return sb, nil
}

func (r *BackendRegistry) Default() (Sandbox, error) {
	return r.Get("")
}

// SandboxError represents a sandbox-related error.
type SandboxError struct {
	Code     string
	Message  string
	Details  string
}

func (e *SandboxError) Error() string {
	if e.Message != "" {
		return e.Code + ": " + e.Message
	}
	return e.Code
}
