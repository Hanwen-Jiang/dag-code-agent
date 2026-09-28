// Package agentcore is the gRPC client for the execution layer (cmd/agent-core).
//
// Dependency hygiene (docs/runtime-completion-design.md section 5.1): this
// package must live OUTSIDE the core packages (harness, httpapi, modelenv).
// The core must stay embeddable, so an embedder never inherits gRPC; the
// workflow layer (cmd/orchestrator) is the only intended importer. The single
// core dependency this package takes is harness.CircuitBreaker, which is
// reused rather than reimplemented (section 5.4).
//
// Scope boundary (section 5.5): agent-core is the EXECUTION layer. It runs WASI
// sandbox tool calls and RAG question answering. It does not run the
// control-plane agent loop; driving durable agent tasks would be an
// orchestrator -> harness client instead, which is out of scope here.
package agentcore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/Hanwen-Jiang/dag-code-agent/proto/agent_core"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// BackendName is the value reported in ExecuteResult.Backend and in the
// orchestrator's StepOutput.Backend for work executed by agent-core.
const BackendName = "agent-core"

// IdempotencyHeader is the TaskMetadata.headers key carrying the deterministic
// idempotency key (docs/runtime-completion-design.md section 5.3). The proto has
// no dedicated field, so the custom-headers map is the carrier the wire format
// offers.
const IdempotencyHeader = "idempotency-key"

const defaultTimeout = 30 * time.Second

// Code classifies an agent-core failure so callers can decide whether to
// degrade to another backend.
type Code string

const (
	CodeUnavailable       Code = "unavailable"
	CodeDeadlineExceeded  Code = "deadline_exceeded"
	CodeCanceled          Code = "canceled"
	CodePermissionDenied  Code = "permission_denied"
	CodeInvalidArgument   Code = "invalid_argument"
	CodeNotFound          Code = "not_found"
	CodeUnimplemented     Code = "unimplemented"
	CodeResourceExhausted Code = "resource_exhausted"
	CodeInternal          Code = "internal"
	CodeInvalidRequest    Code = "invalid_request"
	CodeTaskFailed        Code = "task_failed"
	// CodeStreamInterrupted marks a streaming call that failed after at least
	// one update had already been observed. Such a result is "uncertain": the
	// execution layer may have run a side effect, so the caller must not
	// silently re-run the step on another backend (section 5.3).
	CodeStreamInterrupted Code = "stream_interrupted"
	CodeCircuitOpen       Code = "circuit_open"
	CodeUnknown           Code = "unknown"
)

// ErrUncertain is returned (wrapped in *Error) when a streaming execution
// failed after updates had already arrived, so the outcome cannot be safely
// re-dispatched. Test with errors.Is(err, ErrUncertain).
var ErrUncertain = errors.New("agent-core result is uncertain after partial streaming")

// Error is the client's failure type. It keeps the gRPC status code in a
// backend-neutral Code so the workflow layer does not import grpc/status.
type Error struct {
	Code      Code
	Op        string
	Uncertain bool
	Err       error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return fmt.Sprintf("agentcore: %s: %s", e.Op, e.Code)
	}
	return fmt.Sprintf("agentcore: %s: %s: %v", e.Op, e.Code, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Is reports ErrUncertain for errors produced by an interrupted stream.
func (e *Error) Is(target error) bool {
	return target == ErrUncertain && e != nil && e.Uncertain
}

// CodeOf extracts the client-level code from err, for StepOutput.ErrorCode.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return string(e.Code)
	}
	return string(CodeUnknown)
}

// IsUncertain reports whether err came from a stream that had already produced
// updates, i.e. the step must be recorded as an agent-core result even though it
// failed.
func IsUncertain(err error) bool { return errors.Is(err, ErrUncertain) }

// Health is the Go-friendly view of HealthCheckResponse.
type Health struct {
	// Healthy is true when agent-core can serve requests: HEALTHY or DEGRADED.
	// DEGRADED means agent-core is up with fallback subsystems (for example an
	// in-memory vector store), which is still usable.
	Healthy   bool
	Status    string
	Degraded  bool
	Version   int32
	CheckedAt time.Time
}

// ToolCapability is the Go-friendly view of ToolCapability.
type ToolCapability struct {
	ID          string
	Name        string
	Description string
	Category    string
	IsDangerous bool
}

// Capabilities is the cached result of DiscoverTools (section 5.4 step 2).
type Capabilities struct {
	Tools []ToolCapability
}

// Supports reports whether the cached capability set exposes tool id.
func (c Capabilities) Supports(id string) bool {
	for _, t := range c.Tools {
		if t.ID == id {
			return true
		}
	}
	return false
}

// IDs returns the capability ids, in discovery order.
func (c Capabilities) IDs() []string {
	out := make([]string, 0, len(c.Tools))
	for _, t := range c.Tools {
		out = append(out, t.ID)
	}
	return out
}

// ExecuteRequest is the Go-friendly projection of ExecuteTaskRequest. Callers
// never construct pb messages themselves.
type ExecuteRequest struct {
	SessionID string
	Query     string // tool-call JSON spec or natural-language query
	Context   map[string]any
	Tools     []string // whitelist, from DiscoverTools
	Timeout   time.Duration
	Budget    int

	// IdempotencyKey is the deterministic digest of the step's inputs
	// (section 5.3). The proto has no dedicated field, so it travels in
	// TaskMetadata.headers under IdempotencyHeader and is mirrored into
	// Context["idempotency_key"]. The execution layer dedupes on it.
	IdempotencyKey string
}

// ExecuteResult is the Go-friendly view of a finished execution.
type ExecuteResult struct {
	Status   string
	Output   string
	ToolUsed string
	Backend  string // always BackendName for this client
	Latency  time.Duration
}

// OK reports whether the execution layer reported success.
func (r ExecuteResult) OK() bool { return r.Status == statusCompleted }

// TaskUpdate is the Go-friendly view of a streamed TaskUpdate.
type TaskUpdate struct {
	Iteration int
	Status    string
	ToolName  string
	Output    string
	Success   bool
}

const (
	statusUnspecified = "TASK_STATUS_UNSPECIFIED"
	statusPending     = "TASK_STATUS_PENDING"
	statusRunning     = "TASK_STATUS_RUNNING"
	statusCompleted   = "TASK_STATUS_COMPLETED"
	statusFailed      = "TASK_STATUS_FAILED"
	statusTimedOut    = "TASK_STATUS_TIMED_OUT"
	statusDenied      = "TASK_STATUS_PERMISSION_DENIED"
)

// Client is a gRPC client for pb.AgentService. It is safe for concurrent use.
type Client struct {
	conn    *grpc.ClientConn
	svc     pb.AgentServiceClient
	timeout time.Duration
	breaker *harness.CircuitBreaker

	mu   sync.Mutex
	caps Capabilities
}

// Option configures Dial.
type Option func(*dialConfig)

type dialConfig struct {
	timeout     time.Duration
	breaker     *harness.CircuitBreaker
	dialOptions []grpc.DialOption
}

// WithTimeout sets the per-call deadline used when a request does not set its
// own Timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *dialConfig) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithBreaker installs the shared circuit breaker (harness.CircuitBreaker) used
// to short-circuit after repeated failures (section 5.4).
func WithBreaker(b *harness.CircuitBreaker) Option {
	return func(c *dialConfig) { c.breaker = b }
}

// withDialOptions injects extra gRPC dial options. Used by tests to attach an
// in-process bufconn; production calls keep the default insecure local
// transport (agent-core is reached on the compose/local network).
func withDialOptions(opts ...grpc.DialOption) Option {
	return func(c *dialConfig) { c.dialOptions = append(c.dialOptions, opts...) }
}

// Dial creates a client for addr ("host:port"). It does not connect eagerly:
// when agent-core is down the orchestrator must still start, and Health decides
// whether the backend is usable (section 5.4). ctx is accepted for signature
// symmetry and future use; connection setup is non-blocking by design.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	_ = ctx
	if addr == "" {
		return nil, &Error{Code: CodeInvalidRequest, Op: "dial", Err: errors.New("empty agent-core address")}
	}
	cfg := dialConfig{timeout: defaultTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, cfg.dialOptions...)
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, &Error{Code: CodeUnavailable, Op: "dial", Err: err}
	}
	return &Client{
		conn:    conn,
		svc:     pb.NewAgentServiceClient(conn),
		timeout: cfg.timeout,
		breaker: cfg.breaker,
	}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Health checks the execution layer and reports whether it can serve requests.
// A non-nil error means the RPC itself failed (unreachable, timeout); a nil
// error with Healthy == false means agent-core answered UNHEALTHY.
func (c *Client) Health(ctx context.Context) (Health, error) {
	ctx, cancel := c.callContext(ctx, 0)
	defer cancel()
	if err := c.gate("health"); err != nil {
		return Health{}, err
	}
	resp, err := c.svc.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		c.failure()
		return Health{}, mapError("health", err)
	}
	c.success()
	h := Health{
		Status:   healthStatusName(resp.GetStatus()),
		Version:  resp.GetVersion(),
		Degraded: resp.GetStatus() == pb.HealthStatus_HEALTH_DEGRADED,
		Healthy: resp.GetStatus() == pb.HealthStatus_HEALTH_HEALTHY ||
			resp.GetStatus() == pb.HealthStatus_HEALTH_DEGRADED,
	}
	if ns := resp.GetTimeNs(); ns > 0 {
		h.CheckedAt = time.Unix(0, ns)
	}
	return h, nil
}

// DiscoverTools fetches and caches the execution layer's capabilities.
func (c *Client) DiscoverTools(ctx context.Context) ([]ToolCapability, error) {
	ctx, cancel := c.callContext(ctx, 0)
	defer cancel()
	if err := c.gate("discover_tools"); err != nil {
		return nil, err
	}
	resp, err := c.svc.DiscoverTools(ctx, &pb.DiscoverToolsRequest{})
	if err != nil {
		c.failure()
		return nil, mapError("discover_tools", err)
	}
	c.success()
	tools := make([]ToolCapability, 0, len(resp.GetTools()))
	for _, t := range resp.GetTools() {
		tools = append(tools, ToolCapability{
			ID:          t.GetId(),
			Name:        t.GetName(),
			Description: t.GetDescription(),
			Category:    t.GetCategory(),
			IsDangerous: t.GetIsDangerous(),
		})
	}
	c.mu.Lock()
	c.caps = Capabilities{Tools: tools}
	c.mu.Unlock()
	return tools, nil
}

// Capabilities returns the cached DiscoverTools result.
func (c *Client) Capabilities() Capabilities {
	if c == nil {
		return Capabilities{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.caps
}

// Execute runs one task through the non-streaming RPC. A non-OK TaskStatus is
// reported as an error so the caller can degrade; the partial result is still
// returned.
func (c *Client) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResult, error) {
	pbReq, err := req.toProto(pb.ExecutionMode_EXECUTION_MODE_SYNC)
	if err != nil {
		return ExecuteResult{}, &Error{Code: CodeInvalidRequest, Op: "execute", Err: err}
	}
	ctx, cancel := c.callContext(ctx, req.Timeout)
	defer cancel()
	if err := c.gate("execute"); err != nil {
		return ExecuteResult{}, err
	}
	started := time.Now()
	resp, err := c.svc.ExecuteTask(ctx, pbReq)
	latency := time.Since(started)
	if err != nil {
		c.failure()
		return ExecuteResult{}, mapError("execute", err)
	}
	out := resultFromResponse(resp, latency)
	if err := statusFailure("execute", out); err != nil {
		c.failure()
		return out, err
	}
	c.success()
	return out, nil
}

// Stream executes a task through the server-streaming RPC and hands every
// TaskUpdate to onUpdate. The orchestrator uses that callback to call
// activity.RecordHeartbeat, which is what makes a long StartToCloseTimeout safe
// (section 5.3).
//
// Fallback contract (section 5.3): if the stream cannot be established, or dies
// before the first update, the client retries once with ExecuteTask. If it dies
// AFTER an update was observed, the error wraps ErrUncertain and the returned
// result is marked with BackendName: the caller must not silently rerun the step
// on another backend.
func (c *Client) Stream(ctx context.Context, req ExecuteRequest, onUpdate func(TaskUpdate) error) (ExecuteResult, error) {
	pbReq, err := req.toProto(pb.ExecutionMode_EXECUTION_MODE_STREAM)
	if err != nil {
		return ExecuteResult{}, &Error{Code: CodeInvalidRequest, Op: "stream", Err: err}
	}
	ctx, cancel := c.callContext(ctx, req.Timeout)
	defer cancel()
	if err := c.gate("stream"); err != nil {
		return ExecuteResult{}, err
	}

	started := time.Now()
	stream, err := c.svc.StreamExecuteTask(ctx, pbReq)
	if err != nil {
		// Stream creation failed before any update: one ExecuteTask retry.
		return c.executeAfterStreamFailure(ctx, req, err)
	}

	var last *pb.TaskUpdate
	for {
		update, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			if last == nil {
				return c.executeAfterStreamFailure(ctx, req, recvErr)
			}
			c.failure()
			return resultFromUpdate(last, started), &Error{
				Code:      CodeStreamInterrupted,
				Op:        "stream",
				Uncertain: true,
				Err:       mapError("stream_recv", recvErr),
			}
		}
		last = update
		if onUpdate != nil {
			if cbErr := onUpdate(toTaskUpdate(update)); cbErr != nil {
				c.failure()
				return resultFromUpdate(last, started), &Error{
					Code:      CodeStreamInterrupted,
					Op:        "stream",
					Uncertain: true,
					Err:       fmt.Errorf("update callback: %w", cbErr),
				}
			}
		}
	}

	if last == nil {
		c.failure()
		return ExecuteResult{}, &Error{
			Code: CodeInternal,
			Op:   "stream",
			Err:  errors.New("stream closed without any update"),
		}
	}
	out := resultFromUpdate(last, started)
	if err := statusFailure("stream", out); err != nil {
		c.failure()
		return out, err
	}
	c.success()
	return out, nil
}

// executeAfterStreamFailure retries once with the non-streaming RPC. It is only
// reached before any update has been observed.
func (c *Client) executeAfterStreamFailure(ctx context.Context, req ExecuteRequest, streamErr error) (ExecuteResult, error) {
	if ctx.Err() != nil {
		c.failure()
		return ExecuteResult{}, mapError("stream", streamErr)
	}
	res, execErr := c.Execute(ctx, req)
	if execErr != nil {
		return res, &Error{
			Code: Code(CodeOf(execErr)),
			Op:   "stream_fallback",
			Err:  fmt.Errorf("stream failed (%v) and ExecuteTask fallback failed: %w", streamErr, execErr),
		}
	}
	return res, nil
}

// ---------- request/response projection ----------

func (r ExecuteRequest) toProto(mode pb.ExecutionMode) (*pb.ExecuteTaskRequest, error) {
	ctxStruct, err := structpb.NewStruct(r.Context)
	if err != nil {
		return nil, fmt.Errorf("context is not JSON-compatible: %w", err)
	}
	headers := map[string]string{}
	if r.IdempotencyKey != "" {
		headers[IdempotencyHeader] = r.IdempotencyKey
		if ctxStruct == nil {
			ctxStruct = &structpb.Struct{}
		}
		if ctxStruct.Fields == nil {
			ctxStruct.Fields = map[string]*structpb.Value{}
		}
		ctxStruct.Fields["idempotency_key"] = structpb.NewStringValue(r.IdempotencyKey)
	}
	cfg := &pb.AgentConfig{}
	if r.Timeout > 0 {
		cfg.TimeoutSeconds = int32(r.Timeout.Seconds())
	}
	if r.Budget > 0 {
		cfg.TokenBudget = int32(r.Budget)
	}
	return &pb.ExecuteTaskRequest{
		Metadata: &pb.TaskMetadata{
			SessionId:   r.SessionID,
			Headers:     headers,
			TimestampNs: time.Now().UnixNano(),
		},
		Query:          r.Query,
		Context:        ctxStruct,
		Mode:           mode,
		AvailableTools: r.Tools,
		Config:         cfg,
	}, nil
}

func resultFromResponse(resp *pb.ExecuteTaskResponse, latency time.Duration) ExecuteResult {
	out := ExecuteResult{
		Status:  taskStatusName(resp.GetStatus()),
		Output:  resp.GetOutput(),
		Backend: BackendName,
		Latency: latency,
	}
	if resp.GetSandbox() {
		out.ToolUsed = "wasi"
	}
	return out
}

func resultFromUpdate(update *pb.TaskUpdate, started time.Time) ExecuteResult {
	out := ExecuteResult{
		Status:   taskStatusName(update.GetStatus()),
		ToolUsed: update.GetToolName(),
		Backend:  BackendName,
		Latency:  time.Since(started),
	}
	if res := update.GetResult(); res != nil {
		out.Output = res.GetOutput()
	}
	return out
}

func toTaskUpdate(update *pb.TaskUpdate) TaskUpdate {
	u := TaskUpdate{
		Iteration: int(update.GetIteration()),
		Status:    taskStatusName(update.GetStatus()),
		ToolName:  update.GetToolName(),
	}
	if res := update.GetResult(); res != nil {
		u.Output = res.GetOutput()
		u.Success = res.GetSuccess()
	}
	return u
}

// statusFailure converts a non-OK terminal TaskStatus into an error so the
// caller degrades instead of presenting a failed execution as an answer.
func statusFailure(op string, res ExecuteResult) error {
	switch res.Status {
	case statusCompleted:
		return nil
	case statusFailed, statusTimedOut, statusDenied:
		return &Error{
			Code: CodeTaskFailed,
			Op:   op,
			Err:  fmt.Errorf("agent-core returned %s", res.Status),
		}
	}
	return &Error{
		Code: CodeInternal,
		Op:   op,
		Err:  fmt.Errorf("agent-core returned non-terminal status %s", res.Status),
	}
}

// ---------- breaker handling ----------

func (c *Client) gate(op string) error {
	if c.breaker == nil || c.breaker.Allow() {
		return nil
	}
	return &Error{
		Code: CodeCircuitOpen,
		Op:   op,
		Err:  errors.New("circuit breaker open after repeated agent-core failures"),
	}
}

func (c *Client) success() {
	if c.breaker != nil {
		c.breaker.Success()
	}
}

func (c *Client) failure() {
	if c.breaker != nil {
		c.breaker.Failure()
	}
}

func (c *Client) callContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = c.timeout
	}
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

// ---------- status/error mapping ----------

func mapError(op string, err error) *Error {
	if err == nil {
		return nil
	}
	var existing *Error
	if errors.As(err, &existing) {
		return existing
	}
	code := CodeUnknown
	switch status.Code(err) {
	case codes.Unavailable:
		code = CodeUnavailable
	case codes.DeadlineExceeded:
		code = CodeDeadlineExceeded
	case codes.Canceled:
		code = CodeCanceled
	case codes.PermissionDenied, codes.Unauthenticated:
		code = CodePermissionDenied
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		code = CodeInvalidArgument
	case codes.NotFound:
		code = CodeNotFound
	case codes.Unimplemented:
		code = CodeUnimplemented
	case codes.ResourceExhausted:
		code = CodeResourceExhausted
	case codes.Internal, codes.DataLoss, codes.Unknown:
		code = CodeInternal
	}
	return &Error{Code: code, Op: op, Err: err}
}

func healthStatusName(s pb.HealthStatus) string { return s.String() }

func taskStatusName(s pb.TaskStatus) string { return s.String() }
