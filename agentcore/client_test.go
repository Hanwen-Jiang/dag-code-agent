package agentcore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/Hanwen-Jiang/dag-code-agent/proto/agent_core"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// fakeServer is an in-process pb.AgentServiceServer. gRPC is the contract being
// tested, so the fake speaks the real wire protocol over bufconn.
type fakeServer struct {
	pb.UnimplementedAgentServiceServer

	mu sync.Mutex

	healthResp  *pb.HealthCheckResponse
	healthErr   error
	healthCalls int

	tools       []*pb.ToolCapability
	discoverErr error

	execResp  *pb.ExecuteTaskResponse
	execErr   error
	execCalls int
	lastReq   *pb.ExecuteTaskRequest

	// streamUpdates are sent in order; streamMidErr (if set) is returned after
	// they are sent.
	streamUpdates []*pb.TaskUpdate
	streamMidErr  error
	streamCalls   int
}

func (f *fakeServer) HealthCheck(context.Context, *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthCalls++
	if f.healthErr != nil {
		return nil, f.healthErr
	}
	return f.healthResp, nil
}

func (f *fakeServer) DiscoverTools(context.Context, *pb.DiscoverToolsRequest) (*pb.DiscoverToolsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.discoverErr != nil {
		return nil, f.discoverErr
	}
	return &pb.DiscoverToolsResponse{Tools: f.tools}, nil
}

func (f *fakeServer) ExecuteTask(_ context.Context, in *pb.ExecuteTaskRequest) (*pb.ExecuteTaskResponse, error) {
	f.mu.Lock()
	f.execCalls++
	f.lastReq = in
	resp, err := f.execResp, f.execErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (f *fakeServer) StreamExecuteTask(in *pb.ExecuteTaskRequest, stream grpc.ServerStreamingServer[pb.TaskUpdate]) error {
	f.mu.Lock()
	f.streamCalls++
	f.lastReq = in
	updates := f.streamUpdates
	midErr := f.streamMidErr
	f.mu.Unlock()

	for _, u := range updates {
		if err := stream.Send(u); err != nil {
			return err
		}
	}
	return midErr
}

// startServer boots the fake on an in-process bufconn listener and returns a
// connected client plus a cleanup func.
func startServer(t *testing.T, fake *fakeServer, opts ...Option) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterAgentServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()

	dialOpts := []Option{
		WithTimeout(2 * time.Second),
		withDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})),
	}
	c, err := Dial(context.Background(), "passthrough:///bufnet", append(dialOpts, opts...)...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		srv.Stop()
		_ = lis.Close()
	})
	return c
}

func healthyResp() *pb.HealthCheckResponse {
	return &pb.HealthCheckResponse{Status: pb.HealthStatus_HEALTH_HEALTHY, Version: 3, TimeNs: time.Now().UnixNano()}
}

func completedResp(output string) *pb.ExecuteTaskResponse {
	return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_COMPLETED, Output: output}
}

// ---------- Health / DiscoverTools (section 5.4 health gate) ----------

func TestHealthHealthy(t *testing.T) {
	c := startServer(t, &fakeServer{healthResp: healthyResp()})

	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !h.Healthy || h.Degraded {
		t.Fatalf("want healthy/non-degraded, got %+v", h)
	}
	if h.Status != "HEALTH_HEALTHY" || h.Version != 3 || h.CheckedAt.IsZero() {
		t.Fatalf("unexpected health projection: %+v", h)
	}
}

func TestHealthDegradedIsStillUsable(t *testing.T) {
	fake := &fakeServer{healthResp: &pb.HealthCheckResponse{Status: pb.HealthStatus_HEALTH_DEGRADED}}
	c := startServer(t, fake)

	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !h.Healthy || !h.Degraded {
		t.Fatalf("degraded agent-core must remain usable, got %+v", h)
	}
}

func TestHealthUnhealthyIsNotUsable(t *testing.T) {
	fake := &fakeServer{healthResp: &pb.HealthCheckResponse{Status: pb.HealthStatus_HEALTH_UNHEALTHY}}
	c := startServer(t, fake)

	h, err := c.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if h.Healthy {
		t.Fatalf("UNHEALTHY must not be usable, got %+v", h)
	}
}

func TestHealthFailureMapsToCode(t *testing.T) {
	fake := &fakeServer{healthErr: status.Error(codes.Unavailable, "connection refused")}
	c := startServer(t, fake)

	_, err := c.Health(context.Background())
	if err == nil {
		t.Fatal("want error from unhealthy RPC")
	}
	if got := CodeOf(err); got != string(CodeUnavailable) {
		t.Fatalf("want code %q, got %q (%v)", CodeUnavailable, got, err)
	}
}

func TestHealthAgainstUnreachableAddress(t *testing.T) {
	// Bind and immediately close so the address is definitely not listening.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	c, err := Dial(context.Background(), addr, WithTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Dial must not connect eagerly: %v", err)
	}
	defer c.Close()

	_, err = c.Health(context.Background())
	if err == nil {
		t.Fatal("want error for unreachable agent-core")
	}
	if code := CodeOf(err); code != string(CodeUnavailable) && code != string(CodeDeadlineExceeded) {
		t.Fatalf("want unavailable/deadline_exceeded, got %q (%v)", code, err)
	}
}

func TestDialRejectsEmptyAddress(t *testing.T) {
	if _, err := Dial(context.Background(), ""); err == nil {
		t.Fatal("want error for empty address")
	}
}

func TestDiscoverToolsCachesCapabilities(t *testing.T) {
	fake := &fakeServer{tools: []*pb.ToolCapability{
		{Id: "wasi", Name: "WASI Sandbox", Category: "code"},
		{Id: "retrieve", Name: "Memory Retrieval", Category: "knowledge"},
	}}
	c := startServer(t, fake)

	tools, err := c.DiscoverTools(context.Background())
	if err != nil {
		t.Fatalf("DiscoverTools: %v", err)
	}
	if len(tools) != 2 || tools[0].ID != "wasi" || tools[1].ID != "retrieve" {
		t.Fatalf("unexpected tools: %+v", tools)
	}
	caps := c.Capabilities()
	if !caps.Supports("wasi") || !caps.Supports("retrieve") || caps.Supports("nope") {
		t.Fatalf("capability cache not usable: %+v", caps.IDs())
	}
}

func TestDiscoverToolsFailure(t *testing.T) {
	fake := &fakeServer{discoverErr: status.Error(codes.Internal, "boom")}
	c := startServer(t, fake)

	if _, err := c.DiscoverTools(context.Background()); err == nil {
		t.Fatal("want error")
	} else if CodeOf(err) != string(CodeInternal) {
		t.Fatalf("want internal, got %v", err)
	}
}

// ---------- Execute ----------

func TestExecuteSuccessProjectsRequestAndResult(t *testing.T) {
	fake := &fakeServer{execResp: completedResp("sandbox says hello")}
	c := startServer(t, fake)

	res, err := c.Execute(context.Background(), ExecuteRequest{
		SessionID:      "sess-1",
		Query:          `{"tool":"wasi"}`,
		Context:        map[string]any{"wasm_b64": "AA=="},
		Tools:          []string{"wasi"},
		Budget:         1234,
		Timeout:        5 * time.Second,
		IdempotencyKey: "digest-abc",
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Status != statusCompleted || res.Output != "sandbox says hello" {
		t.Fatalf("unexpected result: %+v", res)
	}
	if res.Backend != BackendName {
		t.Fatalf("want backend %q, got %q", BackendName, res.Backend)
	}
	if res.Latency <= 0 {
		t.Fatalf("want positive latency, got %v", res.Latency)
	}

	fake.mu.Lock()
	req := fake.lastReq
	fake.mu.Unlock()
	if req == nil {
		t.Fatal("server did not receive a request")
	}
	if req.GetMetadata().GetSessionId() != "sess-1" {
		t.Fatalf("session id not projected: %+v", req.GetMetadata())
	}
	if got := req.GetMetadata().GetHeaders()[IdempotencyHeader]; got != "digest-abc" {
		t.Fatalf("idempotency key not carried in headers, got %q", got)
	}
	if got := req.GetContext().GetFields()["idempotency_key"].GetStringValue(); got != "digest-abc" {
		t.Fatalf("idempotency key not mirrored into context, got %q", got)
	}
	if got := req.GetContext().GetFields()["wasm_b64"].GetStringValue(); got != "AA==" {
		t.Fatalf("context not projected: %+v", req.GetContext())
	}
	if len(req.GetAvailableTools()) != 1 || req.GetAvailableTools()[0] != "wasi" {
		t.Fatalf("tool whitelist not projected: %v", req.GetAvailableTools())
	}
	if req.GetMode() != pb.ExecutionMode_EXECUTION_MODE_SYNC {
		t.Fatalf("want sync mode, got %v", req.GetMode())
	}
	if req.GetConfig().GetTimeoutSeconds() != 5 || req.GetConfig().GetTokenBudget() != 1234 {
		t.Fatalf("config not projected: %+v", req.GetConfig())
	}
}

func TestExecuteNonOKStatusIsAnError(t *testing.T) {
	fake := &fakeServer{execResp: &pb.ExecuteTaskResponse{
		Status: pb.TaskStatus_TASK_STATUS_FAILED,
		Output: "sandbox error: no fuel",
	}}
	c := startServer(t, fake)

	res, err := c.Execute(context.Background(), ExecuteRequest{Query: "q"})
	if err == nil {
		t.Fatal("want error for FAILED status so the caller can degrade")
	}
	if CodeOf(err) != string(CodeTaskFailed) {
		t.Fatalf("want task_failed, got %v", err)
	}
	if res.Backend != BackendName || res.Output != "sandbox error: no fuel" {
		t.Fatalf("partial result should be preserved: %+v", res)
	}
}

func TestExecuteErrorMapping(t *testing.T) {
	cases := []struct {
		code codes.Code
		want Code
	}{
		{codes.Unavailable, CodeUnavailable},
		{codes.DeadlineExceeded, CodeDeadlineExceeded},
		{codes.Canceled, CodeCanceled},
		{codes.PermissionDenied, CodePermissionDenied},
		{codes.Unauthenticated, CodePermissionDenied},
		{codes.InvalidArgument, CodeInvalidArgument},
		{codes.NotFound, CodeNotFound},
		{codes.Unimplemented, CodeUnimplemented},
		{codes.ResourceExhausted, CodeResourceExhausted},
		{codes.Internal, CodeInternal},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			fake := &fakeServer{execErr: status.Error(tc.code, "mapped")}
			c := startServer(t, fake)
			_, err := c.Execute(context.Background(), ExecuteRequest{Query: "q"})
			if err == nil {
				t.Fatal("want error")
			}
			if got := CodeOf(err); got != string(tc.want) {
				t.Fatalf("code %v: want %q, got %q", tc.code, tc.want, got)
			}
			// The underlying gRPC status stays inspectable via Unwrap.
			if !errors.Is(err, status.Error(tc.code, "mapped")) {
				if status.Code(errors.Unwrap(err)) != tc.code {
					t.Fatalf("code %v: underlying status lost: %v", tc.code, err)
				}
			}
		})
	}
}

func TestExecuteInvalidContextIsRejectedLocally(t *testing.T) {
	fake := &fakeServer{execResp: completedResp("nope")}
	c := startServer(t, fake)

	_, err := c.Execute(context.Background(), ExecuteRequest{
		Query:   "q",
		Context: map[string]any{"bad": func() {}},
	})
	if err == nil {
		t.Fatal("want error")
	}
	if CodeOf(err) != string(CodeInvalidRequest) {
		t.Fatalf("want invalid_request, got %v", err)
	}
	fake.mu.Lock()
	calls := fake.execCalls
	fake.mu.Unlock()
	if calls != 0 {
		t.Fatalf("invalid request must not reach the server, got %d calls", calls)
	}
}

// ---------- Stream (section 5.3 heartbeat source) ----------

func TestStreamDeliversEveryUpdate(t *testing.T) {
	fake := &fakeServer{streamUpdates: []*pb.TaskUpdate{
		{TaskId: "t1", Iteration: 1, Status: pb.TaskStatus_TASK_STATUS_RUNNING},
		{TaskId: "t1", Iteration: 2, ToolName: "wasi", Status: pb.TaskStatus_TASK_STATUS_RUNNING,
			Result: &pb.ToolResult{Success: true, Output: "partial"}},
		{TaskId: "t1", Iteration: 3, ToolName: "wasi", Status: pb.TaskStatus_TASK_STATUS_COMPLETED,
			Result: &pb.ToolResult{Success: true, Output: "final output"}},
	}}
	c := startServer(t, fake)

	var got []TaskUpdate
	res, err := c.Stream(context.Background(), ExecuteRequest{SessionID: "s", Query: "q"}, func(u TaskUpdate) error {
		got = append(got, u)
		return nil
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 updates, got %d (%+v)", len(got), got)
	}
	if got[0].Iteration != 1 || got[0].Status != statusRunning {
		t.Fatalf("first update wrong: %+v", got[0])
	}
	if got[2].Iteration != 3 || got[2].ToolName != "wasi" || got[2].Output != "final output" || !got[2].Success {
		t.Fatalf("last update wrong: %+v", got[2])
	}
	if res.Status != statusCompleted || res.Output != "final output" || res.Backend != BackendName {
		t.Fatalf("unexpected result: %+v", res)
	}

	fake.mu.Lock()
	req := fake.lastReq
	streamCalls := fake.streamCalls
	fake.mu.Unlock()
	if req.GetMode() != pb.ExecutionMode_EXECUTION_MODE_STREAM {
		t.Fatalf("want stream mode, got %v", req.GetMode())
	}
	if streamCalls != 1 {
		t.Fatalf("want exactly 1 stream call, got %d", streamCalls)
	}
}

func TestStreamFallsBackToExecuteBeforeFirstUpdate(t *testing.T) {
	fake := &fakeServer{
		streamMidErr: status.Error(codes.Unavailable, "stream refused"),
		execResp:     completedResp("from ExecuteTask"),
	}
	c := startServer(t, fake)

	res, err := c.Stream(context.Background(), ExecuteRequest{Query: "q"}, nil)
	if err != nil {
		t.Fatalf("Stream should fall back to ExecuteTask: %v", err)
	}
	if res.Output != "from ExecuteTask" || res.Backend != BackendName {
		t.Fatalf("unexpected fallback result: %+v", res)
	}
	fake.mu.Lock()
	streamCalls, execCalls := fake.streamCalls, fake.execCalls
	fake.mu.Unlock()
	if streamCalls != 1 || execCalls != 1 {
		t.Fatalf("want 1 stream + 1 execute fallback, got %d/%d", streamCalls, execCalls)
	}
}

func TestStreamFailureAfterFirstUpdateIsUncertain(t *testing.T) {
	fake := &fakeServer{
		streamUpdates: []*pb.TaskUpdate{
			{TaskId: "t1", Iteration: 1, Status: pb.TaskStatus_TASK_STATUS_RUNNING},
		},
		streamMidErr: status.Error(codes.Internal, "worker died"),
		execResp:     completedResp("must not be used"),
	}
	c := startServer(t, fake)

	var updates int
	res, err := c.Stream(context.Background(), ExecuteRequest{Query: "q"}, func(TaskUpdate) error {
		updates++
		return nil
	})
	if err == nil {
		t.Fatal("want error after the stream died mid-flight")
	}
	if !IsUncertain(err) || !errors.Is(err, ErrUncertain) {
		t.Fatalf("want ErrUncertain, got %v", err)
	}
	if CodeOf(err) != string(CodeStreamInterrupted) {
		t.Fatalf("want stream_interrupted, got %v", err)
	}
	if updates != 1 {
		t.Fatalf("want 1 delivered update, got %d", updates)
	}
	// The step must stay attributed to agent-core: no silent reroute.
	if res.Backend != BackendName {
		t.Fatalf("want backend %q, got %q", BackendName, res.Backend)
	}
	fake.mu.Lock()
	execCalls := fake.execCalls
	fake.mu.Unlock()
	if execCalls != 0 {
		t.Fatalf("must not fall back to ExecuteTask after updates: %d calls", execCalls)
	}
}

func TestStreamNonOKStatusIsAnError(t *testing.T) {
	fake := &fakeServer{streamUpdates: []*pb.TaskUpdate{
		{TaskId: "t1", Iteration: 1, Status: pb.TaskStatus_TASK_STATUS_FAILED,
			Result: &pb.ToolResult{Success: false, Output: "sandbox exploded"}},
	}}
	c := startServer(t, fake)

	res, err := c.Stream(context.Background(), ExecuteRequest{Query: "q"}, nil)
	if err == nil {
		t.Fatal("want error for FAILED terminal status")
	}
	if CodeOf(err) != string(CodeTaskFailed) {
		t.Fatalf("want task_failed, got %v", err)
	}
	if res.Output != "sandbox exploded" {
		t.Fatalf("partial output lost: %+v", res)
	}
}

func TestStreamWithoutUpdatesIsAnError(t *testing.T) {
	c := startServer(t, &fakeServer{})

	if _, err := c.Stream(context.Background(), ExecuteRequest{Query: "q"}, nil); err == nil {
		t.Fatal("want error when the stream closes without updates")
	}
}

// ---------- CircuitBreaker (section 5.4 short-circuit) ----------

func TestCircuitBreakerShortCircuitsRepeatedFailures(t *testing.T) {
	fake := &fakeServer{execErr: status.Error(codes.Unavailable, "down")}
	breaker := &harness.CircuitBreaker{Threshold: 2, Cooldown: time.Minute}
	c := startServer(t, fake, WithBreaker(breaker))

	for i := 0; i < 2; i++ {
		if _, err := c.Execute(context.Background(), ExecuteRequest{Query: "q"}); err == nil {
			t.Fatalf("call %d: want error", i+1)
		}
	}
	_, err := c.Execute(context.Background(), ExecuteRequest{Query: "q"})
	if err == nil {
		t.Fatal("want short-circuit error once the breaker is open")
	}
	if CodeOf(err) != string(CodeCircuitOpen) {
		t.Fatalf("want circuit_open, got %v", err)
	}
	fake.mu.Lock()
	calls := fake.execCalls
	fake.mu.Unlock()
	if calls != 2 {
		t.Fatalf("open breaker must not reach the server, got %d calls", calls)
	}
}

func TestCircuitBreakerResetsOnSuccess(t *testing.T) {
	fake := &fakeServer{execResp: completedResp("ok")}
	breaker := &harness.CircuitBreaker{Threshold: 2, Cooldown: time.Minute}
	breaker.Failure()
	c := startServer(t, fake, WithBreaker(breaker))

	if _, err := c.Execute(context.Background(), ExecuteRequest{Query: "q"}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if breaker.Failures != 0 {
		t.Fatalf("success must reset failures, got %d", breaker.Failures)
	}
}

// ---------- helpers ----------

func TestCodeOfPlainError(t *testing.T) {
	if got := CodeOf(fmt.Errorf("plain")); got != string(CodeUnknown) {
		t.Fatalf("want unknown, got %q", got)
	}
	if got := CodeOf(nil); got != "" {
		t.Fatalf("want empty for nil, got %q", got)
	}
}
