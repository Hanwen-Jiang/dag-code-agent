package main

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	pb "github.com/Hanwen-Jiang/dag-code-agent/proto/agent_core"
)

func testCore() *agentCore {
	return &agentCore{
		idempotency: harness.NewInMemoryStore(),
		started:     time.Now(),
	}
}

// A retried step must not run twice. The workflow layer derives a stable key per
// step, and before this the key was transmitted but never consumed.
func TestToolExecutionIsDeduplicated(t *testing.T) {
	core := testCore()
	var runs int32
	execute := func() (*pb.ExecuteTaskResponse, error) {
		atomic.AddInt32(&runs, 1)
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_COMPLETED, Output: "hello from wasi", Sandbox: true}, nil
	}
	first, err := core.runToolOnce("orch-step:abc", "wasi", execute)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := core.runToolOnce("orch-step:abc", "wasi", execute)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("the tool ran %d times, want 1", got)
	}
	if first.Output != "hello from wasi" || second.Output != "hello from wasi" {
		t.Fatalf("output was not preserved: %q / %q", first.Output, second.Output)
	}
	if second.Sandbox != first.Sandbox {
		t.Fatal("the cached outcome lost the sandbox flag")
	}
	if _, ok := second.Metadata.GetFields()["deduplicated"]; !ok {
		t.Fatal("a deduplicated response is not marked as such")
	}
	if _, ok := first.Metadata.GetFields()["deduplicated"]; ok {
		t.Fatal("the first execution claimed to be deduplicated")
	}
}

// Different keys are different executions.
func TestDifferentKeysExecuteIndependently(t *testing.T) {
	core := testCore()
	var runs int32
	execute := func() (*pb.ExecuteTaskResponse, error) {
		atomic.AddInt32(&runs, 1)
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_COMPLETED, Output: "ok"}, nil
	}
	for _, key := range []string{"k1", "k2", "k3"} {
		if _, err := core.runToolOnce(key, "wasi", execute); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 3 {
		t.Fatalf("ran %d times, want 3", got)
	}
}

// A failure must stay retryable: memoizing it would turn a transient error into
// a permanent one for the life of the key.
func TestFailuresAreNotMemoized(t *testing.T) {
	core := testCore()
	var runs int32
	execute := func() (*pb.ExecuteTaskResponse, error) {
		atomic.AddInt32(&runs, 1)
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_FAILED, Output: "boom"}, nil
	}
	for i := 0; i < 2; i++ {
		if _, err := core.runToolOnce("retry-me", "wasi", execute); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("a failed execution was memoized (ran %d times, want 2)", got)
	}
}

// An execution error is not cached either.
func TestExecutionErrorsAreNotMemoized(t *testing.T) {
	core := testCore()
	var runs int32
	execute := func() (*pb.ExecuteTaskResponse, error) {
		atomic.AddInt32(&runs, 1)
		return nil, errors.New("sandbox unavailable")
	}
	for i := 0; i < 2; i++ {
		if _, err := core.runToolOnce("err-key", "wasi", execute); err == nil {
			t.Fatal("expected an error")
		}
	}
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("an errored execution was memoized (ran %d times, want 2)", got)
	}
}

func TestRunToolOnceWithoutKeyAlwaysExecutes(t *testing.T) {
	core := testCore()
	var runs int32
	execute := func() (*pb.ExecuteTaskResponse, error) {
		atomic.AddInt32(&runs, 1)
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_COMPLETED, Output: "ok"}, nil
	}
	for i := 0; i < 3; i++ {
		if _, err := core.runToolOnce("", "wasi", execute); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 3 {
		t.Fatalf("ran %d times, want 3", got)
	}
}

// The key is read from the metadata headers the workflow layer populates.
func TestIdempotencyKeyIsReadFromHeaders(t *testing.T) {
	// TaskMetadata.Headers is a plain map[string]string on the wire.
	in := &pb.ExecuteTaskRequest{
		Query:    `{"tool":"wasi"}`,
		Metadata: &pb.TaskMetadata{Headers: map[string]string{"Idempotency-Key": "  orch-step:xyz  "}},
	}
	if got := idempotencyKeyOf(in); got != "orch-step:xyz" {
		t.Fatalf("key=%q, want orch-step:xyz (trimmed, case-insensitive header)", got)
	}
	if got := idempotencyKeyOf(&pb.ExecuteTaskRequest{}); got != "" {
		t.Fatalf("key=%q, want empty when no metadata is present", got)
	}
}
