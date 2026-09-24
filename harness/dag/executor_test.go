package dag

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	h "github.com/Hanwen-Jiang/dag-code-agent/harness"
)

func testManifest(t *testing.T) Manifest {
	t.Helper()
	m, err := LoadManifest("../../configs/software-feature-with-rework-and-uat.json")
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func testExecutor(t *testing.T, m Manifest, outcomes map[string][]string) (*Executor, h.Store) {
	t.Helper()
	store, err := h.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewExecutor(m, store, NewDeterministicRunnerFactory(outcomes))
	if err != nil {
		t.Fatal(err)
	}
	return executor, store
}

func TestCanonicalHappyPathAndUserGate(t *testing.T) {
	m := testManifest(t)
	executor, store := testExecutor(t, m, nil)
	defer store.Close()
	report, err := executor.Execute("happy", Options{Context: context.Background()})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "WAITING_USER" || report.WaitingNodeID != "N-UAT" {
		t.Fatalf("report=%+v", report)
	}
	if report.NodeStates["N-INTEGRATE"] != NodeSucceeded || report.NodeStates["N-DIAGNOSIS"] != NodeBlocked {
		t.Fatalf("states=%+v", report.NodeStates)
	}
	for _, artifact := range report.Artifacts {
		if !strings.HasPrefix(artifact.SHA256, "sha256:") {
			t.Fatalf("artifact=%+v", artifact)
		}
	}
	final, err := executor.ResumeUser("happy", "PASS", "uat-user")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "COMPLETED" || final.NodeStates["N-UAT"] != NodeSucceeded {
		t.Fatalf("final=%+v", final)
	}
	events, err := executor.Replay("happy")
	if err != nil || len(events) != len(final.Events) {
		t.Fatalf("events=%d report=%d err=%v", len(events), len(final.Events), err)
	}
}

func TestReworkUsesFreshAttemptsAndArtifactContracts(t *testing.T) {
	m := testManifest(t)
	executor, store := testExecutor(t, m, map[string][]string{"N-VERIFY-V1": {"FAILED", "FAILED"}})
	defer store.Close()
	report, err := executor.Execute("rework", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "WAITING_USER" {
		t.Fatalf("status=%s", report.Status)
	}
	attempts := []Attempt{}
	for _, attempt := range report.Attempts {
		if attempt.NodeID == "N-VERIFY-V1" {
			attempts = append(attempts, attempt)
		}
	}
	if len(attempts) != 2 || attempts[0].AgentInstanceID == attempts[1].AgentInstanceID {
		t.Fatalf("attempts=%+v", attempts)
	}
	contracts := map[string]bool{}
	for _, artifact := range report.Artifacts {
		contracts[artifact.ContractID] = true
	}
	for _, required := range []string{"DIAGNOSIS-AUTH", "PATCH-V2", "REPORT-V2"} {
		if !contracts[required] {
			t.Fatalf("missing %s in %v", required, contracts)
		}
	}
	final, err := executor.ResumeUser("rework", "APPROVED", "owner")
	if err != nil || final.Status != "COMPLETED" {
		t.Fatalf("final=%+v err=%v", final, err)
	}
}

func TestHarnessNodeRunnerInjectsInputArtifactsAsPreviousResults(t *testing.T) {
	m := testManifest(t)
	store, err := h.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	model := &captureModel{response: h.ModelResponse{Text: "done", FinishReason: "stop"}}
	runtime, err := h.New(h.WithStore(store), h.WithModel(model))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runner := &HarnessNodeRunner{Runtime: runtime, Auth: h.AuthContext{TenantID: "dag-tenant", SubjectID: "scheduler"}}
	node := m.Nodes[0]
	_, err = runner.Run(context.Background(), NodeExecutionContext{
		RunID:           "previous-results",
		ManifestID:      m.ManifestID,
		SpecHash:        m.SpecHash,
		Node:            node,
		AttemptID:       "attempt-1",
		Ordinal:         1,
		AgentInstanceID: "agent-1",
		InputArtifacts:  []Artifact{{ArtifactID: "ART-IN", NodeID: "producer", ContractID: "RESULT", SHA256: mustDigest(map[string]any{"answer": "42"}), Content: map[string]any{"answer": "42"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(model.requests) == 0 || !strings.Contains(providerMessagesText(model.requests[0].Messages), "ART-IN") {
		t.Fatalf("previous result missing from model request: %#v", model.requests)
	}
}

type captureModel struct {
	requests []h.ModelRequest
	response h.ModelResponse
}

func (m *captureModel) Name() string { return "capture" }
func (m *captureModel) Complete(_ context.Context, request h.ModelRequest) (h.ModelResponse, error) {
	m.requests = append(m.requests, request)
	return m.response, nil
}
func (m *captureModel) Stream(ctx context.Context, request h.ModelRequest) (<-chan h.StreamEvent, error) {
	out := make(chan h.StreamEvent, 1)
	close(out)
	return out, nil
}

func providerMessagesText(messages []map[string]any) string {
	var parts []string
	for _, message := range messages {
		if content, ok := message["content"].(string); ok {
			parts = append(parts, content)
		}
	}
	return strings.Join(parts, "\n")
}

func TestCrashRecoveryMarksInflightAttemptAbandoned(t *testing.T) {
	m := testManifest(t)
	executor, store := testExecutor(t, m, nil)
	defer store.Close()
	_, err := executor.Execute("crash", Options{CrashAfterCommits: 2})
	if !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("err=%v", err)
	}
	restarted, err := NewExecutor(m, store, NewDeterministicRunnerFactory(nil))
	if err != nil {
		t.Fatal(err)
	}
	report, err := restarted.Execute("crash", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "WAITING_USER" {
		t.Fatalf("status=%s", report.Status)
	}
	design := []Attempt{}
	for _, attempt := range report.Attempts {
		if attempt.NodeID == "N-DESIGN" {
			design = append(design, attempt)
		}
	}
	if len(design) != 2 || design[0].State != "ABANDONED" || design[1].State != NodeSucceeded || design[0].AgentInstanceID == design[1].AgentInstanceID {
		t.Fatalf("design=%+v", design)
	}
}

func TestLeasePauseResumeCancelAndReject(t *testing.T) {
	m := testManifest(t)
	t.Run("lease", func(t *testing.T) {
		executor, store := testExecutor(t, m, nil)
		defer store.Close()
		leaseID := "external"
		resource := "dag:" + m.ManifestID + ":N-DESIGN"
		ok, err := store.AcquireLease(resource, "other", leaseID, time.Minute)
		if err != nil || !ok {
			t.Fatalf("lease=%v err=%v", ok, err)
		}
		waiting, err := executor.Execute("lease-run", Options{})
		if err != nil || waiting.Status != "WAITING_LEASE" {
			t.Fatalf("waiting=%+v err=%v", waiting, err)
		}
		_, _ = store.ReleaseLease(leaseID, "other")
		resumed, err := executor.Execute("lease-run", Options{})
		if err != nil || resumed.Status != "WAITING_USER" {
			t.Fatalf("resumed=%+v err=%v", resumed, err)
		}
	})
	t.Run("pause_cancel", func(t *testing.T) {
		executor, store := testExecutor(t, m, nil)
		defer store.Close()
		paused, err := executor.Execute("pause", Options{MaxNodeExecutions: 1})
		if err != nil || paused.Status != "PAUSED" {
			t.Fatalf("paused=%+v err=%v", paused, err)
		}
		resumed, err := executor.Execute("pause", Options{})
		if err != nil || resumed.Status != "WAITING_USER" {
			t.Fatalf("resumed=%+v err=%v", resumed, err)
		}
		cancelled, err := executor.Cancel("pause", "test")
		if err != nil || cancelled.Status != "CANCELLED" {
			t.Fatalf("cancel=%+v err=%v", cancelled, err)
		}
	})
	t.Run("reject", func(t *testing.T) {
		executor, store := testExecutor(t, m, nil)
		defer store.Close()
		_, _ = executor.Execute("reject", Options{})
		rejected, err := executor.ResumeUser("reject", "REJECT", "user")
		if err != nil || rejected.Status != "FAILED" || rejected.NodeStates["N-UAT"] != NodeFailed {
			t.Fatalf("rejected=%+v err=%v", rejected, err)
		}
	})
}

func TestUnhandledFailureBlocksAndMissingEvidenceRetries(t *testing.T) {
	m := testManifest(t)
	executor, store := testExecutor(t, m, map[string][]string{"N-DESIGN": {"FAILED", "FAILED"}})
	defer store.Close()
	report, err := executor.Execute("blocked", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "BLOCKED" || report.NodeStates["N-DESIGN"] != NodeFailed || report.NodeStates["N-IMPLEMENT-V1"] != NodeBlocked {
		t.Fatalf("report=%+v", report)
	}
	noEvidence := func(node Node, _ int, _ string) NodeRunner {
		return runnerFunc(func(context.Context, NodeExecutionContext) (NodeExecutionResult, error) {
			artifacts := []map[string]any{}
			for _, contract := range node.OutputContracts {
				artifacts = append(artifacts, map[string]any{"contract_id": contract.ContractID, "content": map[string]any{"x": 1}})
			}
			return NodeExecutionResult{Status: "SUCCEEDED", Artifacts: artifacts}, nil
		})
	}
	other, otherStore := testExecutor(t, m, nil)
	defer otherStore.Close()
	other.Factory = noEvidence
	missing, err := other.Execute("no-evidence", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if missing.Status != "BLOCKED" || missing.NodeStates["N-DESIGN"] != NodeFailed || len(missing.Attempts) != 2 {
		t.Fatalf("missing=%+v", missing)
	}
}

type runnerFunc func(context.Context, NodeExecutionContext) (NodeExecutionResult, error)

func (f runnerFunc) Run(ctx context.Context, input NodeExecutionContext) (NodeExecutionResult, error) {
	return f(ctx, input)
}

func TestManifestValidation(t *testing.T) {
	m := testManifest(t)
	duplicate := m
	duplicate.Nodes = append(append([]Node{}, m.Nodes...), m.Nodes[0])
	if err := ValidateManifest(duplicate); err == nil {
		t.Fatal("duplicate node accepted")
	}
	cycle := m
	cycle.Edges = append(append([]Edge{}, m.Edges...), Edge{EdgeID: "cycle", FromNode: "N-UAT", ToNode: "N-DESIGN", EdgeType: "CONTROL", Gating: true})
	if err := ValidateManifest(cycle); err == nil {
		t.Fatal("cycle accepted")
	}
}

// A hand-off that claims a digest must prove it. This is what stops a producer
// from handing over a value that differs from what it committed.
func TestPreviousResultWithMismatchedDigestIsRefused(t *testing.T) {
	store, err := h.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	model := &captureModel{response: h.ModelResponse{Text: "done", FinishReason: "stop"}}
	runtime, err := h.New(h.WithStore(store), h.WithModel(model))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	m := testManifest(t)
	runner := &HarnessNodeRunner{Runtime: runtime, Auth: h.AuthContext{TenantID: "dag-tenant", SubjectID: "scheduler"}}
	// The digest is computed over one payload while Content carries another.
	result, err := runner.Run(context.Background(), NodeExecutionContext{
		RunID:           "previous-results-tampered",
		ManifestID:      m.ManifestID,
		SpecHash:        m.SpecHash,
		Node:            m.Nodes[0],
		AttemptID:       "attempt-1",
		Ordinal:         1,
		AgentInstanceID: "agent-1",
		InputArtifacts: []Artifact{{
			ArtifactID: "ART-IN", NodeID: "producer", ContractID: "RESULT",
			SHA256:  mustDigest(map[string]any{"answer": "41"}),
			Content: map[string]any{"answer": "42"},
		}},
	})
	// A rejected hand-off is a task-level failure (category CONTEXT), so the
	// node reports FAILED rather than returning an infrastructure error.
	if err != nil {
		t.Fatalf("runner returned an infrastructure error: %v", err)
	}
	if result.Status != "FAILED" || !strings.Contains(result.ErrorMessage, "DIGEST_MISMATCH") {
		t.Fatalf("result=%+v, want FAILED with a digest mismatch", result)
	}
	if len(model.requests) != 0 {
		t.Fatalf("the model was called with an unverified hand-off: %#v", model.requests)
	}
}
