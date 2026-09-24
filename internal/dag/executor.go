package dag

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	h "agent-harness/internal/harness"
)

const (
	NodePending   = "PENDING"
	NodeReady     = "READY"
	NodeLeased    = "LEASED"
	NodeRunning   = "RUNNING"
	NodeSucceeded = "SUCCEEDED"
	NodeFailed    = "FAILED"
	NodeBlocked   = "BLOCKED"
	NodeCancelled = "CANCELLED"
)

var ErrSimulatedCrash = errors.New("SIMULATED_CRASH")

type Attempt struct {
	AttemptID       string `json:"attempt_id"`
	NodeID          string `json:"node_id"`
	AgentInstanceID string `json:"agent_instance_id"`
	Ordinal         int    `json:"ordinal"`
	State           string `json:"state"`
	ErrorCode       string `json:"error_code,omitempty"`
	StartedAt       string `json:"started_at"`
	EndedAt         string `json:"ended_at,omitempty"`
}
type Artifact struct {
	ArtifactID string         `json:"artifact_id"`
	ContractID string         `json:"contract_id"`
	NodeID     string         `json:"producer_node_id"`
	AttemptID  string         `json:"producer_attempt_id"`
	SHA256     string         `json:"sha256"`
	MediaType  string         `json:"media_type"`
	Content    map[string]any `json:"content,omitempty"`
}
type Evidence struct {
	EvidenceID  string `json:"evidence_id"`
	CriterionID string `json:"criterion_id,omitempty"`
	NodeID      string `json:"producer_node_id"`
	AttemptID   string `json:"producer_attempt_id"`
	ArtifactID  string `json:"artifact_id,omitempty"`
	Verdict     string `json:"verdict"`
	Method      string `json:"method"`
	Observed    string `json:"observed,omitempty"`
}

type DAGState struct {
	RunID          string            `json:"run_id"`
	TaskID         string            `json:"task_id"`
	ManifestID     string            `json:"manifest_id"`
	SpecHash       string            `json:"spec_hash"`
	Status         string            `json:"status"`
	NodeStates     map[string]string `json:"node_states"`
	AttemptCounts  map[string]int    `json:"attempt_counts"`
	Attempts       []Attempt         `json:"attempts"`
	Artifacts      []Artifact        `json:"artifacts"`
	Evidence       []Evidence        `json:"evidence"`
	CompletedOrder []string          `json:"completed_order"`
	UserOutcomes   map[string]string `json:"user_outcomes"`
	WaitingNodeID  string            `json:"waiting_node_id,omitempty"`
	Failure        map[string]any    `json:"failure,omitempty"`
}

func (s *DAGState) Normalize() {
	if s.Status == "" {
		s.Status = "RUNNING"
	}
	if s.NodeStates == nil {
		s.NodeStates = map[string]string{}
	}
	if s.AttemptCounts == nil {
		s.AttemptCounts = map[string]int{}
	}
	if s.Attempts == nil {
		s.Attempts = []Attempt{}
	}
	if s.Artifacts == nil {
		s.Artifacts = []Artifact{}
	}
	if s.Evidence == nil {
		s.Evidence = []Evidence{}
	}
	if s.CompletedOrder == nil {
		s.CompletedOrder = []string{}
	}
	if s.UserOutcomes == nil {
		s.UserOutcomes = map[string]string{}
	}
}
func encodeDAG(s DAGState) map[string]any {
	b, _ := json.Marshal(s)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
func decodeDAG(v any) (DAGState, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return DAGState{}, err
	}
	var s DAGState
	err = json.Unmarshal(b, &s)
	s.Normalize()
	return s, err
}

type NodeExecutionContext struct {
	RunID            string
	ManifestID       string
	SpecHash         string
	Node             Node
	AttemptID        string
	Ordinal          int
	AgentInstanceID  string
	InputArtifacts   []Artifact
	ExactConstraints []string
}
type NodeExecutionResult struct {
	Status       string
	Summary      string
	Artifacts    []map[string]any
	Evidence     []map[string]any
	ErrorCode    string
	ErrorMessage string
}
type NodeRunner interface {
	Run(context.Context, NodeExecutionContext) (NodeExecutionResult, error)
}
type RunnerFactory func(Node, int, string) NodeRunner

type DeterministicRunnerFactory struct{ Outcomes map[string][]string }

func NewDeterministicRunnerFactory(outcomes map[string][]string) RunnerFactory {
	f := &DeterministicRunnerFactory{Outcomes: outcomes}
	return f.New
}
func (f *DeterministicRunnerFactory) New(node Node, attempt int, instance string) NodeRunner {
	outcome := "SUCCEEDED"
	if items := f.Outcomes[node.NodeID]; attempt-1 < len(items) {
		outcome = items[attempt-1]
	}
	return &DeterministicRunner{Node: node, Outcome: outcome, InstanceID: instance}
}

type DeterministicRunner struct {
	Node       Node
	Outcome    string
	InstanceID string
}

func (r *DeterministicRunner) Run(_ context.Context, ctx NodeExecutionContext) (NodeExecutionResult, error) {
	status := strings.ToUpper(r.Outcome)
	result := NodeExecutionResult{Status: status, Summary: r.Node.Objective}
	if status != "SUCCEEDED" && status != "PASS" && status != "PASSED" && status != "APPROVED" {
		result.ErrorCode = "RUNNER_FAILED"
		result.ErrorMessage = "deterministic fixture outcome " + status
		return result, nil
	}
	for _, contract := range r.Node.OutputContracts {
		content := map[string]any{"node_id": r.Node.NodeID, "attempt_id": ctx.AttemptID, "objective": r.Node.Objective, "input_artifact_ids": artifactIDs(ctx.InputArtifacts), "outcome": status}
		result.Artifacts = append(result.Artifacts, map[string]any{"contract_id": contract.ContractID, "artifact_kind": contract.ArtifactKind, "media_type": contract.MediaType, "content": content})
		result.Evidence = append(result.Evidence, map[string]any{"criterion_id": firstCriterion(r.Node), "verdict": "PASS", "method": "deterministic-fixture", "observed": status})
	}
	return result, nil
}
func artifactIDs(items []Artifact) []string {
	out := []string{}
	for _, a := range items {
		out = append(out, a.ArtifactID)
	}
	sort.Strings(out)
	return out
}
func firstCriterion(node Node) string {
	if len(node.OutputContracts) > 0 {
		return node.OutputContracts[0].ContractID
	}
	return node.NodeID
}

type HarnessNodeRunner struct {
	Runtime    *h.Runtime
	Auth       h.AuthContext
	InstanceID string
}

func (r *HarnessNodeRunner) Run(ctx context.Context, nodeCtx NodeExecutionContext) (NodeExecutionResult, error) {
	if r.Runtime == nil {
		return NodeExecutionResult{Status: "FAILED", ErrorCode: "NO_RUNTIME"}, nil
	}
	request := h.TaskRequest{TaskID: "dag:" + nodeCtx.RunID + ":" + nodeCtx.Node.NodeID + ":" + nodeCtx.AttemptID, Goal: nodeCtx.Node.Objective, TenantID: r.Auth.TenantID, SubjectID: r.Auth.SubjectID, MaxSteps: 8, ContextWindow: 4096, Constraints: nodeCtx.ExactConstraints}
	result := r.Runtime.Run(ctx, request, r.Auth)
	if result.Status != h.TaskCompleted {
		return NodeExecutionResult{Status: "FAILED", ErrorCode: result.FailureCategory, ErrorMessage: result.State.LastErrorString()}, nil
	}
	artifacts := []map[string]any{}
	evidence := []map[string]any{}
	for _, contract := range nodeCtx.Node.OutputContracts {
		artifacts = append(artifacts, map[string]any{"contract_id": contract.ContractID, "artifact_kind": contract.ArtifactKind, "media_type": contract.MediaType, "content": map[string]any{"summary": result.FinalAnswer}})
		evidence = append(evidence, map[string]any{"criterion_id": contract.ContractID, "verdict": "PASS", "method": "harness-verifier", "observed": result.Status})
	}
	return NodeExecutionResult{Status: "SUCCEEDED", Summary: result.FinalAnswer, Artifacts: artifacts, Evidence: evidence}, nil
}

type Options struct {
	CrashAfterCommits int
	MaxNodeExecutions int
	Context           context.Context
}
type Report struct {
	RunID          string            `json:"run_id"`
	TaskID         string            `json:"task_id"`
	Status         string            `json:"status"`
	NodeStates     map[string]string `json:"node_states"`
	Attempts       []Attempt         `json:"attempts"`
	Artifacts      []Artifact        `json:"artifacts"`
	Evidence       []Evidence        `json:"evidence"`
	CompletedOrder []string          `json:"completed_order"`
	WaitingNodeID  string            `json:"waiting_node_id,omitempty"`
	Failure        map[string]any    `json:"failure,omitempty"`
	Events         []h.Event         `json:"events"`
}

func (r Report) Metrics() map[string]float64 {
	success := 0.
	for _, s := range r.NodeStates {
		if s == NodeSucceeded {
			success++
		}
	}
	return map[string]float64{"node_success_count": success, "attempt_count": float64(len(r.Attempts)), "artifact_count": float64(len(r.Artifacts)), "evidence_count": float64(len(r.Evidence)), "event_count": float64(len(r.Events))}
}

type Executor struct {
	Manifest Manifest
	Store    *h.Store
	Factory  RunnerFactory
	mu       sync.Mutex
	commits  map[string]int
}

func NewExecutor(manifest Manifest, store *h.Store, factory RunnerFactory) (*Executor, error) {
	if err := ValidateManifest(manifest); err != nil {
		return nil, err
	}
	if store == nil {
		var err error
		store, err = h.NewStore(":memory:")
		if err != nil {
			return nil, err
		}
	}
	if factory == nil {
		factory = NewDeterministicRunnerFactory(nil)
	}
	return &Executor{Manifest: manifest, Store: store, Factory: factory, commits: map[string]int{}}, nil
}
func (e *Executor) initial(runID string) DAGState {
	s := DAGState{RunID: runID, TaskID: runID, ManifestID: e.Manifest.ManifestID, SpecHash: e.Manifest.SpecHash, Status: "RUNNING"}
	s.Normalize()
	for _, n := range e.Manifest.Nodes {
		s.NodeStates[n.NodeID] = NodePending
	}
	return s
}
func (e *Executor) load(runID string) (DAGState, h.TaskState, error) {
	task, _, err := e.Store.GetTask(runID)
	if err != nil {
		return DAGState{}, task, err
	}
	s, err := decodeDAG(task.Metadata["dag_state"])
	return s, task, err
}
func (e *Executor) persist(s DAGState, task h.TaskState, event string, payload map[string]any, opts Options) error {
	task.Status = h.TaskRunning
	if s.Status == "COMPLETED" {
		task.Status = h.TaskCompleted
	}
	if s.Status == "CANCELLED" {
		task.Status = h.TaskCancelled
	}
	if s.Status == "FAILED" || s.Status == "BLOCKED" {
		task.Status = h.TaskFailed
	}
	task.Metadata["dag_state"] = encodeDAG(s)
	_, err := e.Store.CommitStateEvent(task, event, payload, nil, nil)
	if err != nil {
		return err
	}
	e.commits[s.RunID]++
	if opts.CrashAfterCommits > 0 && e.commits[s.RunID] >= opts.CrashAfterCommits {
		return ErrSimulatedCrash
	}
	return nil
}
func (e *Executor) create(runID string) (DAGState, error) {
	s := e.initial(runID)
	task := h.TaskState{TaskID: runID, Goal: e.Manifest.Objective(), TenantID: "dag-tenant", SubjectID: "scheduler", Status: h.TaskRunning, Metadata: map[string]any{"dag_state": encodeDAG(s), "request": h.TaskRequest{TaskID: runID, Goal: e.Manifest.Objective(), TenantID: "dag-tenant", SubjectID: "scheduler", ContextWindow: 4096}}}
	task.Normalize()
	_, err := e.Store.CreateTaskWithEvent(task, "DAG_ACCEPTED", map[string]any{"manifest_id": e.Manifest.ManifestID, "spec_hash": e.Manifest.SpecHash})
	return s, err
}

func (e *Executor) Execute(runID string, opts Options) (Report, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	s, _, err := e.load(runID)
	if err != nil {
		if !errors.Is(err, h.ErrNotFound) {
			return Report{}, err
		}
		s, err = e.create(runID)
		if err != nil {
			return Report{}, err
		}
	}
	s.Normalize()
	if s.Status == "PAUSED" || s.Status == "WAITING_LEASE" {
		s.Status = "RUNNING"
		if err := e.persist(s, e.taskFor(s), "DAG_RESUMED", map[string]any{}, opts); err != nil {
			return e.report(s), err
		}
	}
	if err := e.recoverInflight(&s, opts); err != nil {
		return e.report(s), err
	}
	executed := 0
	order, _ := e.Manifest.TopologicalOrder()
	for {
		if s.Status == "COMPLETED" || s.Status == "FAILED" || s.Status == "BLOCKED" || s.Status == "CANCELLED" || s.Status == "WAITING_USER" {
			return e.report(s), nil
		}
		for _, id := range order {
			if s.NodeStates[id] == NodePending {
				decision := ActivationDecision(e.Manifest, id, s.NodeStates)
				if decision == "READY" {
					s.NodeStates[id] = NodeReady
					if err := e.persist(s, e.taskFor(s), "NODE_READY", map[string]any{"node_id": id}, opts); err != nil {
						return e.report(s), err
					}
				} else if decision == "BLOCKED" {
					s.NodeStates[id] = NodeBlocked
					if err := e.persist(s, e.taskFor(s), "NODE_BLOCKED", map[string]any{"node_id": id}, opts); err != nil {
						return e.report(s), err
					}
				}
			}
		}
		ready := []string{}
		for _, id := range order {
			if s.NodeStates[id] == NodeReady {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			if e.allTerminal(s) {
				s.Status = "COMPLETED"
				if err := e.persist(s, e.taskFor(s), "DAG_COMPLETED", map[string]any{}, opts); err != nil {
					return e.report(s), err
				}
				return e.report(s), nil
			}
			if e.hasFailure(s) {
				s.Status = "BLOCKED"
				if err := e.persist(s, e.taskFor(s), "DAG_BLOCKED", map[string]any{"reason": "unreachable nodes"}, opts); err != nil {
					return e.report(s), err
				}
				return e.report(s), nil
			}
			s.Status = "PAUSED"
			if err := e.persist(s, e.taskFor(s), "DAG_PAUSED", map[string]any{}, opts); err != nil {
				return e.report(s), err
			}
			return e.report(s), nil
		}
		for _, id := range ready {
			if opts.MaxNodeExecutions > 0 && executed >= opts.MaxNodeExecutions {
				s.Status = "PAUSED"
				if err := e.persist(s, e.taskFor(s), "DAG_PAUSED", map[string]any{"reason": "execution_limit"}, opts); err != nil {
					return e.report(s), err
				}
				return e.report(s), nil
			}
			node, _ := e.Manifest.Node(id)
			if mode := node.GateMode(); mode == "USER" || mode == "JOINT" {
				s.Status = "WAITING_USER"
				s.WaitingNodeID = id
				if err := e.persist(s, e.taskFor(s), "USER_GATE_OPENED", map[string]any{"node_id": id, "gate_mode": mode}, opts); err != nil {
					return e.report(s), err
				}
				return e.report(s), nil
			}
			leaseID := h.NewID("LEASE")
			if !e.acquire(id, runID, leaseID) {
				s.Status = "WAITING_LEASE"
				if err := e.persist(s, e.taskFor(s), "LEASE_WAITING", map[string]any{"node_id": id}, opts); err != nil {
					return e.report(s), err
				}
				return e.report(s), nil
			}
			attemptNumber := s.AttemptCounts[id] + 1
			s.AttemptCounts[id] = attemptNumber
			attempt := Attempt{AttemptID: h.NewID("ATT"), NodeID: id, AgentInstanceID: h.NewID("AGENT"), Ordinal: len(s.Attempts) + 1, State: NodeLeased, StartedAt: h.UTCNow()}
			s.Attempts = append(s.Attempts, attempt)
			s.NodeStates[id] = NodeLeased
			if err := e.persist(s, e.taskFor(s), "ATTEMPT_LEASED", map[string]any{"node_id": id, "attempt_id": attempt.AttemptID, "agent_instance_id": attempt.AgentInstanceID}, opts); err != nil {
				return e.report(s), err
			}
			s.NodeStates[id] = NodeRunning
			if err := e.persist(s, e.taskFor(s), "ATTEMPT_STARTED", map[string]any{"node_id": id, "attempt_id": attempt.AttemptID}, opts); err != nil {
				return e.report(s), err
			}
			runner := e.Factory(node, attemptNumber, attempt.AgentInstanceID)
			if runner == nil {
				e.release(leaseID, runID)
				return e.report(s), errors.New("runner factory returned nil")
			}
			input := e.inputArtifacts(id, s)
			result, runErr := runner.Run(opts.Context, NodeExecutionContext{RunID: runID, ManifestID: e.Manifest.ManifestID, SpecHash: e.Manifest.SpecHash, Node: node, AttemptID: attempt.AttemptID, Ordinal: attempt.Ordinal, AgentInstanceID: attempt.AgentInstanceID, InputArtifacts: input, ExactConstraints: node.ExactConstraints()})
			e.release(leaseID, runID)
			if runErr != nil {
				result.Status = "FAILED"
				result.ErrorCode = "RUNNER_ERROR"
				result.ErrorMessage = runErr.Error()
			}
			if accepted := e.accept(node, result); accepted && isSuccess(result.Status) {
				for _, raw := range result.Artifacts {
					a := e.materializeArtifact(node, attempt, raw)
					s.Artifacts = append(s.Artifacts, a)
				}
				for _, raw := range result.Evidence {
					s.Evidence = append(s.Evidence, e.materializeEvidence(node, attempt, raw, s.Artifacts))
				}
				s.NodeStates[id] = NodeSucceeded
				s.CompletedOrder = append(s.CompletedOrder, id)
				e.setAttempt(&s, attempt.AttemptID, NodeSucceeded, "", "")
				if err := e.persist(s, e.taskFor(s), "NODE_SUCCEEDED", map[string]any{"node_id": id, "attempt_id": attempt.AttemptID}, opts); err != nil {
					return e.report(s), err
				}
			} else {
				e.setAttempt(&s, attempt.AttemptID, NodeFailed, result.ErrorCode, result.ErrorMessage)
				if attemptNumber < node.MaxAttempts() {
					s.NodeStates[id] = NodeReady
					if err := e.persist(s, e.taskFor(s), "NODE_RETRY_SCHEDULED", map[string]any{"node_id": id, "attempt": attemptNumber, "error": result.ErrorCode}, opts); err != nil {
						return e.report(s), err
					}
				} else {
					s.NodeStates[id] = NodeFailed
					if err := e.persist(s, e.taskFor(s), "NODE_FAILED", map[string]any{"node_id": id, "error": result.ErrorCode}, opts); err != nil {
						return e.report(s), err
					}
				}
			}
			executed++
		}
		s.Status = "RUNNING"
	}
}

func (n Node) ExactConstraints() []string {
	return []string{n.Objective, "acceptance evidence must bind to the current attempt"}
}
func isSuccess(status string) bool {
	switch strings.ToUpper(status) {
	case "SUCCEEDED", "PASS", "PASSED", "APPROVED":
		return true
	}
	return false
}
func (e *Executor) taskFor(s DAGState) h.TaskState {
	task, _, err := e.Store.GetTask(s.RunID)
	if err != nil {
		task = h.TaskState{TaskID: s.RunID, Goal: e.Manifest.Objective(), TenantID: "dag-tenant", SubjectID: "scheduler", Metadata: map[string]any{}}
	}
	task.Metadata["dag_state"] = encodeDAG(s)
	return task
}
func (e *Executor) recoverInflight(s *DAGState, opts Options) error {
	for i := range s.Attempts {
		if s.Attempts[i].State == NodeLeased || s.Attempts[i].State == NodeRunning {
			_, _ = e.Store.ReleaseResourceLease("dag:"+e.Manifest.ManifestID+":"+s.Attempts[i].NodeID, s.RunID)
			s.Attempts[i].State = "ABANDONED"
			s.Attempts[i].EndedAt = h.UTCNow()
			s.NodeStates[s.Attempts[i].NodeID] = NodeReady
			if err := e.persist(*s, e.taskFor(*s), "ATTEMPT_ABANDONED", map[string]any{"node_id": s.Attempts[i].NodeID, "attempt_id": s.Attempts[i].AttemptID}, opts); err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *Executor) allTerminal(s DAGState) bool {
	for _, state := range s.NodeStates {
		if state != NodeSucceeded && state != NodeBlocked && state != NodeFailed && state != NodeCancelled {
			return false
		}
	}
	for _, n := range e.Manifest.Nodes {
		if s.NodeStates[n.NodeID] != NodeFailed {
			continue
		}
		// A failed node may be an intentional rework branch.  It is terminal
		// only when the manifest declares a FAILED edge that can carry the run
		// forward; an unhandled root failure blocks the plan.
		handled := false
		for _, edge := range e.Manifest.Edges {
			if edge.FromNode == n.NodeID && edge.Predicate().Value == NodeFailed {
				handled = true
				break
			}
		}
		if !handled {
			return false
		}
	}
	return true
}
func (e *Executor) hasFailure(s DAGState) bool {
	for _, state := range s.NodeStates {
		if state == NodeFailed || state == NodeBlocked {
			return true
		}
	}
	return false
}
func (e *Executor) acquire(node, run, leaseID string) bool {
	ok, _ := e.Store.AcquireLease("dag:"+e.Manifest.ManifestID+":"+node, run, leaseID, time.Minute)
	return ok
}
func (e *Executor) release(leaseID, run string) { _, _ = e.Store.ReleaseLease(leaseID, run) }
func (e *Executor) setAttempt(s *DAGState, id, state, code, message string) {
	for i := range s.Attempts {
		if s.Attempts[i].AttemptID == id {
			s.Attempts[i].State = state
			s.Attempts[i].ErrorCode = code
			s.Attempts[i].EndedAt = h.UTCNow()
			if message != "" {
				s.Failure = map[string]any{"message": message}
			}
		}
	}
}
func (e *Executor) inputArtifacts(id string, s DAGState) []Artifact {
	required := e.Manifest.RequiredInputs(id, s.NodeStates)
	out := []Artifact{}
	for _, contract := range required {
		for _, a := range s.Artifacts {
			if a.ContractID == contract {
				out = append(out, a)
			}
		}
	}
	return out
}
func (e *Executor) accept(node Node, result NodeExecutionResult) bool {
	if !isSuccess(result.Status) {
		return false
	}
	if len(result.Evidence) == 0 {
		return false
	}
	for _, item := range result.Evidence {
		if verdict := strings.ToUpper(stringValue(item["verdict"])); verdict != "PASS" && verdict != "PASSED" && verdict != "APPROVED" {
			return false
		}
	}
	required := map[string]bool{}
	for _, c := range node.OutputContracts {
		if c.Required {
			required[c.ContractID] = true
		}
	}
	for _, raw := range result.Artifacts {
		if id, ok := raw["contract_id"].(string); ok {
			delete(required, id)
		}
	}
	return len(required) == 0
}
func (e *Executor) materializeArtifact(node Node, attempt Attempt, raw map[string]any) Artifact {
	contract, _ := raw["contract_id"].(string)
	content, _ := raw["content"].(map[string]any)
	digest, _ := h.Digest(content)
	return Artifact{ArtifactID: "ART-" + attempt.AttemptID + "-" + contract, ContractID: contract, NodeID: node.NodeID, AttemptID: attempt.AttemptID, SHA256: digest, MediaType: stringValue(raw["media_type"]), Content: content}
}
func (e *Executor) materializeEvidence(node Node, attempt Attempt, raw map[string]any, artifacts []Artifact) Evidence {
	ev := Evidence{EvidenceID: h.NewID("EVID"), NodeID: node.NodeID, AttemptID: attempt.AttemptID, Verdict: stringValue(raw["verdict"]), Method: stringValue(raw["method"]), Observed: stringValue(raw["observed"])}
	if ev.Verdict == "" {
		ev.Verdict = "PASS"
	}
	if len(artifacts) > 0 {
		ev.ArtifactID = artifacts[len(artifacts)-1].ArtifactID
	}
	ev.CriterionID = stringValue(raw["criterion_id"])
	return ev
}
func stringValue(v any) string { s, _ := v.(string); return s }

func (e *Executor) ResumeUser(runID, outcome, actor string) (Report, error) {
	e.mu.Lock()
	s, task, err := e.load(runID)
	if err != nil {
		e.mu.Unlock()
		return Report{}, err
	}
	if s.Status != "WAITING_USER" {
		e.mu.Unlock()
		return e.report(s), errors.New("RUN_NOT_WAITING_USER")
	}
	nodeID := s.WaitingNodeID
	node, _ := e.Manifest.Node(nodeID)
	s.UserOutcomes[nodeID] = strings.ToUpper(outcome)
	if strings.ToUpper(outcome) != "PASS" && strings.ToUpper(outcome) != "PASSED" && strings.ToUpper(outcome) != "APPROVED" {
		s.Status = "FAILED"
		s.NodeStates[nodeID] = NodeFailed
		s.Failure = map[string]any{"code": "USER_REJECTED", "actor": actor}
		if err := e.persist(s, task, "USER_GATE_REJECTED", s.Failure, Options{}); err != nil {
			e.mu.Unlock()
			return e.report(s), err
		}
		e.mu.Unlock()
		return e.report(s), nil
	}
	s.NodeStates[nodeID] = NodeSucceeded
	s.CompletedOrder = append(s.CompletedOrder, nodeID)
	s.WaitingNodeID = ""
	s.Artifacts = append(s.Artifacts, Artifact{ArtifactID: "ART-" + runID + "-" + nodeID + "-UAT", ContractID: firstContract(node), NodeID: nodeID, AttemptID: "USER-" + actor, SHA256: mustDigest(map[string]any{"outcome": outcome}), MediaType: "application/json", Content: map[string]any{"outcome": outcome, "actor": actor}})
	s.Evidence = append(s.Evidence, Evidence{EvidenceID: h.NewID("EVID"), NodeID: nodeID, AttemptID: "USER-" + actor, Verdict: "PASS", Method: "user-acceptance", Observed: outcome})
	s.Status = "RUNNING"
	if err := e.persist(s, task, "USER_GATE_ACCEPTED", map[string]any{"node_id": nodeID, "outcome": outcome, "actor": actor}, Options{}); err != nil {
		e.mu.Unlock()
		return e.report(s), err
	}
	e.mu.Unlock()
	return e.Execute(runID, Options{})
}
func firstContract(node Node) string {
	if len(node.OutputContracts) > 0 {
		return node.OutputContracts[0].ContractID
	}
	return node.NodeID
}
func mustDigest(v any) string { d, _ := h.Digest(v); return d }
func (e *Executor) Cancel(runID, reason string) (Report, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, task, err := e.load(runID)
	if err != nil {
		return Report{}, err
	}
	s.Status = "CANCELLED"
	for id, state := range s.NodeStates {
		if state != NodeSucceeded && state != NodeFailed && state != NodeBlocked {
			s.NodeStates[id] = NodeCancelled
		}
	}
	s.Failure = map[string]any{"code": "CANCELLED", "reason": reason}
	if err := e.persist(s, task, "DAG_CANCELLED", s.Failure, Options{}); err != nil {
		return e.report(s), err
	}
	return e.report(s), nil
}
func (e *Executor) Replay(runID string) ([]h.Event, error) { return e.Store.Replay(runID) }

// Report reads an existing run without creating or advancing it.
func (e *Executor) Report(runID string) (Report, error) {
	s, _, err := e.load(runID)
	if err != nil {
		return Report{}, err
	}
	return e.report(s), nil
}

func (e *Executor) report(s DAGState) Report {
	events, _ := e.Store.Events(s.RunID)
	return Report{RunID: s.RunID, TaskID: s.TaskID, Status: s.Status, NodeStates: copyStates(s.NodeStates), Attempts: append([]Attempt(nil), s.Attempts...), Artifacts: append([]Artifact(nil), s.Artifacts...), Evidence: append([]Evidence(nil), s.Evidence...), CompletedOrder: append([]string(nil), s.CompletedOrder...), WaitingNodeID: s.WaitingNodeID, Failure: s.Failure, Events: events}
}
func copyStates(v map[string]string) map[string]string {
	out := map[string]string{}
	for k, x := range v {
		out[k] = x
	}
	return out
}
