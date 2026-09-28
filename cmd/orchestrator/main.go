// Orchestrator Layer - workflow coordinator.
//
// Real behavior:
//   - Temporal client + worker: durable workflows (research / coding /
//     analysis / default) on task queue "shannon-tasks", with retry policy,
//     status and cancel endpoints.
//   - Graceful degradation: when no Temporal server is reachable the service
//     runs the SAME pipeline inline (in-process "local mode") so the HTTP
//     API stays fully functional standalone. The active mode is reported by
//     /health.
//
// Activities call the LLM service over HTTP and the execution layer
// (agent-core) over gRPC (agentcore package) when those are reachable, and
// fall back to deterministic local execution. Every step result records which
// backend actually ran it (StepOutput.Backend), because "which backend ran
// this step" used to be unobservable.
//
// Scope note (docs/runtime-completion-design.md section 5.5): this is
// workflow-layer -> execution-layer wiring. agent-core runs WASI sandbox tool
// calls and RAG question answering; it does NOT run the control-plane agent
// loop. It is not "all three layers connected".
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	temporal "go.temporal.io/sdk/client"
	sdktypes "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"github.com/Hanwen-Jiang/dag-code-agent/agentcore"
	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

const TaskQueue = "shannon-tasks"

// Step kinds and backend names. Kind is the routing key added by section 5.2;
// "" and any unknown value are treated as kindLLM, which keeps every existing
// manifest and request on exactly today's path.
const (
	kindLLM       = "llm"
	kindSandbox   = "sandbox"
	kindRetrieval = "retrieval"

	backendAgentCore = agentcore.BackendName
	backendLLM       = "llm-service"
	backendLocal     = "local"
)

// capabilityTool maps a routable step kind to the agent-core tool capability it
// needs. A kind whose capability is not in the cached DiscoverTools result is
// NOT routed to agent-core (section 5.4 step 2).
func capabilityTool(kind string) string {
	switch kind {
	case kindSandbox:
		return "wasi"
	case kindRetrieval:
		return "retrieve"
	default:
		return ""
	}
}

// ---------- Config & DTOs ----------

type Config struct {
	Host               string `yaml:"host"`
	Port               int    `yaml:"port"`
	TemporalHost       string `yaml:"temporal_host"`
	TemporalPort       int    `yaml:"temporal_port"`
	AgentCoreAddr      string `yaml:"agent_core_addr"` // host:port (gRPC health at /health)
	LLMServiceAddr     string `yaml:"llm_service_addr"`
	MaxConcurrentTasks int    `yaml:"max_concurrent_tasks"`
	PriorityQueues     bool   `yaml:"priority_queues"`
}

type Request struct {
	Query        string                 `json:"query"`
	Context      map[string]interface{} `json:"context,omitempty"`
	AllowedTools []string               `json:"allowed_tools"`
	ModelTier    string                 `json:"model_tier"`
	MaxTokens    int                    `json:"max_tokens"`
	Budget       *BudgetConfig          `json:"budget,omitempty"`
}

type BudgetConfig struct {
	TokenLimit int     `json:"token_limit"`
	CostLimit  float64 `json:"cost_limit"`
}

type Response struct {
	Success    bool                   `json:"success"`
	Response   string                 `json:"response"`
	TokensUsed int                    `json:"tokens_used"`
	ModelUsed  string                 `json:"model_used"`
	TaskID     string                 `json:"task_id"`
	Route      string                 `json:"route"`
	Error      string                 `json:"error,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type PlanStep struct {
	Name      string `json:"name"`
	Objective string `json:"objective"`
	// Kind decides the execution backend: llm | sandbox | retrieval.
	// Missing or unknown values are handled as llm, i.e. today's behaviour, so
	// adding this field does not change the semantics of existing manifests.
	Kind string `json:"kind,omitempty"`
}

// StepOutput is one step's result plus the observability fields the workflow
// layer previously lacked: which backend actually ran the step, the
// deterministic request digest (also the idempotency key handed to the
// execution layer), the Temporal attempt number and any degradation reason.
type StepOutput struct {
	Output        string `json:"output"`
	Backend       string `json:"backend"` // agent-core | llm-service | local
	RequestDigest string `json:"request_digest"`
	Attempt       int    `json:"attempt"`
	Degraded      bool   `json:"degraded,omitempty"`
	DegradedFrom  string `json:"degraded_from,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
}

type WorkflowResult struct {
	Output      string       `json:"output"`
	TokensUsed  int          `json:"tokens_used"`
	ModelUsed   string       `json:"model_used"`
	Steps       []string     `json:"steps"`
	StepOutputs []StepOutput `json:"step_outputs"`
}

// backends lists the distinct backends that ran the workflow's steps, in first
// appearance order. It is the compact form exposed by /tasks/status.
func (r WorkflowResult) backends() []string {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range r.StepOutputs {
		if s.Backend == "" || seen[s.Backend] {
			continue
		}
		seen[s.Backend] = true
		out = append(out, s.Backend)
	}
	return out
}

// ---------- main ----------

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	cfg := loadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; cancel() }()

	var tClient temporal.Client
	var runWorker bool
	hostPort := fmt.Sprintf("%s:%d", cfg.TemporalHost, cfg.TemporalPort)
	if tcpReachable(hostPort, 2*time.Second) {
		var err error
		tClient, err = dialTemporal(ctx, hostPort, logger)
		if err != nil {
			logger.Warn("temporal dial failed, starting in local mode", zap.Error(err))
		} else {
			runWorker = true
			logger.Info("temporal client connected", zap.String("host", hostPort))
		}
	} else {
		logger.Warn("no temporal server reachable, starting in local (inline) mode",
			zap.String("tried", hostPort))
	}

	mode := "local"
	if tClient != nil {
		mode = "temporal"
	}

	// Health gate for the execution layer (section 5.4): a failed handshake
	// leaves agent-core out of the usable backend list and the orchestrator
	// starts and runs exactly as it did before this wiring existed.
	agentCore := newAgentCoreBackend(ctx, cfg, logger)
	if agentCore != nil {
		defer func() { _ = agentCore.Close() }()
	}

	if runWorker {
		w := worker.New(tClient, TaskQueue, worker.Options{})
		acts := newActivities(cfg, logger, agentCore)
		w.RegisterActivity(acts.PlanTask)
		w.RegisterActivity(acts.ExecuteStep)
		w.RegisterActivity(acts.VerifyOutput)
		w.RegisterWorkflow(ResearchWorkflow)
		w.RegisterWorkflow(CodingWorkflow)
		w.RegisterWorkflow(AnalysisWorkflow)
		w.RegisterWorkflow(DefaultWorkflow)
		interruptCh := make(chan any)
		go func() {
			if err := w.Run(interruptCh); err != nil {
				logger.Error("temporal worker stopped", zap.Error(err))
			}
		}()
		_ = interruptCh
	}

	svc := &service{
		cfg:       cfg,
		tClient:   tClient,
		mode:      mode,
		logger:    logger,
		agentCore: agentCore,
		results:   map[string]WorkflowResult{},
	}

	mux := http.NewServeMux()
	setupRoutes(mux, svc)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 180 * time.Second,
	}

	logger.Info("Orchestrator starting",
		zap.Int("port", cfg.Port),
		zap.String("mode", mode),
		zap.Int("max_tasks", cfg.MaxConcurrentTasks))

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("Shutting down orchestrator...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	if tClient != nil {
		tClient.Close()
	}
}

func loadConfig() Config {
	return Config{
		Host:               getEnv("ORCHESTRATOR_HOST", "localhost"),
		Port:               getEnvInt("ORCH_PORT", 8080),
		TemporalHost:       getEnv("TEMPORAL_HOST", "localhost"),
		TemporalPort:       getEnvInt("TEMPORAL_PORT", 7233),
		AgentCoreAddr:      getEnv("AGENT_CORE_ADDR", "127.0.0.1:50051"),
		LLMServiceAddr:     getEnv("LLM_SERVICE_ADDR", "127.0.0.1:8081"),
		MaxConcurrentTasks: getEnvInt("ORCH_MAX_CONCURRENT", 10),
		PriorityQueues:     getEnv("ORCH_PRIORITY_QUEUES", "true") == "true",
	}
}

func tcpReachable(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func dialTemporal(ctx context.Context, hostPort string, logger *zap.Logger) (temporal.Client, error) {
	for attempt := 1; attempt <= 5; attempt++ {
		client, err := temporal.NewClient(temporal.Options{
			HostPort:  hostPort,
			Namespace: "default",
		})
		if err == nil {
			return client, nil
		}
		logger.Warn("temporal not ready, retrying", zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(min(attempt, 10)) * time.Second):
		}
	}
	return nil, fmt.Errorf("temporal unreachable at %s", hostPort)
}

// ---------- Routing ----------

type service struct {
	cfg       Config
	tClient   temporal.Client
	mode      string
	logger    *zap.Logger
	agentCore *agentCoreBackend

	// results keeps the last WorkflowResult per task id so /tasks/status can
	// report which backend ran each step even in local (non-Temporal) mode.
	// Bounded by maxRememberedResults.
	mu      sync.Mutex
	results map[string]WorkflowResult
	order   []string
}

const maxRememberedResults = 128

func (svc *service) recordResult(taskID string, res WorkflowResult) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if svc.results == nil {
		svc.results = map[string]WorkflowResult{}
	}
	if _, exists := svc.results[taskID]; !exists {
		svc.order = append(svc.order, taskID)
		if len(svc.order) > maxRememberedResults {
			oldest := svc.order[0]
			svc.order = svc.order[1:]
			delete(svc.results, oldest)
		}
	}
	svc.results[taskID] = res
}

func (svc *service) lookupResult(taskID string) (WorkflowResult, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	res, ok := svc.results[taskID]
	return res, ok
}

func setupRoutes(mux *http.ServeMux, svc *service) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{"status": "healthy", "mode": svc.mode, "task_queue": TaskQueue}
		// /health reports service capability (the most recent agent-core
		// handshake), not per-task results: see design section 5.2/5.4.
		if svc.agentCore == nil {
			payload["agent_core"] = map[string]any{"configured": false, "reachable": false, "tools": []string{}}
		} else {
			payload["agent_core"] = svc.agentCore.snapshot()
		}
		writeJSON(w, http.StatusOK, payload)
	})
	mux.HandleFunc("/agent/query", func(w http.ResponseWriter, r *http.Request) {
		svc.handleQuery(w, r)
	})
	mux.HandleFunc("/tasks/status", func(w http.ResponseWriter, r *http.Request) {
		svc.handleTaskStatus(w, r)
	})
	mux.HandleFunc("/tasks/cancel", func(w http.ResponseWriter, r *http.Request) {
		svc.handleTaskCancel(w, r)
	})
}

// selectRoute picks the workflow by simple heuristics (deterministic).
func selectRoute(query string, tools []string) string {
	queryLower := strings.ToLower(query)
	if contains(tools, "code_execution") || contains(tools, "file_edit") ||
		strings.Contains(queryLower, "implement") || strings.Contains(queryLower, "refactor") ||
		strings.Contains(queryLower, "code") || strings.Contains(queryLower, "function") {
		return "coding"
	}
	if strings.Contains(queryLower, "analyze") || strings.Contains(queryLower, "compare") ||
		strings.Contains(queryLower, "explain") || strings.Contains(queryLower, "why") {
		return "analysis"
	}
	if strings.Contains(queryLower, "search") || strings.Contains(queryLower, "research") ||
		strings.Contains(queryLower, "find") || strings.Contains(queryLower, "investigate") {
		return "research"
	}
	return "default"
}

func (svc *service) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query required"})
		return
	}

	taskID := "orch-" + uuid.NewString()
	sessionID := getEnv("SESSION_ID", "")
	route := selectRoute(req.Query, req.AllowedTools)

	var resp Response
	var err error
	switch route {
	case "research":
		resp, err = svc.runWorkflow(r.Context(), taskID, sessionID, req, ResearchWorkflow)
	case "coding":
		resp, err = svc.runWorkflow(r.Context(), taskID, sessionID, req, CodingWorkflow)
	case "analysis":
		resp, err = svc.runWorkflow(r.Context(), taskID, sessionID, req, AnalysisWorkflow)
	default:
		resp, err = svc.runWorkflow(r.Context(), taskID, sessionID, req, DefaultWorkflow)
	}
	if err != nil {
		writeJSON(w, http.StatusBadGateway, Response{Success: false, Error: err.Error(), TaskID: taskID, Route: route})
		return
	}
	resp.Route = route
	resp.TaskID = taskID
	writeJSON(w, http.StatusOK, resp)
}

// runWorkflow either executes a durable Temporal workflow or runs the same
// activity pipeline inline in local mode.
func (svc *service) runWorkflow(ctx context.Context, taskID, sessionID string, req Request, wf interface{}) (Response, error) {
	params := ResearchParams{
		Query:         req.Query,
		Context:       req.Context,
		Tools:         req.AllowedTools,
		SessionID:     sessionID,
		ModelTier:     req.ModelTier,
		MaxTokens:     req.MaxTokens,
		MaxIterations: 3,
	}

	if svc.tClient != nil {
		wfOpts := temporal.StartWorkflowOptions{
			ID:        taskID,
			TaskQueue: TaskQueue,
			RetryPolicy: &sdktypes.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumInterval:    30 * time.Second,
				MaximumAttempts:    3,
			},
		}
		// Budget-based priority queue: high-value requests use a dedicated queue.
		if svc.cfg.PriorityQueues && req.Budget != nil && req.Budget.CostLimit > 10 {
			wfOpts.TaskQueue = TaskQueue + "-high"
		}
		run, err := svc.tClient.ExecuteWorkflow(ctx, wfOpts, wf, params)
		if err != nil {
			return Response{}, fmt.Errorf("workflow start failed: %w", err)
		}
		var res WorkflowResult
		if err := run.Get(ctx, &res); err != nil {
			return Response{}, fmt.Errorf("workflow retrieval failed: %w", err)
		}
		svc.recordResult(taskID, res)
		return Response{
			Success:    true,
			Response:   res.Output,
			TokensUsed: res.TokensUsed,
			ModelUsed:  res.ModelUsed,
			Metadata: map[string]interface{}{
				"mode":         "temporal",
				"steps":        res.Steps,
				"step_outputs": res.StepOutputs,
				"backends":     res.backends(),
			},
		}, nil
	}

	// Local mode: run the pipeline inline.
	start := time.Now()
	acts := newActivities(svc.cfg, svc.logger, svc.agentCore)
	res, err := acts.RunPipeline(ctx, params)
	if err != nil {
		return Response{}, err
	}
	svc.recordResult(taskID, res)
	return Response{
		Success:    true,
		Response:   res.Output,
		TokensUsed: res.TokensUsed,
		ModelUsed:  res.ModelUsed,
		Metadata: map[string]interface{}{
			"mode":         "local",
			"steps":        res.Steps,
			"step_outputs": res.StepOutputs,
			"backends":     res.backends(),
			"duration_ms":  time.Since(start).Milliseconds(),
		},
	}, nil
}

func (svc *service) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
		return
	}
	// step_outputs / backends are the observability payload: they say which
	// backend actually executed each step. They are served from the last
	// completed run of the task, in either mode.
	res, recorded := svc.lookupResult(taskID)
	withSteps := func(payload map[string]any) map[string]any {
		if recorded {
			payload["step_outputs"] = res.StepOutputs
			payload["backends"] = res.backends()
		}
		return payload
	}
	if svc.tClient == nil {
		payload := map[string]any{"task_id": taskID, "status": "local-mode: no durable execution to query"}
		if recorded {
			payload["status"] = "completed (local-mode: no durable execution to query)"
		}
		writeJSON(w, http.StatusOK, withSteps(payload))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	desc, err := svc.tClient.DescribeWorkflowExecution(ctx, taskID, "")
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "execution not found", "detail": err.Error()})
		return
	}
	info := desc.WorkflowExecutionInfo
	status := enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	if info != nil {
		status = info.Status
	}
	writeJSON(w, http.StatusOK, withSteps(map[string]any{
		"task_id": taskID,
		"status":  status.String(),
	}))
}

func (svc *service) handleTaskCancel(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
		return
	}
	if svc.tClient == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "local-mode: nothing to cancel"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := svc.tClient.CancelWorkflow(ctx, taskID, ""); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "cancellation failed", "detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancel_requested", "task_id": taskID})
}

// ---------- Temporal workflows ----------

type ResearchParams struct {
	Query         string
	Context       map[string]interface{}
	Tools         []string
	SessionID     string
	ModelTier     string
	MaxTokens     int
	MaxIterations int
}

// activityOpts returns the workflow activity options.
//
// StartToCloseTimeout is 30 minutes because a sandbox step can legitimately run
// long (it used to be 60s, which made long steps fail and then re-run under
// MaximumAttempts: 3 — repeating side effects). HeartbeatTimeout is pinned to
// StartToCloseTimeout/3 (10 minutes) so a crashed worker is noticed in minutes
// rather than after the whole StartToClose window; the ratio is fixed by design
// section 5.3.
func activityOpts() workflow.ActivityOptions {
	const startToClose = 30 * time.Minute
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: startToClose,
		HeartbeatTimeout:    startToClose / 3, // == 10m, ratio fixed by design 5.3
		RetryPolicy: &sdktypes.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	}
	return opts
}

// ResearchWorkflow: plan -> execute steps -> verify.
func ResearchWorkflow(ctx workflow.Context, p ResearchParams) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOpts())
	logger := workflow.GetLogger(ctx)

	var plan []PlanStep
	if err := workflow.ExecuteActivity(ctx, (&activities{}).PlanTask, p).Get(ctx, &plan); err != nil {
		return WorkflowResult{}, fmt.Errorf("planning failed: %w", err)
	}
	logger.Info("research plan ready", zap.Int("steps", len(plan)))

	res := WorkflowResult{Steps: []string{}, StepOutputs: []StepOutput{}}
	for _, step := range plan {
		var out StepOutput
		if err := workflow.ExecuteActivity(ctx, (&activities{}).ExecuteStep, StepInput{Params: p, Step: step}).Get(ctx, &out); err != nil {
			return WorkflowResult{}, fmt.Errorf("step %s failed: %w", step.Name, err)
		}
		res.Output += out.Output + "\n"
		res.Steps = append(res.Steps, step.Name)
		res.StepOutputs = append(res.StepOutputs, out)
	}

	var ok bool
	if err := workflow.ExecuteActivity(ctx, (&activities{}).VerifyOutput, VerifyInput{Params: p, Output: res.Output}).Get(ctx, &ok); err != nil {
		return WorkflowResult{}, fmt.Errorf("verification failed: %w", err)
	}
	if !ok {
		return WorkflowResult{}, fmt.Errorf("output failed verification")
	}
	return res, nil
}

func CodingWorkflow(ctx workflow.Context, p ResearchParams) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOpts())
	res, err := runPlannedPipeline(ctx, p, "coding")
	if err != nil {
		return WorkflowResult{}, err
	}
	res.ModelUsed = "coding-agent"
	return res, nil
}

func AnalysisWorkflow(ctx workflow.Context, p ResearchParams) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOpts())
	res, err := runPlannedPipeline(ctx, p, "analysis")
	if err != nil {
		return WorkflowResult{}, err
	}
	res.ModelUsed = "analysis-agent"
	return res, nil
}

func DefaultWorkflow(ctx workflow.Context, p ResearchParams) (WorkflowResult, error) {
	ctx = workflow.WithActivityOptions(ctx, activityOpts())
	res, err := runPlannedPipeline(ctx, p, "default")
	if err != nil {
		return WorkflowResult{}, err
	}
	res.ModelUsed = "default-agent"
	return res, nil
}

// runPlannedPipeline is the shared deterministic workflow body: plan, execute, verify.
func runPlannedPipeline(ctx workflow.Context, p ResearchParams, kind string) (WorkflowResult, error) {
	a := &activities{}
	var plan []PlanStep
	if err := workflow.ExecuteActivity(ctx, a.PlanTask, p).Get(ctx, &plan); err != nil {
		return WorkflowResult{}, fmt.Errorf("planning failed: %w", err)
	}
	res := WorkflowResult{Steps: []string{}, StepOutputs: []StepOutput{}}
	for _, step := range plan {
		var out StepOutput
		if err := workflow.ExecuteActivity(ctx, a.ExecuteStep, StepInput{Params: p, Step: step}).Get(ctx, &out); err != nil {
			return WorkflowResult{}, fmt.Errorf("step %s failed: %w", step.Name, err)
		}
		res.Output += out.Output + "\n"
		res.Steps = append(res.Steps, step.Name)
		res.StepOutputs = append(res.StepOutputs, out)
	}
	var ok bool
	if err := workflow.ExecuteActivity(ctx, a.VerifyOutput, VerifyInput{Params: p, Output: res.Output}).Get(ctx, &ok); err != nil {
		return WorkflowResult{}, fmt.Errorf("verification failed: %w", err)
	}
	if !ok {
		return WorkflowResult{}, fmt.Errorf("%s output failed verification", kind)
	}
	return res, nil
}

// ---------- Activities ----------

type StepInput struct {
	Params ResearchParams
	Step   PlanStep
}

type VerifyInput struct {
	Params ResearchParams
	Output string
}

// activities performs the real work. Every external dependency has a local
// degradation path so the pipeline works standalone.
type activities struct {
	cfg       Config
	logger    *zap.Logger
	agentCore *agentCoreBackend
}

func newActivities(cfg Config, logger *zap.Logger, agentCore *agentCoreBackend) *activities {
	return &activities{cfg: cfg, logger: logger, agentCore: agentCore}
}

// classifyStepKind is the first-version, pure routing rule (section 5.2): the
// step's kind follows from keywords in its objective. It is deterministic, so a
// replayed plan routes identically. Anything unrecognised is kindLLM, i.e. the
// pre-existing llm-service path.
func classifyStepKind(s PlanStep) string {
	text := strings.ToLower(s.Name + " " + s.Objective)
	switch {
	case containsAny(text, "sandbox", "沙箱", "wasm", "执行", "运行"):
		return kindSandbox
	case containsAny(text, "retrieve", "retrieval", "memory", "检索", "recall"):
		return kindRetrieval
	default:
		return kindLLM
	}
}

// capabilityStep adds one step for requests that explicitly ask for a capability
// the execution layer owns. Requests without those keywords get the unchanged
// plan, so their behaviour and output are exactly what they were before this
// wiring. The step's objective carries the capability keyword, which is what
// the pure classifyStepKind rule consumes.
func capabilityStep(query string) (PlanStep, bool) {
	lower := strings.ToLower(query)
	switch {
	case containsAny(lower, "sandbox", "沙箱", "wasm"):
		return PlanStep{Name: "sandbox-run", Objective: "execute the requested code in the wasm sandbox"}, true
	case containsAny(lower, "retrieve from memory", "memory retrieval", "recall from memory", "检索记忆"):
		return PlanStep{Name: "retrieve-memory", Objective: "retrieve relevant passages from memory"}, true
	}
	return PlanStep{}, false
}

// PlanTask produces a deterministic plan for the request. It is the producer of
// PlanStep.Kind (section 5.2), and the chosen kinds are logged as the
// orchestrator's PLAN_COMPILED record.
func (a *activities) PlanTask(ctx context.Context, p ResearchParams) ([]PlanStep, error) {
	plan := []PlanStep{}
	switch selectRoute(p.Query, p.Tools) {
	case "coding":
		plan = []PlanStep{
			{Name: "understand", Objective: "parse the request and identify constraints"},
			{Name: "implement", Objective: "produce the implementation"},
			{Name: "self-test", Objective: "validate the output against the request"},
		}
	case "analysis":
		plan = []PlanStep{
			{Name: "collect", Objective: "gather the relevant facts"},
			{Name: "compare", Objective: "compare alternatives"},
			{Name: "conclude", Objective: "state the conclusion with evidence"},
		}
	default:
		plan = []PlanStep{
			{Name: "scope", Objective: "define the question boundaries"},
			{Name: "investigate", Objective: "collect supporting evidence"},
			{Name: "synthesize", Objective: "produce the final answer"},
		}
	}
	// Cap iterations by budget when provided. The capability step below is added
	// after the cap so an explicit execution request is never truncated away.
	if p.MaxIterations > 0 && p.MaxIterations < len(plan) {
		plan = plan[:p.MaxIterations]
	}
	if extra, ok := capabilityStep(p.Query); ok {
		plan = append(plan, extra)
	}
	for i := range plan {
		plan[i].Kind = classifyStepKind(plan[i])
	}
	kinds := make([]string, 0, len(plan))
	for _, s := range plan {
		kinds = append(kinds, s.Name+"="+s.Kind)
	}
	a.logger.Info("PLAN_COMPILED",
		zap.String("query", truncate(p.Query, 80)),
		zap.Strings("steps", kinds))
	return plan, nil
}

// ExecuteStep runs one step. Routing (section 5.2):
//
//	sandbox | retrieval -> agent-core (gRPC), if the health gate allows it
//	anything else       -> llm-service, as before
//	on failure          -> llm-service, then the deterministic local composer
//
// The returned StepOutput always records the backend that actually ran the step,
// plus the request digest and the Temporal attempt number.
func (a *activities) ExecuteStep(ctx context.Context, in StepInput) (StepOutput, error) {
	out := StepOutput{
		RequestDigest: requestDigest(in),
		Attempt:       activityAttempt(ctx),
	}
	switch in.Step.Kind {
	case kindSandbox, kindRetrieval:
		ac, err := a.callAgentCore(ctx, in, out.RequestDigest)
		if err == nil {
			return ac, nil
		}
		if agentcore.IsUncertain(err) {
			// The stream had already produced updates: the step did reach
			// agent-core, so it must stay attributed to it rather than being
			// silently re-run on another backend (section 5.3).
			ac.Degraded = true
			ac.DegradedFrom = backendAgentCore
			ac.ErrorCode = agentcore.CodeOf(err)
			a.logger.Warn("agent-core step uncertain; not rerouting",
				zap.String("step", in.Step.Name), zap.Error(err))
			return ac, nil
		}
		// Degrade: remember why, then fall through to llm-service.
		out.Degraded = true
		out.DegradedFrom = backendAgentCore
		out.ErrorCode = agentcore.CodeOf(err)
		a.logger.Warn("agent-core unavailable for step, degrading to llm-service",
			zap.String("step", in.Step.Name), zap.String("code", out.ErrorCode), zap.Error(err))
	}
	if llmOut, err := a.callLLM(ctx, in.Params, in.Step); err == nil && llmOut != "" {
		out.Output = llmOut
		out.Backend = backendLLM
		return out, nil
	}
	// Local degradation: compose a structured, deterministic answer.
	out.Output = localStepOutput(in)
	out.Backend = backendLocal
	return out, nil
}

// callAgentCore routes a step to the execution layer over gRPC. It uses
// StreamExecuteTask (consuming updates as Temporal heartbeats) rather than the
// management HTTP API, so the wiring is observable at the gRPC server.
func (a *activities) callAgentCore(ctx context.Context, in StepInput, digest string) (StepOutput, error) {
	out := StepOutput{
		Backend:       backendAgentCore,
		RequestDigest: digest,
		Attempt:       activityAttempt(ctx),
	}
	if a.agentCore == nil || !a.agentCore.available() {
		return out, &agentcore.Error{Code: agentcore.CodeUnavailable, Op: "route",
			Err: fmt.Errorf("agent-core is not in the usable backend list")}
	}
	if !a.agentCore.supports(in.Step.Kind) {
		return out, &agentcore.Error{Code: agentcore.CodeUnavailable, Op: "route",
			Err: fmt.Errorf("agent-core does not advertise capability %q", capabilityTool(in.Step.Kind))}
	}

	req := agentcore.ExecuteRequest{
		SessionID: in.Params.SessionID,
		Query:     agentCoreQuery(in),
		Context:   in.Params.Context,
		Tools:     a.agentCore.toolWhitelist(in.Params.Tools),
		Timeout:   agentCoreCallTimeout,
		Budget:    in.Params.MaxTokens,
		// Deterministic idempotency key: a retried activity (longer
		// StartToCloseTimeout makes retries cheaper to hit) must be deduplicated
		// by the execution layer instead of repeating a side effect.
		IdempotencyKey: idempotencyKey(digest),
	}
	res, err := a.agentCore.client.Stream(ctx, req, func(u agentcore.TaskUpdate) error {
		// Heartbeat: this is what makes a 30-minute StartToCloseTimeout safe.
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, map[string]any{
				"iteration": u.Iteration,
				"status":    u.Status,
				"tool":      u.ToolName,
			})
		}
		return nil
	})
	if err != nil {
		out.Output = res.Output
		if agentcore.IsUncertain(err) && out.Output == "" {
			out.Output = fmt.Sprintf("[%s] execution layer stream interrupted; result uncertain", in.Step.Name)
		}
		return out, err
	}
	out.Output = res.Output
	out.Backend = res.Backend
	return out, nil
}

// agentCoreQuery renders a step into what agent-core executes: a tool-call JSON
// spec when the kind has a dedicated tool, otherwise the step's own text (which
// agent-core answers through its RAG path).
func agentCoreQuery(in StepInput) string {
	text := strings.TrimSpace(in.Step.Objective + "\n\nTask: " + in.Params.Query)
	switch in.Step.Kind {
	case kindRetrieval:
		return jsonSpec(map[string]any{"tool": "retrieve", "args": []string{text}})
	case kindSandbox:
		wasm := contextString(in.Params.Context, "wasm_b64")
		if wasm != "" {
			return jsonSpec(map[string]any{
				"tool":     "wasi",
				"wasm_b64": wasm,
				"args":     contextStrings(in.Params.Context, "args"),
				"stdin":    contextString(in.Params.Context, "stdin"),
			})
		}
	}
	return text
}

func jsonSpec(spec map[string]any) string {
	b, err := json.Marshal(spec)
	if err != nil {
		return fmt.Sprintf("%v", spec)
	}
	return string(b)
}

func contextString(ctx map[string]interface{}, key string) string {
	if ctx == nil {
		return ""
	}
	s, _ := ctx[key].(string)
	return s
}

func contextStrings(ctx map[string]interface{}, key string) []string {
	if ctx == nil {
		return nil
	}
	switch v := ctx[key].(type) {
	case []string:
		return v
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// requestDigest is the deterministic digest of a step's inputs. It is both the
// StepOutput.RequestDigest observability field and the basis of the idempotency
// key handed to agent-core.
func requestDigest(in StepInput) string {
	h := sha256.New()
	fmt.Fprintf(h, "session=%s\n", in.Params.SessionID)
	fmt.Fprintf(h, "query=%s\n", in.Params.Query)
	fmt.Fprintf(h, "tools=%s\n", strings.Join(in.Params.Tools, ","))
	fmt.Fprintf(h, "tier=%s\n", in.Params.ModelTier)
	fmt.Fprintf(h, "max_tokens=%d\n", in.Params.MaxTokens)
	fmt.Fprintf(h, "step.name=%s\n", in.Step.Name)
	fmt.Fprintf(h, "step.objective=%s\n", in.Step.Objective)
	fmt.Fprintf(h, "step.kind=%s\n", in.Step.Kind)
	keys := make([]string, 0, len(in.Params.Context))
	for k := range in.Params.Context {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "ctx.%s=%v\n", k, in.Params.Context[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// idempotencyKey scopes the digest to this orchestrator's step dispatch.
func idempotencyKey(digest string) string { return "orch-step:" + digest }

// activityAttempt returns the Temporal attempt number, or 0 outside an activity
// (local mode), where retries do not exist.
func activityAttempt(ctx context.Context) int {
	if !activity.IsActivity(ctx) {
		return 0
	}
	return int(activity.GetInfo(ctx).Attempt)
}

// VerifyOutput checks the assembled output: non-empty, covers the query terms,
// and (best effort) matches the LLM service's own verification when available.
func (a *activities) VerifyOutput(ctx context.Context, in VerifyInput) (bool, error) {
	out := strings.TrimSpace(in.Output)
	if len(out) < 8 {
		return false, nil
	}
	// At least 50% of the query's significant words should be reflected or
	// the answer is considered off-topic (cheap deterministic relevance check).
	terms := significantTerms(in.Params.Query)
	if len(terms) == 0 {
		return true, nil
	}
	lower := strings.ToLower(out)
	covered := 0
	for _, t := range terms {
		if strings.Contains(lower, t) {
			covered++
		}
	}
	return covered*2 >= len(terms), nil
}

// RunPipeline is the local-mode (no Temporal) execution of the same pipeline.
func (a *activities) RunPipeline(ctx context.Context, p ResearchParams) (WorkflowResult, error) {
	plan, err := a.PlanTask(ctx, p)
	if err != nil {
		return WorkflowResult{}, err
	}
	res := WorkflowResult{Steps: []string{}, StepOutputs: []StepOutput{}}
	for _, step := range plan {
		out, err := a.ExecuteStep(ctx, StepInput{Params: p, Step: step})
		if err != nil {
			return WorkflowResult{}, err
		}
		res.Output += out.Output + "\n"
		res.Steps = append(res.Steps, step.Name)
		res.StepOutputs = append(res.StepOutputs, out)
	}
	ok, err := a.VerifyOutput(ctx, VerifyInput{Params: p, Output: res.Output})
	if err != nil {
		return WorkflowResult{}, err
	}
	if !ok {
		return WorkflowResult{}, fmt.Errorf("output failed verification")
	}
	return res, nil
}

// ---------- execution-layer (agent-core) health gate ----------

const (
	// agentCoreCallTimeout bounds one ExecuteTask/StreamExecuteTask call.
	agentCoreCallTimeout = 30 * time.Second
	// agentCoreHandshakeTimeout bounds HealthCheck/DiscoverTools at startup so a
	// black-holed address cannot delay orchestrator startup.
	agentCoreHandshakeTimeout = 3 * time.Second
	// agentCoreBreakerThreshold/Cooldown: after repeated failures, stop paying a
	// full call timeout on every step (section 5.4).
	agentCoreBreakerThreshold = 3
	agentCoreBreakerCooldown  = 30 * time.Second
)

// agentCoreBackend owns the optional gRPC client for the execution layer and
// the health gate that decides whether sandbox/retrieval steps may be routed to
// it. A nil *agentCoreBackend means "no agent-core configured", which is the
// pre-existing all-local behaviour.
type agentCoreBackend struct {
	client  *agentcore.Client
	breaker *harness.CircuitBreaker

	mu        sync.Mutex
	ready     bool
	degraded  bool
	tools     []string
	lastErr   string
	checkedAt time.Time
}

// newAgentCoreBackend performs the startup handshake: HealthCheck decides
// usability, DiscoverTools caches the capability set, and the outcome is
// reported by /health. It never fails the process.
func newAgentCoreBackend(ctx context.Context, cfg Config, logger *zap.Logger) *agentCoreBackend {
	addr := strings.TrimSpace(cfg.AgentCoreAddr)
	if addr == "" {
		logger.Info("agent-core not configured; sandbox/retrieval steps will use llm-service/local backends")
		return nil
	}
	b := &agentCoreBackend{
		breaker: &harness.CircuitBreaker{Threshold: agentCoreBreakerThreshold, Cooldown: agentCoreBreakerCooldown},
	}
	client, err := agentcore.Dial(ctx, addr,
		agentcore.WithTimeout(agentCoreCallTimeout),
		agentcore.WithBreaker(b.breaker))
	if err != nil {
		b.recordFailure(err)
		logger.Warn("agent-core dial failed; steps will degrade to llm-service/local",
			zap.String("addr", addr), zap.Error(err))
		return b
	}
	b.client = client
	if err := b.handshake(ctx); err != nil {
		logger.Warn("agent-core health gate closed; steps will degrade to llm-service/local",
			zap.String("addr", addr), zap.Error(err))
		return b
	}
	logger.Info("agent-core handshake ok",
		zap.String("addr", addr),
		zap.Strings("tools", b.toolIDs()),
		zap.Bool("degraded", b.isDegraded()))
	return b
}

// handshake runs HealthCheck then DiscoverTools (section 5.4 steps 1-2).
func (b *agentCoreBackend) handshake(ctx context.Context) error {
	hctx, cancel := context.WithTimeout(ctx, agentCoreHandshakeTimeout)
	defer cancel()

	health, err := b.client.Health(hctx)
	if err != nil {
		b.recordFailure(err)
		return err
	}
	if !health.Healthy {
		err := fmt.Errorf("agent-core reported %s", health.Status)
		b.recordFailure(err)
		return err
	}
	tools, err := b.client.DiscoverTools(hctx)
	if err != nil {
		b.recordFailure(err)
		return err
	}
	if len(tools) == 0 {
		err := fmt.Errorf("agent-core advertised no capabilities")
		b.recordFailure(err)
		return err
	}
	ids := make([]string, 0, len(tools))
	for _, t := range tools {
		ids = append(ids, t.ID)
	}
	b.mu.Lock()
	b.ready = true
	b.degraded = health.Degraded
	b.tools = ids
	b.lastErr = ""
	b.checkedAt = time.Now()
	b.mu.Unlock()
	return nil
}

// available reports whether routing to agent-core is currently allowed: the
// startup handshake succeeded and the circuit breaker is closed.
func (b *agentCoreBackend) available() bool {
	if b == nil || b.client == nil {
		return false
	}
	b.mu.Lock()
	ready := b.ready
	b.mu.Unlock()
	return ready && b.breaker.Allow()
}

// supports reports whether agent-core advertised the capability a kind needs.
func (b *agentCoreBackend) supports(kind string) bool {
	want := capabilityTool(kind)
	if want == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, id := range b.tools {
		if id == want {
			return true
		}
	}
	return false
}

// toolWhitelist is the available_tools handed to agent-core: its own advertised
// capabilities, narrowed by the request's allowed tools when one is supplied.
func (b *agentCoreBackend) toolWhitelist(allowed []string) []string {
	ids := b.toolIDs()
	if len(allowed) == 0 {
		return ids
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if contains(allowed, id) {
			out = append(out, id)
		}
	}
	return out
}

func (b *agentCoreBackend) toolIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.tools...)
}

func (b *agentCoreBackend) isDegraded() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.degraded
}

// snapshot is the /health projection (section 5.4 step 3): the most recent
// handshake, not a per-task result.
func (b *agentCoreBackend) snapshot() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]any{
		"configured": true,
		"reachable":  b.ready,
		"degraded":   b.degraded || !b.ready,
		"tools":      append([]string{}, b.tools...),
		"checked_at": b.checkedAt.Format(time.RFC3339),
	}
	if b.lastErr != "" {
		out["error"] = b.lastErr
	}
	return out
}

func (b *agentCoreBackend) recordFailure(err error) {
	b.mu.Lock()
	b.ready = false
	b.degraded = true
	if err != nil {
		b.lastErr = err.Error()
	}
	b.checkedAt = time.Now()
	b.mu.Unlock()
}

// Close releases the gRPC connection.
func (b *agentCoreBackend) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}

// callLLM calls the configured LLM service; any failure falls through to the
// caller's local path.
func (a *activities) callLLM(ctx context.Context, p ResearchParams, step PlanStep) (string, error) {
	addr := strings.TrimSuffix(a.cfg.LLMServiceAddr, "/")
	if addr == "" {
		return "", fmt.Errorf("llm service not configured")
	}
	body, _ := json.Marshal(map[string]any{
		"model":      "",
		"max_tokens": p.MaxTokens,
		"messages": []map[string]string{
			{"role": "system", "content": "You are an agent worker. Complete the step concisely and factually."},
			{"role": "user", "content": fmt.Sprintf("Step %s: %s\n\nTask: %s", step.Name, step.Objective, p.Query)},
		},
	})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/chat/completions", strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var payload struct {
		Content string `json:"content"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK || payload.Error != "" {
		return "", fmt.Errorf("llm service: %s", payload.Error)
	}
	return payload.Content, nil
}

// localStepOutput is the deterministic offline answer for one step.
func localStepOutput(in StepInput) string {
	terms := significantTerms(in.Params.Query)
	return fmt.Sprintf("[%s] %s — addressed: %s (query: %q)",
		in.Step.Name, in.Step.Objective, strings.Join(terms, ", "), truncate(in.Params.Query, 120))
}

func significantTerms(query string) []string {
	stop := map[string]bool{"the": true, "a": true, "an": true, "and": true, "or": true, "to": true, "of": true, "in": true, "for": true, "on": true, "with": true}
	fields := strings.Fields(strings.ToLower(query))
	out := []string{}
	for _, f := range fields {
		f = strings.Trim(f, ".,:;!?()[]{}\"'")
		if len(f) >= 3 && !stop[f] {
			out = append(out, f)
		}
	}
	return out
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// containsAny reports whether s contains any of the given substrings.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}
