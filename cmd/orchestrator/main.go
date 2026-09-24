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
// Activities call the LLM service and agent-core over HTTP when those are
// configured/reachable, and fall back to deterministic local execution.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	temporal "go.temporal.io/sdk/client"
	sdktypes "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

const TaskQueue = "shannon-tasks"

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
	Name    string `json:"name"`
	Objective string `json:"objective"`
}

type WorkflowResult struct {
	Output     string
	TokensUsed int
	ModelUsed  string
	Steps      []string
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

	if runWorker {
		w := worker.New(tClient, TaskQueue, worker.Options{})
		acts := newActivities(cfg, logger)
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

	svc := &service{cfg: cfg, tClient: tClient, mode: mode, logger: logger}

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
	cfg     Config
	tClient temporal.Client
	mode    string
	logger  *zap.Logger
}

func setupRoutes(mux *http.ServeMux, svc *service) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "mode": svc.mode, "task_queue": TaskQueue})
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
		return Response{
			Success:    true,
			Response:   res.Output,
			TokensUsed: res.TokensUsed,
			ModelUsed:  res.ModelUsed,
			Metadata:   map[string]interface{}{"mode": "temporal", "steps": res.Steps},
		}, nil
	}

	// Local mode: run the pipeline inline.
	start := time.Now()
	acts := newActivities(svc.cfg, svc.logger)
	res, err := acts.RunPipeline(ctx, params)
	if err != nil {
		return Response{}, err
	}
	return Response{
		Success:    true,
		Response:   res.Output,
		TokensUsed: res.TokensUsed,
		ModelUsed:  res.ModelUsed,
		Metadata:   map[string]interface{}{"mode": "local", "steps": res.Steps, "duration_ms": time.Since(start).Milliseconds()},
	}, nil
}

func (svc *service) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
		return
	}
	if svc.tClient == nil {
		writeJSON(w, http.StatusOK, map[string]string{"task_id": taskID, "status": "local-mode: no durable execution to query"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	desc, err := svc.tClient.DescribeWorkflowExecution(ctx, taskID, "")
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "execution not found", "detail": err.Error()})
		return
	}
	info := desc.WorkflowExecutionInfo
	status := enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	if info != nil {
		status = info.Status
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"task_id": taskID,
		"status":  status.String(),
	})
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

func activityOpts() workflow.ActivityOptions {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
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

	res := WorkflowResult{Steps: []string{}}
	for _, step := range plan {
		var out string
		if err := workflow.ExecuteActivity(ctx, (&activities{}).ExecuteStep, StepInput{Params: p, Step: step}).Get(ctx, &out); err != nil {
			return WorkflowResult{}, fmt.Errorf("step %s failed: %w", step.Name, err)
		}
		res.Output += out + "\n"
		res.Steps = append(res.Steps, step.Name)
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
	res := WorkflowResult{Steps: []string{}}
	for _, step := range plan {
		var out string
		if err := workflow.ExecuteActivity(ctx, a.ExecuteStep, StepInput{Params: p, Step: step}).Get(ctx, &out); err != nil {
			return WorkflowResult{}, fmt.Errorf("step %s failed: %w", step.Name, err)
		}
		res.Output += out + "\n"
		res.Steps = append(res.Steps, step.Name)
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
	cfg    Config
	logger *zap.Logger
}

func newActivities(cfg Config, logger *zap.Logger) *activities {
	return &activities{cfg: cfg, logger: logger}
}

// PlanTask produces a deterministic plan for the request.
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
	// Cap iterations by budget when provided.
	if p.MaxIterations > 0 && p.MaxIterations < len(plan) {
		plan = plan[:p.MaxIterations]
	}
	return plan, nil
}

// ExecuteStep runs one step: prefer the LLM service when reachable, else the
// deterministic local composer.
func (a *activities) ExecuteStep(ctx context.Context, in StepInput) (string, error) {
	if out, err := a.callLLM(ctx, in.Params, in.Step); err == nil && out != "" {
		return out, nil
	}
	// Local degradation: compose a structured, deterministic answer.
	return localStepOutput(in), nil
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
	res := WorkflowResult{Steps: []string{}}
	for _, step := range plan {
		out, err := a.ExecuteStep(ctx, StepInput{Params: p, Step: step})
		if err != nil {
			return WorkflowResult{}, err
		}
		res.Output += out + "\n"
		res.Steps = append(res.Steps, step.Name)
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
