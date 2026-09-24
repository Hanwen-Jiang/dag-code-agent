package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"agent-harness/internal/dag"
	"agent-harness/internal/harness"
)

type Server struct {
	HarnessStore *harness.Store
	Feedback     harness.FeedbackCollector
	Provider     harness.ModelProviderStatus
	DAGManifest  dag.Manifest
	DAGLoadError error
	runsMu       sync.RWMutex
	runs         map[string]*harnessSession
}
type harnessSession struct {
	mu   sync.Mutex
	env  *harness.DemoEnvironment
	auth harness.AuthContext
}

type harnessRunRequest struct {
	TaskID          string         `json:"task_id"`
	Goal            string         `json:"goal"`
	ProjectID       string         `json:"project_id"`
	ProjectName     string         `json:"project_name"`
	TenantID        string         `json:"tenant_id"`
	SubjectID       string         `json:"subject_id"`
	Model           string         `json:"model"`
	ModelRevision   string         `json:"model_revision"`
	ReasoningEffort string         `json:"reasoning_effort"`
	PolicyVersion   string         `json:"policy_version"`
	ContextWindow   int            `json:"context_window_tokens"`
	MaxSteps        int            `json:"max_steps"`
	MaxToolCalls    int            `json:"max_tool_calls"`
	Constraints     []string       `json:"constraints"`
	Metadata        map[string]any `json:"metadata"`
}
type harnessMessageRequest struct {
	Message string `json:"message"`
}
type dagStartRequest struct {
	Scenario string `json:"scenario"`
}
type dagDecisionRequest struct {
	Outcome string `json:"outcome"`
	Actor   string `json:"actor"`
}

func NewHandler() (http.Handler, func(), error) {
	controlPath := os.Getenv("HARNESS_DB_PATH")
	if controlPath == "" {
		controlPath = ":memory:"
	}
	return NewHandlerWithPaths(controlPath)
}

// NewHandlerWithPaths keeps a variadic second argument for compatibility with
// an earlier combined demo. This package owns only the Harness control plane.
func NewHandlerWithPaths(controlPath string, _ ...string) (http.Handler, func(), error) {
	controlStore, err := harness.NewStore(controlPath)
	if err != nil {
		return nil, func() {}, err
	}
	server := &Server{HarnessStore: controlStore, Feedback: harness.FeedbackCollector{Store: controlStore}, Provider: harness.DescribeModelProvider(), runs: map[string]*harnessSession{}}
	server.DAGManifest, server.DAGLoadError = loadDAGManifest()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.health)
	mux.HandleFunc("/api/model/status", server.modelStatus)
	mux.HandleFunc("/api/harness/demo", server.harnessDemo)
	mux.HandleFunc("/api/harness/runs", server.harnessRuns)
	mux.HandleFunc("/api/harness/runs/", server.harnessRun)
	mux.HandleFunc("/api/feedback", server.feedback)
	mux.HandleFunc("/api/feedback/metrics", server.feedbackMetrics)
	mux.HandleFunc("/api/dag/manifest", server.dagManifest)
	mux.HandleFunc("/api/dag/start", server.dagStart)
	mux.HandleFunc("/api/dag/runs/", server.dagRun)
	return withCORS(mux), func() { _ = controlStore.Close() }, nil
}

func loadDAGManifest() (dag.Manifest, error) {
	configured := os.Getenv("DAG_MANIFEST_PATH")
	candidates := []string{}
	if configured != "" {
		candidates = append(candidates, configured)
	}
	candidates = append(candidates,
		"configs/software-feature-with-rework-and-uat.json",
		"../configs/software-feature-with-rework-and-uat.json",
		"../../configs/software-feature-with-rework-and-uat.json",
		filepath.Join("..", "..", "..", "configs", "software-feature-with-rework-and-uat.json"),
	)
	for _, path := range candidates {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		return dag.LoadManifest(path)
	}
	return dag.Manifest{}, fmt.Errorf("DAG_MANIFEST_NOT_FOUND")
}
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, Last-Event-ID")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "dag-code-agent-harness", "runtime": "go"})
}
func (s *Server) modelStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Provider)
}
func (s *Server) harnessDemo(w http.ResponseWriter, r *http.Request) {
	env, err := harness.BuildDemoEnvironment()
	if err != nil {
		writeError(w, err)
		return
	}
	defer env.Store.Close()
	result := env.Run(r.Context())
	writeJSON(w, http.StatusOK, harnessResponse(env, result))
}

// harnessRuns is the session collection endpoint. A POST runs the deterministic
// adapter synchronously for the local fixture; the response is still a 202 to
// keep the contract compatible with a queued worker implementation.
func (s *Server) harnessRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		states, err := s.HarnessStore.ListTasks()
		if err != nil {
			writeError(w, err)
			return
		}
		items := make([]map[string]any, 0, len(states))
		for _, state := range states {
			storedRequest := taskRequestFromState(state)
			projectID, _ := storedRequest.Metadata["project_id"].(string)
			projectName, _ := storedRequest.Metadata["project_name"].(string)
			items = append(items, map[string]any{"task_id": state.TaskID, "goal": state.Goal, "project_id": projectID, "project_name": projectName, "status": state.Status, "step": state.Step, "model_round": state.ModelRound, "tool_calls": state.ToolCalls, "retry_count": state.RetryCount, "last_error": state.LastError})
		}
		writeJSON(w, http.StatusOK, map[string]any{"sessions": items, "count": len(items)})
		return
	}
	if r.Method == http.MethodPost {
		s.createHarnessRun(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

func (s *Server) createHarnessRun(w http.ResponseWriter, r *http.Request) {
	var input harnessRunRequest
	if r.Body != nil && r.Body != http.NoBody {
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeError(w, err)
			return
		}
	}
	if strings.TrimSpace(input.Goal) == "" {
		input.Goal = "inspect the Harness runtime and return verified JSON"
	}
	if input.TenantID == "" {
		input.TenantID = "demo-tenant"
	}
	if input.SubjectID == "" {
		input.SubjectID = "demo-user"
	}
	if input.TaskID == "" {
		input.TaskID = harness.NewID("TASK")
	}
	if strings.TrimSpace(input.ProjectID) == "" {
		input.ProjectID = "dag-code-agent"
	}
	if strings.TrimSpace(input.ProjectName) == "" {
		input.ProjectName = "dag-code-agent"
	}
	auth := harness.AuthContext{TenantID: input.TenantID, SubjectID: input.SubjectID, Scopes: []string{"memory:read", "artifact:write", "artifact:read", "workspace:read", "workspace:write"}}
	request := harness.TaskRequest{TaskID: input.TaskID, Goal: input.Goal, TenantID: input.TenantID, SubjectID: input.SubjectID, Model: input.Model, ModelRevision: input.ModelRevision, ReasoningEffort: input.ReasoningEffort, PolicyVersion: input.PolicyVersion, ContextWindow: input.ContextWindow, MaxSteps: input.MaxSteps, MaxToolCalls: input.MaxToolCalls, Constraints: input.Constraints, Metadata: input.Metadata}
	if request.Metadata == nil {
		request.Metadata = map[string]any{}
	}
	request.Metadata["session_mode"] = "chat"
	request.Metadata["project_id"] = input.ProjectID
	request.Metadata["project_name"] = input.ProjectName
	env, err := harness.BuildChatEnvironmentWithStore(s.HarnessStore, request, auth)
	if err != nil {
		writeError(w, err)
		return
	}
	session := &harnessSession{env: env, auth: auth}
	s.runsMu.Lock()
	if _, exists := s.runs[input.TaskID]; exists {
		s.runsMu.Unlock()
		writeError(w, errors.New("TASK_ID_ALREADY_REGISTERED"))
		return
	}
	s.runs[input.TaskID] = session
	s.runsMu.Unlock()
	session.mu.Lock()
	result := env.Run(r.Context())
	session.mu.Unlock()
	writeJSON(w, http.StatusAccepted, harnessResponse(env, result))
}

func (s *Server) harnessRun(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/harness/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	taskID := parts[0]
	session, err := s.getHarnessSession(taskID)
	if err != nil {
		writeError(w, err)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, harnessResponse(session.env, s.currentSessionResult(session)))
		return
	}
	if len(parts) >= 2 && parts[1] == "events" && r.Method == http.MethodGet {
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			writeHarnessSSE(r.Context(), w, s.HarnessStore, taskID, eventCursor(r), r.URL.Query().Get("follow") == "1")
			return
		}
		events, eventErr := s.HarnessStore.EventsSince(taskID, eventCursor(r))
		if eventErr != nil {
			writeError(w, eventErr)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "events": events, "count": len(events)})
		return
	}
	if len(parts) >= 2 && parts[1] == "messages" && r.Method == http.MethodPost {
		var input harnessMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeError(w, err)
			return
		}
		if strings.TrimSpace(input.Message) == "" {
			writeError(w, errors.New("MESSAGE_REQUIRED"))
			return
		}
		session.mu.Lock()
		result := session.env.Runtime.SendMessage(r.Context(), taskID, input.Message, session.auth)
		session.mu.Unlock()
		writeJSON(w, http.StatusOK, harnessResponse(session.env, result))
		return
	}
	if len(parts) >= 2 && parts[1] == "cancel" && r.Method == http.MethodPost {
		session.mu.Lock()
		result := session.env.Runtime.Cancel(taskID, session.auth)
		session.mu.Unlock()
		writeJSON(w, http.StatusOK, harnessResponse(session.env, result))
		return
	}
	http.NotFound(w, r)
}

func (s *Server) getHarnessSession(taskID string) (*harnessSession, error) {
	s.runsMu.RLock()
	session := s.runs[taskID]
	s.runsMu.RUnlock()
	if session != nil {
		return session, nil
	}
	state, _, err := s.HarnessStore.GetTask(taskID)
	if err != nil {
		return nil, err
	}
	if _, err := s.HarnessStore.ReplayState(taskID); err != nil {
		return nil, err
	}
	request := taskRequestFromState(state)
	auth := harness.AuthContext{TenantID: state.TenantID, SubjectID: state.SubjectID, Scopes: []string{"memory:read", "artifact:write", "artifact:read", "workspace:read", "workspace:write"}}
	var env *harness.DemoEnvironment
	if mode, _ := state.Metadata["session_mode"].(string); mode == "chat" {
		env, err = harness.BuildChatEnvironmentWithStore(s.HarnessStore, request, auth)
	} else {
		env, err = harness.BuildDemoEnvironmentWithStore(s.HarnessStore, request, auth)
	}
	if err != nil {
		return nil, err
	}
	session = &harnessSession{env: env, auth: auth}
	s.runsMu.Lock()
	if existing := s.runs[taskID]; existing != nil {
		session = existing
	} else {
		s.runs[taskID] = session
	}
	s.runsMu.Unlock()
	return session, nil
}

func taskRequestFromState(state harness.TaskState) harness.TaskRequest {
	request := harness.TaskRequest{TaskID: state.TaskID, Goal: state.Goal, TenantID: state.TenantID, SubjectID: state.SubjectID}
	if raw, ok := state.Metadata["request"]; ok {
		encoded, _ := json.Marshal(raw)
		_ = json.Unmarshal(encoded, &request)
	}
	request.TaskID = state.TaskID
	request.Goal = state.Goal
	request.TenantID = state.TenantID
	request.SubjectID = state.SubjectID
	return request.WithDefaults()
}

func (s *Server) currentSessionResult(session *harnessSession) harness.RunResult {
	session.mu.Lock()
	defer session.mu.Unlock()
	state, _, err := s.HarnessStore.GetTask(session.env.Request.TaskID)
	if err != nil {
		return harness.RunResult{TaskID: session.env.Request.TaskID, Status: harness.TaskFailed, FailureCategory: "STORE", State: state}
	}
	if _, replayErr := s.HarnessStore.ReplayState(state.TaskID); replayErr != nil {
		state.Status = harness.TaskFailed
		state.LastError = map[string]any{"code": "REPLAY", "message": replayErr.Error()}
		return harness.RunResult{TaskID: state.TaskID, Status: state.Status, FailureCategory: "REPLAY", State: state}
	}
	events, _ := s.HarnessStore.Events(state.TaskID)
	return harness.RunResult{TaskID: state.TaskID, Status: state.Status, State: state, FinalAnswer: state.FinalAnswer, Events: events, Metrics: map[string]float64{
		"event_count": float64(len(events)), "model_rounds": float64(state.ModelRound), "steps": float64(state.Step), "tool_calls": float64(state.ToolCalls),
		"turns": float64(state.Turns), "input_tokens": float64(state.InputTokens), "output_tokens": float64(state.OutputTokens),
		"cached_input_tokens": float64(state.CachedTokens), "cost_usd": state.CostUSD,
	}}
}

func harnessResponse(env *harness.DemoEnvironment, result harness.RunResult) map[string]any {
	return map[string]any{"status": result.Status, "task_id": result.TaskID, "final_answer": result.FinalAnswer, "failure_category": result.FailureCategory, "state": result.State, "metrics": result.Metrics, "events": result.Events, "snapshot": env.Snapshot(result)}
}

func eventCursor(r *http.Request) int {
	value := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if value == "" {
		value = strings.TrimSpace(r.URL.Query().Get("since"))
	}
	if dash := strings.LastIndexByte(value, '-'); dash > 0 {
		// Accept Shannon-style stream ids (timestamp-sequence) in addition
		// to this store's numeric sequence cursor. The suffix is the durable
		// journal sequence; using the timestamp would skip the whole stream.
		if suffix := strings.TrimSpace(value[dash+1:]); suffix != "" {
			value = suffix
		}
	}
	cursor, err := strconv.Atoi(value)
	if err != nil || cursor < 0 {
		return 0
	}
	return cursor
}

func writeHarnessSSE(ctx context.Context, w http.ResponseWriter, store *harness.Store, taskID string, after int, follow bool) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, _ := w.(http.Flusher)
	if _, err := fmt.Fprint(w, "retry: 1500\n\n"); err != nil {
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := store.EventsSince(taskID, after)
		if err != nil {
			return
		}
		for _, event := range events {
			payload, _ := json.Marshal(event)
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.Sequence, event.Type, payload); err != nil {
				return
			}
			after = event.Sequence
			if flusher != nil {
				flusher.Flush()
			}
		}
		state, _, stateErr := store.GetTask(taskID)
		if !follow || (stateErr == nil && (state.Status == harness.TaskCompleted || state.Status == harness.TaskFailed || state.Status == harness.TaskCancelled)) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) feedback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var signal harness.FeedbackSignal
	if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
		writeError(w, err)
		return
	}
	tenant := signal.TenantID
	if tenant == "" {
		tenant = "TENANT-DEMO"
	}
	subject := signal.SubjectID
	if subject == "" {
		subject = "anonymous"
	}
	if err := s.Feedback.Record(harness.AuthContext{TenantID: tenant, SubjectID: subject}, signal); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"status": "recorded"})
}
func (s *Server) feedbackMetrics(w http.ResponseWriter, r *http.Request) {
	tenant := r.URL.Query().Get("tenant_id")
	if tenant == "" {
		tenant = "TENANT-DEMO"
	}
	metrics, err := s.Feedback.Metrics(tenant)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, metrics)
}

func (s *Server) dagManifest(w http.ResponseWriter, _ *http.Request) {
	if s.DAGLoadError != nil {
		writeError(w, s.DAGLoadError)
		return
	}
	writeJSON(w, http.StatusOK, s.DAGManifest)
}

func (s *Server) dagStart(w http.ResponseWriter, r *http.Request) {
	if s.DAGLoadError != nil {
		writeError(w, s.DAGLoadError)
		return
	}
	var input dagStartRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, err)
		return
	}
	scenario := strings.ToLower(strings.TrimSpace(input.Scenario))
	if scenario == "" {
		scenario = "happy"
	}
	if scenario != "happy" && scenario != "rework" && scenario != "crash" {
		writeError(w, errors.New("UNKNOWN_DAG_SCENARIO"))
		return
	}
	outcomes := map[string][]string{}
	if scenario == "rework" {
		outcomes["N-VERIFY-V1"] = []string{"FAILED", "FAILED"}
	}
	runID := harness.NewID("DAG")
	executor, err := dag.NewExecutor(s.DAGManifest, s.HarnessStore, dag.NewDeterministicRunnerFactory(outcomes))
	if err != nil {
		writeError(w, err)
		return
	}
	options := dag.Options{Context: r.Context()}
	if scenario == "crash" {
		options.CrashAfterCommits = 2
	}
	recovered := false
	report, runErr := executor.Execute(runID, options)
	if scenario == "crash" && errors.Is(runErr, dag.ErrSimulatedCrash) {
		recovered = true
		executor, runErr = dag.NewExecutor(s.DAGManifest, s.HarnessStore, dag.NewDeterministicRunnerFactory(nil))
		if runErr == nil {
			report, runErr = executor.Execute(runID, dag.Options{Context: context.WithoutCancel(r.Context())})
		}
	}
	if runErr != nil {
		writeError(w, runErr)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"scenario": scenario, "recovered_from_crash": recovered, "report": report})
}

func (s *Server) dagRun(w http.ResponseWriter, r *http.Request) {
	if s.DAGLoadError != nil {
		writeError(w, s.DAGLoadError)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/dag/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	runID := parts[0]
	executor, err := dag.NewExecutor(s.DAGManifest, s.HarnessStore, dag.NewDeterministicRunnerFactory(nil))
	if err != nil {
		writeError(w, err)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		report, runErr := executor.Report(runID)
		if runErr != nil {
			writeError(w, runErr)
			return
		}
		writeJSON(w, http.StatusOK, report)
		return
	}
	if len(parts) >= 2 && parts[1] == "decision" && r.Method == http.MethodPost {
		var input dagDecisionRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeError(w, err)
			return
		}
		outcome := strings.ToUpper(strings.TrimSpace(input.Outcome))
		if input.Actor == "" {
			input.Actor = "uat-user"
		}
		if outcome != "PASS" && outcome != "PASSED" && outcome != "APPROVED" && outcome != "REJECT" {
			writeError(w, errors.New("UNKNOWN_DAG_OUTCOME"))
			return
		}
		report, runErr := executor.ResumeUser(runID, outcome, input.Actor)
		if runErr != nil {
			writeError(w, runErr)
			return
		}
		writeJSON(w, http.StatusOK, report)
		return
	}
	http.NotFound(w, r)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, harness.ErrNotFound) {
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]any{"error": err.Error()})
}
