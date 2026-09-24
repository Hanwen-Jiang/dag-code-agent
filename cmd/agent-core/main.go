// Agent Core - the execution layer of the three-layer stack.
//
// Responsibilities
//   - gRPC AgentService (proto/agent_core): ExecuteTask, StreamExecuteTask,
//     HealthCheck, DiscoverTools.
//   - HTTP management API: /health, /api/tools, /api/execute (WASI round-trip).
//   - Secure code execution in a WASI/wasmtime sandbox (packages/sandbox/wasi).
//   - Retrieval-augmented generation via packages/vectordb, with graceful
//     degradation to an in-memory vector store + local embeddings when Qdrant
//     or llm-service is unavailable.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/embedding"
	"github.com/Hanwen-Jiang/dag-code-agent/packages/sandbox/wasi"
	"github.com/Hanwen-Jiang/dag-code-agent/packages/vectordb"
	pb "github.com/Hanwen-Jiang/dag-code-agent/proto/agent_core"
)

// version is the agent-core build identifier reported by /health.
const version = "1.0.0"

// ---------- configuration ----------

type config struct {
	grpcHost string
	grpcPort int
	httpHost string
	httpPort int

	memoryLimitMB uint64
	fuelLimit     uint64
	execTimeout   time.Duration
	allowedPaths  []string
	maxTableElem  int

	vectorDBHost string
	vectorDBPort int
	vectorTLS    bool
	collection   string
	dimension    int

	llmServiceAddr string // host:port of llm-service; empty => local embeddings

	ragTopK int
}

func loadConfig() config {
	return config{
		grpcHost: getEnv("AGENT_CORE_GRPC_HOST", "0.0.0.0"),
		grpcPort: getEnvInt("AGENT_CORE_GRPC_PORT", 50051),
		httpHost: getEnv("AGENT_CORE_HTTP_HOST", "0.0.0.0"),
		httpPort: getEnvInt("AGENT_CORE_HTTP_PORT", 8090),

		memoryLimitMB: uint64(getEnvInt("AGENT_CORE_MEMORY_MB", 256)),
		fuelLimit:     uint64(getEnvInt("AGENT_CORE_FUEL_LIMIT", 1_000_000_000)),
		execTimeout:   time.Duration(getEnvInt("AGENT_CORE_TIMEOUT_SECONDS", 30)) * time.Second,
		allowedPaths:  splitList(getEnv("AGENT_CORE_ALLOWED_PATHS", "/tmp")),
		maxTableElem:  getEnvInt("AGENT_CORE_MAX_TABLE_ELEMENTS", 10000),

		vectorDBHost: getEnv("QDRANT_HOST", ""),
		vectorDBPort: getEnvInt("QDRANT_PORT", 6334),
		vectorTLS:    getEnv("QDRANT_TLS", "false") == "true",
		collection:   getEnv("AGENT_CORE_COLLECTION", "agent_memory"),
		dimension:    getEnvInt("AGENT_CORE_EMBED_DIM", 256),

		llmServiceAddr: getEnv("LLM_SERVICE_ADDR", ""),

		ragTopK: getEnvInt("AGENT_CORE_RAG_TOPK", 5),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ---------- llm client (for RAG answers) ----------

// llmClient talks to llm-service's /complete endpoint. A nil client returns a
// clear error so callers can degrade (RAG no_llm mode).
type llmClient struct {
	baseURL string
	client  *http.Client
}

func newLLMClient(addr string) *llmClient {
	if addr == "" {
		return nil
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return &llmClient{baseURL: strings.TrimRight(addr, "/"), client: &http.Client{Timeout: 120 * time.Second}}
}

func (c *llmClient) Complete(ctx context.Context, prompt string) (string, error) {
	if c == nil {
		return "", errors.New("llm-service not configured")
	}
	body, _ := json.Marshal(map[string]any{"query": prompt})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/complete", strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("llm-service: HTTP %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	var out struct {
		Content string `json:"content"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("llm-service: bad response: %w", err)
	}
	if out.Error != "" {
		return "", errors.New(out.Error)
	}
	return out.Content, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------- agent core service ----------

type agentCore struct {
	pb.UnimplementedAgentServiceServer

	logger   *zap.Logger
	sandbox  *wasi.WasiExecutor
	rag      *vectordb.RAGPipeline
	llm      *llmClient
	vecName  string
	degraded []string // subsystems running in degraded (local) mode
	cfg      config
	started  time.Time
}

func newAgentCore(logger *zap.Logger, cfg config) (*agentCore, error) {
	sandbox, err := wasi.NewWasiExecutor(wasi.WasiSandboxConfig{
		MemoryLimitMB: cfg.memoryLimitMB,
		FuelLimit:     cfg.fuelLimit,
		Timeout:       cfg.execTimeout,
		AllowedPaths:  cfg.allowedPaths,
	})
	if err != nil {
		return nil, fmt.Errorf("init wasi sandbox: %w", err)
	}

	core := &agentCore{
		logger:  logger,
		sandbox: sandbox,
		cfg:     cfg,
		llm:     newLLMClient(cfg.llmServiceAddr),
		started: time.Now(),
	}
	core.initVectorAndRAG()
	return core, nil
}

// initVectorAndRAG wires the vector DB and RAG pipeline, degrading to a local
// in-memory store + local embeddings when the remote services are
// unreachable. It never fails the process.
func (c *agentCore) initVectorAndRAG() {
	opts := vectordb.VectorDBOptions{
		QdrantHost: c.cfg.vectorDBHost,
		QdrantPort: c.cfg.vectorDBPort,
		UseTLS:     c.cfg.vectorTLS,
		Dimension:  c.cfg.dimension,
		Collection: c.cfg.collection,
		Timeout:    5 * time.Second,
	}
	db, err := vectordb.NewAutoVectorDB(opts)
	if err != nil {
		c.logger.Warn("vector DB unavailable, using memory store", zap.Error(err))
		db = vectordb.NewMemoryDB(c.cfg.dimension)
		c.degraded = append(c.degraded, "vector_db=memory")
	} else {
		c.vecName = db.Name()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.Initialize(ctx); err != nil {
			c.logger.Warn("vector DB init failed, falling back to memory", zap.Error(err))
			db = vectordb.NewMemoryDB(c.cfg.dimension)
			c.vecName = db.Name()
			c.degraded = append(c.degraded, "vector_db=memory")
		}
	}

	embCfg := embedding.EmbeddingServiceConfig{
		Dimension: c.cfg.dimension,
		Timeout:   30 * time.Second,
	}
	if c.cfg.llmServiceAddr != "" {
		base := c.cfg.llmServiceAddr
		if !strings.Contains(base, "://") {
			base = "http://" + base
		}
		embCfg.BaseURL = strings.TrimRight(base, "/")
		embCfg.APIKey = "agent-core"
		embCfg.Model = "local"
	}
	emb, err := embedding.NewEmbeddingService(embCfg)
	if err != nil {
		c.logger.Warn("embedding service unavailable, using local embeddings", zap.Error(err))
		emb = embedding.NewLocalEmbeddingService(c.cfg.dimension)
		c.degraded = append(c.degraded, "embeddings=local")
	}

	c.rag = vectordb.NewRAGPipeline(db, emb, vectordb.RAGConfig{
		Collection:   c.cfg.collection,
		TopK:         c.cfg.ragTopK,
		MinScore:     0.3,
		ChunkSize:    1200,
		ChunkOverlap: 200,
		SystemPrompt: "You are a precise assistant. Answer using the retrieved context and cite sources.",
	})
	c.logger.Info("agent-core ready",
		zap.String("vector_db", c.vecName),
		zap.Strings("degraded", c.degraded))
}

// HealthCheck reports service status, including which subsystems are degraded.
func (c *agentCore) HealthCheck(ctx context.Context, in *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	statusCode := pb.HealthStatus_HEALTH_HEALTHY
	if len(c.degraded) > 0 {
		statusCode = pb.HealthStatus_HEALTH_DEGRADED
	}
	return &pb.HealthCheckResponse{
		Status:  statusCode,
		TimeNs:  timestamppb.Now().AsTime().UnixNano(),
		Version: 1,
	}, nil
}

// DiscoverTools lists the capabilities agent-core exposes.
func (c *agentCore) DiscoverTools(ctx context.Context, in *pb.DiscoverToolsRequest) (*pb.DiscoverToolsResponse, error) {
	tools := []*pb.ToolCapability{
		{
			Id:                  "wasi",
			Name:                "WASI Sandbox",
			Description:         "Execute untrusted WebAssembly modules in a WASI/wasmtime sandbox with fuel, timeout and filesystem isolation.",
			Category:            "code",
			RequiredPermissions: []string{"sandbox.execute"},
			IsDangerous:         false,
		},
		{
			Id:                  "retrieve",
			Name:                "Memory Retrieval",
			Description:         "Retrieve relevant passages from the agent memory store (RAG) using cosine similarity.",
			Category:            "knowledge",
			RequiredPermissions: []string{"memory.read"},
		},
		{
			Id:                  "rag_answer",
			Name:                "RAG Answer",
			Description:         "Answer a query grounded in retrieved memory, with cited sources; degrades to prompt-only when no LLM is configured.",
			Category:            "knowledge",
			RequiredPermissions: []string{"memory.read", "llm.complete"},
		},
	}
	return &pb.DiscoverToolsResponse{Tools: tools}, nil
}

// ExecuteTask runs a task. The Query field carries either a natural-language
// query (routed to RAG) or a tool-call JSON specification:
//
//	{"tool":"wasi","wasm_b64":"...","args":[...],"stdin":"..."}
func (c *agentCore) ExecuteTask(ctx context.Context, in *pb.ExecuteTaskRequest) (*pb.ExecuteTaskResponse, error) {
	if in.GetQuery() == "" {
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_FAILED, Output: "empty query"}, nil
	}

	if spec, ok := parseToolCall(in.GetQuery()); ok {
		return c.executeTool(ctx, spec, in.GetConfig())
	}

	return c.executeRAG(ctx, in.GetQuery())
}

// StreamExecuteTask streams progress: a started update, then the terminal
// result of the (single-shot) execution.
func (c *agentCore) StreamExecuteTask(in *pb.ExecuteTaskRequest, stream grpc.ServerStreamingServer[pb.TaskUpdate]) error {
	ctx := stream.Context()
	taskID := in.GetMetadata().GetSessionId()
	if taskID == "" {
		taskID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	if err := stream.Send(&pb.TaskUpdate{
		TaskId:    taskID,
		Iteration: 1,
		Status:    pb.TaskStatus_TASK_STATUS_RUNNING,
	}); err != nil {
		return err
	}

	res, err := c.ExecuteTask(ctx, in)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	toolName := "rag_answer"
	if spec, ok := parseToolCall(in.GetQuery()); ok {
		toolName = spec.Tool
	}
	return stream.Send(&pb.TaskUpdate{
		TaskId:    taskID,
		Iteration: 2,
		ToolName:  toolName,
		Status:    res.Status,
		Result:    &pb.ToolResult{Success: res.Status == pb.TaskStatus_TASK_STATUS_COMPLETED, Output: res.Output},
	})
}

// ---------- tool dispatch ----------

// toolCall is a JSON tool specification embedded in ExecuteTask.Query.
type toolCall struct {
	Tool    string   `json:"tool"`
	WasmB64 string   `json:"wasm_b64"`
	Args    []string `json:"args"`
	Stdin   string   `json:"stdin"`
}

func parseToolCall(query string) (*toolCall, bool) {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(trimmed, "{") {
		return nil, false
	}
	var spec toolCall
	if err := json.Unmarshal([]byte(trimmed), &spec); err != nil || spec.Tool == "" {
		return nil, false
	}
	return &spec, true
}

func (c *agentCore) executeTool(ctx context.Context, spec *toolCall, cfg *pb.AgentConfig) (*pb.ExecuteTaskResponse, error) {
	switch spec.Tool {
	case "wasi":
		return c.runWASI(ctx, spec)
	case "retrieve":
		return c.runRetrieve(ctx, spec.Args)
	case "rag_answer":
		return c.executeRAG(ctx, strings.Join(spec.Args, " "))
	default:
		return &pb.ExecuteTaskResponse{
			Status: pb.TaskStatus_TASK_STATUS_FAILED,
			Output: "unknown tool: " + spec.Tool,
		}, nil
	}
}

func (c *agentCore) runWASI(ctx context.Context, spec *toolCall) (*pb.ExecuteTaskResponse, error) {
	wasmBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(spec.WasmB64))
	if err != nil {
		return &pb.ExecuteTaskResponse{
			Status: pb.TaskStatus_TASK_STATUS_FAILED,
			Output: "invalid wasm_b64: " + err.Error(),
		}, nil
	}

	res, err := c.sandbox.Execute(ctx, wasi.ExecuteInput{
		WasmBytes: wasmBytes,
		Args:      spec.Args,
		Stdin:     []byte(spec.Stdin),
	})
	if err != nil {
		return &pb.ExecuteTaskResponse{
			Status: pb.TaskStatus_TASK_STATUS_FAILED,
			Output: "sandbox error: " + err.Error(),
		}, nil
	}

	meta, _ := structpb.NewStruct(map[string]any{
		"exit_code":  res.ExitCode,
		"status":     string(res.Status),
		"elapsed_ms": res.ElapsedMS,
		"fuel_used":  res.FuelConsumed,
	})
	out := string(res.Stdout)
	if string(res.Stderr) != "" {
		out += "\n[stderr]\n" + string(res.Stderr)
	}
	statusCode := pb.TaskStatus_TASK_STATUS_COMPLETED
	if res.Status != wasi.ExecSuccess {
		statusCode = pb.TaskStatus_TASK_STATUS_FAILED
		out += "\n[exit " + fmt.Sprint(res.ExitCode) + " " + res.ErrorCode + "] " + res.ErrorMessage
	}
	return &pb.ExecuteTaskResponse{
		Status:   statusCode,
		Output:   out,
		Sandbox:  true,
		Metadata: meta,
	}, nil
}

func (c *agentCore) runRetrieve(ctx context.Context, args []string) (*pb.ExecuteTaskResponse, error) {
	if len(args) == 0 {
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_FAILED, Output: "retrieve: query required"}, nil
	}
	query := strings.Join(args, " ")
	results, err := c.rag.Retrieve(ctx, query, c.cfg.ragTopK, nil)
	if err != nil {
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_FAILED, Output: "retrieve: " + err.Error()}, nil
	}
	var b strings.Builder
	for i, r := range results {
		text, _ := r.Payload["text"].(string)
		src, _ := r.Payload["source"].(string)
		fmt.Fprintf(&b, "%d. [%s] %s\n   %s\n", i+1, fmt.Sprintf("%.3f", r.Score), src, truncate(strings.TrimSpace(text), 300))
	}
	if b.Len() == 0 {
		b.WriteString("no matching passages found\n")
	}
	return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_COMPLETED, Output: b.String()}, nil
}

func (c *agentCore) executeRAG(ctx context.Context, query string) (*pb.ExecuteTaskResponse, error) {
	answer, err := c.rag.QueryWithAnswer(ctx, query, c.llm)
	if err != nil {
		return &pb.ExecuteTaskResponse{Status: pb.TaskStatus_TASK_STATUS_FAILED, Output: "rag: " + err.Error()}, nil
	}
	meta, _ := structpb.NewStruct(map[string]any{
		"mode":         answer.Mode,
		"source_count": len(answer.Sources),
	})
	return &pb.ExecuteTaskResponse{
		Status:   pb.TaskStatus_TASK_STATUS_COMPLETED,
		Output:   answer.Answer,
		Metadata: meta,
	}, nil
}

// ---------- HTTP management API ----------

func (c *agentCore) httpHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":     "healthy",
			"version":    version,
			"vector_db":  c.vecName,
			"degraded":   c.degraded,
			"uptime_sec": int64(time.Since(c.started).Seconds()),
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("/api/tools", func(w http.ResponseWriter, r *http.Request) {
		resp, err := c.DiscoverTools(r.Context(), &pb.DiscoverToolsRequest{})
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		tools := make([]map[string]any, 0, len(resp.Tools))
		for _, t := range resp.Tools {
			tools = append(tools, map[string]any{
				"id":       t.Id,
				"name":     t.Name,
				"desc":     t.Description,
				"category": t.Category,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"tools": tools})
	})

	mux.HandleFunc("/api/execute", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		var req struct {
			Tool    string   `json:"tool"`
			WasmB64 string   `json:"wasm_b64"`
			Args    []string `json:"args"`
			Stdin   string   `json:"stdin"`
			Query   string   `json:"query"` // alternative: knowledge query
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), c.cfg.execTimeout+5*time.Second)
		defer cancel()

		if req.Tool == "" && req.Query != "" {
			resp, err := c.executeRAG(ctx, req.Query)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"status": resp.Status, "output": resp.Output})
			return
		}
		if req.Tool == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tool or query required"})
			return
		}
		spec := &toolCall{Tool: req.Tool, WasmB64: req.WasmB64, Args: req.Args, Stdin: req.Stdin}
		resp, err := c.executeTool(ctx, spec, nil)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":  resp.Status,
			"output":  resp.Output,
			"sandbox": resp.Sandbox,
		})
	})

	// RAG ingestion: index documents into memory for later retrieval.
	mux.HandleFunc("/api/index", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		var req struct {
			Documents []struct {
				ID       string         `json:"id"`
				Source   string         `json:"source"`
				Text     string         `json:"text"`
				Metadata map[string]any `json:"metadata"`
			} `json:"documents"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		docs := make([]vectordb.Document, 0, len(req.Documents))
		for _, d := range req.Documents {
			docs = append(docs, vectordb.Document{ID: d.ID, Source: d.Source, Text: d.Text, Metadata: d.Metadata})
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		if err := c.rag.Index(ctx, docs); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"indexed": len(docs)})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------- main ----------

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	cfg := loadConfig()

	core, err := newAgentCore(logger, cfg)
	if err != nil {
		logger.Fatal("failed to initialize agent-core", zap.Error(err))
	}

	// gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.grpcHost, cfg.grpcPort))
	if err != nil {
		logger.Fatal("grpc: failed to listen", zap.Error(err))
	}
	grpcServer := grpc.NewServer()
	pb.RegisterAgentServiceServer(grpcServer, core)

	// Standard gRPC health service (for grpc_health_probe / orchestrator).
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthSrv)

	// HTTP management server
	httpServer := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.httpHost, cfg.httpPort),
		Handler:      core.httpHandler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 90 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("gRPC server listening", zap.String("addr", fmt.Sprintf("%s:%d", cfg.grpcHost, cfg.grpcPort)))
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc: %w", err)
		}
	}()
	go func() {
		logger.Info("HTTP server listening", zap.String("addr", fmt.Sprintf("%s:%d", cfg.httpHost, cfg.httpPort)))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http: %w", err)
		}
	}()

	logger.Info("agent-core started",
		zap.String("grpc", fmt.Sprintf("%s:%d", cfg.grpcHost, cfg.grpcPort)),
		zap.String("http", fmt.Sprintf("%s:%d", cfg.httpHost, cfg.httpPort)),
		zap.Strings("degraded", core.degraded))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		logger.Error("server error, shutting down", zap.Error(err))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
	core.sandbox.Close()
	logger.Info("agent-core stopped")
}
