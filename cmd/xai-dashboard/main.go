// XAI Dashboard - real-time explainability web console.
//
// Serves the static UI (./static) plus a JSON API and an SSE stream for live
// trace updates:
//
//	GET  /                    static UI
//	GET  /api/traces          recent traces
//	GET  /api/trace?task_id=  single trace
//	GET  /api/attribution?task_id=      attribution report
//	GET  /api/explanation?task_id=      reasoning explanation
//	GET  /api/visualization?task_id=    visualization JSON
//	GET  /ws/traces           SSE live trace stream
//	GET  /health              liveness
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/xai"
)

type Config struct {
	Host                string
	Port                int
	TraceVisualizer     *xai.TraceVisualizer
	AttributionAnalyzer *xai.AttributionAnalyzer
	ReasoningExplainer  *xai.ReasoningExplainer
}

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	traceVis := xai.NewTraceVisualizer(xai.DefaultTraceConfig())
	attribAnalyzer := xai.NewAttributionAnalyzer(xai.DefaultAttributionConfig())
	reasoningExplainer := xai.NewReasoningExplainer(xai.DefaultExplainerConfig())

	// Seed a deterministic demo trace so the dashboard and its API are
	// immediately inspectable offline (degradation pattern: the real system
	// records traces as agents run; with no live agent the seed keeps every
	// endpoint exercised and the UI populated).
	seedDemoTrace(traceVis)

	config := Config{
		Host:                getEnv("XAI_DASHBOARD_HOST", "localhost"),
		Port:                getEnvInt("PORT", 3000),
		TraceVisualizer:     traceVis,
		AttributionAnalyzer: attribAnalyzer,
		ReasoningExplainer:  reasoningExplainer,
	}

	mux := http.NewServeMux()
	setupRoutes(mux, config, logger)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 0, // 0 = no limit (SSE streams are long-lived)
	}

	logger.Info("XAI Dashboard starting", zap.Int("port", config.Port))

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", zap.Error(err))
		}
	}()

	<-sigChan
	logger.Info("Shutting down XAI Dashboard...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
}

func setupRoutes(mux *http.ServeMux, config Config, logger *zap.Logger) {
	// Static UI.
	fileServer := http.FileServer(http.Dir("./static"))
	mux.Handle("/", fileServer)

	mux.HandleFunc("/health", healthHandler)

	mux.HandleFunc("/api/traces", func(w http.ResponseWriter, r *http.Request) {
		traces := config.TraceVisualizer.ListTraces(50)
		writeJSON(w, http.StatusOK, map[string]any{"count": len(traces), "traces": traces})
	})

	mux.HandleFunc("/api/trace", func(w http.ResponseWriter, r *http.Request) {
		taskID := taskIDParam(r)
		if taskID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
			return
		}
		trace, err := config.TraceVisualizer.GetTrace(taskID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, trace)
	})

	mux.HandleFunc("/api/attribution", func(w http.ResponseWriter, r *http.Request) {
		trace, ok := requireTrace(w, r, config)
		if !ok {
			return
		}
		writeJSON(w, http.StatusOK, config.AttributionAnalyzer.Analyze(trace))
	})

	mux.HandleFunc("/api/explanation", func(w http.ResponseWriter, r *http.Request) {
		trace, ok := requireTrace(w, r, config)
		if !ok {
			return
		}
		writeJSON(w, http.StatusOK, config.ReasoningExplainer.Generate(trace))
	})

	mux.HandleFunc("/api/visualization", func(w http.ResponseWriter, r *http.Request) {
		taskID := taskIDParam(r)
		if taskID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
			return
		}
		jsonStr, err := config.TraceVisualizer.VisualizationJSON(taskID)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jsonStr))
	})

	// Live trace stream over SSE. The visualizer fans out every recorded
	// event to registered callbacks; each connection gets its own subscription.
	mux.HandleFunc("/ws/traces", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		connID := config.TraceVisualizer.RegisterCallback(func(event xai.TraceEvent) {
			data, _ := json.Marshal(map[string]any{
				"type":      event.Type,
				"task_id":   event.TaskID,
				"tool":      event.ToolName,
				"iteration": event.Iteration,
				"timestamp": event.Timestamp.Format(time.RFC3339),
			})
			// Best-effort write: a dead connection is cleaned up below.
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		})
		defer config.TraceVisualizer.UnregisterCallback(connID)

		fmt.Fprint(w, "event: connected\ndata: {}\n\n")
		flusher.Flush()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				fmt.Fprint(w, ": ping\n\n")
				flusher.Flush()
			}
		}
	})
}

func taskIDParam(r *http.Request) string {
	if id := r.URL.Query().Get("task_id"); id != "" {
		return id
	}
	// Support both ?task_id= and a trailing /<taskID> for back-compat with the UI.
	segments := splitPath(r.URL.Path)
	if len(segments) >= 2 {
		return segments[len(segments)-1]
	}
	return ""
}

func requireTrace(w http.ResponseWriter, r *http.Request, config Config) (*xai.Trace, bool) {
	taskID := taskIDParam(r)
	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
		return nil, false
	}
	trace, err := config.TraceVisualizer.GetTrace(taskID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return nil, false
	}
	return trace, true
}

func splitPath(p string) []string {
	out := []string{}
	cur := ""
	for _, c := range p {
		if c == '/' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy","timestamp":"` + time.Now().Format(time.RFC3339) + `"}`))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
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

// seedDemoTrace records a small, deterministic trace representing a typical
// agent run (a couple of tool calls, one error, a successful finish). It is
// used so the dashboard has something to explain out of the box.
func seedDemoTrace(tv *xai.TraceVisualizer) {
	taskID := "demo-trace-0001"
	base := time.Now().Add(-2 * time.Minute)
	events := []xai.TraceEvent{
		{Type: "llm_call", TaskID: taskID, Timestamp: base, Iteration: 1, Output: "planning approach", LatencyMS: 120},
		{Type: "tool_call", TaskID: taskID, Timestamp: base.Add(150 * time.Millisecond), Iteration: 2, ToolName: "workspace_read", Output: "read 3 files", LatencyMS: 45},
		{Type: "tool_call", TaskID: taskID, Timestamp: base.Add(300 * time.Millisecond), Iteration: 3, ToolName: "workspace_search", Output: "12 matches", LatencyMS: 38},
		{Type: "error", TaskID: taskID, Timestamp: base.Add(500 * time.Millisecond), Iteration: 4, ToolName: "workspace_test", Error: "go test: 2 failures", LatencyMS: 900},
		{Type: "tool_call", TaskID: taskID, Timestamp: base.Add(1500 * time.Millisecond), Iteration: 5, ToolName: "workspace_test", Output: "go test: 0 failures", LatencyMS: 850},
		{Type: "llm_call", TaskID: taskID, Timestamp: base.Add(2400 * time.Millisecond), Iteration: 6, Output: "final answer", LatencyMS: 210},
	}
	for _, ev := range events {
		tv.Record(ev)
	}
	tv.SetStatus(taskID, xai.TraceCompleted)
}
