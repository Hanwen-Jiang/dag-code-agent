package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Hanwen-Jiang/dag-code-agent/harness/dag"
	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/modelenv"
)

func requestJSON(t *testing.T, client *http.Client, method, url string, input any, output any) int {
	t.Helper()
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(raw)
	}
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if output != nil {
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode
}

func TestHTTPHarnessSessionAndDAGEndToEnd(t *testing.T) {
	store, err := harness.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := modelenv.FromEnv()
	manifest, manifestErr := dag.LoadManifest("../configs/software-feature-with-rework-and-uat.json")
	if manifestErr != nil {
		t.Fatal(manifestErr)
	}
	handler, err := New(Config{Store: store, Provider: settings.Describe(), DAGManifest: manifest, SessionFactory: settings.SessionFactory(store)})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()

	var health map[string]any
	if status := requestJSON(t, client, http.MethodGet, server.URL+"/health", nil, &health); status != http.StatusOK || health["status"] != "ok" || health["service"] != "dag-code-agent-harness" {
		t.Fatalf("health=%v status=%d", health, status)
	}

	var created map[string]any
	if status := requestJSON(t, client, http.MethodPost, server.URL+"/api/harness/runs", map[string]any{"goal": "compile a verified artifact", "reasoning_effort": "medium", "constraints": []string{"return structured evidence"}}, &created); status != http.StatusAccepted || created["status"] != "COMPLETED" {
		t.Fatalf("created=%v status=%d", created, status)
	}
	taskID, _ := created["task_id"].(string)
	if taskID == "" {
		t.Fatalf("missing task id: %v", created)
	}
	metrics, _ := created["metrics"].(map[string]any)
	eventCount, _ := metrics["event_count"].(float64)
	if eventCount <= float64(6) || metrics["tool_calls"] != float64(0) {
		t.Fatalf("metrics=%v", metrics)
	}
	snapshot, _ := created["snapshot"].(map[string]any)
	contextSnapshot, _ := snapshot["context"].(map[string]any)
	if contextSnapshot["prefix_cache_hit"] != true {
		t.Fatalf("context=%v", contextSnapshot)
	}
	durability, _ := snapshot["durability"].(map[string]any)
	if durability["replay_verified"] != true {
		t.Fatalf("durability=%v", durability)
	}

	var listed map[string]any
	if status := requestJSON(t, client, http.MethodGet, server.URL+"/api/harness/runs", nil, &listed); status != http.StatusOK || listed["count"] != float64(1) {
		t.Fatalf("listed=%v status=%d", listed, status)
	}
	var fetched map[string]any
	if status := requestJSON(t, client, http.MethodGet, server.URL+"/api/harness/runs/"+taskID, nil, &fetched); status != http.StatusOK || fetched["status"] != "COMPLETED" {
		t.Fatalf("fetched=%v status=%d", fetched, status)
	}
	var followup map[string]any
	if status := requestJSON(t, client, http.MethodPost, server.URL+"/api/harness/runs/"+taskID+"/messages", map[string]any{"message": "继续检查这个实现并说明下一步"}, &followup); status != http.StatusOK || followup["status"] != "COMPLETED" {
		t.Fatalf("followup=%v status=%d", followup, status)
	}
	followupEvents, _ := followup["events"].([]any)
	if len(followupEvents) <= int(eventCount) {
		t.Fatalf("followup did not append a new turn: before=%v after=%d", eventCount, len(followupEvents))
	}

	response, err := client.Get(server.URL + "/api/harness/runs/" + taskID + "/events")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(raw), "MODEL_REQUESTED") || !strings.Contains(string(raw), "USER_MESSAGE_RECEIVED") || !strings.Contains(string(raw), "TASK_COMPLETED") {
		t.Fatalf("events status=%d body=%s", response.StatusCode, raw)
	}
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/api/harness/runs/"+taskID+"/events", nil)
	request.Header.Set("Accept", "text/event-stream")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.Contains(string(raw), "event: MODEL_REQUESTED") || !strings.Contains(string(raw), "event: TASK_COMPLETED") {
		t.Fatalf("sse status=%d body=%s", response.StatusCode, raw)
	}
	request, _ = http.NewRequest(http.MethodGet, server.URL+"/api/harness/runs/"+taskID+"/events?since=1", nil)
	request.Header.Set("Accept", "text/event-stream")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || strings.Contains(string(raw), "TASK_ACCEPTED") || !strings.Contains(string(raw), "MODEL_REQUESTED") || !strings.Contains(string(raw), "TASK_COMPLETED") {
		t.Fatalf("resumed sse status=%d body=%s", response.StatusCode, raw)
	}

	var demo map[string]any
	if status := requestJSON(t, client, http.MethodGet, server.URL+"/api/harness/demo", nil, &demo); status != http.StatusOK || demo["status"] != "COMPLETED" {
		t.Fatalf("demo=%v status=%d", demo, status)
	}

	var dagStart map[string]any
	if status := requestJSON(t, client, http.MethodPost, server.URL+"/api/dag/start", map[string]any{"scenario": "rework"}, &dagStart); status != http.StatusAccepted {
		t.Fatalf("dag start=%v status=%d", dagStart, status)
	}
	dagReport, ok := dagStart["report"].(map[string]any)
	if !ok || dagReport["status"] != "WAITING_USER" {
		t.Fatalf("dag report=%v", dagStart)
	}
	runID, _ := dagReport["run_id"].(string)
	var dagFinal map[string]any
	if status := requestJSON(t, client, http.MethodPost, server.URL+"/api/dag/runs/"+runID+"/decision", map[string]any{"outcome": "PASS", "actor": "uat-user"}, &dagFinal); status != http.StatusOK || dagFinal["status"] != "COMPLETED" {
		t.Fatalf("dag final=%v status=%d", dagFinal, status)
	}

	if status := requestJSON(t, client, http.MethodPost, server.URL+"/api/feedback", map[string]any{"tenant_id": "demo-tenant", "subject_id": "demo-user", "task_id": taskID, "stage": "complete", "outcome": "completed", "rating": 5}, &map[string]any{}); status != http.StatusCreated {
		t.Fatalf("feedback status=%d", status)
	}
	var feedbackMetrics map[string]any
	if status := requestJSON(t, client, http.MethodGet, server.URL+"/api/feedback/metrics?tenant_id=demo-tenant", nil, &feedbackMetrics); status != http.StatusOK || feedbackMetrics["completed"] != float64(1) {
		t.Fatalf("feedback metrics=%v status=%d", feedbackMetrics, status)
	}
}

func TestHTTPMissingHarnessRun(t *testing.T) {
	store, err := harness.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	settings := modelenv.FromEnv()
	handler, err := New(Config{Store: store, Provider: settings.Describe(), SessionFactory: settings.SessionFactory(store)})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	var body map[string]any
	if status := requestJSON(t, server.Client(), http.MethodGet, server.URL+"/api/harness/runs/DOES-NOT-EXIST", nil, &body); status != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("body=%v status=%d", body, status)
	}
}

func TestEventCursorAcceptsNumericAndTimestampSequenceIDs(t *testing.T) {
	numeric, _ := http.NewRequest(http.MethodGet, "/events?since=7", nil)
	if got := eventCursor(numeric); got != 7 {
		t.Fatalf("numeric cursor=%d", got)
	}
	streamID, _ := http.NewRequest(http.MethodGet, "/events", nil)
	streamID.Header.Set("Last-Event-ID", "1700000000-12")
	if got := eventCursor(streamID); got != 12 {
		t.Fatalf("timestamp-sequence cursor=%d", got)
	}
	malformed, _ := http.NewRequest(http.MethodGet, "/events", nil)
	malformed.Header.Set("Last-Event-ID", "not-a-sequence")
	if got := eventCursor(malformed); got != 0 {
		t.Fatalf("malformed cursor=%d", got)
	}
}
