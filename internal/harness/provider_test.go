package harness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProviderReasoningEffortMapsUltraToMax(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &request)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	adapter := &OpenAICompatibleAdapter{BaseURL: server.URL, APIKey: "TOKEN", ModelName: "gpt-5.6-sol", Client: server.Client()}
	if _, err := adapter.Complete(context.Background(), ModelRequest{Messages: []map[string]any{{"role": "user", "content": "ping"}}, ReasoningEffort: "ultra"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if request["reasoning_effort"] != "max" {
		t.Fatalf("reasoning_effort=%v, want max", request["reasoning_effort"])
	}
}
