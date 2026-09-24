package harness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModelProviderSelectionIsExplicit(t *testing.T) {
	for _, key := range []string{"HARNESS_MODEL_PROVIDER", "HARNESS_MODEL_BASE_URL", "HARNESS_MODEL_API_KEY", "HARNESS_MODEL_NAME", "OPENAI_API_KEY", "DEEPSEEK_API_KEY"} {
		t.Setenv(key, "")
	}

	localAdapter, local := NewChatModelFromEnv()
	if local.Provider != "local" || local.Mode != "offline" || localAdapter.Name() != "local-conversation" {
		t.Fatalf("local provider=%+v adapter=%s", local, localAdapter.Name())
	}

	t.Setenv("HARNESS_MODEL_PROVIDER", "openai-compatible")
	_, incomplete := NewChatModelFromEnv()
	if incomplete.Configured || incomplete.Mode != "remote" {
		t.Fatalf("incomplete provider=%+v", incomplete)
	}

	t.Setenv("HARNESS_MODEL_BASE_URL", "https://api.deepseek.com")
	t.Setenv("HARNESS_MODEL_API_KEY", "TOKEN")
	t.Setenv("HARNESS_MODEL_NAME", "deepseek-chat")
	remoteAdapter, remote := NewChatModelFromEnv()
	if !remote.Configured || remote.Provider != "openai-compatible" || remote.BaseURL != "https://api.deepseek.com" {
		t.Fatalf("remote provider=%+v", remote)
	}
	if remoteAdapter.Name() != "deepseek-chat" {
		t.Fatalf("remote adapter=%s", remoteAdapter.Name())
	}
}

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
