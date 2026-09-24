package modelenv

import (
	"testing"
)

// The environment-driven provider selection moved here from the harness core:
// a library that reads the process environment cannot be embedded predictably.
func TestProviderSelectionIsExplicit(t *testing.T) {
	for _, key := range []string{"HARNESS_MODEL_PROVIDER", "HARNESS_MODEL_BASE_URL", "HARNESS_MODEL_API_KEY", "HARNESS_MODEL_NAME", "OPENAI_API_KEY", "DEEPSEEK_API_KEY"} {
		t.Setenv(key, "")
	}

	localAdapter, local := FromEnv().NewChatModel()
	if local.Provider != "local" || local.Mode != "offline" || localAdapter.Name() != "local-conversation" {
		t.Fatalf("local provider=%+v adapter=%s", local, localAdapter.Name())
	}

	t.Setenv("HARNESS_MODEL_PROVIDER", "openai-compatible")
	_, incomplete := FromEnv().NewChatModel()
	if incomplete.Configured || incomplete.Mode != "remote" {
		t.Fatalf("incomplete provider=%+v", incomplete)
	}

	t.Setenv("HARNESS_MODEL_BASE_URL", "https://api.deepseek.com")
	t.Setenv("HARNESS_MODEL_API_KEY", "TOKEN")
	t.Setenv("HARNESS_MODEL_NAME", "deepseek-chat")
	remoteAdapter, remote := FromEnv().NewChatModel()
	if !remote.Configured || remote.Provider != "openai-compatible" || remote.BaseURL != "https://api.deepseek.com" {
		t.Fatalf("remote provider=%+v", remote)
	}
	if remoteAdapter.Name() != "deepseek-chat" {
		t.Fatalf("remote adapter=%s", remoteAdapter.Name())
	}
}

// A partially configured remote provider must fail loudly rather than silently
// falling back to the offline fixture.
func TestIncompleteRemoteProviderIsUnavailable(t *testing.T) {
	t.Setenv("HARNESS_MODEL_PROVIDER", "openai-compatible")
	t.Setenv("HARNESS_MODEL_BASE_URL", "https://api.deepseek.com")
	t.Setenv("HARNESS_MODEL_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("DEEPSEEK_API_KEY", "")
	t.Setenv("HARNESS_MODEL_NAME", "deepseek-chat")

	adapter, status := FromEnv().NewChatModel()
	if status.Configured {
		t.Fatalf("status=%+v, want unconfigured", status)
	}
	if name := adapter.Name(); name != "openai-compatible:not-configured" {
		t.Fatalf("adapter=%s", name)
	}
}

func TestSettingsResolveDefaults(t *testing.T) {
	for _, key := range []string{"HARNESS_MODEL_PROVIDER", "HARNESS_MODEL_BASE_URL", "HARNESS_MODEL_API_KEY", "OPENAI_API_KEY", "DEEPSEEK_API_KEY", "HARNESS_MODEL_NAME"} {
		t.Setenv(key, "")
	}
	resolved := FromEnv().Resolve()
	if resolved.Provider != "local" || resolved.Model != "deepseek-chat" || resolved.Timeout <= 0 {
		t.Fatalf("resolved=%+v", resolved)
	}
}
