// Package modelenv resolves model-provider configuration from the process
// environment and assembles the conversational session environment used by the
// service binaries.
//
// It exists so that the harness core and the HTTP adapter stay environment
// free: only this package and cmd/* may call os.Getenv. That rule is enforced
// by scripts/embed-guards.sh (guard G3), because a library that reads the
// environment cannot be embedded predictably.
package modelenv

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// Settings is the environment-derived model configuration.
type Settings struct {
	Provider        string
	BaseURL         string
	APIKey          string
	Model           string
	Timeout         time.Duration
	ReasoningEffort string
	WorkspaceRoot   string
}

// FromEnv reads the model/session settings from the process environment.
func FromEnv() Settings {
	return Settings{
		Provider:        strings.ToLower(strings.TrimSpace(os.Getenv("HARNESS_MODEL_PROVIDER"))),
		BaseURL:         strings.TrimRight(strings.TrimSpace(os.Getenv("HARNESS_MODEL_BASE_URL")), "/"),
		APIKey:          firstNonEmptyEnv("HARNESS_MODEL_API_KEY", "OPENAI_API_KEY", "DEEPSEEK_API_KEY"),
		Model:           strings.TrimSpace(os.Getenv("HARNESS_MODEL_NAME")),
		Timeout:         timeoutFromEnv(),
		ReasoningEffort: strings.TrimSpace(os.Getenv("HARNESS_MODEL_REASONING_EFFORT")),
		WorkspaceRoot:   os.Getenv("HARNESS_WORKSPACE_ROOT"),
	}
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func timeoutFromEnv() time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(os.Getenv("HARNESS_MODEL_TIMEOUT_SECONDS")))
	if err != nil || seconds <= 0 {
		seconds = 90
	}
	return time.Duration(seconds) * time.Second
}

// Resolve normalizes the settings into a provider kind and the effective model
// name. It makes no network call.
func (s Settings) Resolve() Settings {
	if s.Model == "" {
		s.Model = "deepseek-chat"
	}
	if s.Provider == "" {
		switch {
		case s.BaseURL != "" || s.APIKey != "":
			s.Provider = "openai-compatible"
		default:
			s.Provider = "local"
		}
	}
	if s.Timeout <= 0 {
		s.Timeout = 90 * time.Second
	}
	return s
}

// Describe reports the safe-to-display projection of the configuration. The
// API key itself never leaves the process.
func (s Settings) Describe() harness.ModelProviderStatus {
	s = s.Resolve()
	if s.Provider == "local" || s.Provider == "offline" || s.Provider == "fixture" {
		return harness.ModelProviderStatus{
			Provider:      "local",
			Mode:          "offline",
			Model:         "local-conversation",
			Configured:    true,
			KeyConfigured: false,
			Detail:        "未配置远程模型，当前使用本地离线适配器",
		}
	}
	configured := s.BaseURL != "" && s.APIKey != "" && s.Model != ""
	detail := "已配置 OpenAI-compatible API"
	if !configured {
		detail = "缺少 HARNESS_MODEL_BASE_URL、HARNESS_MODEL_API_KEY 或 HARNESS_MODEL_NAME"
	}
	return harness.ModelProviderStatus{
		Provider:      "openai-compatible",
		Mode:          "remote",
		Model:         s.Model,
		BaseURL:       s.BaseURL,
		Configured:    configured,
		KeyConfigured: s.APIKey != "",
		Detail:        detail,
	}
}

// NewChatModel is the single construction boundary for conversational
// sessions. The same Runtime, tools, event log and replay path are used for
// both adapters; only the provider changes here.
func (s Settings) NewChatModel() (harness.ModelAdapter, harness.ModelProviderStatus) {
	status := s.Describe()
	if status.Provider == "local" {
		return harness.NewConversationModel(), status
	}
	if !status.Configured {
		return harness.UnavailableModel(status.Provider, status.Detail), status
	}
	return &harness.OpenAICompatibleAdapter{
		BaseURL:    s.BaseURL,
		APIKey:     s.APIKey,
		ModelName:  status.Model,
		Client:     &http.Client{Timeout: s.Timeout},
		MaxRetries: 2,
		RetryBase:  250 * time.Millisecond,
	}, status
}

// SessionFactory adapts the settings to the HTTP adapter's session hook:
// "chat" sessions get the environment-configured model and workspace tools,
// other modes keep the deterministic offline fixture.
func (s Settings) SessionFactory(store harness.Store) func(string, harness.TaskRequest, harness.AuthContext) (*harness.DemoEnvironment, error) {
	return func(mode string, request harness.TaskRequest, auth harness.AuthContext) (*harness.DemoEnvironment, error) {
		if mode == "chat" {
			return s.BuildChatEnvironment(store, request, auth)
		}
		return harness.BuildDemoEnvironmentWithStore(store, request, auth)
	}
}

// BuildChatEnvironment assembles the conversational session environment: the
// offline fixture's tools, memory and evidence plumbing, plus the
// environment-configured chat model and workspace tools.
func (s Settings) BuildChatEnvironment(store harness.Store, request harness.TaskRequest, auth harness.AuthContext) (*harness.DemoEnvironment, error) {
	env, err := harness.BuildDemoEnvironmentWithStore(store, request, auth)
	if err != nil {
		return nil, err
	}
	adapter, provider := s.NewChatModel()
	workspace := harness.NewWorkspaceTools(s.WorkspaceRoot)
	if err := workspace.Register(env.Registry); err != nil {
		return nil, err
	}
	auth.Scopes = append(auth.Scopes, "workspace:read", "workspace:write")
	env.Auth = auth
	env.Adapter = adapter
	env.Provider = provider
	if s.ReasoningEffort != "" {
		env.Request.ReasoningEffort = s.ReasoningEffort
	}
	env.Runtime.SetModel(adapter)
	env.Runtime.SetVerifier(nil)
	env.Request.ResponseSchema = nil
	return env, nil
}
