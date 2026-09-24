package harness

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// ModelProviderStatus is the safe-to-display projection of model configuration.
// It deliberately reports only whether a key exists; the key itself never
// leaves the Go process and is never returned by the HTTP API.
type ModelProviderStatus struct {
	Provider      string `json:"provider"`
	Mode          string `json:"mode"`
	Model         string `json:"model"`
	BaseURL       string `json:"base_url,omitempty"`
	Configured    bool   `json:"configured"`
	KeyConfigured bool   `json:"key_configured"`
	Detail        string `json:"detail"`
}

// DescribeModelProvider resolves the provider without making a network call.
// With no remote settings the project intentionally stays usable offline. A
// partial remote configuration is reported as misconfigured instead of being
// silently replaced by the local conversational adapter.
func DescribeModelProvider() ModelProviderStatus {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("HARNESS_MODEL_PROVIDER")))
	baseURL := strings.TrimSpace(os.Getenv("HARNESS_MODEL_BASE_URL"))
	apiKey := firstNonEmptyEnv("HARNESS_MODEL_API_KEY", "OPENAI_API_KEY", "DEEPSEEK_API_KEY")
	model := strings.TrimSpace(os.Getenv("HARNESS_MODEL_NAME"))
	if model == "" {
		model = "deepseek-chat"
	}

	if provider == "" {
		switch {
		case baseURL != "" || apiKey != "":
			provider = "openai-compatible"
		default:
			provider = "local"
		}
	}

	if provider == "local" || provider == "offline" || provider == "fixture" {
		return ModelProviderStatus{
			Provider:      "local",
			Mode:          "offline",
			Model:         "local-conversation",
			Configured:    true,
			KeyConfigured: false,
			Detail:        "未配置远程模型，当前使用本地离线适配器",
		}
	}

	configured := baseURL != "" && apiKey != "" && model != ""
	detail := "已配置 OpenAI-compatible API"
	if !configured {
		detail = "缺少 HARNESS_MODEL_BASE_URL、HARNESS_MODEL_API_KEY 或 HARNESS_MODEL_NAME"
	}
	return ModelProviderStatus{
		Provider:      "openai-compatible",
		Mode:          "remote",
		Model:         model,
		BaseURL:       redactBaseURL(baseURL),
		Configured:    configured,
		KeyConfigured: apiKey != "",
		Detail:        detail,
	}
}

// NewChatModelFromEnv is the single construction boundary for conversational
// sessions. The same Runtime, tools, event log and replay path are used for
// both adapters; only the model provider changes here.
func NewChatModelFromEnv() (ModelAdapter, ModelProviderStatus) {
	status := DescribeModelProvider()
	if status.Provider == "local" {
		return NewConversationModel(), status
	}
	if !status.Configured {
		return unavailableModel{provider: status.Provider, detail: status.Detail}, status
	}
	return &OpenAICompatibleAdapter{
		BaseURL:    strings.TrimSpace(os.Getenv("HARNESS_MODEL_BASE_URL")),
		APIKey:     firstNonEmptyEnv("HARNESS_MODEL_API_KEY", "OPENAI_API_KEY", "DEEPSEEK_API_KEY"),
		ModelName:  status.Model,
		Client:     &http.Client{Timeout: providerTimeout()},
		MaxRetries: 2,
		RetryBase:  250 * time.Millisecond,
	}, status
}

type unavailableModel struct {
	provider string
	detail   string
}

func (m unavailableModel) Name() string { return m.provider + ":not-configured" }
func (m unavailableModel) Complete(context.Context, ModelRequest) (ModelResponse, error) {
	return ModelResponse{}, &ModelError{Message: "MODEL_PROVIDER_NOT_CONFIGURED: " + m.detail, Provider: m.provider, Retryable: false}
}
func (m unavailableModel) Stream(context.Context, ModelRequest) (<-chan StreamEvent, error) {
	return nil, &ModelError{Message: "MODEL_PROVIDER_NOT_CONFIGURED: " + m.detail, Provider: m.provider, Retryable: false}
}

func firstNonEmptyEnv(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func providerTimeout() time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(os.Getenv("HARNESS_MODEL_TIMEOUT_SECONDS")))
	if err != nil || seconds <= 0 {
		seconds = 90
	}
	return time.Duration(seconds) * time.Second
}

func redactBaseURL(value string) string {
	value = strings.TrimRight(strings.TrimSpace(value), "/")
	if value == "" {
		return ""
	}
	return value
}

var _ ModelAdapter = unavailableModel{}
