package harness

import (
	"context"
	"net/http"
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

// OpenAIAdapterConfig is the explicit configuration for a remote
// OpenAI-compatible adapter. The core never reads the environment; callers
// (typically internal/modelenv) translate env vars into this struct.
type OpenAIAdapterConfig struct {
	BaseURL    string
	APIKey     string
	Model      string
	Timeout    time.Duration
	MaxRetries int
	RetryBase  time.Duration
}

// NewOpenAICompatibleAdapter builds a remote adapter from explicit settings.
func NewOpenAICompatibleAdapter(cfg OpenAIAdapterConfig) *OpenAICompatibleAdapter {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	retries := cfg.MaxRetries
	if retries == 0 {
		retries = 2
	}
	retryBase := cfg.RetryBase
	if retryBase <= 0 {
		retryBase = 250 * time.Millisecond
	}
	return &OpenAICompatibleAdapter{
		BaseURL:    cfg.BaseURL,
		APIKey:     cfg.APIKey,
		ModelName:  cfg.Model,
		Client:     &http.Client{Timeout: timeout},
		MaxRetries: retries,
		RetryBase:  retryBase,
	}
}

// UnavailableModel returns an adapter that fails loudly with
// MODEL_PROVIDER_NOT_CONFIGURED instead of silently degrading a partially
// configured remote provider to the offline fixture.
func UnavailableModel(provider, detail string) ModelAdapter {
	return unavailableModel{provider: provider, detail: detail}
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

var _ ModelAdapter = unavailableModel{}
