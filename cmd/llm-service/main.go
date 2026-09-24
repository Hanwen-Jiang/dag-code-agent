// LLM Service Layer - multi-provider model gateway.
//
// Real behavior:
//   - OpenAI-compatible HTTP providers (OpenAI / DeepSeek / any compatible gateway)
//   - deterministic local offline provider when no API key is configured, so the
//     service is fully runnable standalone (graceful degradation)
//   - token-bucket rate limiting, digest-keyed LRU response cache with TTL,
//     per-model metrics (counters, latency p50/p95, cost tracking)
//
// Endpoints: /health /models /complete /chat/completions /embeddings /metrics
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/embedding"
)

// ---------- Config ----------

type Config struct {
	Host             string        `yaml:"host" json:"host"`
	Port             int           `yaml:"port" json:"port"`
	ModelProviders   []ModelConfig `yaml:"model_providers" json:"model_providers"`
	DefaultModelTier string        `yaml:"default_model_tier" json:"default_model_tier"`
	RateLimitPerMin  int           `yaml:"rate_limit_per_min" json:"rate_limit_per_min"`
	CacheEnabled     bool          `yaml:"cache_enabled" json:"cache_enabled"`
}

type ModelConfig struct {
	Name       string            `yaml:"name" json:"name"`
	APIKey     string            `yaml:"api_key" json:"api_key"`
	BaseURL    string            `yaml:"base_url" json:"base_url"`
	Models     map[string]string `yaml:"models" json:"models"` // model_id -> display name
	Tier       string            `yaml:"tier" json:"tier"`
	TokenPrice float64           `yaml:"token_price" json:"token_price"` // USD per 1K total tokens
}

func (c ModelConfig) Configured() bool { return c.APIKey != "" && c.BaseURL != "" }

func loadLLMConfig() *Config {
	return &Config{
		Host:             getEnv("LLM_SERVICE_HOST", "localhost"),
		Port:             firstEnvInt(8081, "LLM_SERVICE_PORT", "PORT"),
		DefaultModelTier: "production",
		RateLimitPerMin:  getEnvInt("LLM_SERVICE_RATE_LIMIT", 100),
		CacheEnabled:     getEnv("LLM_SERVICE_CACHE", "true") == "true",
		ModelProviders: []ModelConfig{
			{Name: "openai", APIKey: firstNonEmptyEnv("OPENAI_API_KEY"), BaseURL: getEnv("OPENAI_BASE_URL", "https://api.openai.com/v1"), Tier: "production", TokenPrice: 0.003,
				Models: map[string]string{"gpt-4o": "GPT-4o", "gpt-4o-mini": "GPT-4o Mini"}},
			{Name: "deepseek", APIKey: firstNonEmptyEnv("DEEPSEEK_API_KEY"), BaseURL: getEnv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1"), Tier: "production", TokenPrice: 0.002,
				Models: map[string]string{"deepseek-chat": "DeepSeek Chat", "deepseek-reasoner": "DeepSeek Reasoner"}},
		},
	}
}

// ---------- Request / Response DTOs ----------

type Message struct {
	Role      string     `json:"role"`
	Content   string     `json:"content"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
}

type ToolCall struct {
	ID       string   `json:"id,omitempty"`
	Type     string   `json:"type,omitempty"`
	Function Function `json:"function"`
}

type Function struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type Response struct {
	ID        string   `json:"id"`
	ModelUsed string   `json:"model_used"`
	Content   string   `json:"content"`
	Choices   []Choice `json:"choices,omitempty"`
	Usage     Usage    `json:"usage"`
	Cached    bool     `json:"cached"`
	Provider  string   `json:"provider"`
	Error     string   `json:"error,omitempty"`
}

// ---------- Provider abstraction ----------

type CompleteRequest struct {
	Model       string
	Messages    []Message
	MaxTokens   int
	Temperature float64
}

type CompleteResponse struct {
	Content  string
	Model    string
	Provider string
	Usage    Usage
}

type Provider interface {
	Name() string
	Complete(ctx context.Context, req CompleteRequest) (*CompleteResponse, error)
}

// openaiProvider is the real HTTP implementation for any OpenAI-compatible
// endpoint (OpenAI, DeepSeek, local gateways).
type openaiProvider struct {
	cfg    ModelConfig
	client *http.Client
}

func newOpenAIProvider(cfg ModelConfig) *openaiProvider {
	return &openaiProvider{cfg: cfg, client: &http.Client{Timeout: 120 * time.Second}}
}

func (p *openaiProvider) Name() string { return p.cfg.Name }

func (p *openaiProvider) Complete(ctx context.Context, req CompleteRequest) (*CompleteResponse, error) {
	body := map[string]any{
		"model":       req.Model,
		"messages":    req.Messages,
		"max_tokens":  req.MaxTokens,
		"temperature": req.Temperature,
		"stream":      false,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.cfg.BaseURL, "/")+"/chat/completions", strings.NewReader(string(raw)))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.cfg.APIKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("provider %s: %w", p.cfg.Name, err)
	}
	defer resp.Body.Close()
	var payload struct {
		Choices []struct {
			Message      Message `json:"message"`
			FinishReason string  `json:"finish_reason"`
		} `json:"choices"`
		Usage Usage `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("provider %s: decode: %w", p.cfg.Name, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider %s: HTTP %d", p.cfg.Name, resp.StatusCode)
	}
	if len(payload.Choices) == 0 {
		return nil, fmt.Errorf("provider %s: empty choices", p.cfg.Name)
	}
	return &CompleteResponse{
		Content:  payload.Choices[0].Message.Content,
		Model:    req.Model,
		Provider: p.cfg.Name,
		Usage:    payload.Usage,
	}, nil
}

// localProvider is the offline deterministic degradation path: it keeps the
// gateway usable with no keys. It answers with structured, inspectable text.
type localProvider struct{}

func (p *localProvider) Name() string { return "local-offline" }

func (p *localProvider) Complete(ctx context.Context, req CompleteRequest) (*CompleteResponse, error) {
	_ = ctx
	var lastUser string
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			lastUser = req.Messages[i].Content
			break
		}
	}
	if lastUser == "" {
		lastUser = "(no user message)"
	}
	answer := fmt.Sprintf(
		"[local-offline provider] processed %d message(s); last user message: %q. Configure an API key (OPENAI_API_KEY / DEEPSEEK_API_KEY) for a real model.",
		len(req.Messages), truncate(lastUser, 200),
	)
	return &CompleteResponse{
		Content:  answer,
		Model:    req.Model,
		Provider: p.Name(),
		Usage:    Usage{PromptTokens: estimateTokens(req.Messages), CompletionTokens: estimateTokens(answer), TotalTokens: estimateTokens(req.Messages) + estimateTokens(answer)},
	}, nil
}

func estimateTokens(v any) int {
	b, _ := json.Marshal(v)
	return (len(b) + 3) / 4
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---------- Rate limiter (real token bucket) ----------

type RateLimiter struct {
	mu         sync.Mutex
	rate       float64 // tokens per second
	burst      float64
	tokens     float64
	lastRefill time.Time
}

func NewRateLimiter(perMinute int) *RateLimiter {
	if perMinute <= 0 {
		perMinute = 100
	}
	r := float64(perMinute) / 60.0
	return &RateLimiter{rate: r, burst: float64(perMinute), tokens: float64(perMinute), lastRefill: time.Now()}
}

func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.tokens += now.Sub(r.lastRefill).Seconds() * r.rate
	if r.tokens > r.burst {
		r.tokens = r.burst
	}
	r.lastRefill = now
	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

// ---------- Response cache (LRU + TTL) ----------

type ResponseCache struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	items map[string]*cacheEntry
	order []string // newest first
}

type cacheEntry struct {
	value     *CompleteResponse
	expiresAt time.Time
}

func NewResponseCache(cap int, ttl time.Duration) *ResponseCache {
	if cap <= 0 {
		cap = 1000
	}
	return &ResponseCache{cap: cap, ttl: ttl, items: map[string]*cacheEntry{}}
}

func (c *ResponseCache) Get(key string) (*CompleteResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expiresAt) {
		delete(c.items, key)
		c.removeOrder(key)
		return nil, false
	}
	c.moveToFront(key)
	out := *e.value
	return &out, true
}

func (c *ResponseCache) Set(key string, value *CompleteResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.items[key]; !exists {
		c.order = append([]string{key}, c.order...)
		if len(c.order) > c.cap {
			evict := c.order[len(c.order)-1]
			c.order = c.order[:len(c.order)-1]
			delete(c.items, evict)
		}
	} else {
		c.moveToFront(key)
	}
	c.items[key] = &cacheEntry{value: value, expiresAt: time.Now().Add(c.ttl)}
}

func (c *ResponseCache) removeOrder(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

func (c *ResponseCache) moveToFront(key string) {
	c.removeOrder(key)
	c.order = append([]string{key}, c.order...)
}

// ---------- Metrics ----------

type MetricsCollector struct {
	mu      sync.Mutex
	models  map[string]*modelMetrics
	started time.Time
}

type modelMetrics struct {
	requests    int
	errors      int
	cacheHits   int
	tokens      int
	costUSD     float64
	latenciesMS []float64
	lastLatency time.Time
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{models: map[string]*modelMetrics{}, started: time.Now()}
}

func (m *MetricsCollector) Record(model string, tokens int, costUSD float64, latMS float64, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mm := m.models[model]
	if mm == nil {
		mm = &modelMetrics{}
		m.models[model] = mm
	}
	mm.requests++
	mm.tokens += tokens
	mm.costUSD += costUSD
	mm.latenciesMS = append(mm.latenciesMS, latMS)
	if !ok {
		mm.errors++
	}
}

func (m *MetricsCollector) RecordCacheHit(model string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mm := m.models[model]
	if mm == nil {
		mm = &modelMetrics{}
		m.models[model] = mm
	}
	mm.cacheHits++
}

func (m *MetricsCollector) Snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{"uptime_seconds": time.Since(m.started).Seconds(), "models": map[string]any{}}
	for name, mm := range m.models {
		lats := append([]float64(nil), mm.latenciesMS...)
		out["models"].(map[string]any)[name] = map[string]any{
			"requests": mm.requests, "errors": mm.errors, "cache_hits": mm.cacheHits,
			"total_tokens": mm.tokens, "cost_usd": mm.costUSD,
			"latency_p50_ms": percentile(lats, 0.50), "latency_p95_ms": percentile(lats, 0.95),
		}
	}
	return out
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted) && sorted[j] < sorted[i]; j++ {
			sortSwap(&sorted[i], &sorted[j])
		}
	}
	idx := int(p * float64(len(sorted)-1))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func sortSwap(a, b *float64) { *a, *b = *b, *a }

// ---------- Service ----------

type LLMService struct {
	config      *Config
	providers   map[string]Provider // provider name -> provider
	local       Provider
	cache       *ResponseCache
	metrics     *MetricsCollector
	rateLimiter *RateLimiter
	embeddings  embedding.EmbeddingService
	logger      *zap.Logger
	mu          sync.RWMutex
}

func NewLLMService(config *Config, logger *zap.Logger) *LLMService {
	svc := &LLMService{
		config:      config,
		providers:   map[string]Provider{},
		local:       &localProvider{},
		cache:       NewResponseCache(1000, 10*time.Minute),
		metrics:     NewMetricsCollector(),
		rateLimiter: NewRateLimiter(config.RateLimitPerMin),
		embeddings:  embedding.NewLocalEmbeddingService(256),
		logger:      logger,
	}
	for _, mc := range config.ModelProviders {
		if mc.Configured() {
			svc.providers[mc.Name] = newOpenAIProvider(mc)
		} else {
			logger.Warn("provider not configured (missing api_key/base_url), using offline degradation",
				zap.String("provider", mc.Name))
		}
	}
	if len(svc.providers) == 0 {
		logger.Warn("no real providers configured; all completions will use the local offline provider")
	}
	return svc
}

// selectModelForTier returns (modelID, providerName, provider, ok).
func (svc *LLMService) selectModelForTier(tier string) (string, string, Provider, bool) {
	for _, mc := range svc.config.ModelProviders {
		if mc.Tier != tier {
			continue
		}
		for modelID := range mc.Models {
			p, ok := svc.providers[mc.Name]
			if ok {
				return modelID, mc.Name, p, true
			}
		}
	}
	// Degradation: fall back to any configured provider in any tier, then local.
	for _, mc := range svc.config.ModelProviders {
		for modelID := range mc.Models {
			if p, ok := svc.providers[mc.Name]; ok {
				return modelID, mc.Name, p, true
			}
		}
	}
	return "local", "local-offline", svc.local, true
}

func (svc *LLMService) providerForModel(modelID string) (string, Provider, bool) {
	for _, mc := range svc.config.ModelProviders {
		if _, ok := mc.Models[modelID]; ok {
			if p, ok := svc.providers[mc.Name]; ok {
				return mc.Name, p, true
			}
		}
	}
	return "", nil, false
}

func (svc *LLMService) Complete(ctx context.Context, modelID string, messages []Message, maxTokens int) (*Response, error) {
	if !svc.rateLimiter.Allow() {
		return nil, fmt.Errorf("rate limit exceeded (per-minute budget exhausted)")
	}

	var provider Provider
	var providerName, model string
	if modelID != "" {
		if pn, p, ok := svc.providerForModel(modelID); ok {
			providerName, provider, model = pn, p, modelID
		} else {
			return nil, fmt.Errorf("unknown model %q (use /models to list)", modelID)
		}
	} else {
		var ok bool
		model, providerName, provider, ok = svc.selectModelForTier(svc.config.DefaultModelTier)
		if !ok {
			return nil, fmt.Errorf("no model available for tier %s", svc.config.DefaultModelTier)
		}
	}

	compReq := CompleteRequest{Model: model, Messages: messages, MaxTokens: maxTokens, Temperature: 0.7}
	key := cacheKeyForRequest(model, providerName, compReq)
	if svc.config.CacheEnabled {
		if cached, hit := svc.cache.Get(key); hit {
			svc.metrics.RecordCacheHit(model)
			resp := toResponse(cached)
			resp.Cached = true
			return resp, nil
		}
	}

	start := time.Now()
	result, err := provider.Complete(ctx, compReq)
	lat := float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		svc.metrics.Record(model, 0, 0, lat, false)
		// Degradation: a failed real provider falls back to the offline provider
		// so the gateway keeps serving (flagged in the response).
		if providerName != "local-offline" {
			svc.logger.Warn("provider failed, degrading to local offline provider",
				zap.String("provider", providerName), zap.Error(err))
			fallback, ferr := svc.local.Complete(ctx, compReq)
			if ferr != nil {
				return nil, err
			}
			result = fallback
		} else {
			return nil, err
		}
	}
	svc.metrics.Record(model, result.Usage.TotalTokens, svc.costFor(model, result.Usage.TotalTokens), lat, err == nil)
	if svc.config.CacheEnabled {
		svc.cache.Set(key, result)
	}
	return toResponse(result), nil
}

func (svc *LLMService) costFor(modelID string, tokens int) float64 {
	for _, mc := range svc.config.ModelProviders {
		if _, ok := mc.Models[modelID]; ok {
			return float64(tokens) / 1000.0 * mc.TokenPrice
		}
	}
	return 0
}

func toResponse(r *CompleteResponse) *Response {
	return &Response{
		ID:        uuid.NewString(),
		ModelUsed: r.Model,
		Provider:  r.Provider,
		Content:   r.Content,
		Usage:     r.Usage,
		Choices:   []Choice{{Index: 0, Message: Message{Role: "assistant", Content: r.Content}, FinishReason: "stop"}},
	}
}

func cacheKeyForRequest(model, provider string, req CompleteRequest) string {
	b, _ := json.Marshal(map[string]any{"model": model, "provider": provider, "messages": req.Messages, "max_tokens": req.MaxTokens})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ---------- HTTP handlers ----------

func setupRoutes(mux *http.ServeMux, svc *LLMService, logger *zap.Logger) {
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) { handleModels(w, r, svc) })
	mux.HandleFunc("/complete", func(w http.ResponseWriter, r *http.Request) { handleComplete(w, r, svc, logger) })
	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) { handleChatCompletions(w, r, svc, logger) })
	mux.HandleFunc("/embeddings", func(w http.ResponseWriter, r *http.Request) { handleEmbeddings(w, r, svc) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, svc.metrics.Snapshot())
	})
}

type completeRequest struct {
	Model     string    `json:"model"`
	Messages  []Message `json:"messages"`
	Query     string    `json:"query"` // convenience single-query form
	MaxTokens int       `json:"max_tokens"`
	Tier      string    `json:"model_tier"`
}

func (c completeRequest) normalized() []Message {
	if len(c.Messages) > 0 {
		return c.Messages
	}
	if c.Query != "" {
		return []Message{{Role: "user", Content: c.Query}}
	}
	return nil
}

func handleComplete(w http.ResponseWriter, r *http.Request, svc *LLMService, logger *zap.Logger) {
	var req completeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if len(req.normalized()) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "messages or query required"})
		return
	}
	if req.Tier != "" {
		// Tier selection: ignore explicit model, pick from the tier.
		req.Model = ""
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	resp, err := svc.Complete(ctx, req.Model, req.normalized(), req.MaxTokens)
	if err != nil {
		logger.Warn("completion failed", zap.Error(err))
		writeJSON(w, http.StatusBadGateway, Response{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleChatCompletions(w http.ResponseWriter, r *http.Request, svc *LLMService, logger *zap.Logger) {
	var req struct {
		Model     string    `json:"model"`
		Messages  []Message `json:"messages"`
		MaxTokens int       `json:"max_tokens"`
		Stream    bool      `json:"stream"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.Stream {
		// Streaming is served by the control plane's SSE projection; this
		// gateway is non-streaming. Report it explicitly instead of hanging.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "stream=true not supported by this gateway; use non-streaming requests"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()
	resp, err := svc.Complete(ctx, req.Model, req.Messages, req.MaxTokens)
	if err != nil {
		logger.Warn("chat completion failed", zap.Error(err))
		writeJSON(w, http.StatusBadGateway, Response{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleEmbeddings(w http.ResponseWriter, r *http.Request, svc *LLMService) {
	var req struct {
		Texts []string `json:"texts"`
		Input any      `json:"input"` // OpenAI-compatible: string or []string
		Model string   `json:"model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	texts := req.Texts
	switch v := req.Input.(type) {
	case string:
		texts = []string{v}
	case []any:
		for _, t := range v {
			if s, ok := t.(string); ok {
				texts = append(texts, s)
			}
		}
	}
	if len(texts) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "texts or input required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	vectors, err := svc.embeddings.Embed(ctx, texts)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model":      "local-embedding",
		"dimension":  svc.embeddings.Dimension(),
		"embeddings": vectors,
		"usage":      Usage{TotalTokens: estimateTokens(texts)},
	})
}

func handleModels(w http.ResponseWriter, r *http.Request, svc *LLMService) {
	models := []map[string]any{}
	for _, mc := range svc.config.ModelProviders {
		for id, display := range mc.Models {
			_, _, ok := svc.providerForModel(id)
			models = append(models, map[string]any{
				"id": id, "name": display, "provider": mc.Name, "tier": mc.Tier,
				"configured":         ok,
				"token_price_usd_1k": mc.TokenPrice,
			})
		}
	}
	models = append(models, map[string]any{"id": "local", "name": "Local Offline (degraded)", "provider": "local-offline", "tier": "offline", "configured": true})
	writeJSON(w, http.StatusOK, map[string]any{"count": len(models), "models": models, "default_tier": svc.config.DefaultModelTier})
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
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// firstEnvInt returns the first set environment variable among keys that
// parses as an int, else def.
func firstEnvInt(def int, keys ...string) int {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
	}
	return def
}

func firstNonEmptyEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func main() {
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	cfg := loadLLMConfig()
	service := NewLLMService(cfg, logger)

	mux := http.NewServeMux()
	setupRoutes(mux, service, logger)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 120 * time.Second,
	}

	logger.Info("LLM Service starting",
		zap.Int("port", cfg.Port),
		zap.Int("configured_providers", len(service.providers)),
		zap.Bool("cache", cfg.CacheEnabled))

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("server error", zap.Error(err))
	}
}
