package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// defaultHTTPTimeout bounds requests when the caller passes no timeout.
const defaultHTTPTimeout = 30 * time.Second

// maxResponseBody caps how much of a non-2xx response body is retained in
// error messages (guards against huge error pages).
const maxResponseBody = 4 << 10

// HTTPEmbeddingService is a pure net/http client for OpenAI-compatible
// /embeddings endpoints. It POSTs {"model": ..., "input": [...]} and parses
// data[].embedding, ordering results by the reported index.
type HTTPEmbeddingService struct {
	baseURL   string
	apiKey    string
	model     string
	dimension int
	client    *http.Client
}

// NewHTTPEmbeddingService constructs an HTTPEmbeddingService. The baseURL is
// an OpenAI-compatible base (e.g. "https://api.openai.com/v1"); the service
// POSTs to baseURL + "/embeddings". A non-positive dimension is sanitized to
// 384; a non-positive timeout becomes 30s.
func NewHTTPEmbeddingService(baseURL, apiKey, model string, dimension int, timeout time.Duration) *HTTPEmbeddingService {
	if dimension <= 0 {
		dimension = 384
	}
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	return &HTTPEmbeddingService{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		model:     model,
		dimension: dimension,
		client:    &http.Client{Timeout: timeout},
	}
}

// Embed embeds all texts in a single request, preserving input order.
func (s *HTTPEmbeddingService) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]any{
		"model": s.model,
		"input": texts,
	}); err != nil {
		return nil, fmt.Errorf("embedding: encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint(), &buf)
	if err != nil {
		return nil, fmt.Errorf("embedding: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.apiKey)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding: request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("embedding: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding: http %d: %s",
			resp.StatusCode, strings.TrimSpace(string(truncateBytes(body, maxResponseBody))))
	}

	var parsed struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("embedding: decode response: %w", err)
	}
	if len(parsed.Data) != len(texts) {
		return nil, fmt.Errorf("embedding: expected %d vectors, got %d", len(texts), len(parsed.Data))
	}

	// Order by the provider's index field (OpenAI may return out of order).
	sort.SliceStable(parsed.Data, func(i, j int) bool {
		return parsed.Data[i].Index < parsed.Data[j].Index
	})

	out := make([][]float32, len(parsed.Data))
	for i, item := range parsed.Data {
		if len(item.Embedding) != s.dimension {
			return nil, fmt.Errorf("embedding: vector %d has dimension %d, expected %d",
				i, len(item.Embedding), s.dimension)
		}
		out[i] = item.Embedding
	}
	return out, nil
}

// Dimension implements EmbeddingService.
func (s *HTTPEmbeddingService) Dimension() int { return s.dimension }

// Mode implements EmbeddingService.
func (s *HTTPEmbeddingService) Mode() string { return ModeHTTP }

func (s *HTTPEmbeddingService) endpoint() string {
	return s.baseURL + "/embeddings"
}

func truncateBytes(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[:n]
}
