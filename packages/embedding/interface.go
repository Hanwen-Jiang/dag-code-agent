// Package embedding provides a unified abstraction over text-embedding
// providers.
//
// It offers:
//   - HTTPEmbeddingService: a pure net/http client for any OpenAI-compatible
//     /embeddings endpoint (POST {model, input} -> data[].embedding).
//   - LocalEmbeddingService: a deterministic, dependency-free hashed n-gram
//     bag-of-features embedding. This is a graceful-degradation path, not a
//     semantic model: it yields stable vectors for identical input and
//     reasonable cosine similarity for overlapping text, but it does not
//     capture meaning.
//   - NewEmbeddingService: a factory that prefers HTTP when configured,
//     pings it at startup, and falls back to the local service (log-free) on
//     failure. The returned service always exposes Mode() ("http"|"local").
package embedding

import (
	"context"
	"fmt"
	"time"
)

// EmbeddingService converts text to fixed-dimension float32 vectors.
type EmbeddingService interface {
	// Embed returns one vector per input text, in input order.
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	// Dimension returns the embedding dimensionality.
	Dimension() int
}

// ModeReporter is implemented by embedding services that can report which
// backend they are currently using. Services returned by
// NewEmbeddingService always implement this interface.
type ModeReporter interface {
	// Mode reports the active backend: "http" or "local".
	Mode() string
}

// ModeHTTP indicates a live HTTP embeddings backend.
const ModeHTTP = "http"

// ModeLocal indicates the deterministic degraded (hashed n-gram) backend.
const ModeLocal = "local"

// EmbeddingServiceConfig selects and configures the embedding backend.
type EmbeddingServiceConfig struct {
	// BaseURL is the OpenAI-compatible API base, e.g.
	// "https://api.openai.com/v1". When BaseURL or APIKey is empty the local
	// degraded service is used.
	BaseURL string
	// APIKey authenticates the HTTP service.
	APIKey string
	// Model is the embedding model name sent in the request body.
	Model string
	// Dimension is the expected vector dimension (e.g. 1536).
	Dimension int
	// Timeout bounds HTTP requests. Zero means 30s.
	Timeout time.Duration
}

// NewEmbeddingService builds an EmbeddingService from cfg.
//
// Selection rule:
//   - If cfg.BaseURL and cfg.APIKey are both non-empty, an HTTPEmbeddingService
//     is created and pinged at startup (a single tiny embed call). If the ping
//     fails, the service degrades to a LocalEmbeddingService — no logging, no
//     panic; the caller can still detect which mode is active via Mode().
//   - Otherwise, a LocalEmbeddingService is returned directly.
//
// The returned service is always functional on return and always implements
// ModeReporter (Mode() returns "http" or "local"). The only error is a
// non-positive dimension.
func NewEmbeddingService(cfg EmbeddingServiceConfig) (EmbeddingService, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("embedding: dimension must be positive, got %d", cfg.Dimension)
	}
	if cfg.BaseURL == "" || cfg.APIKey == "" {
		return NewLocalEmbeddingService(cfg.Dimension), nil
	}

	svc := NewHTTPEmbeddingService(cfg.BaseURL, cfg.APIKey, cfg.Model, cfg.Dimension, cfg.Timeout)
	// Startup ping: a real, minimal embed call. On any failure, fall back to
	// the deterministic local service (log-free degradation path).
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeoutOrDefault())
	defer cancel()
	if _, err := svc.Embed(ctx, []string{"ping"}); err != nil {
		return NewLocalEmbeddingService(cfg.Dimension), nil
	}
	return svc, nil
}

// timeoutOrDefault returns the configured timeout or a sensible default.
func (c EmbeddingServiceConfig) timeoutOrDefault() time.Duration {
	if c.Timeout <= 0 {
		return 30 * time.Second
	}
	return c.Timeout
}
