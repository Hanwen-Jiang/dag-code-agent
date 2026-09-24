package embedding

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

// modeOf reports the backend mode of a factory-produced service, asserting it
// implements ModeReporter.
func modeOf(svc EmbeddingService) string {
	mr, ok := svc.(ModeReporter)
	if !ok {
		return "<no mode>"
	}
	return mr.Mode()
}

func cosine(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func TestLocalEmbeddingDeterminism(t *testing.T) {
	svc := NewLocalEmbeddingService(256)
	ctx := context.Background()

	texts := []string{
		"The quick brown fox jumps over the lazy dog.",
		"Vector databases store embeddings for similarity search.",
		"Go is a statically typed compiled language.",
	}
	first, err := svc.Embed(ctx, texts)
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	if len(first) != len(texts) {
		t.Fatalf("expected %d vectors, got %d", len(texts), len(first))
	}
	for i, v := range first {
		if len(v) != 256 {
			t.Fatalf("vector %d has dimension %d, want 256", i, len(v))
		}
	}

	second, err := svc.Embed(ctx, texts)
	if err != nil {
		t.Fatalf("Embed (repeat) failed: %v", err)
	}
	for i := range first {
		if !reflect.DeepEqual(first[i], second[i]) {
			t.Fatalf("vector %d is not deterministic across calls", i)
		}
	}

	// A separately constructed service must produce identical vectors.
	other := NewLocalEmbeddingService(256)
	third, err := other.Embed(ctx, []string{texts[1]})
	if err != nil {
		t.Fatalf("Embed (other svc) failed: %v", err)
	}
	if !reflect.DeepEqual(first[1], third[0]) {
		t.Fatalf("vectors differ across service instances")
	}
}

func TestLocalEmbeddingNormalized(t *testing.T) {
	svc := NewLocalEmbeddingService(128)
	ctx := context.Background()
	vecs, err := svc.Embed(ctx, []string{"some text to normalize"})
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	var norm float64
	for _, v := range vecs[0] {
		norm += float64(v) * float64(v)
	}
	if math.Abs(norm-1.0) > 1e-4 {
		t.Fatalf("expected L2 norm ~1, got %v", norm)
	}
}

func TestLocalEmbeddingSimilarityOrdering(t *testing.T) {
	svc := NewLocalEmbeddingService(256)
	ctx := context.Background()

	vecs, err := svc.Embed(ctx, []string{
		"the cat sat on the mat and watched the birds",   // A
		"the cat watched the birds from the mat",         // B (overlaps A heavily)
		"quarterly financial reports were filed on time", // C (disjoint)
	})
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}

	simAB := cosine(vecs[0], vecs[1])
	simAC := cosine(vecs[0], vecs[2])
	simBC := cosine(vecs[1], vecs[2])

	t.Logf("sim(A,B)=%.4f sim(A,C)=%.4f sim(B,C)=%.4f", simAB, simAC, simBC)

	if !(simAB > simAC) {
		t.Fatalf("expected A~B more similar than A~C: %v vs %v", simAB, simAC)
	}
	if !(simAB > simBC) {
		t.Fatalf("expected A~B more similar than B~C: %v vs %v", simAB, simBC)
	}
	if simAB <= 0 {
		t.Fatalf("expected positive similarity for overlapping text, got %v", simAB)
	}
}

func TestLocalEmbeddingEmptyText(t *testing.T) {
	svc := NewLocalEmbeddingService(64)
	ctx := context.Background()
	vecs, err := svc.Embed(ctx, []string{""})
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	if len(vecs) != 1 || len(vecs[0]) != 64 {
		t.Fatalf("unexpected result shape: %d vectors", len(vecs))
	}
	for _, v := range vecs[0] {
		if v != 0 {
			t.Fatalf("expected zero vector for empty text, got non-zero element %v", v)
		}
	}
}

func TestLocalEmbeddingDimensionSanitized(t *testing.T) {
	svc := NewLocalEmbeddingService(0)
	if got := svc.Dimension(); got != 256 {
		t.Fatalf("expected sanitized dimension 256, got %d", got)
	}
	if got := svc.Mode(); got != ModeLocal {
		t.Fatalf("expected mode %q, got %q", ModeLocal, got)
	}
}

func TestLocalEmbeddingCancelledContext(t *testing.T) {
	svc := NewLocalEmbeddingService(64)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.Embed(ctx, []string{"x"}); err == nil {
		t.Fatalf("expected error for cancelled context")
	}
}

// startEmbeddingServer starts an in-process, loopback-only OpenAI-compatible
// /embeddings server. It replies with deterministic vectors and, for requests
// carrying the header "X-Reverse: 1", returns them out of order to exercise
// index-based reordering. If the Authorization header is not
// "Bearer test-key" it answers 500 to exercise the factory's ping-failure
// fallback.
func startEmbeddingServer(t *testing.T, dim int) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/embeddings" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"message":"bad api key"}}`))
			return
		}
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Model != "test-model" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"unknown model"}}`))
			return
		}
		data := make([]map[string]any, 0, len(req.Input))
		for i, in := range req.Input {
			vec := make([]float32, dim)
			for d := range vec {
				vec[d] = float32((len(in)+d)%7) / 7.0
			}
			data = append(data, map[string]any{"embedding": vec, "index": i})
		}
		if len(data) > 1 && r.Header.Get("X-Reverse") == "1" {
			for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
				data[i], data[j] = data[j], data[i]
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "model": req.Model})
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTPServiceModeAndDimension(t *testing.T) {
	svc := NewHTTPEmbeddingService("https://api.example.com/v1/", "key", "model", 384, 0)
	if svc.Mode() != ModeHTTP {
		t.Fatalf("expected mode %q, got %q", ModeHTTP, svc.Mode())
	}
	if svc.Dimension() != 384 {
		t.Fatalf("expected dimension 384, got %d", svc.Dimension())
	}
	if got := svc.endpoint(); got != "https://api.example.com/v1/embeddings" {
		t.Fatalf("unexpected endpoint %q", got)
	}

	// Sanitization: non-positive dimension -> 384.
	svc2 := NewHTTPEmbeddingService("http://x", "key", "model", 0, 0)
	if svc2.Dimension() != 384 {
		t.Fatalf("expected sanitized dimension 384, got %d", svc2.Dimension())
	}
}

func TestHTTPServiceEmbedsViaInProcessServer(t *testing.T) {
	ts := startEmbeddingServer(t, 32)
	svc := NewHTTPEmbeddingService(ts.URL, "test-key", "test-model", 32, 5*time.Second)

	inputs := []string{"alpha", "beta gamma"}
	vecs, err := svc.Embed(context.Background(), inputs)
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	if len(vecs) != 2 {
		t.Fatalf("expected 2 vectors, got %d", len(vecs))
	}
	for i, v := range vecs {
		if len(v) != 32 {
			t.Fatalf("vector %d has dimension %d, want 32", i, len(v))
		}
	}
	// The server derives vec[d] from the input length, so input order is
	// recoverable: vector 0 must match the value derived from "alpha" (len 5).
	want0 := float32((len("alpha")+0)%7) / 7.0
	if vecs[0][0] != want0 {
		t.Fatalf("expected first vector[0]=%v (from input 0), got %v", want0, vecs[0][0])
	}
	want1 := float32((len("beta gamma")+1)%7) / 7.0
	if vecs[1][1] != want1 {
		t.Fatalf("expected second vector[1]=%v (from input 1), got %v", want1, vecs[1][1])
	}
}

func TestHTTPServiceReordersOutOfOrderResponse(t *testing.T) {
	// A dedicated server variant that always reverses data[] to prove the
	// client sorts by the index field.
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input []string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		data := make([]map[string]any, 0, len(req.Input))
		for i, in := range req.Input {
			vec := make([]float32, 8)
			for d := range vec {
				vec[d] = float32(len(in))
			}
			data = append(data, map[string]any{"embedding": vec, "index": i})
		}
		for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
			data[i], data[j] = data[j], data[i]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	svc := NewHTTPEmbeddingService(srv.URL, "test-key", "test-model", 8, 5*time.Second)
	vecs, err := svc.Embed(context.Background(), []string{"aa", "bbbb"})
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	// vecs[0] must correspond to "aa" (len 2), vecs[1] to "bbbb" (len 4).
	if vecs[0][0] != 2 {
		t.Fatalf("expected vector 0 derived from 'aa' (2), got %v", vecs[0][0])
	}
	if vecs[1][0] != 4 {
		t.Fatalf("expected vector 1 derived from 'bbbb' (4), got %v", vecs[1][0])
	}
}

func TestHTTPServiceErrorOnBadStatus(t *testing.T) {
	ts := startEmbeddingServer(t, 32)
	svc := NewHTTPEmbeddingService(ts.URL, "bad-key", "test-model", 32, 5*time.Second)
	if _, err := svc.Embed(context.Background(), []string{"x"}); err == nil {
		t.Fatalf("expected error for 500 response, got nil")
	}
}

func TestHTTPServiceErrorOnDimensionMismatch(t *testing.T) {
	// Server emits 32-dim vectors; client expects 16 -> must error.
	ts := startEmbeddingServer(t, 32)
	svc := NewHTTPEmbeddingService(ts.URL, "test-key", "test-model", 16, 5*time.Second)
	if _, err := svc.Embed(context.Background(), []string{"x"}); err == nil {
		t.Fatalf("expected dimension-mismatch error, got nil")
	}
}

func TestNewEmbeddingServiceWithoutCredsReturnsLocal(t *testing.T) {
	svc, err := NewEmbeddingService(EmbeddingServiceConfig{Dimension: 128})
	if err != nil {
		t.Fatalf("NewEmbeddingService failed: %v", err)
	}
	if got := modeOf(svc); got != ModeLocal {
		t.Fatalf("expected local mode without credentials, got %q", got)
	}
	if svc.Dimension() != 128 {
		t.Fatalf("expected dimension 128, got %d", svc.Dimension())
	}
}

func TestNewEmbeddingServiceInvalidDimension(t *testing.T) {
	if _, err := NewEmbeddingService(EmbeddingServiceConfig{Dimension: 0}); err == nil {
		t.Fatalf("expected error for non-positive dimension")
	}
}

func TestNewEmbeddingServiceFallsBackToLocalOnPingFailure(t *testing.T) {
	// Port 1 has nothing listening: the startup ping must fail and the factory
	// must degrade to the local service (log-free, no error).
	svc, err := NewEmbeddingService(EmbeddingServiceConfig{
		BaseURL:   "http://127.0.0.1:1",
		APIKey:    "test-key",
		Model:     "test-model",
		Dimension: 64,
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("NewEmbeddingService should not fail on unreachable endpoint, got: %v", err)
	}
	if got := modeOf(svc); got != ModeLocal {
		t.Fatalf("expected fallback to local mode, got %q", got)
	}
	// The degraded service must actually work.
	vecs, err := svc.Embed(context.Background(), []string{"hello world"})
	if err != nil {
		t.Fatalf("degraded Embed failed: %v", err)
	}
	if len(vecs) != 1 || len(vecs[0]) != 64 {
		t.Fatalf("unexpected degraded vector shape")
	}
}

func TestNewEmbeddingServiceFallsBackToLocalOnRejectedPing(t *testing.T) {
	// Endpoint is reachable but rejects the ping (500) -> still degrade.
	ts := startEmbeddingServer(t, 64)
	svc, err := NewEmbeddingService(EmbeddingServiceConfig{
		BaseURL:   ts.URL,
		APIKey:    "bad-key", // server answers 500
		Model:     "test-model",
		Dimension: 64,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewEmbeddingService should not fail on rejected ping, got: %v", err)
	}
	if got := modeOf(svc); got != ModeLocal {
		t.Fatalf("expected fallback to local mode after rejected ping, got %q", got)
	}
}

func TestNewEmbeddingServiceWithCredsReachableServer(t *testing.T) {
	ts := startEmbeddingServer(t, 32)
	svc, err := NewEmbeddingService(EmbeddingServiceConfig{
		BaseURL:   ts.URL,
		APIKey:    "test-key",
		Model:     "test-model",
		Dimension: 32,
		Timeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewEmbeddingService failed: %v", err)
	}
	if got := modeOf(svc); got != ModeHTTP {
		t.Fatalf("expected http mode for reachable endpoint, got %q", got)
	}
	// And it must actually embed end-to-end through the factory result.
	vecs, err := svc.Embed(context.Background(), []string{"probe"})
	if err != nil {
		t.Fatalf("Embed via factory service failed: %v", err)
	}
	if len(vecs) != 1 || len(vecs[0]) != 32 {
		t.Fatalf("unexpected vector shape from http service")
	}
	// Deterministic server value for "probe" (len 5): (5+0)%7/7.
	want := float32((len("probe")+0)%7) / 7.0
	if vecs[0][0] != want {
		t.Fatalf("expected vector[0]=%v, got %v", want, vecs[0][0])
	}
}
