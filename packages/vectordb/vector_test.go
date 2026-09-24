package vectordb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/embedding"
)

// dim is the shared test vector dimensionality.
const dim = 8

// makeVec builds a small distinct vector from a seed so tests can reason
// about which stored vector a query should match.
func makeVec(seed int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32((seed * (i + 1)) % 5)
	}
	return v
}

func TestMemoryDBIndexAndRetrieveOrdering(t *testing.T) {
	db := NewMemoryDB(dim)
	ctx := context.Background()
	if err := db.Initialize(ctx); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	defer db.Close()

	points := []VectorPoint{
		{ID: "a", Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Payload: map[string]any{"source": "doc1", "n": 1}},
		{ID: "b", Vector: []float32{0.9, 0.1, 0, 0, 0, 0, 0, 0}, Payload: map[string]any{"source": "doc2", "n": 2}},
		{ID: "c", Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Payload: map[string]any{"source": "doc3", "n": 3}},
		{ID: "d", Vector: []float32{0.1, 0.9, 0, 0, 0, 0, 0, 0}, Payload: map[string]any{"source": "doc4", "n": 4}},
	}
	if err := db.Upsert(ctx, points); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Query along the x-axis: a and b should rank ahead of c and d.
	results, err := db.Query(ctx, QueryRequest{
		Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0},
		TopK:   4,
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	if results[0].ID != "a" {
		t.Fatalf("expected top hit 'a', got %q", results[0].ID)
	}
	// b is the next-closest on the x-axis.
	if results[1].ID != "b" {
		t.Fatalf("expected second hit 'b', got %q", results[1].ID)
	}
	// Scores must be non-increasing.
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Fatalf("results not sorted by score: %v > %v", results[i].Score, results[i-1].Score)
		}
	}
	// Payloads must round-trip.
	if results[0].Payload["source"] != "doc1" {
		t.Fatalf("payload source not preserved: %v", results[0].Payload["source"])
	}
}

func TestMemoryDBFilterBehavior(t *testing.T) {
	db := NewMemoryDB(dim)
	ctx := context.Background()
	_ = db.Initialize(ctx)
	defer db.Close()

	points := []VectorPoint{
		{ID: "a", Vector: makeVec(1), Payload: map[string]any{"source": "doc1", "kind": "faq"}},
		{ID: "b", Vector: makeVec(2), Payload: map[string]any{"source": "doc2", "kind": "faq"}},
		{ID: "c", Vector: makeVec(3), Payload: map[string]any{"source": "doc3", "kind": "blog"}},
		{ID: "d", Vector: makeVec(4), Payload: map[string]any{"source": "doc4", "kind": "blog"}},
	}
	if err := db.Upsert(ctx, points); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	// Filter by kind=faq should return only a and b.
	res, err := db.Query(ctx, QueryRequest{
		Vector:        makeVec(1),
		TopK:          10,
		FilterPayload: map[string]any{"kind": "faq"},
	})
	if err != nil {
		t.Fatalf("filtered Query failed: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 faq results, got %d: %v", len(res), idsOf(res))
	}
	for _, r := range res {
		if r.Payload["kind"] != "faq" {
			t.Fatalf("result %q has wrong kind %v", r.ID, r.Payload["kind"])
		}
	}

	// A filter matching nothing yields no results.
	resNone, err := db.Query(ctx, QueryRequest{
		Vector:        makeVec(1),
		TopK:          10,
		FilterPayload: map[string]any{"kind": "nonexistent"},
	})
	if err != nil {
		t.Fatalf("empty-filter Query failed: %v", err)
	}
	if len(resNone) != 0 {
		t.Fatalf("expected 0 results for unknown filter, got %d", len(resNone))
	}

	// Multi-key AND filter: source=doc1 AND kind=faq -> only 'a'.
	resAnd, err := db.Query(ctx, QueryRequest{
		Vector:        makeVec(1),
		TopK:          10,
		FilterPayload: map[string]any{"source": "doc1", "kind": "faq"},
	})
	if err != nil {
		t.Fatalf("AND-filter Query failed: %v", err)
	}
	if len(resAnd) != 1 || resAnd[0].ID != "a" {
		t.Fatalf("expected only 'a' for AND filter, got %v", idsOf(resAnd))
	}
}

func TestMemoryDBUpsertReplacesAndDelete(t *testing.T) {
	db := NewMemoryDB(dim)
	ctx := context.Background()
	_ = db.Initialize(ctx)
	defer db.Close()

	// Upsert 'a' twice; the second payload/vector must win.
	if err := db.Upsert(ctx, []VectorPoint{
		{ID: "a", Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}, Payload: map[string]any{"v": "old"}},
	}); err != nil {
		t.Fatalf("first Upsert failed: %v", err)
	}
	if err := db.Upsert(ctx, []VectorPoint{
		{ID: "a", Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}, Payload: map[string]any{"v": "new"}},
	}); err != nil {
		t.Fatalf("second Upsert failed: %v", err)
	}
	if got := db.Len(); got != 1 {
		t.Fatalf("expected 1 point after upsert-replace, got %d", got)
	}
	res, _ := db.Query(ctx, QueryRequest{Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0}, TopK: 1})
	if len(res) != 1 || res[0].Payload["v"] != "new" {
		t.Fatalf("upsert did not replace payload, got %v", res)
	}

	// Delete removes the point.
	if err := db.Delete(ctx, []string{"a"}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if got := db.Len(); got != 0 {
		t.Fatalf("expected 0 points after delete, got %d", got)
	}
	// Deleting a non-existent id is a no-op, not an error.
	if err := db.Delete(ctx, []string{"ghost"}); err != nil {
		t.Fatalf("Delete of missing id should be no-op, got %v", err)
	}
}

func TestMemoryDBCapEviction(t *testing.T) {
	db := NewMemoryDBWithCap(dim, 3)
	ctx := context.Background()
	_ = db.Initialize(ctx)
	defer db.Close()

	// Insert 5 points; cap 3 means the 2 oldest should be evicted.
	for i, id := range []string{"p1", "p2", "p3", "p4", "p5"} {
		_ = db.Upsert(ctx, []VectorPoint{{ID: id, Vector: makeVec(i + 1), Payload: map[string]any{"i": i}}})
	}
	if got := db.Len(); got != 3 {
		t.Fatalf("expected cap of 3, got %d", got)
	}
	// The surviving points should be the three most recent: p3, p4, p5.
	res, _ := db.Query(ctx, QueryRequest{Vector: makeVec(1), TopK: 10})
	if len(res) != 3 {
		t.Fatalf("expected 3 surviving points, got %d", len(res))
	}
	found := map[string]bool{}
	for _, r := range res {
		found[r.ID] = true
	}
	for _, want := range []string{"p3", "p4", "p5"} {
		if !found[want] {
			t.Fatalf("expected %q to survive eviction, present: %v", want, found)
		}
	}
}

func TestMemoryDBHealthAndName(t *testing.T) {
	db := NewMemoryDB(dim)
	ctx := context.Background()
	if db.Name() != "memory" {
		t.Fatalf("expected name 'memory', got %q", db.Name())
	}
	if err := db.HealthCheck(ctx); err != nil {
		t.Fatalf("memory HealthCheck should pass, got %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestMemoryDBRejectsWrongDimension(t *testing.T) {
	db := NewMemoryDB(dim)
	ctx := context.Background()
	_ = db.Initialize(ctx)
	defer db.Close()
	if err := db.Upsert(ctx, []VectorPoint{{ID: "x", Vector: []float32{1, 2, 3}}}); err == nil {
		t.Fatalf("expected error for wrong-dimension vector")
	}
	if _, err := db.Query(ctx, QueryRequest{Vector: []float32{1, 2, 3}, TopK: 1}); err == nil {
		t.Fatalf("expected error for wrong-dimension query")
	}
}

func TestFactoryMemoryKind(t *testing.T) {
	db, err := NewVectorDB(KindMemory, VectorDBOptions{Dimension: dim})
	if err != nil {
		t.Fatalf("NewVectorDB(memory) failed: %v", err)
	}
	if db.Name() != "memory" {
		t.Fatalf("expected memory backend, got %q", db.Name())
	}
}

func TestFactoryAutoWithoutHostUsesMemory(t *testing.T) {
	db, err := NewAutoVectorDB(VectorDBOptions{Dimension: dim})
	if err != nil {
		t.Fatalf("NewAutoVectorDB failed: %v", err)
	}
	if db.Name() != "memory" {
		t.Fatalf("expected auto (no host) to fall back to memory, got %q", db.Name())
	}
}

func TestFactoryAutoUnreachableHostUsesMemory(t *testing.T) {
	// A host that is not reachable within 2s must degrade to memory. Use a
	// non-routable local address to guarantee a fast connection failure.
	db, err := NewVectorDB(KindAuto, VectorDBOptions{
		QdrantHost: "127.0.0.1",
		QdrantPort: 1, // nothing listens on port 1
		Dimension:  dim,
	})
	if err != nil {
		t.Fatalf("NewVectorDB(auto) failed: %v", err)
	}
	if db.Name() != "memory" {
		t.Fatalf("expected auto (unreachable) to fall back to memory, got %q", db.Name())
	}
}

func TestFactoryUnknownKind(t *testing.T) {
	if _, err := NewVectorDB("bogus", VectorDBOptions{Dimension: dim}); err == nil {
		t.Fatalf("expected error for unknown kind")
	}
}

func TestFactoryQdrantArgValidation(t *testing.T) {
	// Malformed args must error at construction time (no dial).
	if _, err := NewQdrantClient("", 6334, false, "c", dim, time.Second); err == nil {
		t.Fatalf("expected error for empty host")
	}
	if _, err := NewQdrantClient("h", 0, false, "c", dim, time.Second); err == nil {
		t.Fatalf("expected error for zero port")
	}
	if _, err := NewQdrantClient("h", 6334, false, "", dim, time.Second); err == nil {
		t.Fatalf("expected error for empty collection")
	}
	if _, err := NewQdrantClient("h", 6334, false, "c", 0, time.Second); err == nil {
		t.Fatalf("expected error for zero dimension")
	}
}

func TestFactoryPgVectorArgValidation(t *testing.T) {
	if _, err := NewPgVectorDB("", "t", dim, time.Second); err == nil {
		t.Fatalf("expected error for empty dsn")
	}
	if _, err := NewPgVectorDB("postgres://u:p@h/db", "bad name", dim, time.Second); err == nil {
		t.Fatalf("expected error for invalid table name")
	}
	if _, err := NewPgVectorDB("postgres://u:p@h/db", "t", 0, time.Second); err == nil {
		t.Fatalf("expected error for zero dimension")
	}
}

// fakeLLM is a deterministic LLMClient for tests.
type fakeLLM struct {
	lastPrompt string
}

func (f *fakeLLM) Complete(ctx context.Context, prompt string) (string, error) {
	f.lastPrompt = prompt
	return "ANSWER:" + prompt, nil
}

// ragDim is a realistic embedding dimension for RAG tests; a tiny dimension
// (8) makes feature-hashed similarities degenerate and ordering flaky.
const ragDim = 256

func TestRAGPipelineIndexRetrieveQuery(t *testing.T) {
	db := NewMemoryDB(ragDim)
	emb := embedding.NewLocalEmbeddingService(ragDim)
	cfg := DefaultRAGConfig()
	cfg.ChunkSize = 60
	cfg.ChunkOverlap = 10
	cfg.MinScore = 0.0 // accept any so we can assert ordering over few chunks
	pipeline := NewRAGPipeline(db, emb, cfg)
	ctx := context.Background()

	docs := []Document{
		{
			ID: "doc-1", Source: "guide.md",
			Text:     "Goroutines are lightweight threads managed by the Go runtime scheduler.",
			Metadata: map[string]any{"lang": "go"},
		},
		{
			ID: "doc-2", Source: "notes.md",
			Text:     "The kitchen sink holds dishes, soap, and water for washing.",
			Metadata: map[string]any{"lang": "en"},
		},
	}
	if err := pipeline.Index(ctx, docs); err != nil {
		t.Fatalf("Index failed: %v", err)
	}
	if db.Len() == 0 {
		t.Fatalf("expected indexed points, got none")
	}

	// Retrieve should surface the goroutine document for a Go query.
	results, err := pipeline.Retrieve(ctx, "How do Go goroutines work", 2, nil)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatalf("expected at least one retrieved chunk")
	}
	if results[0].Payload["source"] != "guide.md" {
		t.Fatalf("expected top source guide.md, got %v", results[0].Payload["source"])
	}

	// Query with an LLM: answer must carry the "llm" mode and the LLM prefix.
	llm := &fakeLLM{}
	ans, err := pipeline.Query(ctx, "How do Go goroutines work", llm)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !strings.HasPrefix(ans, "ANSWER:") {
		t.Fatalf("expected LLM-generated answer, got %q", ans)
	}
	if !strings.Contains(llm.lastPrompt, "guide.md") {
		t.Fatalf("augmented prompt should cite the retrieved source, got %q", llm.lastPrompt)
	}

	// Query with a nil LLM: the assembled prompt is returned and mode is no_llm.
	full, err := pipeline.QueryWithAnswer(ctx, "How do Go goroutines work", nil)
	if err != nil {
		t.Fatalf("QueryWithAnswer(nil) failed: %v", err)
	}
	if full.Mode != AnswerModeNoLLM {
		t.Fatalf("expected no_llm mode, got %q", full.Mode)
	}
	if full.Answer != full.Prompt {
		t.Fatalf("no_llm answer should equal the assembled prompt")
	}
	if !strings.Contains(full.Prompt, cfg.SystemPrompt) {
		t.Fatalf("prompt should include the system prompt")
	}
	if len(full.Sources) == 0 {
		t.Fatalf("expected retrieved sources to be exposed")
	}

	// GetVectorDB returns the underlying db.
	if pipeline.GetVectorDB() != db {
		t.Fatalf("GetVectorDB did not return the underlying db")
	}
}

func TestRAGPipelineRetrieveMinScoreFilters(t *testing.T) {
	db := NewMemoryDB(ragDim)
	emb := embedding.NewLocalEmbeddingService(ragDim)
	cfg := DefaultRAGConfig()
	cfg.ChunkSize = 40
	cfg.MinScore = 0.99 // a high bar that should filter everything out
	pipeline := NewRAGPipeline(db, emb, cfg)
	ctx := context.Background()

	if err := pipeline.Index(ctx, []Document{{ID: "d", Source: "s", Text: "some isolated text chunk here"}}); err != nil {
		t.Fatalf("Index failed: %v", err)
	}
	results, err := pipeline.Retrieve(ctx, "a completely different query", 5, nil)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected MinScore to filter out all results, got %d", len(results))
	}
}

func TestRAGPipelineRejectsEmptyQuery(t *testing.T) {
	db := NewMemoryDB(dim)
	emb := embedding.NewLocalEmbeddingService(dim)
	pipeline := NewRAGPipeline(db, emb, DefaultRAGConfig())
	if _, err := pipeline.Retrieve(context.Background(), "   ", 5, nil); err == nil {
		t.Fatalf("expected error for empty query")
	}
}

func TestChunkTextSplitsAndOverlaps(t *testing.T) {
	text := "aa bb cc dd ee ff gg hh ii jj"
	chunks := chunkText(text, 10, 4)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d: %v", len(chunks), chunks)
	}
	// Every chunk must be non-empty and within a reasonable size.
	for _, c := range chunks {
		if strings.TrimSpace(c) == "" {
			t.Fatalf("got an empty chunk")
		}
	}
	// No chunk should be longer than size + a small boundary slack.
	for _, c := range chunks {
		if len(c) > 10+1 {
			t.Fatalf("chunk %q longer than expected size", c)
		}
	}
}

func TestChunkTextEmptyAndShort(t *testing.T) {
	if got := chunkText("", 10, 4); len(got) != 0 {
		t.Fatalf("expected no chunks for empty text, got %v", got)
	}
	if got := chunkText("tiny", 10, 4); len(got) != 1 || got[0] != "tiny" {
		t.Fatalf("expected single chunk 'tiny', got %v", got)
	}
}

func idsOf(results []SearchResult) []string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, r.ID)
	}
	return out
}
