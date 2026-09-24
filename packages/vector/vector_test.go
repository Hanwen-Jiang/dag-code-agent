package vector

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
)

// hashEmbedder is a deterministic, dependency-free embedding service used
// for tests: hashed bag-of-words in 16 dimensions, L2-normalized.
type hashEmbedder struct{}

func (hashEmbedder) Name() string              { return "hash" }
func (hashEmbedder) SupportBatching() bool     { return true }
func (hashEmbedder) MaxBatchSize() int         { return 64 }
func (h hashEmbedder) EstimateTokens(text string) int {
	return len(strings.Fields(text))
}

const testEmbedDim = 256

func (h hashEmbedder) EmbedText(ctx context.Context, text string) ([]float32, error) {
	vec := make([]float32, testEmbedDim)
	for _, w := range tokenize(text) {
		hv := uint32(2166136261)
		for i := 0; i < len(w); i++ {
			hv ^= uint32(w[i])
			hv *= 16777619
		}
		vec[int(hv%testEmbedDim)]++
	}
	norm := 0.0
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	if norm > 0 {
		norm = math.Sqrt(norm)
		for i := range vec {
			vec[i] = float32(float64(vec[i]) / norm)
		}
	}
	return vec, nil
}

func (h hashEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for _, t := range texts {
		v, err := h.EmbedText(ctx, t)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func mustEmbed(t *testing.T, emb EmbeddingService, text string) []float32 {
	t.Helper()
	v, err := emb.EmbedText(context.Background(), text)
	if err != nil {
		t.Fatalf("embed %q: %v", text, err)
	}
	return v
}

func TestMemoryStoreCosineSearch(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("test")
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}

	recs := []VectorRecord{
		{ID: "a", Vector: mustEmbed(t, emb, "go unit tests for maps"), Text: "go unit tests for maps", Metadata: map[string]any{"kind": "doc"}, TenantID: "t1", Namespace: "ns1"},
		{ID: "b", Vector: mustEmbed(t, emb, "python web server with flask"), Text: "python web server with flask", Metadata: map[string]any{"kind": "doc"}, TenantID: "t1", Namespace: "ns1"},
		{ID: "c", Vector: mustEmbed(t, emb, "go unit tests for slices and channels"), Text: "go unit tests for slices and channels", Metadata: map[string]any{"kind": "other"}, TenantID: "t2", Namespace: "ns1"},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}

	// Query shares vocabulary with "a" most closely.
	q := mustEmbed(t, emb, "go unit tests")
	matches, err := store.Search(ctx, q, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 3 {
		t.Fatalf("got %d matches", len(matches))
	}
	if matches[0].ID != "a" {
		t.Fatalf("top match = %s, want a", matches[0].ID)
	}
	if matches[0].DistanceType != "cosine" {
		t.Fatalf("distance type = %s", matches[0].DistanceType)
	}
	if !strings.Contains(matches[0].Text, "go unit tests") {
		t.Fatalf("text not preserved: %q", matches[0].Text)
	}

	// Metadata + tenant filters.
	filtered, err := store.Search(ctx, q, 10, map[string]any{"tenant_id": "t2", "kind": "other"})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0].ID != "c" {
		t.Fatalf("filtered = %v", filtered)
	}

	// Dimension mismatch is rejected.
	if err := store.Upsert(ctx, []VectorRecord{{ID: "bad", Vector: []float32{1, 2}}}); err == nil {
		t.Fatal("expected dimension mismatch error")
	}
	// Empty vector is rejected.
	if err := store.Upsert(ctx, []VectorRecord{{ID: "empty", Vector: nil}}); err == nil {
		t.Fatal("expected empty record error")
	}
}

func TestMemoryStoreDeleteAndDeterminism(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("d")
	recs := []VectorRecord{
		{ID: "x", Vector: mustEmbed(t, emb, "red apple pie")},
		{ID: "y", Vector: mustEmbed(t, emb, "blueberry pie")},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, []string{"x", "missing"}); err != nil {
		t.Fatal(err)
	}
	if store.Count() != 1 {
		t.Fatalf("count = %d, want 1", store.Count())
	}
	matches, err := store.Search(ctx, mustEmbed(t, emb, "pie"), 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].ID != "y" {
		t.Fatalf("after delete: %v", matches)
	}

	// Two identical vectors: tie broken by ID ascending (deterministic).
	store2 := NewMemoryStore("t")
	if err := store2.Upsert(ctx, []VectorRecord{
		{ID: "bb", Vector: []float32{1, 0, 0}},
		{ID: "aa", Vector: []float32{1, 0, 0}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store2.Search(ctx, []float32{1, 0, 0}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != "aa" || got[1].ID != "bb" {
		t.Fatalf("tie-break order: %s, %s", got[0].ID, got[1].ID)
	}
}

func TestMemoryStoreBM25SearchByQuery(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("q")
	recs := []VectorRecord{
		{ID: "r1", Vector: mustEmbed(t, emb, "one"), Text: "the quick brown fox jumps over the lazy dog"},
		{ID: "r2", Vector: mustEmbed(t, emb, "two"), Text: "quick fox and a quick fox run"},
		{ID: "r3", Vector: mustEmbed(t, emb, "three"), Text: "unrelated content about coffee brewing"},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	matches, err := store.SearchByQuery(ctx, "quick fox", 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("got %d", len(matches))
	}
	// r2 contains both terms (and more of them) and must outrank r1.
	if matches[0].ID != "r2" {
		t.Fatalf("top = %s, want r2 (scores: %v)", matches[0].ID, []float32{matches[0].Score})
	}
	if matches[0].DistanceType != "bm25" {
		t.Fatalf("distance = %s", matches[0].DistanceType)
	}

	// No matching text => empty, no error.
	none, err := store.SearchByQuery(ctx, "zebra", 5, nil)
	if err != nil || len(none) != 0 {
		t.Fatalf("zebra: %v %v", none, err)
	}
}

func TestRetrieverImpl(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("r")
	recs := []VectorRecord{
		{ID: "d1", Vector: mustEmbed(t, emb, "go concurrency channels"), Text: "go concurrency channels"},
		{ID: "d2", Vector: mustEmbed(t, emb, "rust ownership model"), Text: "rust ownership model"},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	r := NewRetriever(store, emb)

	// Default: cosine, topK clamps results.
	res, err := r.Search(ctx, "go concurrency", SearchOptions{TopK: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != "d1" {
		t.Fatalf("top = %v", res)
	}
	if !res[0].UsedForRAG || res[0].Query != "go concurrency" {
		t.Fatalf("attribution missing: %+v", res[0])
	}
	if res[0].DistanceType != "cosine" {
		t.Fatalf("metric = %s", res[0].DistanceType)
	}

	// MinScore filters out low-similarity documents.
	res, err = r.Search(ctx, "go concurrency", SearchOptions{TopK: 10, MinScore: 0.99})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 0 {
		t.Fatalf("minscore should filter everything, got %d", len(res))
	}

	// Dot metric is applied and labeled.
	res, err = r.Search(ctx, "go concurrency", SearchOptions{TopK: 10, Metric: "dot"})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].DistanceType != "dot" {
		t.Fatalf("metric = %s", res[0].DistanceType)
	}

	// Rerank blends in lexical overlap.
	res, err = r.Search(ctx, "go concurrency", SearchOptions{TopK: 10, Rerank: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("rerank returned nothing")
	}
}

func TestChunkDocument(t *testing.T) {
	doc := Document{
		ID:        "doc-1",
		Content:   "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau",
		ChunkSize: 20,
		Overlap:   5,
	}
	chunks := ChunkDocument(doc)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	// First chunk starts at 0 and is within size+slack.
	if chunks[0].StartPos != 0 || chunks[0].EndPos > 25 {
		t.Fatalf("chunk 0: %+v", chunks[0])
	}
	if chunks[0].ID != "doc-1_chunk_0" {
		t.Fatalf("chunk id = %s", chunks[0].ID)
	}
	// Overlap: next chunk starts before the previous ends.
	if chunks[1].StartPos >= chunks[0].EndPos {
		t.Fatalf("no overlap: %d >= %d", chunks[1].StartPos, chunks[0].EndPos)
	}
	// Last chunk covers the tail of the document.
	last := chunks[len(chunks)-1]
	if last.EndPos != len(doc.Content) {
		t.Fatalf("last chunk ends at %d, want %d", last.EndPos, len(doc.Content))
	}
	// Deterministic: same input twice gives identical chunking.
	again := ChunkDocument(doc)
	if len(again) != len(chunks) {
		t.Fatal("nondeterministic chunking")
	}
	for i := range chunks {
		if chunks[i].StartPos != again[i].StartPos || chunks[i].EndPos != again[i].EndPos {
			t.Fatalf("nondeterministic boundaries at %d", i)
		}
	}
}

func TestIndexManagerAddDocument(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("idx")
	im := NewIndexManager(store, emb)

	doc := Document{
		ID:      "paper-1",
		Content: "vector databases store embeddings. embeddings power semantic search. semantic search finds similar text. text can be chunked into pieces for indexing.",
		Metadata: map[string]any{"source": "notes"},
		ChunkSize: 40,
		Overlap:   8,
	}
	if err := im.AddDocument(ctx, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Chunks) == 0 {
		t.Fatal("document was not chunked")
	}
	if store.Count() != len(doc.Chunks) {
		t.Fatalf("store count %d != chunk count %d", store.Count(), len(doc.Chunks))
	}

	// Chunks are retrievable by dense search and carry chunk metadata.
	matches, err := store.Search(ctx, mustEmbed(t, emb, "embeddings semantic search"), 3, map[string]any{"document_id": "paper-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("no matches for document")
	}
	first := matches[0]
	if fmt.Sprint(first.Metadata["document_id"]) != "paper-1" {
		t.Fatalf("document_id = %v", first.Metadata["document_id"])
	}
	if _, ok := first.Metadata["chunk_index"]; !ok {
		t.Fatalf("chunk_index missing: %v", first.Metadata)
	}

	// Disabled auto-indexing is rejected.
	im.SetAutoIndexEnabled(false)
	if err := im.AddDocument(ctx, &doc); err == nil {
		t.Fatal("expected error when auto-indexing disabled")
	}

	// Nil dependencies are rejected.
	if err := NewIndexManager(nil, nil).AddDocument(ctx, &doc); err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestQueryClassifier(t *testing.T) {
	c := NewQueryClassifier()
	cases := []struct {
		query string
		want  QueryComplexity
	}{
		{"what is the capital of France", SimpleQuery},
		{"compare redis vs memcached for caching", ComparativeQuery},
		{"summarize all the chapters of the book", AggregationQuery},
		{"what is goroutine scheduling and how does the GC pause work", MultiHopQuery},
	}
	for _, tc := range cases {
		if got := c.Classify(tc.query); got != tc.want {
			t.Fatalf("Classify(%q) = %s, want %s", tc.query, got, tc.want)
		}
	}
}

func TestAdaptiveRetrieverSimple(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("adaptive")
	recs := []VectorRecord{
		{ID: "s1", Vector: mustEmbed(t, emb, "database sharding strategies"), Text: "database sharding strategies"},
		{ID: "s2", Vector: mustEmbed(t, emb, "kitchen recipes for pasta"), Text: "kitchen recipes for pasta"},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	r := NewAdaptiveRetriever(map[string]VectorStore{"main": store}, emb)
	res, err := r.Search(ctx, "database sharding", SearchOptions{TopK: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 || res[0].ID != "s1" {
		t.Fatalf("simple search: %v", res)
	}

	// No stores configured => error.
	if _, err := NewAdaptiveRetriever(nil, emb).Search(ctx, "x", SearchOptions{}); err == nil {
		t.Fatal("expected error with no stores")
	}
	// No embedder => error.
	if _, err := NewAdaptiveRetriever(map[string]VectorStore{"main": store}, nil).Search(ctx, "x", SearchOptions{}); err == nil {
		t.Fatal("expected error with no embedder")
	}
}

func TestAdaptiveRetrieverMultiHopMerge(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("mh")
	// doc1 covers BOTH sub-query topics with high frequency; docs 2 and 3
	// use vocabulary disjoint from the sub-queries, so (despite small
	// embedding hash collisions) doc1 strictly dominates the summed score.
	recs := []VectorRecord{
		{ID: "doc1", Vector: mustEmbed(t, emb, "alpha alpha beta beta"), Text: "alpha alpha beta beta"},
		{ID: "doc2", Vector: mustEmbed(t, emb, "epsilon zeta eta theta"), Text: "epsilon zeta eta theta"},
		{ID: "doc3", Vector: mustEmbed(t, emb, "iota kappa lambda mu"), Text: "iota kappa lambda mu"},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	r := NewAdaptiveRetriever(map[string]VectorStore{"m": store}, emb)

	// Multi-hop query: two sub-questions joined by "and".
	res, err := r.Search(ctx, "what is alpha and what is beta", SearchOptions{TopK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("no results")
	}
	// The document covering both topics must rank first (scores are summed
	// across sub-queries).
	if res[0].ID != "doc1" {
		t.Fatalf("top = %s (score %v), want doc1", res[0].ID, res[0].Score)
	}

	// Determinism: identical query twice => identical ordering.
	res2, err := r.Search(ctx, "what is alpha and what is beta", SearchOptions{TopK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(res2) {
		t.Fatal("nondeterministic length")
	}
	for i := range res {
		if res[i].ID != res2[i].ID {
			t.Fatalf("nondeterministic order at %d: %s vs %s", i, res[i].ID, res2[i].ID)
		}
	}
}

func TestAdaptiveRetrieverComparativeAndAggregation(t *testing.T) {
	ctx := context.Background()
	emb := hashEmbedder{}
	store := NewMemoryStore("cmp")
	recs := []VectorRecord{
		{ID: "a1", Vector: mustEmbed(t, emb, "redis in memory key value store"), Text: "redis in memory key value store"},
		{ID: "a2", Vector: mustEmbed(t, emb, "memcached in memory key value cache"), Text: "memcached in memory key value cache"},
		{ID: "a3", Vector: mustEmbed(t, emb, "gardening tips for tomatoes"), Text: "gardening tips for tomatoes"},
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	r := NewAdaptiveRetriever(map[string]VectorStore{"c": store}, emb)

	// Comparative: both entity documents should be retrieved (not just one).
	res, err := r.Search(ctx, "compare redis vs memcached", SearchOptions{TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, m := range res {
		ids[m.ID] = true
	}
	if !ids["a1"] || !ids["a2"] {
		t.Fatalf("comparative search missed an entity: %v", ids)
	}

	// Aggregation: wide fetch, clamped to TopK.
	for i := 0; i < 30; i++ {
		recs = append(recs, VectorRecord{
			ID:     fmt.Sprintf("bulk-%d", i),
			Vector: mustEmbed(t, emb, fmt.Sprintf("chapter %d summary of the book", i)),
			Text:   fmt.Sprintf("chapter %d summary of the book", i),
		})
	}
	if err := store.Upsert(ctx, recs); err != nil {
		t.Fatal(err)
	}
	res, err = r.Search(ctx, "summarize the book chapters", SearchOptions{TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 5 {
		t.Fatalf("aggregation topK = %d, want 5", len(res))
	}
}

func TestVectorErrorFormat(t *testing.T) {
	e := &VectorError{Code: ErrDimensionMismatch, Message: "bad dim", Details: "3 vs 16"}
	if !strings.Contains(e.Error(), "DIMENSION_MISMATCH") || !strings.Contains(e.Error(), "3 vs 16") {
		t.Fatalf("format = %q", e.Error())
	}
}
