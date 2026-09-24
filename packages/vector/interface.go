// Package vector provides unified abstraction over vector databases and
// embedding services. It supports multiple backends (in-memory, PGVector) and
// embedding providers, plus a retriever layer with query-type-aware search.
package vector

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// VectorStore defines the interface for vector storage and retrieval.
type VectorStore interface {
	// Name returns the store identifier.
	Name() string

	// Initialize prepares the store (creates tables, indexes, etc.).
	Initialize(ctx context.Context) error

	// Upsert adds or updates vectors with metadata.
	Upsert(ctx context.Context, records []VectorRecord) error

	// Delete removes vectors by IDs.
	Delete(ctx context.Context, ids []string) error

	// Search finds similar vectors using cosine similarity.
	Search(ctx context.Context, query []float32, topK int, filters map[string]any) ([]VectorMatch, error)

	// SearchByQuery performs hybrid/text search over stored text.
	SearchByQuery(ctx context.Context, text string, topK int, filters map[string]any) ([]VectorMatch, error)

	// Close releases resources.
	Close() error
}

// VectorRecord is a single vector entry with metadata.
type VectorRecord struct {
	ID          string
	Vector      []float32       // Embedding vector
	Metadata    map[string]any  // Structured metadata for filtering
	Text        string          // Original text (for debugging)
	CreatedAt   time.Time
	UpdatedAt   time.Time
	TenantID    string          // Multi-tenant isolation
	Namespace   string          // Logical grouping
}

// VectorMatch is a search result with similarity score.
type VectorMatch struct {
	ID           string
	Score        float32       // Similarity score (higher = more similar)
	Vector       []float32
	Metadata     map[string]any
	Text         string
	DistanceType string        // e.g., "cosine", "dot", "euclidean"
}

// EmbeddingService provides text-to-vector conversion.
type EmbeddingService interface {
	// Name returns the provider name.
	Name() string

	// EmbedText converts text to vector.
	EmbedText(ctx context.Context, text string) ([]float32, error)

	// EmbedDocuments converts multiple texts to vectors (batch optimized).
	EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error)

	// EstimateTokens counts tokens in text for cost estimation.
	EstimateTokens(text string) int

	// SupportBatching returns whether batching is supported.
	SupportBatching() bool

	// MaxBatchSize returns maximum batch size if supported.
	MaxBatchSize() int
}

// Retriever combines embedding + search into a single operation.
type Retriever interface {
	// Search retrieves relevant documents given a query.
	Search(ctx context.Context, query string, options SearchOptions) ([]RetrievalResult, error)
}

// SearchOptions configures retrieval behavior.
type SearchOptions struct {
	TopK       int            // Number of results (default 10)
	MinScore   float32        // Minimum similarity threshold
	Filters    map[string]any // Metadata filters
	Metric     string         // Distance metric: "cosine" (default), "dot", "euclidean"
	Rerank     bool           // Whether to rerank results
	RerankTopK int            // Top-K for reranking
}

// RetrievalResult includes retrieved content with context.
type RetrievalResult struct {
	VectorMatch
	ChunkIndex int
	CourseID   string
	SectionID  string
	Query      string // Original query for attribution
	UsedForRAG bool   // Whether this was used in RAG
}

// RetrieverImpl is the canonical Retriever: embed the query, search the
// store, apply metric/rescoring, MinScore and TopK.
type RetrieverImpl struct {
	store    VectorStore
	embedder EmbeddingService
}

// NewRetriever creates a Retriever over a store and embedder.
func NewRetriever(store VectorStore, embedder EmbeddingService) *RetrieverImpl {
	return &RetrieverImpl{store: store, embedder: embedder}
}

func (r *RetrieverImpl) Search(ctx context.Context, query string, opts SearchOptions) ([]RetrievalResult, error) {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	if opts.Metric == "" {
		opts.Metric = "cosine"
	}
	queryVec, err := r.embedder.EmbedText(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embedding query: %w", err)
	}

	// Over-fetch so MinScore filtering and reranking have headroom.
	fetchK := opts.TopK * 2
	if fetchK < opts.TopK {
		fetchK = opts.TopK
	}
	matches, err := r.store.Search(ctx, queryVec, fetchK, opts.Filters)
	if err != nil {
		return nil, err
	}

	results := make([]RetrievalResult, 0, len(matches))
	for _, m := range matches {
		score := m.Score
		switch opts.Metric {
		case "dot":
			score = dotProduct(queryVec, m.Vector)
			m.DistanceType = "dot"
		case "euclidean":
			score = 1 / (1 + euclideanDistance(queryVec, m.Vector))
			m.DistanceType = "euclidean"
		default:
			m.DistanceType = "cosine"
		}
		if opts.Rerank {
			score = rerankScore(score, query, m.Text)
		}
		m.Score = score
		if score < opts.MinScore {
			continue
		}
		results = append(results, RetrievalResult{
			VectorMatch: m,
			Query:       query,
			UsedForRAG:  true,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
	if len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	return results, nil
}

// rerankScore blends dense similarity with a cheap lexical overlap signal
// (deterministic, no extra model required).
func rerankScore(dense float32, query, text string) float32 {
	overlap := jaccard(tokenize(query), tokenize(text))
	return 0.7*dense + 0.3*float32(overlap)
}

func dotProduct(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < n; i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return float32(sum)
}

func euclideanDistance(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 0
	}
	var sum float64
	for i := 0; i < n; i++ {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(math.Sqrt(sum))
}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	var words []string
	var sb strings.Builder
	flush := func() {
		if sb.Len() > 0 {
			words = append(words, sb.String())
			sb.Reset()
		}
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return words
}

func jaccard(a, b []string) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	setA := make(map[string]bool, len(a))
	for _, w := range a {
		setA[w] = true
	}
	inter := 0
	for _, w := range b {
		if setA[w] {
			inter++
		}
	}
	union := len(setA)
	for _, w := range b {
		if !setA[w] {
			union++
		}
	}
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

// Document represents a chunkable document.
type Document struct {
	ID        string
	Content   string
	Metadata  map[string]any
	Chunks    []DocumentChunk
	ChunkSize int // Target chunk size (chars); 0 = 500
	Overlap   int // Chunk overlap (chars); 0 = 50
}

// DocumentChunk is a piece of a document.
type DocumentChunk struct {
	ID         string
	Content    string
	StartPos   int
	EndPos     int
	DocumentID string
}

// IndexManager handles automatic index creation and maintenance.
type IndexManager struct {
	store          VectorStore
	embedder       EmbeddingService
	autoIndexEnabled bool
	indexPrefix    string
	batchSize      int
	interval       time.Duration
}

// NewIndexManager creates an index manager with the given store and embedder.
func NewIndexManager(store VectorStore, embedder EmbeddingService) *IndexManager {
	return &IndexManager{
		store:              store,
		embedder:           embedder,
		autoIndexEnabled:   true,
		batchSize:          100,
		interval:           time.Hour,
	}
}

// SetAutoIndexEnabled toggles automatic indexing.
func (m *IndexManager) SetAutoIndexEnabled(enabled bool) {
	m.autoIndexEnabled = enabled
}

// SetBatchSize overrides the bulk upsert batch size.
func (m *IndexManager) SetBatchSize(n int) {
	if n > 0 {
		m.batchSize = n
	}
}

// AddDocument chunks the document (storing the chunks on doc.Chunks),
// embeds every chunk, and upserts them into the backing store in batches.
func (m *IndexManager) AddDocument(ctx context.Context, doc *Document) error {
	if !m.autoIndexEnabled {
		return fmt.Errorf("auto-indexing is disabled")
	}
	if m.store == nil || m.embedder == nil {
		return fmt.Errorf("index manager requires a store and an embedder")
	}
	chunks := ChunkDocument(*doc)
	doc.Chunks = chunks
	if len(chunks) == 0 {
		return nil
	}

	now := time.Now()
	for start := 0; start < len(chunks); start += m.batchSize {
		end := start + m.batchSize
		if end > len(chunks) {
			end = len(chunks)
		}
		batch := chunks[start:end]

		texts := make([]string, len(batch))
		for i, c := range batch {
			texts[i] = c.Content
		}
		vecs, err := m.embedder.EmbedDocuments(ctx, texts)
		if err != nil {
			return fmt.Errorf("embedding batch: %w", err)
		}

		records := make([]VectorRecord, 0, len(batch))
		for i, c := range batch {
			meta := make(map[string]any, len(doc.Metadata)+2)
			for k, v := range doc.Metadata {
				meta[k] = v
			}
			meta["document_id"] = doc.ID
			meta["chunk_index"] = start + i
			records = append(records, VectorRecord{
				ID:        c.ID,
				Vector:    vecs[i],
				Metadata:  meta,
				Text:      c.Content,
				CreatedAt: now,
				UpdatedAt: now,
			})
		}
		if err := m.store.Upsert(ctx, records); err != nil {
			return err
		}
	}
	return nil
}

// ChunkDocument splits a document into deterministic chunks of approximately
// ChunkSize characters with Overlap characters of overlap. Chunk boundaries
// fall on whitespace where possible.
func ChunkDocument(doc Document) []DocumentChunk {
	size := doc.ChunkSize
	if size <= 0 {
		size = 500
	}
	overlap := doc.Overlap
	if overlap < 0 {
		overlap = 0
	}
	if overlap > size {
		overlap = size / 2
	}

	content := doc.Content
	var chunks []DocumentChunk
	pos := 0
	idx := 0
	for pos < len(content) {
		remaining := len(content) - pos
		cut := pos + size
		if cut > len(content) {
			cut = len(content)
		} else {
			// Prefer to break on whitespace near the target size.
			cut = nextWhitespace(content, pos, cut)
		}
		chunkContent := content[pos:cut]
		chunks = append(chunks, DocumentChunk{
			ID:         fmt.Sprintf("%s_chunk_%d", doc.ID, idx),
			Content:    chunkContent,
			StartPos:   pos,
			EndPos:     cut,
			DocumentID: doc.ID,
		})
		idx++
		next := cut - overlap
		if next <= pos {
			next = cut
		}
		if next == cut && cut == len(content) {
			break
		}
		pos = next
		_ = remaining
	}
	return chunks
}

// nextWhitespace returns the last whitespace offset in [start, target], or
// target itself when none exists.
func nextWhitespace(s string, start, target int) int {
	if target > len(s) {
		target = len(s)
	}
	for i := target - 1; i > start; i-- {
		if s[i] == ' ' || s[i] == '\n' || s[i] == '\t' {
			return i
		}
	}
	return target
}

// QueryComplexity classifies query difficulty.
type QueryComplexity string

const (
	SimpleQuery      QueryComplexity = "simple"      // Single entity lookup
	MultiHopQuery    QueryComplexity = "multi_hop"   // Requires reasoning across docs
	ComparativeQuery QueryComplexity = "comparative" // Needs comparison
	AggregationQuery QueryComplexity = "aggregation" // Needs summarization
)

// QueryClassifier analyzes query structure and intent.
type QueryClassifier struct{}

// NewQueryClassifier creates a classifier.
func NewQueryClassifier() *QueryClassifier {
	return &QueryClassifier{}
}

// Classify deterministically classifies a query by surface structure.
func (c *QueryClassifier) Classify(query string) QueryComplexity {
	lower := strings.ToLower(query)

	// Comparative signals.
	for _, sig := range []string{"compare", "versus", " vs ", "difference between", "differences between"} {
		if strings.Contains(lower, sig) {
			return ComparativeQuery
		}
	}
	// Aggregation signals.
	for _, sig := range []string{"summarize", "summary", "aggregate", "overall", "in total", "how many", "list all"} {
		if strings.Contains(lower, sig) {
			return AggregationQuery
		}
	}
	// Multi-hop signals: multiple conjunctions or multiple question words.
	andCount := strings.Count(lower, " and ")
	questionWords := 0
	for _, q := range []string{"what", "how", "why", "when"} {
		questionWords += countWord(lower, q)
	}
	if andCount >= 2 || questionWords >= 2 {
		return MultiHopQuery
	}
	return SimpleQuery
}

// countWord counts whole-word occurrences of w in s.
func countWord(s, w string) int {
	count := 0
	for _, field := range strings.Fields(s) {
		if field == w {
			count++
		}
	}
	return count
}

// AdaptiveRetriever intelligently chooses retrieval strategy based on query
// complexity.
type AdaptiveRetriever struct {
	stores          map[string]VectorStore
	embedder        EmbeddingService
	queryClassifier QueryClassifier
}

// NewAdaptiveRetriever wires an adaptive retriever over named stores. The
// primary store is the lexicographically first key (deterministic).
func NewAdaptiveRetriever(stores map[string]VectorStore, embedder EmbeddingService) *AdaptiveRetriever {
	return &AdaptiveRetriever{
		stores:          stores,
		embedder:        embedder,
		queryClassifier: *NewQueryClassifier(),
	}
}

// primaryStore returns the deterministic primary store.
func (r *AdaptiveRetriever) primaryStore() (VectorStore, error) {
	if len(r.stores) == 0 {
		return nil, fmt.Errorf("no vector stores configured")
	}
	keys := make([]string, 0, len(r.stores))
	for k := range r.stores {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return r.stores[keys[0]], nil
}

// Search adapts retrieval based on query type.
func (r *AdaptiveRetriever) Search(ctx context.Context, query string, opts SearchOptions) ([]RetrievalResult, error) {
	if r.embedder == nil {
		return nil, fmt.Errorf("adaptive retriever requires an embedder")
	}
	complexity := r.queryClassifier.Classify(query)

	switch complexity {
	case MultiHopQuery:
		return r.multiHopSearch(ctx, query, opts)
	case ComparativeQuery:
		return r.comparativeSearch(ctx, query, opts)
	case AggregationQuery:
		return r.aggregationSearch(ctx, query, opts)
	default:
		return r.simpleSearch(ctx, query, opts)
	}
}

func (r *AdaptiveRetriever) simpleSearch(ctx context.Context, query string, opts SearchOptions) ([]RetrievalResult, error) {
	store, err := r.primaryStore()
	if err != nil {
		return nil, err
	}
	v, err := r.embedder.EmbedText(ctx, query)
	if err != nil {
		return nil, err
	}
	matches, err := store.Search(ctx, v, opts.TopK, opts.Filters)
	if err != nil {
		return nil, err
	}
	results := make([]RetrievalResult, len(matches))
	for i, m := range matches {
		results[i] = RetrievalResult{VectorMatch: m, Query: query, UsedForRAG: true}
	}
	return results, nil
}

// multiHopSearch decomposes a conjunctive query into sub-queries (split on
// " and "), searches each independently, and merges results by summing
// similarity scores per document.
func (r *AdaptiveRetriever) multiHopSearch(ctx context.Context, query string, opts SearchOptions) ([]RetrievalResult, error) {
	store, err := r.primaryStore()
	if err != nil {
		return nil, err
	}
	lower := strings.ToLower(query)
	if !strings.Contains(lower, " and ") {
		return r.simpleSearch(ctx, query, opts)
	}

	subQueries := strings.Split(lower, " and ")
	topK := opts.TopK
	if topK <= 0 {
		topK = 10
	}

	merged := make(map[string]*RetrievalResult)
	var order []string
	for _, sub := range subQueries {
		sub = strings.TrimSpace(sub)
		if sub == "" {
			continue
		}
		v, err := r.embedder.EmbedText(ctx, sub)
		if err != nil {
			return nil, err
		}
		matches, err := store.Search(ctx, v, topK*2, opts.Filters)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			if existing, ok := merged[m.ID]; ok {
				existing.Score += m.Score
			} else {
				merged[m.ID] = &RetrievalResult{VectorMatch: m, Query: query, UsedForRAG: true}
				order = append(order, m.ID)
			}
		}
	}

	results := make([]RetrievalResult, 0, len(merged))
	for _, id := range order {
		results = append(results, *merged[id])
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
	if opts.TopK > 0 && len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	return results, nil
}

// comparativeSearch extracts the entities around comparison connectors and
// retrieves each independently, so the caller can contrast their documents.
func (r *AdaptiveRetriever) comparativeSearch(ctx context.Context, query string, opts SearchOptions) ([]RetrievalResult, error) {
	store, err := r.primaryStore()
	if err != nil {
		return nil, err
	}
	lower := strings.ToLower(query)
	for _, sep := range []string{" difference between ", " difference of ", " vs ", " versus ", " compare "} {
		if strings.Contains(lower, sep) {
			lower = strings.ReplaceAll(lower, sep, " ;; ")
		}
	}
	parts := strings.Split(lower, ";;")
	entities := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		entities = append(entities, p)
	}
	if len(entities) < 2 {
		return r.simpleSearch(ctx, query, opts)
	}

	topK := opts.TopK
	if topK <= 0 {
		topK = 10
	}
	perEntity := topK
	if perEntity > 5 {
		perEntity = 5
	}

	seen := make(map[string]bool)
	results := make([]RetrievalResult, 0)
	for _, entity := range entities {
		v, err := r.embedder.EmbedText(ctx, entity)
		if err != nil {
			return nil, err
		}
		matches, err := store.Search(ctx, v, perEntity, opts.Filters)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			if seen[m.ID] {
				continue
			}
			seen[m.ID] = true
			results = append(results, RetrievalResult{
				VectorMatch: m,
				Query:       query,
				UsedForRAG:  true,
			})
		}
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
	if opts.TopK > 0 && len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	return results, nil
}

// aggregationSearch retrieves a wider candidate set (aggregations need more
// context) and returns the top opts.TopK by score.
func (r *AdaptiveRetriever) aggregationSearch(ctx context.Context, query string, opts SearchOptions) ([]RetrievalResult, error) {
	store, err := r.primaryStore()
	if err != nil {
		return nil, err
	}
	v, err := r.embedder.EmbedText(ctx, query)
	if err != nil {
		return nil, err
	}
	wide := opts.TopK * 4
	if wide < 20 {
		wide = 20
	}
	matches, err := store.Search(ctx, v, wide, opts.Filters)
	if err != nil {
		return nil, err
	}
	results := make([]RetrievalResult, 0, len(matches))
	for _, m := range matches {
		results = append(results, RetrievalResult{VectorMatch: m, Query: query, UsedForRAG: true})
	}
	if opts.TopK > 0 && len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	return results, nil
}
