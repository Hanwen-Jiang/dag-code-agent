package harness

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/embedding"
)

// errEmbeddingCardinality reports a service that returned the wrong number of
// vectors, which would otherwise silently misalign documents and scores.
var errEmbeddingCardinality = errors.New("harness: embedding service returned the wrong number of vectors")

// Memory ranking seam.
//
// Retrieve ordered memories by lexical overlap: shared tokens, nothing else. A
// query that shares no token with a record could not reach it, however obviously
// the record answered the question, and there was no way to install a better
// ranker — packages/vector and packages/embedding reached no memory path.
//
// The seam below is that way in. It is deliberately an interface over records
// rather than a vector index: an embedder with a real embedding service gets
// semantic recall, an embedder with none gets the deterministic local one, and
// the runtime does not have to own an index to make either work.
//
// What the DEFAULT does not do: LocalEmbeddingService is hashed word and
// character-bigram features, not a semantic model. It tolerates near-matches and
// typos ("vector"/"vectors", "recieve"/"receive"); it does not know that
// "authenticate" and "login credentials" are related. Claiming otherwise would
// be a lie the tests could not catch, so the tests assert the mechanism and the
// fallback rather than a semantic gain the local service cannot deliver.

// MemoryRanker orders candidate memories for a query.
//
// It receives every live record, not a pre-filtered subset: a lexical pre-filter
// would drop exactly the records a semantic ranker exists to find.
type MemoryRanker interface {
	Rank(ctx context.Context, query string, records []MemoryRecord) ([]MemoryRecord, error)
}

// WithMemoryRanker installs a ranker for memory retrieval.
func WithMemoryRanker(ranker MemoryRanker) Option {
	return func(c *runtimeConfig) error {
		c.memoryRanker = ranker
		return nil
	}
}

// EmbeddingRanker ranks memories by embedding similarity.
type EmbeddingRanker struct {
	// Service computes the vectors. Nil selects the deterministic local one.
	Service embedding.EmbeddingService
	// Dimension for the local default.
	Dimension int
}

const defaultMemoryEmbeddingDimension = 256

func (r *EmbeddingRanker) service() embedding.EmbeddingService {
	if r != nil && r.Service != nil {
		return r.Service
	}
	dimension := defaultMemoryEmbeddingDimension
	if r != nil && r.Dimension > 0 {
		dimension = r.Dimension
	}
	return embedding.NewLocalEmbeddingService(dimension)
}

// Rank orders records by cosine similarity to the query.
//
// Ties break on confidence and then on creation time, matching Retrieve's
// existing tie-break, so installing a ranker does not make the order unstable.
func (r *EmbeddingRanker) Rank(ctx context.Context, query string, records []MemoryRecord) ([]MemoryRecord, error) {
	if len(records) == 0 {
		return records, nil
	}
	service := r.service()
	texts := make([]string, 0, len(records)+1)
	texts = append(texts, query)
	for _, record := range records {
		texts = append(texts, memoryDocument(record))
	}
	vectors, err := service.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}
	if len(vectors) != len(texts) {
		return nil, errEmbeddingCardinality
	}
	queryVector := vectors[0]
	type scored struct {
		record MemoryRecord
		score  float64
	}
	ranked := make([]scored, 0, len(records))
	for index, record := range records {
		ranked = append(ranked, scored{record: record, score: cosineSimilarity(queryVector, vectors[index+1])})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		if ranked[i].record.Confidence != ranked[j].record.Confidence {
			return ranked[i].record.Confidence > ranked[j].record.Confidence
		}
		return ranked[i].record.CreatedAt > ranked[j].record.CreatedAt
	})
	out := make([]MemoryRecord, 0, len(ranked))
	for _, item := range ranked {
		out = append(out, item.record)
	}
	return out, nil
}

// memoryDocument is the text a record is embedded as. It matches the document
// Retrieve scores lexically, so the two rankers see the same evidence and a
// comparison between them is about the ranking, not about the fields read.
func memoryDocument(record MemoryRecord) string {
	parts := make([]string, 0, 3+len(record.Tags))
	parts = append(parts, record.Key, record.Content, record.Namespace)
	parts = append(parts, record.Tags...)
	return strings.Join(parts, " ")
}

// cosineSimilarity compares two vectors, treating a zero vector as no
// similarity rather than as a division by zero.
func cosineSimilarity(a, b []float32) float64 {
	length := len(a)
	if len(b) < length {
		length = len(b)
	}
	var dot, normA, normB float64
	for i := 0; i < length; i++ {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}
