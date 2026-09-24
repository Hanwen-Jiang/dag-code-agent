// Package vectordb provides vector storage, similarity retrieval, and a
// retrieval-augmented generation (RAG) pipeline.
//
// It implements Chapter 21: Memory Systems from AI Agent Architecture with
// pluggable backends (Qdrant, pgvector, in-memory) and graceful degradation:
// the "auto" factory kind falls back to the thread-safe in-memory backend when
// no reachable Qdrant endpoint is configured.
package vectordb

import (
	"context"
	"time"
)

// VectorDB defines the core storage and retrieval surface shared by all
// backends. Implementations must be safe for concurrent use.
type VectorDB interface {
	// Name returns the backend kind, e.g. "qdrant", "pgvector", "memory".
	Name() string

	// Initialize prepares the backend (dials, ensures schema/collection
	// exist). Constructors never dial; connection failures surface here.
	Initialize(ctx context.Context) error

	// Upsert inserts or replaces vectors by ID.
	Upsert(ctx context.Context, points []VectorPoint) error

	// Query returns the top-K most similar points for the given vector,
	// optionally filtered by exact payload match.
	Query(ctx context.Context, req QueryRequest) ([]SearchResult, error)

	// Delete removes points by ID.
	Delete(ctx context.Context, ids []string) error

	// Close releases backend resources. Subsequent calls are undefined.
	Close() error

	// HealthCheck verifies the backend is reachable and usable.
	HealthCheck(ctx context.Context) error
}

// VectorPoint is a single stored vector with its identity and payload.
type VectorPoint struct {
	ID      string
	Vector  []float32
	Payload map[string]any
}

// QueryRequest describes a similarity search.
type QueryRequest struct {
	// Collection scopes the search on backends that support multiple
	// collections. It may be empty, in which case the backend default
	// collection is used.
	Collection string
	// Vector is the query vector; its dimension must match the stored ones.
	Vector []float32
	// TopK is the maximum number of results to return.
	TopK int
	// FilterPayload, when non-nil, restricts results to points whose payload
	// contains every key with an exactly matching value.
	FilterPayload map[string]any
}

// SearchResult is a single hit from a similarity query.
type SearchResult struct {
	ID      string
	Score   float64 // Similarity score (higher = more similar).
	Payload map[string]any
}

// CollectionOptions defines vector collection configuration.
type CollectionOptions struct {
	Dim        int            // Vector dimension (e.g., 1536 for text-embedding-3-small)
	Metric     DistanceMetric // Similarity metric (cosine, euclidean, dot_product)
	Replicas   int            // Replication factor (for distributed deployments)
	TTL        *time.Duration // Automatic expiration (optional)
	HNSWParams *HNSWParams    // Index optimization parameters
}

// HNSWParams optimizes the hierarchical navigable small world graph index.
type HNSWParams struct {
	M           int     // Number of bi-directional links per layer (16 is default)
	EFConstruct int     // Index construction parameter (200 is default)
	Alpha       float32 // Search param scale factor (0.8 recommended for cosine)
}

// Collection describes a stored collection.
type Collection struct {
	Name    string
	Vectors uint64
	Params  *CollectionParams
}

// CollectionParams carries backend-specific collection metadata.
type CollectionParams struct {
	Dimension int
	Metric    DistanceMetric
}

// DistanceMetric defines a similarity measurement method.
type DistanceMetric string

const (
	Cosine     DistanceMetric = "cosine"      // Most common for embeddings
	Euclidean  DistanceMetric = "euclidean"   // L2 distance
	DotProduct DistanceMetric = "dot_product" // Inner product
)

// Memory System Types (from Chapter 21).
type MemoryType string

const (
	WorkingMemory    MemoryType = "working"    // Short-term session context
	EpisodicMemory   MemoryType = "episodic"   // Event-based conversation logs
	SemanticMemory   MemoryType = "semantic"   // Knowledge/facts database
	ProceduralMemory MemoryType = "procedural" // Tool skills and patterns
)

// Collection prefixes for different memory types.
const (
	CollectionWorking    = "mem_working"
	CollectionEpisodic   = "mem_episodic"
	CollectionSemantic   = "mem_semantic"
	CollectionProcedural = "mem_procedural"
)
