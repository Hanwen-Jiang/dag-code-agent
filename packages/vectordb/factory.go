package vectordb

import (
	"fmt"
	"math"
	"net"
	"time"
)

// VectorDBOptions carries backend-specific settings for NewVectorDB.
type VectorDBOptions struct {
	// QdrantHost/QdrantPort/UseTLS configure the qdrant backend.
	QdrantHost string
	QdrantPort int
	UseTLS     bool
	// PGDSN configures the pgvector backend (PostgreSQL connection string).
	PGDSN string
	// Dimension is the vector dimension for the backing store.
	Dimension int
	// Collection is the Qdrant collection (and pgvector table) name.
	Collection string
	// Timeout bounds individual backend operations.
	Timeout time.Duration
}

// autoReachProbeTimeout bounds the TCP reachability probe used by the "auto"
// kind when deciding whether a Qdrant endpoint is usable.
const autoReachProbeTimeout = 2 * time.Second

// Backend kind identifiers accepted by NewVectorDB.
const (
	KindQdrant   = "qdrant"
	KindPgVector = "pgvector"
	KindMemory   = "memory"
	KindAuto     = "auto"
)

// NewVectorDB constructs a VectorDB of the given kind. kind is one of
// "qdrant", "pgvector", "memory", or "auto".
//
//   - "qdrant": real Qdrant backend (dials on Initialize).
//   - "pgvector": real pgvector backend (dials on Initialize).
//   - "memory": thread-safe in-process backend.
//   - "auto": graceful-degradation default. Uses Qdrant when opts.QdrantHost
//     is set and the endpoint answers a TCP connect within 2 seconds;
//     otherwise falls back to the in-memory backend so callers always get a
//     working VectorDB.
func NewVectorDB(kind string, opts VectorDBOptions) (VectorDB, error) {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	switch kind {
	case KindQdrant:
		if opts.Collection == "" {
			return nil, fmt.Errorf("vectordb: qdrant kind requires opts.Collection")
		}
		if opts.Dimension <= 0 {
			return nil, fmt.Errorf("vectordb: qdrant kind requires opts.Dimension > 0")
		}
		return NewQdrantClient(opts.QdrantHost, opts.QdrantPort, opts.UseTLS, opts.Collection, opts.Dimension, opts.Timeout)
	case KindPgVector:
		if opts.PGDSN == "" {
			return nil, fmt.Errorf("vectordb: pgvector kind requires opts.PGDSN")
		}
		if opts.Collection == "" {
			return nil, fmt.Errorf("vectordb: pgvector kind requires opts.Collection (table name)")
		}
		if opts.Dimension <= 0 {
			return nil, fmt.Errorf("vectordb: pgvector kind requires opts.Dimension > 0")
		}
		return NewPgVectorDB(opts.PGDSN, opts.Collection, opts.Dimension, opts.Timeout)
	case KindMemory:
		if opts.Dimension <= 0 {
			return nil, fmt.Errorf("vectordb: memory kind requires opts.Dimension > 0")
		}
		return NewMemoryDB(opts.Dimension), nil
	case KindAuto:
		return newAutoVectorDB(opts), nil
	default:
		return nil, fmt.Errorf("vectordb: unknown kind %q (want %q, %q, %q, or %q)",
			kind, KindQdrant, KindPgVector, KindMemory, KindAuto)
	}
}

// NewAutoVectorDB is a convenience wrapper for NewVectorDB("auto", opts).
func NewAutoVectorDB(opts VectorDBOptions) (VectorDB, error) {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	return newAutoVectorDB(opts), nil
}

// newAutoVectorDB implements the graceful-degradation selection: Qdrant when a
// host is configured and reachable within 2 seconds, otherwise in-memory.
func newAutoVectorDB(opts VectorDBOptions) VectorDB {
	dimension := opts.Dimension
	if dimension <= 0 {
		dimension = 384
	}
	collection := opts.Collection
	if collection == "" {
		collection = "auto"
	}

	if opts.QdrantHost != "" && qdrantReachable(opts.QdrantHost, opts.QdrantPort) {
		if db, err := NewQdrantClient(opts.QdrantHost, opts.QdrantPort, opts.UseTLS, collection, dimension, opts.Timeout); err == nil {
			return db
		}
	}
	return NewMemoryDB(dimension)
}

// qdrantReachable reports whether host:port accepts a TCP connection within
// autoReachProbeTimeout. A default gRPC port (6334) is used when port is 0.
func qdrantReachable(host string, port int) bool {
	if port <= 0 {
		port = 6334
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), autoReachProbeTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// cosineSimilarity returns the cosine similarity between two equal-length
// vectors, in [-1, 1]. Vectors of mismatched length or with a zero-norm
// component yield 0.
func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}
