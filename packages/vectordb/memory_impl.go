package vectordb

import (
	"container/list"
	"context"
	"fmt"
	"sort"
	"sync"
)

// defaultMemoryCap bounds the number of points held by the in-memory backend.
// The least recently upserted points are evicted first when the cap is hit.
const defaultMemoryCap = 100_000

// MemoryDB implements VectorDB as a thread-safe in-process store. It is the
// graceful-degradation default: no external dependencies, deterministic
// behavior, and useful for tests and lightweight deployments.
//
// Semantics:
//   - Similarity is cosine similarity computed per query.
//   - Filters apply exact-match on payload fields (string fields compare
//     directly; other types compare by their "%v" rendering).
//   - The store is capped at memoryCap points (default 100k); when full, the
//     least recently upserted points are evicted (LRU-ish; upserts refresh
//     recency).
type MemoryDB struct {
	dimension int
	memoryCap int

	mu      sync.RWMutex
	points  map[string]*memoryPoint
	ordered *list.List // front = newest, back = oldest (eviction source)
	seq     uint64     // monotonically increasing upsert counter
}

// memoryPoint is a stored point with its vector and payload.
type memoryPoint struct {
	id      string
	vector  []float32
	payload map[string]any
	seq     uint64 // upsert order, used as a stable tie-breaker
	entry   *list.Element
}

// NewMemoryDB constructs an in-memory vector DB with the given dimension and
// the default 100k point cap.
func NewMemoryDB(dimension int) *MemoryDB {
	return &MemoryDB{
		dimension: dimension,
		memoryCap: defaultMemoryCap,
		points:    make(map[string]*memoryPoint),
		ordered:   list.New(),
	}
}

// NewMemoryDBWithCap constructs an in-memory vector DB with an explicit cap.
// A cap <= 0 falls back to the default cap.
func NewMemoryDBWithCap(dimension, cap int) *MemoryDB {
	if cap <= 0 {
		cap = defaultMemoryCap
	}
	return &MemoryDB{
		dimension: dimension,
		memoryCap: cap,
		points:    make(map[string]*memoryPoint),
		ordered:   list.New(),
	}
}

// Name implements VectorDB.
func (m *MemoryDB) Name() string { return "memory" }

// Initialize is a no-op for the in-memory backend (kept for interface
// uniformity).
func (m *MemoryDB) Initialize(ctx context.Context) error { return nil }

// Upsert inserts or replaces points. Upserts refresh a point's recency.
func (m *MemoryDB) Upsert(ctx context.Context, points []VectorPoint) error {
	if len(points) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range points {
		if p.ID == "" {
			return fmt.Errorf("vectordb: memory point has empty ID")
		}
		if len(p.Vector) == 0 {
			return fmt.Errorf("vectordb: memory point %q has an empty vector", p.ID)
		}
		if len(p.Vector) != m.dimension {
			return fmt.Errorf("vectordb: memory point %q has dimension %d, expected %d",
				p.ID, len(p.Vector), m.dimension)
		}
		m.upsertLocked(p.ID, p.Vector, p.Payload)
	}
	return nil
}

// upsertLocked inserts or replaces one point and evicts oldest entries beyond
// the cap. Caller holds m.mu.
func (m *MemoryDB) upsertLocked(id string, vector []float32, payload map[string]any) {
	if existing, ok := m.points[id]; ok {
		existing.vector = cloneVector(vector)
		existing.payload = clonePayload(payload)
		existing.seq = m.nextSeqLocked()
		m.ordered.MoveToFront(existing.entry)
		return
	}

	ele := m.ordered.PushFront(&memoryPoint{
		id:      id,
		vector:  cloneVector(vector),
		payload: clonePayload(payload),
		seq:     m.nextSeqLocked(),
	})
	mp := ele.Value.(*memoryPoint)
	mp.entry = ele
	m.points[id] = mp

	for m.ordered.Len() > m.memoryCap {
		back := m.ordered.Back()
		if back == nil {
			break
		}
		old := back.Value.(*memoryPoint)
		m.ordered.Remove(back)
		delete(m.points, old.id)
	}
}

// nextSeqLocked returns the next sequence number. Caller holds m.mu.
func (m *MemoryDB) nextSeqLocked() uint64 {
	m.seq++
	return m.seq
}

// Query returns the top-K most similar points by cosine similarity, optionally
// filtered by exact payload match.
func (m *MemoryDB) Query(ctx context.Context, req QueryRequest) ([]SearchResult, error) {
	if len(req.Vector) == 0 {
		return nil, fmt.Errorf("vectordb: memory query vector is empty")
	}
	if len(req.Vector) != m.dimension {
		return nil, fmt.Errorf("vectordb: memory query vector dimension %d, expected %d",
			len(req.Vector), m.dimension)
	}
	topK := req.TopK
	if topK <= 0 {
		topK = 10
	}

	m.mu.RLock()
	candidates := make([]*memoryPoint, 0, len(m.points))
	for _, p := range m.points {
		if !payloadMatches(p.payload, req.FilterPayload) {
			continue
		}
		candidates = append(candidates, p)
	}
	m.mu.RUnlock()

	type scored struct {
		pt    *memoryPoint
		score float64
	}
	scoredList := make([]scored, 0, len(candidates))
	for _, p := range candidates {
		scoredList = append(scoredList, scored{pt: p, score: cosineSimilarity(req.Vector, p.vector)})
	}
	// Sort by score descending; tie-break by seq descending (newer first) for
	// deterministic output.
	sort.Slice(scoredList, func(i, j int) bool {
		if scoredList[i].score != scoredList[j].score {
			return scoredList[i].score > scoredList[j].score
		}
		return scoredList[i].pt.seq > scoredList[j].pt.seq
	})

	if len(scoredList) > topK {
		scoredList = scoredList[:topK]
	}

	results := make([]SearchResult, 0, len(scoredList))
	for _, s := range scoredList {
		results = append(results, SearchResult{
			ID:      s.pt.id,
			Score:   s.score,
			Payload: clonePayload(s.pt.payload),
		})
	}
	return results, nil
}

// Delete removes the given point IDs.
func (m *MemoryDB) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		if p, ok := m.points[id]; ok {
			m.ordered.Remove(p.entry)
			delete(m.points, id)
		}
	}
	return nil
}

// Close is a no-op for the in-memory backend.
func (m *MemoryDB) Close() error { return nil }

// HealthCheck is always healthy for the in-memory backend.
func (m *MemoryDB) HealthCheck(ctx context.Context) error { return nil }

// Len returns the current number of stored points (test/diagnostics helper).
func (m *MemoryDB) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.points)
}

// payloadMatches reports whether every filter key exists in the payload with
// an exactly matching value (string fields compare directly; other types
// compare by their "%v" rendering).
func payloadMatches(payload map[string]any, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}
	for k, want := range filter {
		got, ok := payload[k]
		if !ok {
			return false
		}
		if !looseEqual(got, want) {
			return false
		}
	}
	return true
}

// looseEqual compares two payload values for filter matching.
func looseEqual(got, want any) bool {
	gs, gotIsStr := got.(string)
	ws, wantIsStr := want.(string)
	if gotIsStr && wantIsStr {
		return gs == ws
	}
	return fmt.Sprintf("%v", got) == fmt.Sprintf("%v", want)
}

func cloneVector(v []float32) []float32 {
	if v == nil {
		return nil
	}
	out := make([]float32, len(v))
	copy(out, v)
	return out
}

func clonePayload(p map[string]any) map[string]any {
	if p == nil {
		return nil
	}
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}
