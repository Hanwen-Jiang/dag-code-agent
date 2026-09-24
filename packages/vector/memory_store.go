package vector

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
)

// MemoryStore is an in-memory VectorStore implementation with cosine
// similarity search and a lightweight BM25 text search. It is safe for
// concurrent use and suitable for tests and lightweight deployments.
type MemoryStore struct {
	mu   sync.RWMutex
	name string
	dim  int
	recs map[string]*VectorRecord // id -> record
	// Document frequency cache for BM25, invalidated on mutation.
	dfCache map[string]int
	dfDirty bool
	closed  bool
}

// NewMemoryStore creates an in-memory store. name is the store identifier.
func NewMemoryStore(name string) *MemoryStore {
	if name == "" {
		name = "memory"
	}
	return &MemoryStore{
		name:    name,
		recs:    make(map[string]*VectorRecord),
		dfDirty: true,
	}
}

func (s *MemoryStore) Name() string { return s.name }

func (s *MemoryStore) Initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &VectorError{Code: ErrStoreClosed, Message: "store is closed"}
	}
	return nil
}

// Dimension returns the vector dimension this store uses (0 until the first
// upsert fixes it).
func (s *MemoryStore) Dimension() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dim
}

// Count returns the number of stored records.
func (s *MemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.recs)
}

func (s *MemoryStore) Upsert(ctx context.Context, records []VectorRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &VectorError{Code: ErrStoreClosed, Message: "store is closed"}
	}
	for i := range records {
		rec := records[i]
		if rec.ID == "" {
			return &VectorError{Code: ErrEmptyRecord, Message: "record with empty ID"}
		}
		if len(rec.Vector) == 0 {
			return &VectorError{Code: ErrEmptyRecord, Message: fmt.Sprintf("record %s has an empty vector", rec.ID)}
		}
		if s.dim == 0 {
			s.dim = len(rec.Vector)
		} else if len(rec.Vector) != s.dim {
			return &VectorError{
				Code:    ErrDimensionMismatch,
				Message: fmt.Sprintf("record %s has dimension %d, store expects %d", rec.ID, len(rec.Vector), s.dim),
			}
		}
		if rec.Metadata == nil {
			rec.Metadata = map[string]any{}
		}
		if rec.CreatedAt.IsZero() {
			rec.CreatedAt = rec.UpdatedAt
		}
		if rec.UpdatedAt.IsZero() {
			rec.UpdatedAt = rec.CreatedAt
		}
		// Store a defensive copy of the vector.
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		rec.Vector = vec
		s.recs[rec.ID] = &rec
	}
	s.dfDirty = true
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &VectorError{Code: ErrStoreClosed, Message: "store is closed"}
	}
	for _, id := range ids {
		if _, ok := s.recs[id]; !ok {
			// Deletes of missing IDs are idempotent.
			continue
		}
		delete(s.recs, id)
	}
	s.dfDirty = true
	return nil
}

func (s *MemoryStore) Search(ctx context.Context, query []float32, topK int, filters map[string]any) ([]VectorMatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, &VectorError{Code: ErrStoreClosed, Message: "store is closed"}
	}
	if topK <= 0 {
		topK = 10
	}
	if len(query) == 0 {
		return nil, nil
	}
	if s.dim != 0 && len(query) != s.dim {
		return nil, &VectorError{
			Code:    ErrDimensionMismatch,
			Message: fmt.Sprintf("query has dimension %d, store expects %d", len(query), s.dim),
		}
	}

	ids := s.filteredIDs(filters)
	type scored struct {
		id    string
		score float32
	}
	matches := make([]scored, 0, len(ids))
	for _, id := range ids {
		rec := s.recs[id]
		if len(rec.Vector) == 0 {
			continue
		}
		matches = append(matches, scored{id: id, score: cosineSimilarity(query, rec.Vector)})
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].score != matches[j].score {
			return matches[i].score > matches[j].score
		}
		return matches[i].id < matches[j].id
	})
	if len(matches) > topK {
		matches = matches[:topK]
	}
	out := make([]VectorMatch, 0, len(matches))
	for _, m := range matches {
		rec := s.recs[m.id]
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		meta := make(map[string]any, len(rec.Metadata))
		for k, v := range rec.Metadata {
			meta[k] = v
		}
		out = append(out, VectorMatch{
			ID:           m.id,
			Score:        m.score,
			Vector:       vec,
			Metadata:     meta,
			Text:         rec.Text,
			DistanceType: "cosine",
		})
	}
	return out, nil
}

// filteredIDs returns record IDs matching all filters. Filters may reference
// "tenant_id" and "namespace" in addition to any metadata key (exact match).
func (s *MemoryStore) filteredIDs(filters map[string]any) []string {
	ids := make([]string, 0, len(s.recs))
	for id := range s.recs {
		if s.recordMatches(s.recs[id], filters) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (s *MemoryStore) recordMatches(rec *VectorRecord, filters map[string]any) bool {
	for k, want := range filters {
		switch k {
		case "tenant_id":
			if !valueEquals(rec.TenantID, want) {
				return false
			}
		case "namespace":
			if !valueEquals(rec.Namespace, want) {
				return false
			}
		default:
			got, ok := rec.Metadata[k]
			if !ok || !valueEquals(got, want) {
				return false
			}
		}
	}
	return true
}

func valueEquals(got, want any) bool {
	if got == nil || want == nil {
		return got == want
	}
	return fmt.Sprintf("%v", got) == fmt.Sprintf("%v", want)
}

// SearchByQuery performs a lightweight BM25 text search over stored Text
// fields. Results are scored with the standard Okapi BM25 formula
// (k1=1.5, b=0.75).
func (s *MemoryStore) SearchByQuery(ctx context.Context, text string, topK int, filters map[string]any) ([]VectorMatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, &VectorError{Code: ErrStoreClosed, Message: "store is closed"}
	}
	if topK <= 0 {
		topK = 10
	}
	queryTerms := tokenize(text)
	if len(queryTerms) == 0 || len(s.recs) == 0 {
		return nil, nil
	}

	// Term frequencies per document, plus document frequency for IDF.
	df := make(map[string]int)
	docTF := make(map[string]map[string]int)
	docLen := make(map[string]int)
	for id, rec := range s.recs {
		if !s.recordMatches(rec, filters) {
			continue
		}
		terms := tokenize(rec.Text)
		docLen[id] = len(terms)
		tf := make(map[string]int)
		for _, w := range terms {
			tf[w]++
		}
		for w, n := range tf {
			if n > 0 {
				df[w]++
			}
		}
		docTF[id] = tf
	}
	nDocs := len(docTF)
	if nDocs == 0 {
		return nil, nil
	}
	avgLen := 0.0
	for _, l := range docLen {
		avgLen += float64(l)
	}
	avgLen /= float64(nDocs)

	const k1 = 1.5
	const b = 0.75

	ids := make([]string, 0, nDocs)
	for id := range docTF {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	type scored struct {
		id    string
		score float32
	}
	var matches []scored
	for _, id := range ids {
		tf := docTF[id]
		var score float64
		for _, w := range queryTerms {
			n, ok := tf[w]
			if !ok {
				continue
			}
			idf := math.Log(1 + (float64(nDocs)-float64(df[w])+0.5)/(float64(df[w])+0.5))
			tfNorm := float64(n) * (k1 + 1) / (float64(n) + k1*(1-b+b*float64(docLen[id])/avgLen))
			score += idf * tfNorm
		}
		if score > 0 {
			matches = append(matches, scored{id: id, score: float32(score)})
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].score != matches[j].score {
			return matches[i].score > matches[j].score
		}
		return matches[i].id < matches[j].id
	})
	if len(matches) > topK {
		matches = matches[:topK]
	}
	out := make([]VectorMatch, 0, len(matches))
	for _, m := range matches {
		rec := s.recs[m.id]
		vec := make([]float32, len(rec.Vector))
		copy(vec, rec.Vector)
		meta := make(map[string]any, len(rec.Metadata))
		for k, v := range rec.Metadata {
			meta[k] = v
		}
		out = append(out, VectorMatch{
			ID:           m.id,
			Score:        m.score,
			Vector:       vec,
			Metadata:     meta,
			Text:         rec.Text,
			DistanceType: "bm25",
		})
	}
	return out, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// cosineSimilarity computes the cosine similarity between two equal-length
// vectors.
func cosineSimilarity(a, b []float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	if n == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := 0; i < n; i++ {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}
