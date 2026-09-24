package embedding

import (
	"context"
	"hash/fnv"
	"math"
	"unicode"
)

// LocalEmbeddingService is a deterministic, dependency-free embedding used as
// the graceful-degradation path when no HTTP embedding endpoint is available.
//
// It is NOT a semantic model. It hashes a bag of text features — full
// lowercased word n-grams (weight 1.0) plus character bigrams within each
// word (weight 0.5) — with FNV-1a and projects each feature into a
// fixed-dimension vector using hash-derived signs (a sign-hashing /
// feature-hashing scheme). The result is L2-normalized.
//
// Properties:
//   - Stable: identical input always yields bit-identical output.
//   - Similarity: texts sharing words/fragments share features, so their
//     cosine similarity is higher than for disjoint texts.
//   - Not semantic: paraphrases with different surface forms score low.
//
// Dimension must be positive at construction.
type LocalEmbeddingService struct {
	dimension int
}

// NewLocalEmbeddingService constructs a local degraded embedding service of
// the given dimension. A non-positive dimension is sanitized to 256.
func NewLocalEmbeddingService(dimension int) *LocalEmbeddingService {
	if dimension <= 0 {
		dimension = 256
	}
	return &LocalEmbeddingService{dimension: dimension}
}

// Embed returns one L2-normalized vector per input text, in input order.
// It never fails for non-empty input (empty texts yield zero vectors).
func (s *LocalEmbeddingService) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = s.embedOne(t)
	}
	return out, nil
}

// Dimension implements EmbeddingService.
func (s *LocalEmbeddingService) Dimension() int { return s.dimension }

// Mode() string — see interface.go for the Mode() contract.
func (s *LocalEmbeddingService) Mode() string { return ModeLocal }

// embedOne builds the feature-hashed, L2-normalized vector for a single text.
func (s *LocalEmbeddingService) embedOne(text string) []float32 {
	vec := make([]float32, s.dimension)

	for _, w := range tokenize(text) {
		// Word n-gram feature (weight 1.0).
		s.addFeature(vec, "w:"+w, 1.0)
		// Character bigrams inside the word (weight 0.5). They give partial
		// credit to near-matches ("vector"/"vectors") and typos.
		for i := 0; i+1 < len(w); i++ {
			s.addFeature(vec, "c:"+w[i:i+2], 0.5)
		}
	}

	// L2-normalize; leave the zero vector as-is for empty text.
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	if norm == 0 {
		return vec
	}
	inv := float32(1.0 / math.Sqrt(norm))
	for i := range vec {
		vec[i] *= inv
	}
	return vec
}

// addFeature accumulates a signed, hashed contribution of the feature into
// vec at a hash-derived index.
func (s *LocalEmbeddingService) addFeature(vec []float32, feature string, weight float32) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(feature))
	sum := h.Sum64()
	idx := int(sum % uint64(s.dimension))
	sign := float32(1.0)
	if sum&(1<<63) != 0 {
		sign = -1.0
	}
	vec[idx] += sign * weight
}

// tokenize lowercases (casefolds) text and returns its alphanumeric word
// runs. Non-letters/digits act as separators, so punctuation and whitespace
// are dropped.
func tokenize(text string) []string {
	var words []string
	var cur []rune
	flush := func() {
		if len(cur) > 0 {
			words = append(words, string(cur))
			cur = nil
		}
	}
	for _, r := range text {
		lr := unicode.ToLower(r)
		if unicode.IsLetter(lr) || unicode.IsDigit(lr) {
			cur = append(cur, lr)
			continue
		}
		flush()
	}
	flush()
	return words
}
