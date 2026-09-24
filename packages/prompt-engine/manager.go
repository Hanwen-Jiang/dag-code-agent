package prompt_engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// PromptError is the standard error for prompt engine operations.
type PromptError struct {
	Code    string
	Message string
	Details string
}

func (e *PromptError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("[%s] %s: %s", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// PromptManager manages the prompt lifecycle including version control.
type PromptManager struct {
	engine *Engine
	mu     sync.RWMutex
	stored map[string][]*TemplateVersion
}

// NewPromptManager creates a manager with a fresh engine.
func NewPromptManager() *PromptManager {
	return &PromptManager{
		engine: NewEngine(),
		stored: make(map[string][]*TemplateVersion),
	}
}

// Engine exposes the underlying template engine.
func (m *PromptManager) Engine() *Engine { return m.engine }

// Create registers a new version of a prompt. The version is explicit;
// duplicate versions are rejected.
func (m *PromptManager) Create(name, version, content string, metadata map[string]any) (*TemplateVersion, error) {
	if err := m.engine.Register(name, version, content, metadata); err != nil {
		return nil, err
	}
	v := m.engine.templates[name+"@"+version]
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stored[name] = append(m.stored[name], v)
	return v, nil
}

// GetLatest returns the latest version by semantic versioning.
func (m *PromptManager) GetLatest(name string) (*TemplateVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	versions, ok := m.stored[name]
	if !ok || len(versions) == 0 {
		return nil, &PromptError{Code: "NOT_FOUND", Message: "no versions for " + name}
	}
	best := versions[0]
	for _, v := range versions[1:] {
		if compareSemver(v.Version, best.Version) > 0 {
			best = v
		}
	}
	return best, nil
}

// GetVersion returns a specific version.
func (m *PromptManager) GetVersion(name, version string) (*TemplateVersion, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.stored[name] {
		if v.Version == version {
			return v, nil
		}
	}
	return nil, &PromptError{Code: "VERSION_NOT_FOUND", Message: version + " not found"}
}

// ListVersions lists all versions for a prompt in insertion order.
func (m *PromptManager) ListVersions(name string) []*TemplateVersion {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]*TemplateVersion(nil), m.stored[name]...)
}

// Diff compares two versions line-by-line and reports added/removed lines.
func (m *PromptManager) Diff(name, oldVersion, newVersion string) (*DiffResult, error) {
	oldTmpl, err := m.GetVersion(name, oldVersion)
	if err != nil {
		return nil, err
	}
	newTmpl, err := m.GetVersion(name, newVersion)
	if err != nil {
		return nil, err
	}

	oldLines := strings.Split(oldTmpl.Content, "\n")
	newLines := strings.Split(newTmpl.Content, "\n")
	oldSet := make(map[string]bool, len(oldLines))
	newSet := make(map[string]bool, len(newLines))
	for _, l := range oldLines {
		if l != "" {
			oldSet[l] = true
		}
	}
	for _, l := range newLines {
		if l != "" {
			newSet[l] = true
		}
	}
	added := make([]string, 0)
	removed := make([]string, 0)
	for _, l := range newLines {
		if l != "" && !oldSet[l] {
			added = append(added, l)
		}
	}
	for _, l := range oldLines {
		if l != "" && !newSet[l] {
			removed = append(removed, l)
		}
	}

	return &DiffResult{
		Old:     oldTmpl.Content,
		New:     newTmpl.Content,
		Added:   added,
		Removed: removed,
	}, nil
}

// DiffResult shows the differences between two versions.
type DiffResult struct {
	Old     string
	New     string
	Added   []string // lines present only in New
	Removed []string // lines present only in Old
}

// Example is a single few-shot example.
type Example struct {
	ID      string
	Input   string
	Output  string
	Context map[string]any
}

// embedder is the minimal embedding surface the few-shot selector needs.
type embedder interface {
	Embed(ctx context.Context, text string) []float32
}

// FewShotSelector selects the examples most similar to a query for few-shot
// prompting. It uses a pluggable embedder (default: deterministic hashed
// word features) so it has no external dependencies.
type FewShotSelector struct {
	embedder embedder
	topK     int
}

// NewFewShotSelector creates a selector using the built-in deterministic
// embedder. topK <= 0 means "no fixed cap".
func NewFewShotSelector(topK int) *FewShotSelector {
	return &FewShotSelector{embedder: newHashEmbedder(), topK: topK}
}

// NewFewShotSelectorWithEmbedder creates a selector over a custom embedder.
func NewFewShotSelectorWithEmbedder(emb embedder, topK int) *FewShotSelector {
	if emb == nil {
		emb = newHashEmbedder()
	}
	return &FewShotSelector{embedder: emb, topK: topK}
}

// SelectSimilarExamples retrieves the most similar examples for a query.
// limit <= 0 falls back to topK (or all examples if topK is 0).
func (s *FewShotSelector) SelectSimilarExamples(ctx context.Context, query string, examples []Example, limit int) ([]Example, error) {
	if len(examples) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = s.topK
	}
	if limit <= 0 || limit > len(examples) {
		limit = len(examples)
	}

	queryVec := s.embedder.Embed(ctx, query)
	type scoredExample struct {
		example Example
		score   float32
	}
	items := make([]scoredExample, 0, len(examples))
	for _, ex := range examples {
		items = append(items, scoredExample{
			example: ex,
			score:   cosineSimilarity(queryVec, s.embedder.Embed(ctx, ex.Input)),
		})
	}
	// Deterministic ordering: score desc, then ID asc.
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].example.ID < items[j].example.ID
	})

	out := make([]Example, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, items[i].example)
	}
	return out, nil
}

// hashEmbedder is a deterministic word-hash embedder (offline, dependency-free).
type hashEmbedder struct{}

func newHashEmbedder() *hashEmbedder { return &hashEmbedder{} }

const hashEmbedDim = 64

func (h *hashEmbedder) Embed(ctx context.Context, text string) []float32 {
	vec := make([]float32, hashEmbedDim)
	for _, w := range hashTokenize(text) {
		sum := fnv64a(w)
		idx := int(sum % uint64(hashEmbedDim))
		vec[idx]++
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
	return vec
}

func hashTokenize(text string) []string {
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

func fnv64a(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

func cosineSimilarity(a, b []float32) float32 {
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
	return float32(dot / (math.Sqrt(normA) * math.Sqrt(normB)))
}
