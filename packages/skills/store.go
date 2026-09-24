package skills

import (
	"fmt"
	"sync"
)

// MemoryStore is a thread-safe in-memory SkillStore.
type MemoryStore struct {
	mu      sync.RWMutex
	patterns map[string]*PatternDefinition
	// versionLinks records oldID -> newID supersessions for audit.
	versionLinks map[string]string
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		patterns:     make(map[string]*PatternDefinition),
		versionLinks: make(map[string]string),
	}
}

// Save stores a copy of the pattern.
func (s *MemoryStore) Save(pattern *PatternDefinition) error {
	if pattern == nil {
		return fmt.Errorf("nil pattern")
	}
	cp := *pattern
	cp.ExecutionPlan = append([]SkillAction(nil), pattern.ExecutionPlan...)
	cp.TestCases = append([]PatternTest(nil), pattern.TestCases...)
	cp.ExampleQueries = append([]string(nil), pattern.ExampleQueries...)
	cp.Trigger.Keywords = append([]string(nil), pattern.Trigger.Keywords...)
	cp.ContextRequirements = append([]string(nil), pattern.ContextRequirements...)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.patterns[pattern.ID] = &cp
	return nil
}

// Load returns a copy of the pattern or an error if missing.
func (s *MemoryStore) Load(id string) (*PatternDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.patterns[id]
	if !ok {
		return nil, fmt.Errorf("pattern not found: %s", id)
	}
	cp := *p
	return &cp, nil
}

// List returns all stored patterns.
func (s *MemoryStore) List() ([]*PatternDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*PatternDefinition, 0, len(s.patterns))
	for _, p := range s.patterns {
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

// Delete removes a pattern.
func (s *MemoryStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.patterns[id]; !ok {
		return fmt.Errorf("pattern not found: %s", id)
	}
	delete(s.patterns, id)
	return nil
}

// UpdateVersion records an old -> new supersession link.
func (s *MemoryStore) UpdateVersion(oldID, newID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versionLinks[oldID] = newID
	return nil
}

// VersionOf returns the new ID that superseded oldID, if any.
func (s *MemoryStore) VersionOf(oldID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.versionLinks[oldID]
	return id, ok
}
