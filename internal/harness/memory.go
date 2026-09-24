package harness

import (
	"errors"
	"sort"
	"strings"
	"time"
)

type MemoryManager struct{ Store *Store }

func NewMemoryManager(store *Store) *MemoryManager { return &MemoryManager{Store: store} }

func (m *MemoryManager) Write(auth AuthContext, memoryType, namespace, key, content string, tags []string, confidence float64, expiresAt string, expectedVersion int) (MemoryRecord, error) {
	if m.Store == nil {
		return MemoryRecord{}, errors.New("memory store is nil")
	}
	// Version check and insert must be one critical section; otherwise two
	// writers can both observe the same latest record and publish version n+1.
	m.Store.mu.Lock()
	defer m.Store.mu.Unlock()
	records, err := m.Store.MemoryRecords(auth, namespace)
	if err != nil {
		return MemoryRecord{}, err
	}
	var latest *MemoryRecord
	for i := range records {
		if records[i].Key == key && !records[i].Tombstone && (latest == nil || records[i].Version > latest.Version) {
			latest = &records[i]
		}
	}
	version := 1
	supersedes := ""
	if latest != nil {
		if expectedVersion > 0 && latest.Version != expectedVersion {
			return MemoryRecord{}, errors.New("VERSION_CONFLICT")
		}
		version = latest.Version + 1
		supersedes = latest.MemoryID
	} else if expectedVersion > 1 {
		return MemoryRecord{}, errors.New("VERSION_CONFLICT")
	}
	if confidence <= 0 {
		confidence = .5
	}
	r := MemoryRecord{MemoryID: NewID("MEM"), TenantID: auth.TenantID, SubjectID: auth.SubjectID, MemoryType: memoryType, Namespace: namespace, Key: key, Content: content, Tags: tags, Importance: .5, Confidence: confidence, Version: version, Source: "agent", CreatedAt: UTCNow(), ExpiresAt: expiresAt, Supersedes: supersedes}
	return r, m.Store.PutMemory(auth, r)
}
func (m *MemoryManager) Retrieve(auth AuthContext, query string, namespace string) ([]MemoryRecord, error) {
	records, err := m.Store.MemoryRecords(auth, namespace)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	latest := map[string]MemoryRecord{}
	seen := map[string]bool{}
	for _, r := range records {
		key := r.Namespace + "\x00" + r.Key
		if seen[key] {
			continue
		}
		seen[key] = true
		if r.Tombstone {
			continue
		}
		if r.ExpiresAt != "" {
			if expiry, e := time.Parse(time.RFC3339Nano, r.ExpiresAt); e == nil && expiry.Before(now) {
				continue
			}
		}
		latest[key] = r
	}
	type scoredMemory struct {
		record MemoryRecord
		score  int
	}
	scored := []scoredMemory{}
	needle := strings.ToLower(strings.TrimSpace(query))
	for _, r := range latest {
		document := strings.Join(append([]string{r.Key, r.Content, r.Namespace}, r.Tags...), " ")
		score := lexicalOverlap(needle, document)
		if needle == "" || score > 0 {
			scored = append(scored, scoredMemory{record: r, score: score})
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		if scored[i].record.Confidence != scored[j].record.Confidence {
			return scored[i].record.Confidence > scored[j].record.Confidence
		}
		return scored[i].record.CreatedAt > scored[j].record.CreatedAt
	})
	out := make([]MemoryRecord, 0, len(scored))
	for _, item := range scored {
		out = append(out, item.record)
	}
	return out, nil
}
func (m *MemoryManager) Forget(auth AuthContext, id string) (bool, error) {
	if m.Store == nil {
		return false, errors.New("memory store is nil")
	}
	m.Store.mu.Lock()
	defer m.Store.mu.Unlock()
	return m.Store.TombstoneMemory(auth, id)
}
