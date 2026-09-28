package harness

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The ranker seam exists so memory retrieval can be something other than shared
// tokens. These tests assert the MECHANISM, not a semantic gain: the default
// local embedding is hashed word and character-bigram features, which tolerates
// near-matches but does not know that "authenticate" relates to "login
// credentials". A test claiming otherwise would be asserting something the
// implementation cannot deliver.

// reverseRanker makes the ordering observable without depending on any embedding
// quality at all.
type reverseRanker struct{ seen int }

func (r *reverseRanker) Rank(_ context.Context, _ string, records []MemoryRecord) ([]MemoryRecord, error) {
	r.seen = len(records)
	out := make([]MemoryRecord, len(records))
	for i, record := range records {
		out[len(records)-1-i] = record
	}
	return out, nil
}

type failingRanker struct{}

func (failingRanker) Rank(context.Context, string, []MemoryRecord) ([]MemoryRecord, error) {
	return nil, errors.New("embedding service unavailable")
}

func rankerManager(t *testing.T, ranker MemoryRanker) (*MemoryManager, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	manager := NewMemoryManager(store)
	manager.SetClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	manager.SetRanker(ranker)
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	for key, content := range map[string]string{"alpha": "first memory about databases", "beta": "second memory"} {
		if _, err := manager.Write(auth, "fact", "ns", key, content, nil, 0.9, "", 0); err != nil {
			t.Fatal(err)
		}
	}
	return manager, auth
}

// An installed ranker decides the order.
func TestInstalledRankerControlsOrder(t *testing.T) {
	ranker := &reverseRanker{}
	manager, auth := rankerManager(t, ranker)
	records, err := manager.Retrieve(auth, "databases", "ns")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("Retrieve returned %d records, want 2", len(records))
	}
	// The ranker saw everything, so its reversal is total rather than partial.
	if ranker.seen != 2 {
		t.Fatalf("the ranker saw %d records, want every live one (2)", ranker.seen)
	}
	if records[0].Key != "beta" || records[1].Key != "alpha" {
		t.Fatalf("the ranker did not control the order: %q, %q", records[0].Key, records[1].Key)
	}
}

// The critical mechanism: a ranker must see records that lexical filtering would
// have discarded, or a semantic ranker could never find them.
func TestRankerSeesRecordsLexicalFilteringWouldDrop(t *testing.T) {
	ranker := &reverseRanker{}
	manager, auth := rankerManager(t, ranker)
	// This query shares no token with "second memory".
	if _, err := manager.Retrieve(auth, "databases", "ns"); err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if ranker.seen != 2 {
		t.Fatalf("the ranker saw %d records; a lexical pre-filter dropped the ones it exists to find", ranker.seen)
	}
}

// Lexical retrieval alone would return only the token-sharing record, which is
// the behaviour the ranker replaces.
func TestWithoutARankerLexicalFilteringStillApplies(t *testing.T) {
	manager, auth := rankerManager(t, nil)
	records, err := manager.Retrieve(auth, "databases", "ns")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Key != "alpha" {
		t.Fatalf("lexical retrieval returned %d records, want only the matching one", len(records))
	}
}

// A ranker is an enhancement, not a dependency: an embedding outage must degrade
// the ranking, not fail retrieval.
func TestRankerFailureFallsBackToLexical(t *testing.T) {
	manager, auth := rankerManager(t, failingRanker{})
	records, err := manager.Retrieve(auth, "databases", "ns")
	if err != nil {
		t.Fatalf("a ranker failure failed retrieval outright: %v", err)
	}
	if len(records) != 1 || records[0].Key != "alpha" {
		t.Fatalf("the fallback did not produce the lexical result: %d records", len(records))
	}
}

// The default ranker is deterministic, so an index or an order built from it is
// reproducible across runs.
func TestEmbeddingRankerIsDeterministic(t *testing.T) {
	manager, auth := rankerManager(t, &EmbeddingRanker{Dimension: 128})
	first, err := manager.Retrieve(auth, "memory about databases", "ns")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	second, err := manager.Retrieve(auth, "memory about databases", "ns")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatalf("record counts differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Key != second[i].Key {
			t.Fatalf("order is unstable at %d: %q vs %q", i, first[i].Key, second[i].Key)
		}
	}
}

// Near-matches are what the local embedding can honestly claim: character
// bigrams give partial credit for a plural or a typo where exact token overlap
// gives none.
func TestEmbeddingRankerToleratesNearMatches(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	manager := NewMemoryManager(store)
	manager.SetClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	manager.SetRanker(&EmbeddingRanker{Dimension: 256})
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
	if _, err := manager.Write(auth, "fact", "ns", "vectors", "the vectors are stored here", nil, 0.9, "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(auth, "fact", "ns", "unrelated", "weather in another city", nil, 0.9, "", 0); err != nil {
		t.Fatal(err)
	}
	// Singular query against a plural record: no exact token overlap.
	records, err := manager.Retrieve(auth, "vector", "ns")
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("a near-match query returned nothing")
	}
	if records[0].Key != "vectors" {
		t.Fatalf("near-match ranking put %q first", records[0].Key)
	}
}

// A misaligned embedding response must be an error rather than silently pairing
// documents with the wrong scores.
type shortEmbedding struct{}

func (shortEmbedding) Embed(context.Context, []string) ([][]float32, error) {
	return [][]float32{{1, 0}}, nil
}
func (shortEmbedding) Dimension() int { return 2 }

func TestEmbeddingRankerRejectsMisalignedVectors(t *testing.T) {
	ranker := &EmbeddingRanker{Service: shortEmbedding{}}
	_, err := ranker.Rank(context.Background(), "query", []MemoryRecord{{Key: "a"}, {Key: "b"}})
	if err == nil {
		t.Fatal("a short embedding response was accepted, so scores would be attached to the wrong records")
	}
}
