package harness

import (
	"testing"
	"time"
)

// List exists because Retrieve answers "what is relevant to this query" and a
// caller sometimes needs "what is there" — enumerating memories to index them
// for semantic search, which cannot be expressed as a query.
//
// It shares Retrieve's projection rather than filtering for itself, so the two
// cannot disagree about what is live. These tests pin that agreement, because
// the way two copies would drift is the dangerous direction: one of them
// returning a tombstoned or expired record as live.

func memoryTestManager(t *testing.T) (*MemoryManager, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	manager := NewMemoryManager(store)
	manager.SetClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)))
	return manager, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

func TestListReturnsLiveRecords(t *testing.T) {
	manager, auth := memoryTestManager(t)
	if _, err := manager.Write(auth, "fact", "ns", "alpha", "the first fact", nil, 0.9, "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(auth, "fact", "ns", "beta", "the second fact", nil, 0.8, "", 0); err != nil {
		t.Fatal(err)
	}
	records, err := manager.List(auth, "ns")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("List returned %d records, want 2", len(records))
	}
	// Deterministic order, so an index built from it is reproducible.
	if records[0].Key != "alpha" || records[1].Key != "beta" {
		t.Fatalf("List order is not by key: %q, %q", records[0].Key, records[1].Key)
	}
}

// A tombstoned record must not be listed. This is the failure that matters: an
// index built from a listing that included tombstones would resurrect deleted
// content into the model's context.
func TestListExcludesTombstonedRecords(t *testing.T) {
	manager, auth := memoryTestManager(t)
	if _, err := manager.Write(auth, "fact", "ns", "kept", "still here", nil, 0.9, "", 0); err != nil {
		t.Fatal(err)
	}
	removed, err := manager.Write(auth, "fact", "ns", "removed", "should vanish", nil, 0.9, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Forget takes a memory ID, not a key. Passing a key removes nothing and
	// reports that through its bool, which is why the result is checked rather
	// than discarded.
	forgot, err := manager.Forget(auth, removed.MemoryID)
	if err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if !forgot {
		t.Fatal("Forget reported that it removed nothing")
	}
	records, err := manager.List(auth, "ns")
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.Key == "removed" {
			t.Fatal("List returned a deleted record as live")
		}
	}
	if len(records) != 1 || records[0].Key != "kept" {
		t.Fatalf("List returned %d records, want only the kept one", len(records))
	}
}

func TestListExcludesExpiredRecords(t *testing.T) {
	manager, auth := memoryTestManager(t)
	past := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	future := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if _, err := manager.Write(auth, "fact", "ns", "stale", "expired", nil, 0.9, past, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(auth, "fact", "ns", "fresh", "still valid", nil, 0.9, future, 0); err != nil {
		t.Fatal(err)
	}
	records, err := manager.List(auth, "ns")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Key != "fresh" {
		t.Fatalf("List returned the wrong set: %d records", len(records))
	}
}

// The two views must agree about liveness. If they disagree, one of them is
// feeding the model content the other considers gone.
func TestListAndRetrieveAgreeOnLiveness(t *testing.T) {
	manager, auth := memoryTestManager(t)
	if _, err := manager.Write(auth, "fact", "ns", "alpha", "the database password rotation policy", nil, 0.9, "", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Write(auth, "fact", "ns", "beta", "unrelated content", nil, 0.5, "", 0); err != nil {
		t.Fatal(err)
	}
	gone, err := manager.Write(auth, "fact", "ns", "gone", "database", nil, 0.9, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if forgot, err := manager.Forget(auth, gone.MemoryID); err != nil || !forgot {
		t.Fatalf("Forget: forgot=%v err=%v", forgot, err)
	}

	listed, err := manager.List(auth, "ns")
	if err != nil {
		t.Fatal(err)
	}
	retrieved, err := manager.Retrieve(auth, "database", "ns")
	if err != nil {
		t.Fatal(err)
	}
	live := map[string]bool{}
	for _, record := range listed {
		live[record.Key] = true
	}
	for _, record := range retrieved {
		if !live[record.Key] {
			t.Fatalf("Retrieve returned %q, which List considers gone", record.Key)
		}
	}
	if len(retrieved) == 0 {
		t.Fatal("the query matched nothing, so the agreement check was vacuous")
	}
	// Enumerating everything then filtering must not admit anything Retrieve
	// excluded as dead.
	if len(listed) != 2 {
		t.Fatalf("List returned %d live records, want 2", len(listed))
	}
}

// Enumeration is tenant-scoped like every other memory operation.
func TestListIsTenantScoped(t *testing.T) {
	manager, auth := memoryTestManager(t)
	if _, err := manager.Write(auth, "fact", "ns", "alpha", "mine", nil, 0.9, "", 0); err != nil {
		t.Fatal(err)
	}
	other := AuthContext{TenantID: "other", SubjectID: "subject", Scopes: []string{"read", "write"}}
	records, err := manager.List(other, "ns")
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("another tenant's memories leaked: %d records", len(records))
	}
}
