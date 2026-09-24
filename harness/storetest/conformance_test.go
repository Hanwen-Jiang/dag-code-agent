package storetest_test

import (
	"testing"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/harness/storetest"
)

// The whole point of the abstraction: two unrelated backends must satisfy the
// same executable contract. If a change to the runtime needs a new store
// guarantee, it is added here once and both backends have to keep up.
func TestSQLiteStoreConformance(t *testing.T) {
	storetest.RunAll(t, func(t *testing.T) harness.Store {
		t.Helper()
		store, err := harness.NewSQLiteStore(":memory:")
		if err != nil {
			t.Fatalf("NewSQLiteStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestInMemoryStoreConformance(t *testing.T) {
	storetest.RunAll(t, func(t *testing.T) harness.Store {
		t.Helper()
		return harness.NewInMemoryStore()
	})
}
