// Package storetest is the conformance suite every harness.Store backend must
// pass.
//
// The point is to turn the control plane's correctness promises into an
// executable contract: an embedder who writes a Postgres (or Redis, or D1)
// backend can prove it behaves like the reference implementation before
// trusting it with a journal.
//
// It deliberately does NOT test hash-chain tampering: a backend stores opaque
// events, so tampering is simulated at the SQL layer in the harness package's
// own tests. What is checked here is the storage contract — atomic conditional
// appends, isolation, idempotency, lease expiry and content-addressed
// artifacts.
package storetest

import (
	"errors"
	"testing"
	"time"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// ClockSetter is implemented by backends that accept an injected clock.
type ClockSetter interface{ SetClock(harness.Clock) }

// Options controls which capability groups are exercised.
type Options struct {
	// SkipLeases skips lease conformance (a core-only backend has no leases).
	SkipLeases bool
	// SkipMemory skips memory conformance.
	SkipMemory bool
	// SkipArtifacts skips artifact conformance.
	SkipArtifacts bool
	// SkipFeedback skips feedback conformance.
	SkipFeedback bool
	// SkipIdempotency skips idempotency conformance.
	SkipIdempotency bool
}

// RunAll runs the full suite against a fresh store from newStore.
func RunAll(t *testing.T, newStore func(t *testing.T) harness.Store) {
	t.Helper()
	RunCore(t, newStore)
	RunFull(t, newStore, Options{})
}

// RunCore runs only the TaskStore/EventStore contract, which is what a
// CoreStore backend must satisfy.
func RunCore(t *testing.T, newStore func(t *testing.T) harness.Store) {
	t.Helper()
	t.Run("Core/TaskLifecycle", func(t *testing.T) { testTaskLifecycle(t, newStore(t)) })
	t.Run("Core/ResumeJournal", func(t *testing.T) { testResumeJournal(t, newStore(t)) })
}

// RunFull runs every capability group, honouring Options.
func RunFull(t *testing.T, newStore func(t *testing.T) harness.Store, opts Options) {
	t.Helper()
	if !opts.SkipIdempotency {
		t.Run("Idempotency/FirstWriterWins", func(t *testing.T) { testIdempotency(t, newStore(t)) })
	}
	if !opts.SkipMemory {
		t.Run("Memory/VersionedWrite", func(t *testing.T) { testMemoryVersioned(t, newStore(t)) })
		t.Run("Memory/ScopeIsolation", func(t *testing.T) { testMemoryIsolation(t, newStore(t)) })
	}
	if !opts.SkipLeases {
		t.Run("Lease/MutualExclusion", func(t *testing.T) { testLeaseExclusion(t, newStore(t)) })
		t.Run("Lease/ExpiryReclaim", func(t *testing.T) { testLeaseExpiry(t, newStore(t)) })
	}
	if !opts.SkipArtifacts {
		t.Run("Artifact/ContentAddressed", func(t *testing.T) { testArtifact(t, newStore(t)) })
	}
	if !opts.SkipFeedback {
		t.Run("Feedback/RoundTrip", func(t *testing.T) { testFeedback(t, newStore(t)) })
	}
}

func newJournal(store harness.CoreStore) *harness.Journal {
	return harness.NewJournal(store, harness.SystemClock())
}

func seedTask(t *testing.T, store harness.CoreStore, taskID string) harness.TaskState {
	t.Helper()
	state := harness.TaskState{TaskID: taskID, Goal: "conformance", TenantID: "tenant", SubjectID: "subject", Status: harness.TaskReady}
	state.Normalize()
	if _, err := newJournal(store).Create(state, "TASK_ACCEPTED", map[string]any{"goal": state.Goal}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return state
}

// testTaskLifecycle checks the append contract: genesis, monotonic sequence,
// optimistic concurrency and refusal of non-contiguous events.
func testTaskLifecycle(t *testing.T, store harness.Store) {
	state := seedTask(t, store, "conformance-lifecycle")

	stored, version, err := store.GetTask(state.TaskID)
	if err != nil || version != 1 || stored.TaskID != state.TaskID {
		t.Fatalf("GetTask version=%d err=%v", version, err)
	}

	// Creating the same task twice must fail rather than fork the journal.
	if _, err := newJournal(store).Create(state, "TASK_ACCEPTED", map[string]any{}); err == nil {
		t.Fatal("duplicate CreateTask succeeded")
	}

	// A second create with a different id must succeed and be listed.
	seedTask(t, store, "conformance-lifecycle-2")
	tasks, err := store.ListTasks()
	if err != nil || len(tasks) < 2 {
		t.Fatalf("ListTasks len=%d err=%v", len(tasks), err)
	}

	// Missing tasks are reported as ErrNotFound, not as an empty state.
	if _, _, err := store.GetTask("does-not-exist"); !errors.Is(err, harness.ErrNotFound) {
		t.Fatalf("GetTask(missing) err=%v, want ErrNotFound", err)
	}
	if events, err := store.Events("does-not-exist"); err != nil || len(events) != 0 {
		t.Fatalf("Events(missing) len=%d err=%v", len(events), err)
	}

	// Commit two events and check the version/sequence walk.
	state.Status = harness.TaskRunning
	if _, err := newJournal(store).Commit(state, "STEP_STARTED", map[string]any{"step": 1}); err != nil {
		t.Fatalf("Commit 1: %v", err)
	}
	_, version, err = store.GetTask(state.TaskID)
	if err != nil || version != 2 {
		t.Fatalf("after commit version=%d err=%v", version, err)
	}
	tail, ok, err := store.LatestEvent(state.TaskID)
	if err != nil || !ok || tail.Sequence != 2 || tail.TaskVersion != 2 {
		t.Fatalf("LatestEvent seq=%d version=%d ok=%v err=%v", tail.Sequence, tail.TaskVersion, ok, err)
	}

	// A stale writer must be refused by the primitive itself.
	wrongVersion, wrongSequence := 99, 99
	if err := store.AppendEvent(state, &wrongVersion, &tail.Sequence, tail); !errors.Is(err, harness.ErrStoreConflict) {
		t.Fatalf("stale AppendEvent err=%v, want ErrStoreConflict", err)
	}
	if err := store.AppendEvent(state, nil, &wrongSequence, tail); !errors.Is(err, harness.ErrStoreConflict) {
		t.Fatalf("stale sequence err=%v, want ErrStoreConflict", err)
	}

	// EventsSince backs SSE resume.
	suffix, err := store.EventsSince(state.TaskID, 1)
	if err != nil || len(suffix) != 1 || suffix[0].Sequence != 2 {
		t.Fatalf("EventsSince len=%d err=%v", len(suffix), err)
	}
}

// testResumeJournal checks that a backend preserves enough for a fresh process
// to verify the chain and rebuild the materialized state.
func testResumeJournal(t *testing.T, store harness.Store) {
	state := seedTask(t, store, "conformance-resume")
	journal := newJournal(store)
	for step := 1; step <= 3; step++ {
		state.Step = step
		state.Status = harness.TaskRunning
		if _, err := journal.Commit(state, "STEP_STARTED", map[string]any{"step": step}); err != nil {
			t.Fatalf("commit %d: %v", step, err)
		}
	}

	// A brand-new journal over the same store must verify the chain.
	replayed, err := harness.NewJournal(store, harness.SystemClock()).Replay(state.TaskID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 4 {
		t.Fatalf("replayed %d events, want 4", len(replayed))
	}
	rebuilt, err := harness.NewJournal(store, harness.SystemClock()).ReplayState(state.TaskID)
	if err != nil {
		t.Fatalf("ReplayState: %v", err)
	}
	if rebuilt.Step != state.Step {
		t.Fatalf("rebuilt step=%d want %d", rebuilt.Step, state.Step)
	}

	// Checkpoints must be addressable by sequence as well as by tail.
	checkpoint, ok, err := store.CheckpointAt(state.TaskID, 2)
	if err != nil || !ok || checkpoint.Sequence != 2 {
		t.Fatalf("CheckpointAt ok=%v seq=%d err=%v", ok, checkpoint.Sequence, err)
	}
	if _, ok, err := store.CheckpointAt(state.TaskID, 999); err != nil || ok {
		t.Fatalf("CheckpointAt(missing) ok=%v err=%v", ok, err)
	}
	latest, err := store.LatestCheckpoint(state.TaskID)
	if err != nil || latest.Sequence != len(replayed) {
		t.Fatalf("LatestCheckpoint seq=%d err=%v", latest.Sequence, err)
	}
}

func testIdempotency(t *testing.T, store harness.Store) {
	type payload struct {
		Value string `json:"value"`
	}
	first, err := store.PutIdempotency("tool", "key-1", payload{Value: "a"})
	if err != nil || !first {
		t.Fatalf("first PutIdempotency ok=%v err=%v", first, err)
	}
	second, err := store.PutIdempotency("tool", "key-1", payload{Value: "b"})
	if err != nil || second {
		t.Fatalf("second PutIdempotency ok=%v err=%v (must be false)", second, err)
	}
	var out payload
	found, err := store.GetIdempotency("tool", "key-1", &out)
	if err != nil || !found || out.Value != "a" {
		t.Fatalf("GetIdempotency found=%v out=%+v err=%v", found, out, err)
	}
	if found, err := store.GetIdempotency("tool", "absent", &out); err != nil || found {
		t.Fatalf("GetIdempotency(absent) found=%v err=%v", found, err)
	}
	// Scopes are independent.
	if ok, err := store.PutIdempotency("other", "key-1", payload{Value: "c"}); err != nil || !ok {
		t.Fatalf("scoped PutIdempotency ok=%v err=%v", ok, err)
	}
}

func testMemoryVersioned(t *testing.T, store harness.Store) {
	memory, ok := store.(harness.MemoryStore)
	if !ok {
		t.Skip("backend does not implement MemoryStore")
	}
	auth := harness.AuthContext{TenantID: "tenant", SubjectID: "subject"}

	first := harness.MemoryRecord{MemoryID: "MEM-1", MemoryType: "semantic", Namespace: "ns", Key: "k", Content: "v1", Version: 1}
	stored, err := memory.PutMemoryVersioned(auth, first, 0)
	if err != nil || !stored {
		t.Fatalf("PutMemoryVersioned(first) stored=%v err=%v", stored, err)
	}
	// Version 1 exists, so an unversioned (expected 0) write must be refused.
	if stored, err := memory.PutMemoryVersioned(auth, harness.MemoryRecord{MemoryID: "MEM-2", Namespace: "ns", Key: "k", Content: "v2", Version: 2}, 0); err != nil || stored {
		t.Fatalf("stale memory write stored=%v err=%v, want refused", stored, err)
	}
	second := harness.MemoryRecord{MemoryID: "MEM-2", MemoryType: "semantic", Namespace: "ns", Key: "k", Content: "v2", Version: 2}
	if stored, err := memory.PutMemoryVersioned(auth, second, 1); err != nil || !stored {
		t.Fatalf("PutMemoryVersioned(v2) stored=%v err=%v", stored, err)
	}
	records, err := memory.MemoryRecords(auth, "ns")
	if err != nil || len(records) != 2 {
		t.Fatalf("MemoryRecords len=%d err=%v", len(records), err)
	}
	if records[0].Version != 2 {
		t.Fatalf("MemoryRecords[0].Version=%d, want newest first", records[0].Version)
	}
	if tombstoned, err := memory.TombstoneMemory(auth, "MEM-2"); err != nil || !tombstoned {
		t.Fatalf("TombstoneMemory ok=%v err=%v", tombstoned, err)
	}
	// Tombstoning is idempotent: a repeat may report "matched" or "unchanged"
	// depending on the backend, but it must not error and must not resurrect.
	if _, err := memory.TombstoneMemory(auth, "MEM-2"); err != nil {
		t.Fatalf("repeat TombstoneMemory err=%v", err)
	}
	after, err := memory.MemoryRecords(auth, "ns")
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range after {
		if record.MemoryID == "MEM-2" && !record.Tombstone {
			t.Fatalf("MEM-2 resurrected: %+v", record)
		}
	}
	if _, err := memory.TombstoneMemory(auth, "MEM-absent"); err != nil {
		t.Fatalf("TombstoneMemory(absent) err=%v", err)
	}
}

func testMemoryIsolation(t *testing.T, store harness.Store) {
	memory, ok := store.(harness.MemoryStore)
	if !ok {
		t.Skip("backend does not implement MemoryStore")
	}
	alice := harness.AuthContext{TenantID: "tenant-a", SubjectID: "alice"}
	bob := harness.AuthContext{TenantID: "tenant-b", SubjectID: "bob"}
	if _, err := memory.PutMemoryVersioned(alice, harness.MemoryRecord{MemoryID: "MEM-A", Namespace: "ns", Key: "k", Content: "alice", Version: 1}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.PutMemoryVersioned(bob, harness.MemoryRecord{MemoryID: "MEM-B", Namespace: "ns", Key: "k", Content: "bob", Version: 1}, 0); err != nil {
		t.Fatal(err)
	}
	records, err := memory.MemoryRecords(alice, "ns")
	if err != nil || len(records) != 1 || records[0].Content != "alice" {
		t.Fatalf("alice sees %+v err=%v", records, err)
	}
	// Namespace filtering must not leak across namespaces.
	if other, err := memory.MemoryRecords(alice, "other-ns"); err != nil || len(other) != 0 {
		t.Fatalf("namespace filter len=%d err=%v", len(other), err)
	}
	// Tombstoning another identity's record must not succeed.
	if ok, err := memory.TombstoneMemory(bob, "MEM-A"); err != nil || ok {
		t.Fatalf("cross-identity tombstone ok=%v err=%v", ok, err)
	}
}

func testLeaseExclusion(t *testing.T, store harness.Store) {
	leases, ok := store.(harness.LeaseStore)
	if !ok {
		t.Skip("backend does not implement LeaseStore")
	}
	acquired, err := leases.AcquireLease("node-1", "owner-a", "lease-a", time.Minute)
	if err != nil || !acquired {
		t.Fatalf("AcquireLease ok=%v err=%v", acquired, err)
	}
	// Mutual exclusion: a second owner cannot take a live lease.
	if acquired, err := leases.AcquireLease("node-1", "owner-b", "lease-b", time.Minute); err != nil || acquired {
		t.Fatalf("second AcquireLease ok=%v err=%v, want refused", acquired, err)
	}
	// Renewal is owner-scoped.
	if renewed, err := leases.RenewLease("lease-a", "owner-b", time.Minute); err != nil || renewed {
		t.Fatalf("cross-owner renew ok=%v err=%v, want false", renewed, err)
	}
	if renewed, err := leases.RenewLease("lease-a", "owner-a", time.Minute); err != nil || !renewed {
		t.Fatalf("renew ok=%v err=%v", renewed, err)
	}
	// Release by resource key without knowing the lease id.
	if released, err := leases.ReleaseResourceLease("node-1", "owner-a"); err != nil || !released {
		t.Fatalf("ReleaseResourceLease ok=%v err=%v", released, err)
	}
	if acquired, err := leases.AcquireLease("node-1", "owner-b", "lease-b", time.Minute); err != nil || !acquired {
		t.Fatalf("re-acquire after release ok=%v err=%v", acquired, err)
	}
	if released, err := leases.ReleaseLease("lease-b", "owner-b"); err != nil || !released {
		t.Fatalf("ReleaseLease ok=%v err=%v", released, err)
	}
}

// testLeaseExpiry proves TTL handling drives off the injected clock, which is
// exactly what an embedder needs to test scheduling without sleeping.
func testLeaseExpiry(t *testing.T, store harness.Store) {
	leases, ok := store.(harness.LeaseStore)
	if !ok {
		t.Skip("backend does not implement LeaseStore")
	}
	setter, ok := store.(ClockSetter)
	if !ok {
		t.Skip("backend does not support clock injection")
	}
	clock := harness.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	setter.SetClock(clock)

	if acquired, err := leases.AcquireLease("node-ttl", "owner-a", "lease-ttl", 30*time.Second); err != nil || !acquired {
		t.Fatalf("AcquireLease ok=%v err=%v", acquired, err)
	}
	// Still inside the TTL.
	clock.Advance(29 * time.Second)
	if acquired, err := leases.AcquireLease("node-ttl", "owner-b", "lease-ttl-b", 30*time.Second); err != nil || acquired {
		t.Fatalf("acquire inside TTL ok=%v err=%v, want refused", acquired, err)
	}
	// Past the TTL the lease is reclaimable.
	clock.Advance(2 * time.Second)
	if acquired, err := leases.AcquireLease("node-ttl", "owner-b", "lease-ttl-b", 30*time.Second); err != nil || !acquired {
		t.Fatalf("acquire after expiry ok=%v err=%v, want granted", acquired, err)
	}
}

func testArtifact(t *testing.T, store harness.Store) {
	artifacts, ok := store.(harness.ArtifactStore)
	if !ok {
		t.Skip("backend does not implement ArtifactStore")
	}
	auth := harness.AuthContext{TenantID: "tenant", SubjectID: "subject"}
	first, err := artifacts.PutArtifact(auth, "ART-1", map[string]any{"content": "verified"})
	if err != nil || first.SHA256 == "" {
		t.Fatalf("PutArtifact sha=%q err=%v", first.SHA256, err)
	}
	// Same id + same content is idempotent.
	again, err := artifacts.PutArtifact(auth, "ART-1", map[string]any{"content": "verified"})
	if err != nil || again.SHA256 != first.SHA256 {
		t.Fatalf("idempotent PutArtifact sha=%q err=%v", again.SHA256, err)
	}
	// Same id + different content is a conflict, not a silent overwrite.
	if _, err := artifacts.PutArtifact(auth, "ART-1", map[string]any{"content": "rewritten"}); err == nil {
		t.Fatal("overwriting artifact content succeeded, want conflict")
	}
	loaded, found, err := artifacts.GetArtifact(auth, "ART-1")
	if err != nil || !found || loaded.Content["content"] != "verified" {
		t.Fatalf("GetArtifact found=%v content=%v err=%v", found, loaded.Content, err)
	}
	if _, found, err := artifacts.GetArtifact(auth, "ART-absent"); err != nil || found {
		t.Fatalf("GetArtifact(absent) found=%v err=%v", found, err)
	}
	// Artifacts are scoped to tenant+subject.
	other := harness.AuthContext{TenantID: "tenant", SubjectID: "other"}
	if _, found, err := artifacts.GetArtifact(other, "ART-1"); err != nil || found {
		t.Fatalf("cross-subject GetArtifact found=%v err=%v", found, err)
	}
	if _, err := artifacts.PutArtifact(auth, "", map[string]any{}); err == nil {
		t.Fatal("PutArtifact with empty id succeeded")
	}
}

func testFeedback(t *testing.T, store harness.Store) {
	feedback, ok := store.(harness.FeedbackStore)
	if !ok {
		t.Skip("backend does not implement FeedbackStore")
	}
	signal := harness.FeedbackSignal{TaskID: "task-1", TenantID: "tenant", SubjectID: "subject", Source: "test", Stage: "final", Outcome: "accepted", Category: "quality", Rating: 5}
	if err := feedback.PutFeedback(signal); err != nil {
		t.Fatalf("PutFeedback: %v", err)
	}
	items, err := feedback.Feedback("tenant")
	if err != nil || len(items) != 1 {
		t.Fatalf("Feedback len=%d err=%v", len(items), err)
	}
	if items[0].FeedbackID == "" || items[0].CreatedAt == "" {
		t.Fatalf("Feedback missing generated fields: %+v", items[0])
	}
	if other, err := feedback.Feedback("other-tenant"); err != nil || len(other) != 0 {
		t.Fatalf("Feedback(other) len=%d err=%v", len(other), err)
	}
}
