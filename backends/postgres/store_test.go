package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/harness/storetest"
)

// dsnEnv is the environment variable that points at a scratch PostgreSQL
// database used for conformance testing, for example
// "postgres://postgres@127.0.0.1:5432/postgres?sslmode=disable".
const dsnEnv = "HARNESS_TEST_POSTGRES_DSN"

// TestPostgresConformance runs the shared harness.Store conformance suite
// against a live database. It skips when no DSN is configured, so `go test`
// stays green on machines without PostgreSQL.
func TestPostgresConformance(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skip("HARNESS_TEST_POSTGRES_DSN not set")
	}
	storetest.RunAll(t, func(t *testing.T) harness.Store {
		return newIsolatedStore(t, dsn)
	})
}

// TestMigrateIdempotent proves Migrate can run repeatedly, which is what
// production does on every boot. It skips when no DSN is configured.
func TestMigrateIdempotent(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skip("HARNESS_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	store, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if err := store.Migrate(ctx); err != nil {
			t.Fatalf("Migrate #%d: %v", attempt, err)
		}
	}
}

// TestConcurrentAppendEventStalePreconditions guards the claim that
// AppendEvent's preconditions are enforced inside the write transaction: many
// writers holding the same (version, sequence) precondition must produce
// exactly one committed event and N-1 conflicts, never two events at the same
// sequence. It skips without a DSN.
//
// The event hash is synthetic on purpose: this test targets the storage
// primitive's conditional write (the chain itself is the core's invariant,
// covered by the conformance suite's Replay checks).
func TestConcurrentAppendEventStalePreconditions(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skip("HARNESS_TEST_POSTGRES_DSN not set")
	}
	store := newIsolatedStore(t, dsn)
	state := harness.TaskState{TaskID: "race-append", Goal: "race", TenantID: "tenant", SubjectID: "subject", Status: harness.TaskReady}
	state.Normalize()
	if _, err := harness.NewJournal(store, harness.SystemClock()).Create(state, "TASK_ACCEPTED", map[string]any{}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	tail, ok, err := store.LatestEvent(state.TaskID)
	if err != nil || !ok {
		t.Fatalf("LatestEvent ok=%v err=%v", ok, err)
	}
	expectedVersion, expectedSequence := 1, tail.Sequence
	next := harness.Event{
		TaskID:       state.TaskID,
		Sequence:     tail.Sequence + 1,
		Type:         "STEP_STARTED",
		Payload:      map[string]any{"writer": "race"},
		PreviousHash: tail.EventHash,
		EventHash:    "sha256:synthetic-for-precondition-test",
		TaskVersion:  expectedVersion + 1,
	}

	const writers = 8
	var wg sync.WaitGroup
	results := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = store.AppendEvent(state, &expectedVersion, &expectedSequence, next)
		}(i)
	}
	wg.Wait()
	successes, conflicts := 0, 0
	for i, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, harness.ErrStoreConflict):
			conflicts++
		default:
			t.Fatalf("writer %d err=%v, want nil or ErrStoreConflict", i, err)
		}
	}
	if successes != 1 || conflicts != writers-1 {
		t.Fatalf("successes=%d conflicts=%d, want 1/%d", successes, conflicts, writers-1)
	}
	events, err := store.Events(state.TaskID)
	if err != nil || len(events) != 2 {
		t.Fatalf("Events len=%d err=%v, want genesis plus one winner", len(events), err)
	}
	if _, version, err := store.GetTask(state.TaskID); err != nil || version != 2 {
		t.Fatalf("GetTask version=%d err=%v, want 2", version, err)
	}
}

// TestConcurrentCommitKeepsJournalContiguous races whole Journal.Commit calls,
// which re-read the version and the tail before appending. Any number of them
// may win (a late reader legitimately appends the next event), but the journal
// must stay contiguous and replayable and the materialized version must match
// the number of events. It skips without a DSN.
func TestConcurrentCommitKeepsJournalContiguous(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skip("HARNESS_TEST_POSTGRES_DSN not set")
	}
	store := newIsolatedStore(t, dsn)
	state := harness.TaskState{TaskID: "race-commit", Goal: "race", TenantID: "tenant", SubjectID: "subject", Status: harness.TaskReady}
	state.Normalize()
	if _, err := harness.NewJournal(store, harness.SystemClock()).Create(state, "TASK_ACCEPTED", map[string]any{}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	const writers = 8
	var wg sync.WaitGroup
	results := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = harness.NewJournal(store, harness.SystemClock()).Commit(state, "STEP_STARTED", map[string]any{"writer": i})
		}(i)
	}
	wg.Wait()
	successes := 0
	for i, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, harness.ErrStoreConflict):
		default:
			t.Fatalf("writer %d err=%v, want nil or ErrStoreConflict", i, err)
		}
	}
	if successes < 1 {
		t.Fatal("no concurrent commit succeeded")
	}
	events, err := harness.NewJournal(store, harness.SystemClock()).Replay(state.TaskID)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(events) != successes+1 {
		t.Fatalf("events=%d successes=%d, want events=successes+1", len(events), successes)
	}
	if _, version, err := store.GetTask(state.TaskID); err != nil || version != len(events) {
		t.Fatalf("GetTask version=%d err=%v, want %d", version, err, len(events))
	}
}

// TestConcurrentPutMemoryVersioned guards the advisory-lock chain: N writers
// that all expect "no live record" must produce exactly one stored record.
// Without the lock, several could pass the MAX(version) check concurrently.
// It skips without a DSN.
func TestConcurrentPutMemoryVersioned(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skip("HARNESS_TEST_POSTGRES_DSN not set")
	}
	store := newIsolatedStore(t, dsn)
	auth := harness.AuthContext{TenantID: "tenant", SubjectID: "subject"}

	const writers = 8
	var wg sync.WaitGroup
	stored := make([]bool, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			record := harness.MemoryRecord{
				MemoryID:  harness.NewID("MEM"),
				Namespace: "ns",
				Key:       "k",
				Content:   "value",
				Version:   1,
			}
			stored[i], errs[i] = store.PutMemoryVersioned(auth, record, 0)
		}(i)
	}
	wg.Wait()
	winners := 0
	for i := range stored {
		if errs[i] != nil {
			t.Fatalf("writer %d err=%v", i, errs[i])
		}
		if stored[i] {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d, want exactly 1", winners)
	}
	records, err := store.MemoryRecords(auth, "ns")
	if err != nil || len(records) != 1 {
		t.Fatalf("MemoryRecords len=%d err=%v, want exactly 1", len(records), err)
	}
}

// TestConcurrentMailboxAppendKeepsTopicSequence guards the per-topic sequence
// assignment the collaboration contract promises: racing sends must land on
// distinct positions in one topic's sequence space, and a key replayed
// concurrently must still deduplicate to the original message. Both rely on the
// transaction-scoped advisory lock over (tenant, subject, workspace, topic);
// without it two writers could read the same MAX(sequence) and collide on the
// mailbox primary key. It skips without a DSN.
func TestConcurrentMailboxAppendKeepsTopicSequence(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skip("HARNESS_TEST_POSTGRES_DSN not set")
	}
	store := newIsolatedStore(t, dsn)
	auth := harness.AuthContext{TenantID: "tenant", SubjectID: "subject"}
	if _, err := store.EnsureWorkspace(auth, "ws-race", nil); err != nil {
		t.Fatalf("EnsureWorkspace: %v", err)
	}

	// Even writers race on one shared idempotency key; odd writers each use a
	// fresh key. So the topic must end up with 1 + writers/2 messages.
	const writers = 8
	var wg sync.WaitGroup
	messages := make([]harness.MailboxMessage, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "idem-shared"
			if i%2 == 1 {
				key = harness.NewID("idem")
			}
			messages[i], errs[i] = store.AppendMailbox(auth, harness.MailboxMessage{
				WorkspaceID: "ws-race", Topic: "work", FromAgent: "a", ToAgent: "b",
				Protocol: "json/v1", MessageType: "result", IdempotencyKey: key,
				Payload: map[string]any{"writer": i},
			})
		}(i)
	}
	wg.Wait()

	// Deduplicated replays legitimately return the same (id, sequence), so the
	// invariant is per distinct message: no two messages may share a position.
	sequenceOf := map[string]int64{}
	sequenceOwner := map[int64]string{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
		id := messages[i].MessageID
		if previous, ok := sequenceOf[id]; ok && previous != messages[i].Sequence {
			t.Fatalf("message %s reported sequences %d and %d", id, previous, messages[i].Sequence)
		}
		sequenceOf[id] = messages[i].Sequence
	}
	for id, sequence := range sequenceOf {
		if owner, ok := sequenceOwner[sequence]; ok {
			t.Fatalf("sequence %d assigned to both %s and %s", sequence, owner, id)
		}
		sequenceOwner[sequence] = id
	}
	// The four shared-key writers must all observe the same stored message.
	if messages[0].MessageID != messages[2].MessageID || messages[0].MessageID != messages[6].MessageID {
		t.Fatalf("shared idempotency key was not deduplicated: %q vs %q vs %q", messages[0].MessageID, messages[2].MessageID, messages[6].MessageID)
	}
	if len(sequenceOf) != 1+writers/2 {
		t.Fatalf("distinct messages=%d, want %d", len(sequenceOf), 1+writers/2)
	}
	for position := int64(1); position <= 1+writers/2; position++ {
		if _, ok := sequenceOwner[position]; !ok {
			t.Fatalf("sequence space has no message at %d: %v", position, sequenceOwner)
		}
	}

	// Receive sees exactly the distinct messages, in sequence order.
	received, err := store.MailboxMessages(auth, "ws-race", "work", "b", 100)
	if err != nil {
		t.Fatalf("MailboxMessages: %v", err)
	}
	if len(received) != 1+writers/2 {
		t.Fatalf("received %d messages, want %d", len(received), 1+writers/2)
	}
	for i := 1; i < len(received); i++ {
		if received[i-1].Sequence >= received[i].Sequence {
			t.Fatalf("receive not ordered: %d then %d", received[i-1].Sequence, received[i].Sequence)
		}
	}

	// An empty recipient is a caller error, never a silent no-op ack.
	if acked, err := store.AckMailbox(auth, received[0].MessageID, ""); err == nil || acked {
		t.Fatalf("AckMailbox(empty recipient) ok=%v err=%v, want error", acked, err)
	}
}

// newIsolatedStore returns a store whose tables live in a freshly created
// schema, so every conformance subtest starts from an empty database and
// repeated runs never collide on task ids. The schema is dropped and the pool
// closed when the test ends.
//
// The DSN user therefore needs CREATE on the database. Point the DSN at a
// scratch database, never at production data.
func newIsolatedStore(t *testing.T, dsn string) *Store {
	t.Helper()
	ctx := context.Background()
	schema := "harness_test_" + strings.ReplaceAll(strings.TrimPrefix(harness.NewID("s"), "s-"), "-", "_")

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for schema setup: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		_ = admin.Close(ctx)
		t.Fatalf("create schema %s: %v", schema, err)
	}
	_ = admin.Close(ctx)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	// search_path is sent in the startup packet for every pooled connection,
	// so Migrate and all later statements resolve against the test schema.
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	store := NewFromPool(pool)
	if err := store.Migrate(ctx); err != nil {
		pool.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx := context.Background()
		conn, err := pgx.Connect(cleanupCtx, dsn)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(cleanupCtx) }()
		_, _ = conn.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	return store
}
