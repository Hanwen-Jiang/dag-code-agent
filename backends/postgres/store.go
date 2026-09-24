// Package postgres implements the harness.Store contract on top of
// PostgreSQL using github.com/jackc/pgx/v5 and pgxpool.
//
// The package lives outside harness/ on purpose: the embeddable core must not
// pull pgx (or any network driver) into its build closure. Only programs that
// explicitly import this package take the PostgreSQL dependency.
//
// The backend implements the primitives, never the invariants. The event hash
// chain, the state-digest binding and replay verification stay in harness
// (see harness/journal.go); this package only performs atomic conditional
// writes and returns a wrapped harness.ErrStoreConflict when a precondition
// fails, exactly like SQLiteStore (harness/store.go) — the reference
// implementation it mirrors.
//
// Every method that must be atomic runs inside a single pgx transaction:
//
//   - CreateTask inserts the task row, its genesis event and the genesis
//     checkpoint together, failing with ErrStoreConflict on a duplicate task.
//   - AppendEvent locks the task row (SELECT ... FOR UPDATE), checks the
//     expected version and the journal tail inside the transaction, and then
//     inserts the event, updates the task row and inserts the checkpoint.
//   - PutMemoryVersioned takes a transaction-scoped advisory lock on the
//     (tenant, subject, namespace, key) chain before comparing the current
//     maximum live version, so two racing writers cannot both publish version
//     n+1 under READ COMMITTED.
//   - PutArtifact uses INSERT ... ON CONFLICT DO NOTHING plus a digest
//     comparison, so the same artifact id can only be rewritten with identical
//     content.
//
// The harness.Store methods do not take a context, so the backend uses
// context.Background() for the statements it issues. Embedders that need
// deadlines should wrap the pool itself (pgxpool.Config) or use a DSN-level
// statement timeout.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// Compile-time assertions: the backend satisfies the full composed surface,
// not just CoreStore, and it implements the optional collaboration interfaces
// (workspaces/topics + mailbox) rather than merely omitting them.
var (
	_ harness.Store          = (*Store)(nil)
	_ harness.WorkspaceStore = (*Store)(nil)
	_ harness.MailboxStore   = (*Store)(nil)
)

// Store is a PostgreSQL-backed harness.Store. It is safe for concurrent use:
// pgxpool hands each statement its own connection and the atomic operations
// rely on row locks, unique constraints or advisory locks rather than on
// process-local state.
type Store struct {
	pool  *pgxpool.Pool
	clock harness.Clock
}

// New opens a pgxpool for dsn, verifies connectivity and runs Migrate. The
// returned store owns the pool and must be closed by the caller.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	store := &Store{pool: pool, clock: harness.SystemClock()}
	if err := store.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

// NewFromPool wraps an existing pool without running Migrate. The caller keeps
// ownership of the pool (closing the store closes it, so do not close it
// twice) and remains responsible for calling Migrate at least once.
func NewFromPool(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, clock: harness.SystemClock()}
}

// SetClock replaces the time source used for timestamps and lease expiry. It
// must be called before the store is shared with other goroutines.
func (s *Store) SetClock(clock harness.Clock) { s.clock = clockOrSystem(clock) }

// Close releases the connection pool. It always returns nil; pgxpool.Close
// has no error path.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// Ping reports whether the database is reachable. It is not part of
// harness.Store; it exists so health checks can probe a pool that already
// exists.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// --- helpers ---------------------------------------------------------------

func (s *Store) clockNow() time.Time { return clockOrSystem(s.clock).Now() }

func (s *Store) now() string { return s.clockNow().UTC().Format(time.RFC3339Nano) }

func clockOrSystem(clock harness.Clock) harness.Clock {
	if clock == nil {
		return harness.SystemClock()
	}
	return clock
}

// canonical delegates to the core's canonical JSON encoder so that payloads
// written by this backend hash identically to payloads written by SQLiteStore
// or InMemoryStore.
func canonical(v any) ([]byte, error) { return harness.CanonicalJSON(v) }

// encodeState materializes a task snapshot for the tasks/checkpoints tables.
// The chain fields themselves are computed in harness/journal.go.
func encodeState(state harness.TaskState) (string, error) {
	state.Normalize()
	b, err := canonical(state)
	return string(b), err
}

func decodeState(value string) (harness.TaskState, error) {
	var state harness.TaskState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return state, err
	}
	state.Normalize()
	return state, nil
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// isUniqueViolation reports whether err is a Postgres unique-constraint error
// (SQLSTATE 23505). It is how "already exists" preconditions are detected
// without a separate SELECT that could race.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// withTx runs fn in a transaction and commits it when fn returns nil.
func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	// Rollback after a successful commit is a no-op that reports ErrTxClosed,
	// so it is safe to leave it as an unconditional cleanup.
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- TaskStore -------------------------------------------------------------

// CreateTask atomically creates the task row, its genesis event and the
// genesis checkpoint. The event arrives fully computed from the core journal
// (see harness/journal.go); this method never recomputes chain fields. A
// duplicate task id is reported as a wrapped harness.ErrStoreConflict.
func (s *Store) CreateTask(state harness.TaskState, genesis harness.Event) error {
	state.Normalize()
	if genesis.Sequence != 1 {
		return fmt.Errorf("%w: genesis event must be sequence 1", harness.ErrStoreConflict)
	}
	stateJSON, err := encodeState(state)
	if err != nil {
		return err
	}
	payloadJSON, err := canonical(genesis.Payload)
	if err != nil {
		return err
	}
	createdAt := genesis.CreatedAt
	if createdAt == "" {
		createdAt = s.now()
	}
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_tasks(task_id,state_json,version,updated_at) VALUES($1,$2,$3,$4)`,
			state.TaskID, stateJSON, genesis.TaskVersion, createdAt); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: task %s already exists", harness.ErrStoreConflict, state.TaskID)
			}
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_events(task_id,sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			state.TaskID, genesis.Sequence, genesis.Type, string(payloadJSON), genesis.PreviousHash, genesis.EventHash, createdAt, genesis.TaskVersion); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO harness_checkpoints(task_id,sequence,state_json,created_at) VALUES($1,$2,$3,$4)`,
			state.TaskID, genesis.Sequence, stateJSON, createdAt)
		return err
	})
}

// AppendEvent atomically appends a fully computed event together with the
// state it materializes. Every precondition is checked inside the same
// transaction as the write, so a stale caller fails with ErrStoreConflict
// instead of forking the journal.
//
// The task row is read with FOR UPDATE: under READ COMMITTED that serializes
// concurrent appends for the same task, which is what makes the
// expectedVersion / expectedSequence checks atomic rather than advisory.
func (s *Store) AppendEvent(state harness.TaskState, expectedVersion, expectedSequence *int, next harness.Event) error {
	state.Normalize()
	stateJSON, err := encodeState(state)
	if err != nil {
		return err
	}
	payloadJSON, err := canonical(next.Payload)
	if err != nil {
		return err
	}
	ctx := context.Background()
	return s.withTx(ctx, func(tx pgx.Tx) error {
		var currentVersion int
		if err := tx.QueryRow(ctx,
			`SELECT version FROM harness_tasks WHERE task_id=$1 FOR UPDATE`, state.TaskID).Scan(&currentVersion); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: task %s", harness.ErrStoreConflict, state.TaskID)
			}
			return err
		}
		if expectedVersion != nil && currentVersion != *expectedVersion {
			return fmt.Errorf("%w: version=%d expected=%d", harness.ErrStoreConflict, currentVersion, *expectedVersion)
		}
		var seq int
		var previous string
		err := tx.QueryRow(ctx,
			`SELECT sequence,event_hash FROM harness_events WHERE task_id=$1 ORDER BY sequence DESC LIMIT 1`, state.TaskID).Scan(&seq, &previous)
		if errors.Is(err, pgx.ErrNoRows) {
			seq = 0
			previous = "GENESIS"
		} else if err != nil {
			return err
		}
		if expectedSequence != nil && seq != *expectedSequence {
			return fmt.Errorf("%w: sequence=%d expected=%d", harness.ErrStoreConflict, seq, *expectedSequence)
		}
		// Defense in depth: the core computed these fields, but a backend that
		// accepted a non-contiguous event would corrupt the chain. Refuse it.
		if next.Sequence != seq+1 || next.PreviousHash != previous {
			return fmt.Errorf("%w: event does not extend the journal tail", harness.ErrStoreConflict)
		}
		if next.TaskVersion != currentVersion+1 {
			return fmt.Errorf("%w: version=%d expected=%d", harness.ErrStoreConflict, next.TaskVersion, currentVersion+1)
		}
		createdAt := next.CreatedAt
		if createdAt == "" {
			createdAt = s.now()
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_events(task_id,sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
			state.TaskID, next.Sequence, next.Type, string(payloadJSON), next.PreviousHash, next.EventHash, createdAt, next.TaskVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE harness_tasks SET state_json=$1,version=$2,updated_at=$3 WHERE task_id=$4`,
			stateJSON, next.TaskVersion, createdAt, state.TaskID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_checkpoints(task_id,sequence,state_json,created_at) VALUES($1,$2,$3,$4)`,
			state.TaskID, next.Sequence, stateJSON, createdAt); err != nil {
			return err
		}
		return nil
	})
}

// GetTask returns the materialized state and its version, or harness.ErrNotFound
// when the task does not exist.
func (s *Store) GetTask(taskID string) (harness.TaskState, int, error) {
	var stateJSON string
	var version int
	err := s.pool.QueryRow(context.Background(),
		`SELECT state_json,version FROM harness_tasks WHERE task_id=$1`, taskID).Scan(&stateJSON, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return harness.TaskState{}, 0, harness.ErrNotFound
	}
	if err != nil {
		return harness.TaskState{}, 0, err
	}
	state, err := decodeState(stateJSON)
	return state, version, err
}

// ListTasks returns materialized task state in update order. The session API
// uses this projection for a lightweight inbox; event payloads remain behind
// the per-session event endpoint so listing never loads an entire trajectory.
func (s *Store) ListTasks() ([]harness.TaskState, error) {
	rows, err := s.pool.Query(context.Background(),
		`SELECT state_json FROM harness_tasks ORDER BY updated_at DESC, task_id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []harness.TaskState
	for rows.Next() {
		var stateJSON string
		if err := rows.Scan(&stateJSON); err != nil {
			return nil, err
		}
		state, err := decodeState(stateJSON)
		if err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	return result, rows.Err()
}

// --- EventStore ------------------------------------------------------------

// Events returns the whole journal for a task, oldest first. A task with no
// events yields an empty slice and no error.
func (s *Store) Events(taskID string) ([]harness.Event, error) {
	return s.EventsSince(taskID, 0)
}

// EventsSince returns the append-only journal suffix after sequence. It is
// used by reconnecting clients and the SSE projection so a browser can resume
// from Last-Event-ID without downloading the whole trajectory again.
func (s *Store) EventsSince(taskID string, sequence int) ([]harness.Event, error) {
	if sequence < 0 {
		sequence = 0
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version FROM harness_events WHERE task_id=$1 AND sequence>$2 ORDER BY sequence`,
		taskID, sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []harness.Event
	for rows.Next() {
		var e harness.Event
		var payload string
		if err := rows.Scan(&e.Sequence, &e.Type, &payload, &e.PreviousHash, &e.EventHash, &e.CreatedAt, &e.TaskVersion); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(payload), &e.Payload); err != nil {
			return nil, err
		}
		e.TaskID = taskID
		events = append(events, e)
	}
	return events, rows.Err()
}

// LatestEvent returns the journal tail. ok is false when the task has no
// events yet, which the core treats as "the next event is genesis".
func (s *Store) LatestEvent(taskID string) (harness.Event, bool, error) {
	var e harness.Event
	var payload string
	err := s.pool.QueryRow(context.Background(),
		`SELECT sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version FROM harness_events WHERE task_id=$1 ORDER BY sequence DESC LIMIT 1`,
		taskID).Scan(&e.Sequence, &e.Type, &payload, &e.PreviousHash, &e.EventHash, &e.CreatedAt, &e.TaskVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return harness.Event{}, false, nil
	}
	if err != nil {
		return harness.Event{}, false, err
	}
	if err := json.Unmarshal([]byte(payload), &e.Payload); err != nil {
		return harness.Event{}, false, err
	}
	e.TaskID = taskID
	return e, true, nil
}

// CheckpointAt returns the state snapshot written alongside a given event
// sequence. ok is false when that sequence has no checkpoint.
func (s *Store) CheckpointAt(taskID string, sequence int) (harness.Checkpoint, bool, error) {
	var c harness.Checkpoint
	var stateJSON string
	err := s.pool.QueryRow(context.Background(),
		`SELECT sequence,state_json,created_at FROM harness_checkpoints WHERE task_id=$1 AND sequence=$2`,
		taskID, sequence).Scan(&c.Sequence, &stateJSON, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return harness.Checkpoint{}, false, nil
	}
	if err != nil {
		return harness.Checkpoint{}, false, err
	}
	c.TaskID = taskID
	c.StateJSON = stateJSON
	c.State, err = decodeState(stateJSON)
	if err != nil {
		return harness.Checkpoint{}, false, err
	}
	return c, true, nil
}

// LatestCheckpoint returns the most recent checkpoint, or harness.ErrNotFound
// when the task has none.
func (s *Store) LatestCheckpoint(taskID string) (harness.Checkpoint, error) {
	var c harness.Checkpoint
	var stateJSON string
	err := s.pool.QueryRow(context.Background(),
		`SELECT sequence,state_json,created_at FROM harness_checkpoints WHERE task_id=$1 ORDER BY sequence DESC LIMIT 1`,
		taskID).Scan(&c.Sequence, &stateJSON, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, harness.ErrNotFound
	}
	if err != nil {
		return c, err
	}
	c.TaskID = taskID
	c.StateJSON = stateJSON
	c.State, err = decodeState(stateJSON)
	return c, err
}

// --- IdempotencyStore ------------------------------------------------------

// PutIdempotency records a result under (scope, key). ok is false when the key
// already existed; the first writer wins. ON CONFLICT DO NOTHING makes the
// check-and-insert a single atomic statement.
func (s *Store) PutIdempotency(scope, key string, value any) (bool, error) {
	b, err := canonical(value)
	if err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(context.Background(),
		`INSERT INTO harness_idempotency(scope,idem_key,result_json,created_at) VALUES($1,$2,$3,$4) ON CONFLICT (scope,idem_key) DO NOTHING`,
		scope, key, string(b), s.now())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// GetIdempotency reads a previously stored result. ok is false when absent.
func (s *Store) GetIdempotency(scope, key string, out any) (bool, error) {
	var raw string
	err := s.pool.QueryRow(context.Background(),
		`SELECT result_json FROM harness_idempotency WHERE scope=$1 AND idem_key=$2`, scope, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if out != nil {
		if err := json.Unmarshal([]byte(raw), out); err != nil {
			return false, err
		}
	}
	return true, nil
}

// --- MemoryStore -----------------------------------------------------------

// PutMemory stores a record unconditionally.
func (s *Store) PutMemory(auth harness.AuthContext, record harness.MemoryRecord) error {
	tags, err := canonical(record.Tags)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(context.Background(),
		`INSERT INTO harness_memories(memory_id,tenant_id,subject_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,expires_at,supersedes,tombstone) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		record.MemoryID, auth.TenantID, auth.SubjectID, record.MemoryType, record.Namespace, record.Key, record.Content, string(tags), record.Importance, record.Confidence, record.Version, record.Source, record.CreatedAt, nullable(record.ExpiresAt), nullable(record.Supersedes), boolInt(record.Tombstone))
	return err
}

// PutMemoryVersioned inserts a memory record only when the latest live version
// for the same (tenant, subject, namespace, key) chain equals expectedVersion.
// The read and the insert share one transaction, and a transaction-scoped
// advisory lock on the chain serializes racing writers: under READ COMMITTED a
// plain MAX(version) read would not see a concurrent uncommitted insert, so
// without the lock two writers could both publish version n+1. ok is false
// when the precondition did not hold.
func (s *Store) PutMemoryVersioned(auth harness.AuthContext, record harness.MemoryRecord, expectedVersion int) (bool, error) {
	tags, err := canonical(record.Tags)
	if err != nil {
		return false, err
	}
	ok := false
	ctx := context.Background()
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		// hashtext keeps the lock key small; a hash collision across chains
		// only costs extra serialization, never correctness.
		lockKey := auth.TenantID + "\x1f" + auth.SubjectID + "\x1f" + record.Namespace + "\x1f" + record.Key
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, lockKey); err != nil {
			return err
		}
		var current int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(version),0) FROM harness_memories WHERE tenant_id=$1 AND subject_id=$2 AND namespace=$3 AND memory_key=$4 AND tombstone=0`,
			auth.TenantID, auth.SubjectID, record.Namespace, record.Key).Scan(&current); err != nil {
			return err
		}
		if current != expectedVersion {
			return nil
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_memories(memory_id,tenant_id,subject_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,expires_at,supersedes,tombstone) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
			record.MemoryID, auth.TenantID, auth.SubjectID, record.MemoryType, record.Namespace, record.Key, record.Content, string(tags), record.Importance, record.Confidence, record.Version, record.Source, record.CreatedAt, nullable(record.ExpiresAt), nullable(record.Supersedes), boolInt(record.Tombstone)); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
}

// MemoryRecords returns records for the authenticated identity, newest version
// first. An empty namespace means "every namespace".
func (s *Store) MemoryRecords(auth harness.AuthContext, namespace string) ([]harness.MemoryRecord, error) {
	query := `SELECT memory_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,COALESCE(expires_at,''),COALESCE(supersedes,''),tombstone FROM harness_memories WHERE tenant_id=$1 AND subject_id=$2`
	args := []any{auth.TenantID, auth.SubjectID}
	if namespace != "" {
		query += ` AND namespace=$3`
		args = append(args, namespace)
	}
	query += ` ORDER BY version DESC, created_at DESC`
	rows, err := s.pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []harness.MemoryRecord
	for rows.Next() {
		var r harness.MemoryRecord
		var tags string
		var tomb int
		if err := rows.Scan(&r.MemoryID, &r.MemoryType, &r.Namespace, &r.Key, &r.Content, &tags, &r.Importance, &r.Confidence, &r.Version, &r.Source, &r.CreatedAt, &r.ExpiresAt, &r.Supersedes, &tomb); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(tags), &r.Tags)
		r.TenantID = auth.TenantID
		r.SubjectID = auth.SubjectID
		r.Tombstone = tomb != 0
		result = append(result, r)
	}
	return result, rows.Err()
}

// TombstoneMemory hides a record without deleting its history. ok is false
// when no row matched the id inside the caller's tenant/subject scope.
func (s *Store) TombstoneMemory(auth harness.AuthContext, id string) (bool, error) {
	tag, err := s.pool.Exec(context.Background(),
		`UPDATE harness_memories SET tombstone=1 WHERE memory_id=$1 AND tenant_id=$2 AND subject_id=$3`,
		id, auth.TenantID, auth.SubjectID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// --- LeaseStore ------------------------------------------------------------

// AcquireLease takes resourceKey for ownerID until now+ttl. ok is false when a
// live lease is already held; an expired lease is reclaimable.
//
// Expiry is driven by the injected clock, never by time.Now(), so tests can
// cross a TTL without sleeping. The conditional delete and the insert share a
// transaction and the insert is guarded by the UNIQUE constraint on
// resource_key, so a race resolves to exactly one winner.
func (s *Store) AcquireLease(resourceKey, ownerID, leaseID string, ttl time.Duration) (bool, error) {
	now := float64(s.clockNow().UnixNano()) / 1e9
	acquired := false
	ctx := context.Background()
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM harness_leases WHERE resource_key=$1 AND expires_at<=$2`, resourceKey, now); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx,
			`INSERT INTO harness_leases(lease_id,resource_key,owner_id,expires_at,acquired_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
			leaseID, resourceKey, ownerID, now+ttl.Seconds(), s.now())
		if err != nil {
			return err
		}
		acquired = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return false, err
	}
	return acquired, nil
}

// RenewLease extends a live lease owned by ownerID. Renewing an expired or
// foreign lease reports ok=false instead of resurrecting it.
func (s *Store) RenewLease(leaseID, ownerID string, ttl time.Duration) (bool, error) {
	now := float64(s.clockNow().UnixNano()) / 1e9
	tag, err := s.pool.Exec(context.Background(),
		`UPDATE harness_leases SET expires_at=$1 WHERE lease_id=$2 AND owner_id=$3 AND expires_at>$4`,
		now+ttl.Seconds(), leaseID, ownerID, now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseLease deletes a lease by id, scoped to its owner.
func (s *Store) ReleaseLease(leaseID, ownerID string) (bool, error) {
	tag, err := s.pool.Exec(context.Background(),
		`DELETE FROM harness_leases WHERE lease_id=$1 AND owner_id=$2`, leaseID, ownerID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseResourceLease deletes whichever lease currently holds resourceKey for
// ownerID, without the caller having to remember the lease id.
func (s *Store) ReleaseResourceLease(resourceKey, ownerID string) (bool, error) {
	tag, err := s.pool.Exec(context.Background(),
		`DELETE FROM harness_leases WHERE resource_key=$1 AND owner_id=$2`, resourceKey, ownerID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// --- ArtifactStore ---------------------------------------------------------

// PutArtifact commits an artifact once for an identity scope. A second write
// with the same id is only accepted when its content digest matches; this
// prevents a retried tool from silently replacing evidence with a different
// value. A mismatch is reported as a wrapped harness.ErrStoreConflict.
func (s *Store) PutArtifact(auth harness.AuthContext, artifactID string, content map[string]any) (harness.ArtifactRecord, error) {
	if artifactID == "" {
		return harness.ArtifactRecord{}, errors.New("ARTIFACT_ID_REQUIRED")
	}
	if content == nil {
		content = map[string]any{}
	}
	contentJSON, err := canonical(content)
	if err != nil {
		return harness.ArtifactRecord{}, err
	}
	digest, err := harness.Digest(content)
	if err != nil {
		return harness.ArtifactRecord{}, err
	}
	record := harness.ArtifactRecord{ArtifactID: artifactID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Content: content, SHA256: digest, CreatedAt: s.now()}
	ctx := context.Background()
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO harness_artifacts(tenant_id,subject_id,artifact_id,content_json,sha256,created_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,subject_id,artifact_id) DO NOTHING`,
			auth.TenantID, auth.SubjectID, artifactID, string(contentJSON), digest, record.CreatedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			return nil
		}
		// The artifact already exists: only an identical digest may be
		// returned, and the caller gets the originally stored record.
		var existingJSON, existingHash, createdAt string
		if err := tx.QueryRow(ctx,
			`SELECT content_json,sha256,created_at FROM harness_artifacts WHERE tenant_id=$1 AND subject_id=$2 AND artifact_id=$3`,
			auth.TenantID, auth.SubjectID, artifactID).Scan(&existingJSON, &existingHash, &createdAt); err != nil {
			return err
		}
		if existingHash != digest {
			return fmt.Errorf("%w: artifact %s content changed", harness.ErrStoreConflict, artifactID)
		}
		var existing map[string]any
		if err := json.Unmarshal([]byte(existingJSON), &existing); err != nil {
			return err
		}
		record.Content = existing
		record.SHA256 = existingHash
		record.CreatedAt = createdAt
		return nil
	})
	return record, err
}

// GetArtifact reads an artifact under the same tenant/subject boundary used by
// the tool runtime. It deliberately returns a boolean so callers can map a
// missing artifact to a model-visible observation instead of a store error.
func (s *Store) GetArtifact(auth harness.AuthContext, artifactID string) (harness.ArtifactRecord, bool, error) {
	var record harness.ArtifactRecord
	var contentJSON string
	err := s.pool.QueryRow(context.Background(),
		`SELECT content_json,sha256,created_at FROM harness_artifacts WHERE tenant_id=$1 AND subject_id=$2 AND artifact_id=$3`,
		auth.TenantID, auth.SubjectID, artifactID).Scan(&contentJSON, &record.SHA256, &record.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return harness.ArtifactRecord{}, false, nil
	}
	if err != nil {
		return harness.ArtifactRecord{}, false, err
	}
	if err := json.Unmarshal([]byte(contentJSON), &record.Content); err != nil {
		return harness.ArtifactRecord{}, false, err
	}
	record.ArtifactID = artifactID
	record.TenantID = auth.TenantID
	record.SubjectID = auth.SubjectID
	return record, true, nil
}

// --- FeedbackStore ---------------------------------------------------------

// PutFeedback records an outcome signal, generating the id and the timestamp
// when the caller left them empty.
func (s *Store) PutFeedback(signal harness.FeedbackSignal) error {
	if signal.FeedbackID == "" {
		signal.FeedbackID = harness.NewID("FDBK")
	}
	if signal.CreatedAt == "" {
		signal.CreatedAt = s.now()
	}
	metadata, err := canonical(signal.Metadata)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(context.Background(),
		`INSERT INTO harness_feedback(feedback_id,task_id,tenant_id,subject_id,source,stage,outcome,category,rating,comment,variant,cohort,metadata_json,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		signal.FeedbackID, signal.TaskID, signal.TenantID, signal.SubjectID, signal.Source, signal.Stage, signal.Outcome, signal.Category, signal.Rating, signal.Comment, signal.Variant, signal.Cohort, string(metadata), signal.CreatedAt)
	return err
}

// Feedback returns every signal recorded for a tenant, oldest first.
func (s *Store) Feedback(tenantID string) ([]harness.FeedbackSignal, error) {
	rows, err := s.pool.Query(context.Background(),
		`SELECT feedback_id,task_id,tenant_id,subject_id,source,stage,outcome,category,rating,comment,variant,cohort,metadata_json,created_at FROM harness_feedback WHERE tenant_id=$1 ORDER BY created_at,feedback_id`,
		tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []harness.FeedbackSignal{}
	for rows.Next() {
		var item harness.FeedbackSignal
		var metadata string
		if err := rows.Scan(&item.FeedbackID, &item.TaskID, &item.TenantID, &item.SubjectID, &item.Source, &item.Stage, &item.Outcome, &item.Category, &item.Rating, &item.Comment, &item.Variant, &item.Cohort, &metadata, &item.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(metadata), &item.Metadata)
		out = append(out, item)
	}
	return out, rows.Err()
}

// --- WorkspaceStore / MailboxStore -----------------------------------------

// requireWorkspace enforces workspace existence inside a transaction, scoped to
// the caller's tenant/subject. It exists so every workspace-scoped write in
// this backend reports the shared harness.ErrWorkspaceNotFound sentinel instead
// of a raw foreign-key violation, exactly like requireWorkspace in
// harness/store.go. The workspace id comes from the call, never from a message
// body, and the identity always comes from the AuthContext.
func requireWorkspace(ctx context.Context, tx pgx.Tx, auth harness.AuthContext, workspaceID string) error {
	var exists int
	err := tx.QueryRow(ctx,
		`SELECT 1 FROM harness_workspaces WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3`,
		auth.TenantID, auth.SubjectID, workspaceID).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return harness.ErrWorkspaceNotFound
	}
	return err
}

// EnsureWorkspace creates or returns a tenant-scoped workspace.
//
// ON CONFLICT DO NOTHING makes re-ensuring idempotent without a race: the
// insert and the read-back share one transaction, so a retried agent start can
// never overwrite the metadata (or the timestamps) of an existing workspace.
func (s *Store) EnsureWorkspace(auth harness.AuthContext, workspaceID string, metadata map[string]any) (harness.Workspace, error) {
	if workspaceID == "" {
		return harness.Workspace{}, errors.New("WORKSPACE_ID_REQUIRED")
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadataJSON, err := canonical(metadata)
	if err != nil {
		return harness.Workspace{}, err
	}
	now := s.now()
	var raw string
	workspace := harness.Workspace{WorkspaceID: workspaceID, TenantID: auth.TenantID, SubjectID: auth.SubjectID}
	ctx := context.Background()
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_workspaces(tenant_id,subject_id,workspace_id,metadata_json,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,subject_id,workspace_id) DO NOTHING`,
			auth.TenantID, auth.SubjectID, workspaceID, string(metadataJSON), now, now); err != nil {
			return err
		}
		return tx.QueryRow(ctx,
			`SELECT metadata_json,created_at,updated_at FROM harness_workspaces WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3`,
			auth.TenantID, auth.SubjectID, workspaceID).Scan(&raw, &workspace.CreatedAt, &workspace.UpdatedAt)
	})
	if err != nil {
		return harness.Workspace{}, err
	}
	_ = json.Unmarshal([]byte(raw), &workspace.Metadata)
	return workspace, nil
}

// PutTopicValue writes one topic key with compare-and-swap on expectedVersion.
// ok is false (with a nil error) when the stored version does not match, which
// lets the core re-read and retry instead of treating it as a failure.
//
// A transaction-scoped advisory lock serializes writers on one
// (tenant, subject, workspace, topic, key) chain: under READ COMMITTED a plain
// MAX(version) read would not see a concurrent uncommitted write, so two
// writers holding the same expectedVersion could both publish it. This is the
// same defence PutMemoryVersioned uses for its version chain.
func (s *Store) PutTopicValue(auth harness.AuthContext, workspaceID, topic, key string, value map[string]any, expectedVersion int) (harness.TopicValue, bool, error) {
	if workspaceID == "" || topic == "" || key == "" {
		return harness.TopicValue{}, false, errors.New("TOPIC_SCOPE_REQUIRED")
	}
	if value == nil {
		value = map[string]any{}
	}
	valueJSON, err := canonical(value)
	if err != nil {
		return harness.TopicValue{}, false, err
	}
	digest, err := harness.Digest(value)
	if err != nil {
		return harness.TopicValue{}, false, err
	}
	now := s.now()
	var result harness.TopicValue
	ok := false
	ctx := context.Background()
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		// Check the workspace explicitly rather than relying on the foreign key
		// so that every backend reports the same WORKSPACE_NOT_FOUND error.
		if err := requireWorkspace(ctx, tx, auth, workspaceID); err != nil {
			return err
		}
		lockKey := auth.TenantID + "\x1f" + auth.SubjectID + "\x1f" + workspaceID + "\x1f" + topic + "\x1f" + key
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, lockKey); err != nil {
			return err
		}
		var current int
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(version),0) FROM harness_topics WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3 AND topic=$4 AND topic_key=$5`,
			auth.TenantID, auth.SubjectID, workspaceID, topic, key).Scan(&current); err != nil {
			return err
		}
		if current != expectedVersion {
			return nil
		}
		version := current + 1
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_topics(tenant_id,subject_id,workspace_id,topic,topic_key,value_json,digest,version,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (tenant_id,subject_id,workspace_id,topic,topic_key) DO UPDATE SET value_json=excluded.value_json,digest=excluded.digest,version=excluded.version,updated_at=excluded.updated_at`,
			auth.TenantID, auth.SubjectID, workspaceID, topic, key, string(valueJSON), digest, version, now); err != nil {
			return err
		}
		result = harness.TopicValue{WorkspaceID: workspaceID, Topic: topic, Key: key, Value: value, Digest: digest, Version: version, UpdatedAt: now}
		ok = true
		return nil
	})
	return result, ok, err
}

// TopicValues returns the key/value projection of one topic, ordered by key.
// A topic that does not exist (or belongs to another tenant/subject) yields an
// empty slice, matching the SQLite and in-memory backends.
func (s *Store) TopicValues(auth harness.AuthContext, workspaceID, topic string) ([]harness.TopicValue, error) {
	rows, err := s.pool.Query(context.Background(),
		`SELECT topic,topic_key,value_json,digest,version,updated_at FROM harness_topics WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3 AND topic=$4 ORDER BY topic_key`,
		auth.TenantID, auth.SubjectID, workspaceID, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []harness.TopicValue{}
	for rows.Next() {
		var item harness.TopicValue
		var raw string
		if err := rows.Scan(&item.Topic, &item.Key, &raw, &item.Digest, &item.Version, &item.UpdatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &item.Value); err != nil {
			return nil, err
		}
		item.WorkspaceID = workspaceID
		values = append(values, item)
	}
	return values, rows.Err()
}

// AppendMailbox durably appends one envelope and assigns its per-topic
// sequence.
//
// The idempotency key identifies one message inside one topic's sequence space,
// so the deduplication lookup is keyed by
// (tenant, subject, workspace, topic, from_agent, idempotency_key) and a
// replayed send returns the originally stored message instead of appending a
// second one. The advisory lock covers exactly that sequence space, so
// concurrent appends cannot both observe the same MAX(sequence) and collide on
// the mailbox primary key.
func (s *Store) AppendMailbox(auth harness.AuthContext, message harness.MailboxMessage) (harness.MailboxMessage, error) {
	if message.WorkspaceID == "" || message.Topic == "" || message.FromAgent == "" {
		return harness.MailboxMessage{}, errors.New("MAILBOX_SCOPE_REQUIRED")
	}
	if message.MessageID == "" {
		message.MessageID = harness.NewID("MSG")
	}
	if message.Payload == nil {
		message.Payload = map[string]any{}
	}
	payload, err := canonical(message.Payload)
	if err != nil {
		return harness.MailboxMessage{}, err
	}
	now := s.now()
	ctx := context.Background()
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		if err := requireWorkspace(ctx, tx, auth, message.WorkspaceID); err != nil {
			return err
		}
		lockKey := auth.TenantID + "\x1f" + auth.SubjectID + "\x1f" + message.WorkspaceID + "\x1f" + message.Topic
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, lockKey); err != nil {
			return err
		}
		if message.IdempotencyKey != "" {
			var existing harness.MailboxMessage
			var raw string
			var acked int
			// Topic is part of the idempotency scope: the same key in another
			// topic is a different message.
			err := tx.QueryRow(ctx,
				`SELECT message_id,sequence,from_agent,to_agent,protocol,message_type,correlation_id,payload_json,created_at,acked FROM harness_mailbox WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3 AND topic=$4 AND from_agent=$5 AND idempotency_key=$6`,
				auth.TenantID, auth.SubjectID, message.WorkspaceID, message.Topic, message.FromAgent, message.IdempotencyKey).
				Scan(&existing.MessageID, &existing.Sequence, &existing.FromAgent, &existing.ToAgent, &existing.Protocol, &existing.MessageType, &existing.CorrelationID, &raw, &existing.CreatedAt, &acked)
			if err == nil {
				_ = json.Unmarshal([]byte(raw), &existing.Payload)
				existing.Acked = acked != 0
				existing.WorkspaceID = message.WorkspaceID
				existing.Topic = message.Topic
				existing.IdempotencyKey = message.IdempotencyKey
				existing.TenantID, existing.SubjectID = auth.TenantID, auth.SubjectID
				message = existing
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		var sequence int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence),0)+1 FROM harness_mailbox WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3 AND topic=$4`,
			auth.TenantID, auth.SubjectID, message.WorkspaceID, message.Topic).Scan(&sequence); err != nil {
			return err
		}
		message.Sequence, message.CreatedAt = sequence, now
		message.TenantID, message.SubjectID = auth.TenantID, auth.SubjectID
		message.Acked = false
		_, err := tx.Exec(ctx,
			`INSERT INTO harness_mailbox(tenant_id,subject_id,workspace_id,topic,sequence,message_id,from_agent,to_agent,protocol,message_type,correlation_id,idempotency_key,payload_json,created_at,acked) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,0)`,
			auth.TenantID, auth.SubjectID, message.WorkspaceID, message.Topic, message.Sequence, message.MessageID, message.FromAgent, message.ToAgent, message.Protocol, message.MessageType, message.CorrelationID, message.IdempotencyKey, string(payload), message.CreatedAt)
		return err
	})
	if err != nil {
		return harness.MailboxMessage{}, err
	}
	return message, nil
}

// MailboxMessages returns unacked messages for one recipient, oldest first.
//
// Direct messages use the row's acked flag and are only visible to the
// addressed recipient. Broadcast messages (an empty to_agent) are acknowledged
// per recipient through harness_mailbox_acks, so one agent reading a broadcast
// cannot hide it from the other agents on the topic.
func (s *Store) MailboxMessages(auth harness.AuthContext, workspaceID, topic, recipient string, limit int) ([]harness.MailboxMessage, error) {
	if limit <= 0 {
		limit = 64
	}
	rows, err := s.pool.Query(context.Background(),
		`SELECT sequence,message_id,from_agent,to_agent,protocol,message_type,correlation_id,idempotency_key,payload_json,created_at,acked FROM harness_mailbox WHERE tenant_id=$1 AND subject_id=$2 AND workspace_id=$3 AND topic=$4 AND (
  (to_agent=$5 AND acked=0)
  OR (to_agent='' AND NOT EXISTS (SELECT 1 FROM harness_mailbox_acks a WHERE a.tenant_id=harness_mailbox.tenant_id AND a.subject_id=harness_mailbox.subject_id AND a.message_id=harness_mailbox.message_id AND a.recipient=$6))
 ) ORDER BY sequence LIMIT $7`,
		auth.TenantID, auth.SubjectID, workspaceID, topic, recipient, recipient, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := []harness.MailboxMessage{}
	for rows.Next() {
		var message harness.MailboxMessage
		var raw string
		var acked int
		if err := rows.Scan(&message.Sequence, &message.MessageID, &message.FromAgent, &message.ToAgent, &message.Protocol, &message.MessageType, &message.CorrelationID, &message.IdempotencyKey, &raw, &message.CreatedAt, &acked); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &message.Payload); err != nil {
			return nil, err
		}
		message.Acked = acked != 0
		message.TenantID, message.SubjectID = auth.TenantID, auth.SubjectID
		message.WorkspaceID, message.Topic = workspaceID, topic
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

// AckMailbox acknowledges one message for one recipient. It is idempotent.
//
// A direct message is acknowledged on its own row and only by the addressed
// recipient; anybody else gets (false, nil). A broadcast message is
// acknowledged per recipient, so every other agent still receives it.
func (s *Store) AckMailbox(auth harness.AuthContext, messageID, recipient string) (bool, error) {
	if recipient == "" {
		return false, errors.New("MAILBOX_RECIPIENT_REQUIRED")
	}
	acknowledged := false
	ctx := context.Background()
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var toAgent string
		err := tx.QueryRow(ctx,
			`SELECT to_agent FROM harness_mailbox WHERE tenant_id=$1 AND subject_id=$2 AND message_id=$3`,
			auth.TenantID, auth.SubjectID, messageID).Scan(&toAgent)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if toAgent != "" {
			if toAgent != recipient {
				// Only the addressed recipient may ack a direct message.
				return nil
			}
			tag, err := tx.Exec(ctx,
				`UPDATE harness_mailbox SET acked=1 WHERE tenant_id=$1 AND subject_id=$2 AND message_id=$3 AND to_agent=$4`,
				auth.TenantID, auth.SubjectID, messageID, recipient)
			if err != nil {
				return err
			}
			acknowledged = tag.RowsAffected() == 1
			return nil
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO harness_mailbox_acks(tenant_id,subject_id,message_id,recipient,acked_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT (tenant_id,subject_id,message_id,recipient) DO NOTHING`,
			auth.TenantID, auth.SubjectID, messageID, recipient, s.now()); err != nil {
			return err
		}
		// Idempotent: a repeat ack still reports success.
		acknowledged = true
		return nil
	})
	return acknowledged, err
}

// --- Migrate ---------------------------------------------------------------

// migrateDDL is the full schema, mirroring harness/store.go's SQLite init()
// with Postgres types (REAL becomes DOUBLE PRECISION) plus the implicit
// UNIQUE constraint SQLite declares inline on leases.resource_key. Every
// statement is idempotent, so Migrate is safe to run on every boot.
var migrateDDL = []string{
	`CREATE TABLE IF NOT EXISTS harness_tasks (
 task_id TEXT PRIMARY KEY, state_json TEXT NOT NULL, version INTEGER NOT NULL, updated_at TEXT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS harness_events (
 task_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_type TEXT NOT NULL,
 payload_json TEXT NOT NULL, previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
 created_at TEXT NOT NULL, task_version INTEGER NOT NULL,
 PRIMARY KEY(task_id, sequence), FOREIGN KEY(task_id) REFERENCES harness_tasks(task_id)
)`,
	`CREATE TABLE IF NOT EXISTS harness_checkpoints (
 task_id TEXT NOT NULL, sequence INTEGER NOT NULL, state_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(task_id, sequence), FOREIGN KEY(task_id) REFERENCES harness_tasks(task_id)
)`,
	`CREATE TABLE IF NOT EXISTS harness_memories (
 memory_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL,
 memory_type TEXT NOT NULL, namespace TEXT NOT NULL, memory_key TEXT NOT NULL,
 content TEXT NOT NULL, tags_json TEXT NOT NULL, importance DOUBLE PRECISION NOT NULL, confidence DOUBLE PRECISION NOT NULL,
 version INTEGER NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT,
 supersedes TEXT, tombstone INTEGER NOT NULL DEFAULT 0
)`,
	`CREATE INDEX IF NOT EXISTS idx_harness_memory_scope ON harness_memories(tenant_id, subject_id, namespace)`,
	`CREATE TABLE IF NOT EXISTS harness_idempotency (
 scope TEXT NOT NULL, idem_key TEXT NOT NULL, result_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(scope, idem_key)
)`,
	`CREATE TABLE IF NOT EXISTS harness_leases (
 lease_id TEXT PRIMARY KEY, resource_key TEXT NOT NULL UNIQUE, owner_id TEXT NOT NULL,
 expires_at DOUBLE PRECISION NOT NULL, acquired_at TEXT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS harness_feedback (
 feedback_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL,
 source TEXT NOT NULL, stage TEXT NOT NULL, outcome TEXT NOT NULL, category TEXT NOT NULL,
 rating INTEGER NOT NULL, comment TEXT NOT NULL, variant TEXT NOT NULL, cohort TEXT NOT NULL,
 metadata_json TEXT NOT NULL, created_at TEXT NOT NULL
)`,
	`CREATE TABLE IF NOT EXISTS harness_artifacts (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, artifact_id TEXT NOT NULL,
 content_json TEXT NOT NULL, sha256 TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, artifact_id)
)`,
	// Collaboration tables. They mirror harness/store.go's SQLite schema with
	// Postgres types (SQLite's 64-bit INTEGER becomes BIGINT) and the same
	// indexes, so a workspace-scoped write cannot exist without its workspace
	// and the mailbox keeps the same visibility rules on both backends.
	`CREATE TABLE IF NOT EXISTS harness_workspaces (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
 metadata_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, workspace_id)
)`,
	`CREATE TABLE IF NOT EXISTS harness_topics (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
 topic TEXT NOT NULL, topic_key TEXT NOT NULL, value_json TEXT NOT NULL,
 digest TEXT NOT NULL, version INTEGER NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, workspace_id, topic, topic_key),
 FOREIGN KEY(tenant_id, subject_id, workspace_id) REFERENCES harness_workspaces(tenant_id, subject_id, workspace_id)
)`,
	`CREATE TABLE IF NOT EXISTS harness_mailbox (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
 topic TEXT NOT NULL, sequence BIGINT NOT NULL, message_id TEXT NOT NULL,
 from_agent TEXT NOT NULL, to_agent TEXT NOT NULL, protocol TEXT NOT NULL,
 message_type TEXT NOT NULL, correlation_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 payload_json TEXT NOT NULL, created_at TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(tenant_id, subject_id, workspace_id, topic, sequence),
 UNIQUE(tenant_id, subject_id, message_id)
)`,
	`CREATE INDEX IF NOT EXISTS idx_harness_mailbox_receive ON harness_mailbox(tenant_id, subject_id, workspace_id, topic, to_agent, acked, sequence)`,
	// Idempotency is scoped to one topic's sequence space, so topic is part of
	// the unique key. (This backend never shipped an index without topic, so
	// unlike the SQLite migration there is nothing to drop on upgrade.)
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_harness_mailbox_idempotency_topic ON harness_mailbox(tenant_id, subject_id, workspace_id, topic, from_agent, idempotency_key) WHERE idempotency_key <> ''`,
	// Broadcast messages (to_agent = '') need per-recipient acknowledgement: a
	// single acked flag would let the first reader hide the message from every
	// other agent on the topic.
	`CREATE TABLE IF NOT EXISTS harness_mailbox_acks (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, message_id TEXT NOT NULL,
 recipient TEXT NOT NULL, acked_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, message_id, recipient)
)`,
}

// Migrate creates the tables and indexes this backend needs. It is idempotent:
// every statement uses IF NOT EXISTS and the whole schema is applied in one
// transaction, so a partially migrated database is never observed.
func (s *Store) Migrate(ctx context.Context) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, statement := range migrateDDL {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("postgres: migrate: %w", err)
			}
		}
		return nil
	})
}
