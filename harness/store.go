package harness

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrStoreConflict = errors.New("store conflict")
	ErrNotFound      = errors.New("not found")
)

type Event struct {
	TaskID       string         `json:"task_id"`
	Sequence     int            `json:"sequence"`
	Type         string         `json:"event_type"`
	Payload      map[string]any `json:"payload,omitempty"`
	PreviousHash string         `json:"previous_hash"`
	EventHash    string         `json:"event_hash"`
	CreatedAt    string         `json:"created_at"`
	TaskVersion  int            `json:"task_version"`
}

type Checkpoint struct {
	TaskID    string    `json:"task_id"`
	Sequence  int       `json:"sequence"`
	StateJSON string    `json:"-"`
	State     TaskState `json:"state"`
	CreatedAt string    `json:"created_at"`
}

// ArtifactRecord is the durable side-effect projection used by tools that
// create readable evidence. The event journal records that the tool ran; this
// table keeps the value available after a worker or HTTP process restarts.
type ArtifactRecord struct {
	ArtifactID string         `json:"artifact_id"`
	TenantID   string         `json:"tenant_id"`
	SubjectID  string         `json:"subject_id"`
	Content    map[string]any `json:"content"`
	SHA256     string         `json:"sha256"`
	CreatedAt  string         `json:"created_at"`
}

// SQLiteStore is the default Store backend. It is pure Go (no CGO) and
// serializes writes on a single connection so that an event append and its
// materialized state update commit in one transaction.
//
// Because writes are serialized per instance, several embedded runtimes that
// share one *file* database will contend with each other. Use distinct files,
// in-memory stores, or a different backend for multi-instance embedding.
type SQLiteStore struct {
	db    *sql.DB
	mu    sync.Mutex
	clock Clock
}

// SetClock replaces the time source used for timestamps and lease expiry. It
// must be called before the store is shared with other goroutines.
func (s *SQLiteStore) SetClock(clock Clock) { s.clock = clockOrSystem(clock) }

func (s *SQLiteStore) now() string { return utcNow(s.clock) }

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		path = ":memory:"
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &SQLiteStore{db: db, clock: SystemClock()}
	if _, err = db.Exec(`PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;`); err != nil {
		db.Close()
		return nil, err
	}
	if err = store.init(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) init() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS harness_tasks (
 task_id TEXT PRIMARY KEY, state_json TEXT NOT NULL, version INTEGER NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS harness_events (
 task_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_type TEXT NOT NULL,
 payload_json TEXT NOT NULL, previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL,
 created_at TEXT NOT NULL, task_version INTEGER NOT NULL,
 PRIMARY KEY(task_id, sequence), FOREIGN KEY(task_id) REFERENCES harness_tasks(task_id)
);
CREATE TABLE IF NOT EXISTS harness_checkpoints (
 task_id TEXT NOT NULL, sequence INTEGER NOT NULL, state_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(task_id, sequence), FOREIGN KEY(task_id) REFERENCES harness_tasks(task_id)
);
CREATE TABLE IF NOT EXISTS harness_memories (
 memory_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL,
 memory_type TEXT NOT NULL, namespace TEXT NOT NULL, memory_key TEXT NOT NULL,
 content TEXT NOT NULL, tags_json TEXT NOT NULL, importance REAL NOT NULL, confidence REAL NOT NULL,
 version INTEGER NOT NULL, source TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT,
 supersedes TEXT, tombstone INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_harness_memory_scope ON harness_memories(tenant_id, subject_id, namespace);
CREATE TABLE IF NOT EXISTS harness_idempotency (
 scope TEXT NOT NULL, idem_key TEXT NOT NULL, result_json TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(scope, idem_key)
);
CREATE TABLE IF NOT EXISTS harness_leases (
 lease_id TEXT PRIMARY KEY, resource_key TEXT NOT NULL UNIQUE, owner_id TEXT NOT NULL,
 expires_at REAL NOT NULL, acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS harness_feedback (
 feedback_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL,
 source TEXT NOT NULL, stage TEXT NOT NULL, outcome TEXT NOT NULL, category TEXT NOT NULL,
 rating INTEGER NOT NULL, comment TEXT NOT NULL, variant TEXT NOT NULL, cohort TEXT NOT NULL,
 metadata_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS harness_artifacts (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, artifact_id TEXT NOT NULL,
 content_json TEXT NOT NULL, sha256 TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, artifact_id)
);
CREATE TABLE IF NOT EXISTS harness_workspaces (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
 metadata_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, workspace_id)
);
CREATE TABLE IF NOT EXISTS harness_topics (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
 topic TEXT NOT NULL, topic_key TEXT NOT NULL, value_json TEXT NOT NULL,
 digest TEXT NOT NULL, version INTEGER NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, workspace_id, topic, topic_key),
 FOREIGN KEY(tenant_id, subject_id, workspace_id) REFERENCES harness_workspaces(tenant_id, subject_id, workspace_id)
);
CREATE TABLE IF NOT EXISTS harness_mailbox (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
 topic TEXT NOT NULL, sequence INTEGER NOT NULL, message_id TEXT NOT NULL,
 from_agent TEXT NOT NULL, to_agent TEXT NOT NULL, protocol TEXT NOT NULL,
 message_type TEXT NOT NULL, correlation_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 payload_json TEXT NOT NULL, created_at TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(tenant_id, subject_id, workspace_id, topic, sequence),
 UNIQUE(tenant_id, subject_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_harness_mailbox_receive ON harness_mailbox(tenant_id, subject_id, workspace_id, topic, to_agent, acked, sequence);
-- Idempotency is scoped to one topic's sequence space, so topic is part of the
-- unique key. The old index (without topic) is dropped once on upgrade; the
-- DROP is a no-op on every later startup.
DROP INDEX IF EXISTS idx_harness_mailbox_idempotency;
CREATE UNIQUE INDEX IF NOT EXISTS idx_harness_mailbox_idempotency_topic ON harness_mailbox(tenant_id, subject_id, workspace_id, topic, from_agent, idempotency_key) WHERE idempotency_key <> '';
-- Broadcast messages (to_agent = '') need per-recipient acknowledgement: a
-- single acked flag would let the first reader hide the message from every
-- other agent on the topic.
CREATE TABLE IF NOT EXISTS harness_mailbox_acks (
 tenant_id TEXT NOT NULL, subject_id TEXT NOT NULL, message_id TEXT NOT NULL,
 recipient TEXT NOT NULL, acked_at TEXT NOT NULL,
 PRIMARY KEY(tenant_id, subject_id, message_id, recipient)
);
`)
	return err
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

func canonical(v any) ([]byte, error) { return CanonicalJSON(v) }

// encodeState materializes a task snapshot for the tasks/checkpoints tables.
// The chain fields themselves are computed in journal.go.
func encodeState(state TaskState) (string, error) {
	state.Normalize()
	b, err := canonical(state)
	return string(b), err
}

func decodeState(value string) (TaskState, error) {
	var state TaskState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return state, err
	}
	state.Normalize()
	return state, nil
}

func (s *SQLiteStore) withTx(fn func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// CreateTask atomically creates the task row, its genesis event and the
// genesis checkpoint. The event arrives fully computed from the core journal
// (see journal.go); this method never recomputes chain fields.
func (s *SQLiteStore) CreateTask(state TaskState, genesis Event) error {
	state.Normalize()
	stateJSON, err := encodeState(state)
	if err != nil {
		return err
	}
	if genesis.Sequence != 1 {
		return fmt.Errorf("%w: genesis event must be sequence 1", ErrStoreConflict)
	}
	payloadJSON, err := canonical(genesis.Payload)
	if err != nil {
		return err
	}
	createdAt := genesis.CreatedAt
	if createdAt == "" {
		createdAt = s.now()
	}
	return s.withTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO harness_tasks(task_id,state_json,version,updated_at) VALUES(?,?,?,?)`, state.TaskID, stateJSON, genesis.TaskVersion, createdAt); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO harness_events(task_id,sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version) VALUES(?,?,?,?,?,?,?,?)`, state.TaskID, genesis.Sequence, genesis.Type, string(payloadJSON), genesis.PreviousHash, genesis.EventHash, createdAt, genesis.TaskVersion); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO harness_checkpoints(task_id,sequence,state_json,created_at) VALUES(?,?,?,?)`, state.TaskID, genesis.Sequence, stateJSON, createdAt)
		return err
	})
}

// AppendEvent atomically appends a fully computed event together with the
// state it materializes. Every precondition is checked inside the same
// transaction as the write, so a stale caller fails with ErrStoreConflict
// instead of forking the journal.
func (s *SQLiteStore) AppendEvent(state TaskState, expectedVersion, expectedSequence *int, next Event) error {
	state.Normalize()
	stateJSON, err := encodeState(state)
	if err != nil {
		return err
	}
	payloadJSON, err := canonical(next.Payload)
	if err != nil {
		return err
	}
	return s.withTx(func(tx *sql.Tx) error {
		var currentVersion int
		if err := tx.QueryRow(`SELECT version FROM harness_tasks WHERE task_id=?`, state.TaskID).Scan(&currentVersion); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: task %s", ErrStoreConflict, state.TaskID)
			}
			return err
		}
		if expectedVersion != nil && currentVersion != *expectedVersion {
			return fmt.Errorf("%w: version=%d expected=%d", ErrStoreConflict, currentVersion, *expectedVersion)
		}
		var seq int
		var previous string
		err := tx.QueryRow(`SELECT sequence,event_hash FROM harness_events WHERE task_id=? ORDER BY sequence DESC LIMIT 1`, state.TaskID).Scan(&seq, &previous)
		if errors.Is(err, sql.ErrNoRows) {
			seq = 0
			previous = "GENESIS"
		} else if err != nil {
			return err
		}
		if expectedSequence != nil && seq != *expectedSequence {
			return fmt.Errorf("%w: sequence=%d expected=%d", ErrStoreConflict, seq, *expectedSequence)
		}
		// Defense in depth: the core computed these fields, but a backend that
		// accepted a non-contiguous event would corrupt the chain. Refuse it.
		if next.Sequence != seq+1 || next.PreviousHash != previous {
			return fmt.Errorf("%w: event does not extend the journal tail", ErrStoreConflict)
		}
		if next.TaskVersion != currentVersion+1 {
			return fmt.Errorf("%w: version=%d expected=%d", ErrStoreConflict, next.TaskVersion, currentVersion+1)
		}
		createdAt := next.CreatedAt
		if createdAt == "" {
			createdAt = s.now()
		}
		if _, err := tx.Exec(`INSERT INTO harness_events(task_id,sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version) VALUES(?,?,?,?,?,?,?,?)`, state.TaskID, next.Sequence, next.Type, string(payloadJSON), next.PreviousHash, next.EventHash, createdAt, next.TaskVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE harness_tasks SET state_json=?,version=?,updated_at=? WHERE task_id=?`, stateJSON, next.TaskVersion, createdAt, state.TaskID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO harness_checkpoints(task_id,sequence,state_json,created_at) VALUES(?,?,?,?)`, state.TaskID, next.Sequence, stateJSON, createdAt); err != nil {
			return err
		}
		return nil
	})
}

func (s *SQLiteStore) GetTask(taskID string) (TaskState, int, error) {
	var stateJSON string
	var version int
	err := s.db.QueryRow(`SELECT state_json,version FROM harness_tasks WHERE task_id=?`, taskID).Scan(&stateJSON, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskState{}, 0, ErrNotFound
	}
	if err != nil {
		return TaskState{}, 0, err
	}
	state, err := decodeState(stateJSON)
	return state, version, err
}

// ListTasks returns materialized task state in update order. The session API
// uses this projection for a lightweight inbox; event payloads remain behind
// the per-session event endpoint so listing never loads an entire trajectory.
func (s *SQLiteStore) ListTasks() ([]TaskState, error) {
	rows, err := s.db.Query(`SELECT state_json FROM harness_tasks ORDER BY updated_at DESC, task_id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []TaskState{}
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

func (s *SQLiteStore) Events(taskID string) ([]Event, error) {
	return s.eventsFrom(taskID, 0)
}

// EventsSince returns the append-only journal suffix after sequence. It is
// used by reconnecting clients and the SSE projection so a browser can resume
// from Last-Event-ID without downloading the whole trajectory again.
func (s *SQLiteStore) EventsSince(taskID string, sequence int) ([]Event, error) {
	if sequence < 0 {
		sequence = 0
	}
	return s.eventsFrom(taskID, sequence)
}

func (s *SQLiteStore) eventsFrom(taskID string, sequence int) ([]Event, error) {
	rows, err := s.db.Query(`SELECT sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version FROM harness_events WHERE task_id=? AND sequence>? ORDER BY sequence`, taskID, sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var e Event
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
func (s *SQLiteStore) LatestEvent(taskID string) (Event, bool, error) {
	var e Event
	var payload string
	err := s.db.QueryRow(`SELECT sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version FROM harness_events WHERE task_id=? ORDER BY sequence DESC LIMIT 1`, taskID).Scan(&e.Sequence, &e.Type, &payload, &e.PreviousHash, &e.EventHash, &e.CreatedAt, &e.TaskVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return Event{}, false, nil
	}
	if err != nil {
		return Event{}, false, err
	}
	if err := json.Unmarshal([]byte(payload), &e.Payload); err != nil {
		return Event{}, false, err
	}
	e.TaskID = taskID
	return e, true, nil
}

// CheckpointAt returns the state snapshot written alongside a given event
// sequence. ok is false when that sequence has no checkpoint.
func (s *SQLiteStore) CheckpointAt(taskID string, sequence int) (Checkpoint, bool, error) {
	var c Checkpoint
	var stateJSON string
	err := s.db.QueryRow(`SELECT sequence,state_json,created_at FROM harness_checkpoints WHERE task_id=? AND sequence=?`, taskID, sequence).Scan(&c.Sequence, &stateJSON, &c.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, err
	}
	c.TaskID = taskID
	c.StateJSON = stateJSON
	c.State, err = decodeState(stateJSON)
	if err != nil {
		return Checkpoint{}, false, err
	}
	return c, true, nil
}

// Replay, ReplayState and the state-digest verification live in journal.go:
// they are invariants of the core, not of any single backend. A backend only
// has to serve Events/CheckpointAt/GetTask correctly.

func (s *SQLiteStore) LatestCheckpoint(taskID string) (Checkpoint, error) {
	var c Checkpoint
	var stateJSON string
	err := s.db.QueryRow(`SELECT sequence,state_json,created_at FROM harness_checkpoints WHERE task_id=? ORDER BY sequence DESC LIMIT 1`, taskID).Scan(&c.Sequence, &stateJSON, &c.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, err
	}
	c.TaskID = taskID
	c.StateJSON = stateJSON
	c.State, err = decodeState(stateJSON)
	return c, err
}

func (s *SQLiteStore) PutIdempotency(scope, key string, value any) (bool, error) {
	b, err := canonical(value)
	if err != nil {
		return false, err
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`INSERT OR IGNORE INTO harness_idempotency(scope,idem_key,result_json,created_at) VALUES(?,?,?,?)`, scope, key, string(b), now)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *SQLiteStore) GetIdempotency(scope, key string, out any) (bool, error) {
	var raw string
	err := s.db.QueryRow(`SELECT result_json FROM harness_idempotency WHERE scope=? AND idem_key=?`, scope, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
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

func (s *SQLiteStore) PutMemory(auth AuthContext, record MemoryRecord) error {
	tags, _ := canonical(record.Tags)
	_, err := s.db.Exec(`INSERT INTO harness_memories(memory_id,tenant_id,subject_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,expires_at,supersedes,tombstone) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		record.MemoryID, auth.TenantID, auth.SubjectID, record.MemoryType, record.Namespace, record.Key, record.Content, string(tags), record.Importance, record.Confidence, record.Version, record.Source, record.CreatedAt, nullable(record.ExpiresAt), nullable(record.Supersedes), boolInt(record.Tombstone))
	return err
}

// PutMemoryVersioned inserts a memory record only when the latest live version
// for the same (tenant, subject, namespace, key) chain equals expectedVersion.
// The read and the insert share one transaction, so two racing writers cannot
// both publish version n+1.
func (s *SQLiteStore) PutMemoryVersioned(auth AuthContext, record MemoryRecord, expectedVersion int) (bool, error) {
	tags, _ := canonical(record.Tags)
	ok := false
	err := s.withTx(func(tx *sql.Tx) error {
		var current int
		if err := tx.QueryRow(`SELECT COALESCE(MAX(version),0) FROM harness_memories WHERE tenant_id=? AND subject_id=? AND namespace=? AND memory_key=? AND tombstone=0`,
			auth.TenantID, auth.SubjectID, record.Namespace, record.Key).Scan(&current); err != nil {
			return err
		}
		if current != expectedVersion {
			return nil
		}
		if _, err := tx.Exec(`INSERT INTO harness_memories(memory_id,tenant_id,subject_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,expires_at,supersedes,tombstone) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			record.MemoryID, auth.TenantID, auth.SubjectID, record.MemoryType, record.Namespace, record.Key, record.Content, string(tags), record.Importance, record.Confidence, record.Version, record.Source, record.CreatedAt, nullable(record.ExpiresAt), nullable(record.Supersedes), boolInt(record.Tombstone)); err != nil {
			return err
		}
		ok = true
		return nil
	})
	return ok, err
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

func (s *SQLiteStore) MemoryRecords(auth AuthContext, namespace string) ([]MemoryRecord, error) {
	query := `SELECT memory_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,COALESCE(expires_at,''),COALESCE(supersedes,''),tombstone FROM harness_memories WHERE tenant_id=? AND subject_id=?`
	args := []any{auth.TenantID, auth.SubjectID}
	if namespace != "" {
		query += ` AND namespace=?`
		args = append(args, namespace)
	}
	query += ` ORDER BY version DESC, created_at DESC`
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MemoryRecord
	for rows.Next() {
		var r MemoryRecord
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

func (s *SQLiteStore) TombstoneMemory(auth AuthContext, id string) (bool, error) {
	result, err := s.db.Exec(`UPDATE harness_memories SET tombstone=1 WHERE memory_id=? AND tenant_id=? AND subject_id=?`, id, auth.TenantID, auth.SubjectID)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *SQLiteStore) AcquireLease(resourceKey, ownerID, leaseID string, ttl time.Duration) (bool, error) {
	now := float64(s.clock.Now().UnixNano()) / 1e9
	expires := now + ttl.Seconds()
	s.mu.Lock()
	defer s.mu.Unlock()
	// Expired leases are eligible for replacement.  The conditional delete is
	// kept in the same transaction as the insert to make worker races explicit.
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	_, _ = tx.Exec(`DELETE FROM harness_leases WHERE resource_key=? AND expires_at<=?`, resourceKey, float64(now))
	_, err = tx.Exec(`INSERT INTO harness_leases(lease_id,resource_key,owner_id,expires_at,acquired_at) VALUES(?,?,?,?,?)`, leaseID, resourceKey, ownerID, expires, s.now())
	if err != nil {
		_ = tx.Rollback()
		return false, nil
	}
	err = tx.Commit()
	return err == nil, err
}

func (s *SQLiteStore) RenewLease(leaseID, ownerID string, ttl time.Duration) (bool, error) {
	now := float64(s.clock.Now().UnixNano()) / 1e9
	result, err := s.db.Exec(`UPDATE harness_leases SET expires_at=? WHERE lease_id=? AND owner_id=? AND expires_at>?`, now+ttl.Seconds(), leaseID, ownerID, now)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *SQLiteStore) ReleaseLease(leaseID, ownerID string) (bool, error) {
	result, err := s.db.Exec(`DELETE FROM harness_leases WHERE lease_id=? AND owner_id=?`, leaseID, ownerID)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *SQLiteStore) ReleaseResourceLease(resourceKey, ownerID string) (bool, error) {
	result, err := s.db.Exec(`DELETE FROM harness_leases WHERE resource_key=? AND owner_id=?`, resourceKey, ownerID)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

// PutArtifact commits an artifact once for an identity scope. A second write
// with the same id is only accepted when its content digest matches; this
// prevents a retried tool from silently replacing evidence with a different
// value.
func (s *SQLiteStore) PutArtifact(auth AuthContext, artifactID string, content map[string]any) (ArtifactRecord, error) {
	if artifactID == "" {
		return ArtifactRecord{}, errors.New("ARTIFACT_ID_REQUIRED")
	}
	if content == nil {
		content = map[string]any{}
	}
	contentJSON, err := canonical(content)
	if err != nil {
		return ArtifactRecord{}, err
	}
	digest, err := Digest(content)
	if err != nil {
		return ArtifactRecord{}, err
	}
	record := ArtifactRecord{ArtifactID: artifactID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Content: content, SHA256: digest, CreatedAt: s.now()}
	err = s.withTx(func(tx *sql.Tx) error {
		var existingJSON, existingHash, createdAt string
		err := tx.QueryRow(`SELECT content_json,sha256,created_at FROM harness_artifacts WHERE tenant_id=? AND subject_id=? AND artifact_id=?`, auth.TenantID, auth.SubjectID, artifactID).Scan(&existingJSON, &existingHash, &createdAt)
		if err == nil {
			if existingHash != digest {
				return fmt.Errorf("%w: artifact %s content changed", ErrStoreConflict, artifactID)
			}
			var existing map[string]any
			if err := json.Unmarshal([]byte(existingJSON), &existing); err != nil {
				return err
			}
			record.Content = existing
			record.SHA256 = existingHash
			record.CreatedAt = createdAt
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.Exec(`INSERT INTO harness_artifacts(tenant_id,subject_id,artifact_id,content_json,sha256,created_at) VALUES(?,?,?,?,?,?)`, auth.TenantID, auth.SubjectID, artifactID, string(contentJSON), digest, record.CreatedAt)
		return err
	})
	return record, err
}

// GetArtifact reads an artifact under the same tenant/subject boundary used
// by the tool runtime. It deliberately returns a boolean so callers can map a
// missing artifact to a model-visible observation instead of a store error.
func (s *SQLiteStore) GetArtifact(auth AuthContext, artifactID string) (ArtifactRecord, bool, error) {
	var record ArtifactRecord
	var contentJSON string
	err := s.db.QueryRow(`SELECT content_json,sha256,created_at FROM harness_artifacts WHERE tenant_id=? AND subject_id=? AND artifact_id=?`, auth.TenantID, auth.SubjectID, artifactID).Scan(&contentJSON, &record.SHA256, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return ArtifactRecord{}, false, nil
	}
	if err != nil {
		return ArtifactRecord{}, false, err
	}
	if err := json.Unmarshal([]byte(contentJSON), &record.Content); err != nil {
		return ArtifactRecord{}, false, err
	}
	record.ArtifactID = artifactID
	record.TenantID = auth.TenantID
	record.SubjectID = auth.SubjectID
	return record, true, nil
}

// EnsureWorkspace creates or returns a tenant-scoped workspace. Reusing an
// existing workspace does not overwrite metadata, which keeps initialization
// idempotent for retried agent starts.
func (s *SQLiteStore) EnsureWorkspace(auth AuthContext, workspaceID string, metadata map[string]any) (Workspace, error) {
	if workspaceID == "" {
		return Workspace{}, errors.New("WORKSPACE_ID_REQUIRED")
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadataJSON, err := canonical(metadata)
	if err != nil {
		return Workspace{}, err
	}
	now := s.now()
	err = s.withTx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR IGNORE INTO harness_workspaces(tenant_id,subject_id,workspace_id,metadata_json,created_at,updated_at) VALUES(?,?,?,?,?,?)`, auth.TenantID, auth.SubjectID, workspaceID, string(metadataJSON), now, now)
		return err
	})
	if err != nil {
		return Workspace{}, err
	}
	var raw string
	var workspace Workspace
	err = s.db.QueryRow(`SELECT metadata_json,created_at,updated_at FROM harness_workspaces WHERE tenant_id=? AND subject_id=? AND workspace_id=?`, auth.TenantID, auth.SubjectID, workspaceID).Scan(&raw, &workspace.CreatedAt, &workspace.UpdatedAt)
	if err != nil {
		return Workspace{}, err
	}
	_ = json.Unmarshal([]byte(raw), &workspace.Metadata)
	workspace.WorkspaceID, workspace.TenantID, workspace.SubjectID = workspaceID, auth.TenantID, auth.SubjectID
	return workspace, nil
}

func (s *SQLiteStore) PutTopicValue(auth AuthContext, workspaceID, topic, key string, value map[string]any, expectedVersion int) (TopicValue, bool, error) {
	if workspaceID == "" || topic == "" || key == "" {
		return TopicValue{}, false, errors.New("TOPIC_SCOPE_REQUIRED")
	}
	if value == nil {
		value = map[string]any{}
	}
	valueJSON, err := canonical(value)
	if err != nil {
		return TopicValue{}, false, err
	}
	digest, err := Digest(value)
	if err != nil {
		return TopicValue{}, false, err
	}
	now := s.now()
	var result TopicValue
	ok := false
	err = s.withTx(func(tx *sql.Tx) error {
		// Check the workspace explicitly rather than relying on the foreign key
		// so that every backend reports the same WORKSPACE_NOT_FOUND error.
		if err := requireWorkspace(tx, auth, workspaceID); err != nil {
			return err
		}
		var current int
		err := tx.QueryRow(`SELECT COALESCE(MAX(version),0) FROM harness_topics WHERE tenant_id=? AND subject_id=? AND workspace_id=? AND topic=? AND topic_key=?`, auth.TenantID, auth.SubjectID, workspaceID, topic, key).Scan(&current)
		if err != nil {
			return err
		}
		if current != expectedVersion {
			return nil
		}
		version := current + 1
		if _, err := tx.Exec(`INSERT INTO harness_topics(tenant_id,subject_id,workspace_id,topic,topic_key,value_json,digest,version,updated_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,subject_id,workspace_id,topic,topic_key) DO UPDATE SET value_json=excluded.value_json,digest=excluded.digest,version=excluded.version,updated_at=excluded.updated_at`, auth.TenantID, auth.SubjectID, workspaceID, topic, key, string(valueJSON), digest, version, now); err != nil {
			return err
		}
		result = TopicValue{WorkspaceID: workspaceID, Topic: topic, Key: key, Value: value, Digest: digest, Version: version, UpdatedAt: now}
		ok = true
		return nil
	})
	return result, ok, err
}

// ErrWorkspaceNotFound is returned when a workspace-scoped write targets a
// workspace that does not exist for the caller's tenant/subject.
var ErrWorkspaceNotFound = errors.New("WORKSPACE_NOT_FOUND")

// requireWorkspace enforces workspace existence inside a transaction. It is
// shared by every workspace-scoped write so that SQLite and the in-memory
// backend agree on both the failure and its error value.
func requireWorkspace(tx *sql.Tx, auth AuthContext, workspaceID string) error {
	var exists int
	err := tx.QueryRow(`SELECT 1 FROM harness_workspaces WHERE tenant_id=? AND subject_id=? AND workspace_id=?`, auth.TenantID, auth.SubjectID, workspaceID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrWorkspaceNotFound
	}
	return err
}

func (s *SQLiteStore) TopicValues(auth AuthContext, workspaceID, topic string) ([]TopicValue, error) {
	rows, err := s.db.Query(`SELECT topic,topic_key,value_json,digest,version,updated_at FROM harness_topics WHERE tenant_id=? AND subject_id=? AND workspace_id=? AND topic=? ORDER BY topic_key`, auth.TenantID, auth.SubjectID, workspaceID, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []TopicValue{}
	for rows.Next() {
		var item TopicValue
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

func (s *SQLiteStore) AppendMailbox(auth AuthContext, message MailboxMessage) (MailboxMessage, error) {
	if message.WorkspaceID == "" || message.Topic == "" || message.FromAgent == "" {
		return MailboxMessage{}, errors.New("MAILBOX_SCOPE_REQUIRED")
	}
	if message.MessageID == "" {
		message.MessageID = NewID("MSG")
	}
	if message.Payload == nil {
		message.Payload = map[string]any{}
	}
	payload, err := canonical(message.Payload)
	if err != nil {
		return MailboxMessage{}, err
	}
	now := s.now()
	err = s.withTx(func(tx *sql.Tx) error {
		if err := requireWorkspace(tx, auth, message.WorkspaceID); err != nil {
			return err
		}
		if message.IdempotencyKey != "" {
			var existing MailboxMessage
			var raw string
			// Topic is part of the idempotency scope: a key identifies one
			// message within one topic's sequence space. Including it keeps
			// this lookup identical to the in-memory backend.
			err := tx.QueryRow(`SELECT message_id,sequence,from_agent,to_agent,protocol,message_type,correlation_id,payload_json,created_at,acked FROM harness_mailbox WHERE tenant_id=? AND subject_id=? AND workspace_id=? AND topic=? AND from_agent=? AND idempotency_key=?`, auth.TenantID, auth.SubjectID, message.WorkspaceID, message.Topic, message.FromAgent, message.IdempotencyKey).Scan(&existing.MessageID, &existing.Sequence, &existing.FromAgent, &existing.ToAgent, &existing.Protocol, &existing.MessageType, &existing.CorrelationID, &raw, &existing.CreatedAt, &existing.Acked)
			if err == nil {
				_ = json.Unmarshal([]byte(raw), &existing.Payload)
				existing.WorkspaceID = message.WorkspaceID
				existing.Topic = message.Topic
				existing.IdempotencyKey = message.IdempotencyKey
				message = existing
				message.TenantID, message.SubjectID = auth.TenantID, auth.SubjectID
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		var sequence int64
		if err := tx.QueryRow(`SELECT COALESCE(MAX(sequence),0)+1 FROM harness_mailbox WHERE tenant_id=? AND subject_id=? AND workspace_id=? AND topic=?`, auth.TenantID, auth.SubjectID, message.WorkspaceID, message.Topic).Scan(&sequence); err != nil {
			return err
		}
		message.Sequence, message.CreatedAt = sequence, now
		message.TenantID, message.SubjectID = auth.TenantID, auth.SubjectID
		_, err := tx.Exec(`INSERT INTO harness_mailbox(tenant_id,subject_id,workspace_id,topic,sequence,message_id,from_agent,to_agent,protocol,message_type,correlation_id,idempotency_key,payload_json,created_at,acked) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)`, auth.TenantID, auth.SubjectID, message.WorkspaceID, message.Topic, message.Sequence, message.MessageID, message.FromAgent, message.ToAgent, message.Protocol, message.MessageType, message.CorrelationID, message.IdempotencyKey, string(payload), message.CreatedAt)
		return err
	})
	if err != nil {
		return MailboxMessage{}, err
	}
	return message, nil
}

// MailboxMessages returns unacked messages for one recipient.
//
// Direct messages use the row's acked flag. Broadcast messages (to_agent = '')
// are acknowledged per recipient through harness_mailbox_acks, so one agent
// reading a broadcast cannot hide it from the other agents on the topic.
func (s *SQLiteStore) MailboxMessages(auth AuthContext, workspaceID, topic, recipient string, limit int) ([]MailboxMessage, error) {
	if limit <= 0 {
		limit = 64
	}
	rows, err := s.db.Query(`SELECT sequence,message_id,from_agent,to_agent,protocol,message_type,correlation_id,idempotency_key,payload_json,created_at,acked FROM harness_mailbox WHERE tenant_id=? AND subject_id=? AND workspace_id=? AND topic=? AND (
  (to_agent=? AND acked=0)
  OR (to_agent='' AND NOT EXISTS (SELECT 1 FROM harness_mailbox_acks a WHERE a.tenant_id=harness_mailbox.tenant_id AND a.subject_id=harness_mailbox.subject_id AND a.message_id=harness_mailbox.message_id AND a.recipient=?))
 ) ORDER BY sequence LIMIT ?`, auth.TenantID, auth.SubjectID, workspaceID, topic, recipient, recipient, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	messages := []MailboxMessage{}
	for rows.Next() {
		var message MailboxMessage
		var raw string
		if err := rows.Scan(&message.Sequence, &message.MessageID, &message.FromAgent, &message.ToAgent, &message.Protocol, &message.MessageType, &message.CorrelationID, &message.IdempotencyKey, &raw, &message.CreatedAt, &message.Acked); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &message.Payload); err != nil {
			return nil, err
		}
		message.TenantID, message.SubjectID = auth.TenantID, auth.SubjectID
		message.WorkspaceID, message.Topic = workspaceID, topic
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

// AckMailbox acknowledges one message for one recipient. It is idempotent.
//
// A direct message is acknowledged on its own row, and only the addressed
// recipient may do so. A broadcast message is acknowledged per recipient, so
// every other agent still receives it.
func (s *SQLiteStore) AckMailbox(auth AuthContext, messageID, recipient string) (bool, error) {
	if recipient == "" {
		return false, errors.New("MAILBOX_RECIPIENT_REQUIRED")
	}
	acknowledged := false
	err := s.withTx(func(tx *sql.Tx) error {
		var toAgent string
		err := tx.QueryRow(`SELECT to_agent FROM harness_mailbox WHERE tenant_id=? AND subject_id=? AND message_id=?`, auth.TenantID, auth.SubjectID, messageID).Scan(&toAgent)
		if errors.Is(err, sql.ErrNoRows) {
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
			result, err := tx.Exec(`UPDATE harness_mailbox SET acked=1 WHERE tenant_id=? AND subject_id=? AND message_id=? AND to_agent=?`, auth.TenantID, auth.SubjectID, messageID, recipient)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n == 1 {
				acknowledged = true
			}
			return nil
		}
		result, err := tx.Exec(`INSERT OR IGNORE INTO harness_mailbox_acks(tenant_id,subject_id,message_id,recipient,acked_at) VALUES(?,?,?,?,?)`, auth.TenantID, auth.SubjectID, messageID, recipient, s.now())
		if err != nil {
			return err
		}
		// Idempotent: a repeat ack still reports success.
		acknowledged = true
		_ = result
		return nil
	})
	return acknowledged, err
}

func (s *SQLiteStore) PutFeedback(signal FeedbackSignal) error {
	if signal.FeedbackID == "" {
		signal.FeedbackID = NewID("FDBK")
	}
	if signal.CreatedAt == "" {
		signal.CreatedAt = s.now()
	}
	metadata, err := canonical(signal.Metadata)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO harness_feedback(feedback_id,task_id,tenant_id,subject_id,source,stage,outcome,category,rating,comment,variant,cohort,metadata_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, signal.FeedbackID, signal.TaskID, signal.TenantID, signal.SubjectID, signal.Source, signal.Stage, signal.Outcome, signal.Category, signal.Rating, signal.Comment, signal.Variant, signal.Cohort, string(metadata), signal.CreatedAt)
	return err
}

func (s *SQLiteStore) Feedback(tenantID string) ([]FeedbackSignal, error) {
	rows, err := s.db.Query(`SELECT feedback_id,task_id,tenant_id,subject_id,source,stage,outcome,category,rating,comment,variant,cohort,metadata_json,created_at FROM harness_feedback WHERE tenant_id=? ORDER BY created_at,feedback_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FeedbackSignal{}
	for rows.Next() {
		var item FeedbackSignal
		var metadata string
		if err := rows.Scan(&item.FeedbackID, &item.TaskID, &item.TenantID, &item.SubjectID, &item.Source, &item.Stage, &item.Outcome, &item.Category, &item.Rating, &item.Comment, &item.Variant, &item.Cohort, &metadata, &item.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(metadata), &item.Metadata)
		out = append(out, item)
	}
	return out, rows.Err()
}
