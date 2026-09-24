package harness

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

type Store struct {
	db *sql.DB
	mu sync.Mutex
}

func NewStore(path string) (*Store, error) {
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
	store := &Store{db: db}
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

func (s *Store) init() error {
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
);`)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

func canonical(v any) ([]byte, error) { return CanonicalJSON(v) }

func hashEvent(previous, eventType string, payload map[string]any, sequence int) (string, error) {
	b, err := canonical(map[string]any{"previous_hash": previous, "sequence": sequence, "event_type": eventType, "payload": payload})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func encodeState(state TaskState) (string, error) {
	state.Normalize()
	b, err := canonical(state)
	return string(b), err
}

func stateDigest(state TaskState) string {
	digest, _ := Digest(state)
	return digest
}

func eventPayload(state TaskState, payload map[string]any) map[string]any {
	result := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		result[key] = value
	}
	// The digest is part of the hashed event payload. It binds the materialized
	// state/checkpoint written in the same transaction to the journal entry.
	result["_state_digest"] = stateDigest(state)
	return result
}

func decodeState(value string) (TaskState, error) {
	var state TaskState
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return state, err
	}
	state.Normalize()
	return state, nil
}

func (s *Store) withTx(fn func(*sql.Tx) error) error {
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

func (s *Store) CreateTaskWithEvent(state TaskState, eventType string, payload map[string]any) (Event, error) {
	state.Normalize()
	stateJSON, err := encodeState(state)
	if err != nil {
		return Event{}, err
	}
	now := UTCNow()
	payload = eventPayload(state, payload)
	h, err := hashEvent("GENESIS", eventType, payload, 1)
	if err != nil {
		return Event{}, err
	}
	e := Event{TaskID: state.TaskID, Sequence: 1, Type: eventType, Payload: payload,
		PreviousHash: "GENESIS", EventHash: h, CreatedAt: now, TaskVersion: 1}
	err = s.withTx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO harness_tasks(task_id,state_json,version,updated_at) VALUES(?,?,?,?)`, state.TaskID, stateJSON, 1, now); err != nil {
			return err
		}
		payloadJSON, _ := canonical(payload)
		if _, err := tx.Exec(`INSERT INTO harness_events(task_id,sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version) VALUES(?,?,?,?,?,?,?,?)`, state.TaskID, 1, eventType, string(payloadJSON), "GENESIS", h, now, 1); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO harness_checkpoints(task_id,sequence,state_json,created_at) VALUES(?,?,?,?)`, state.TaskID, 1, stateJSON, now)
		return err
	})
	return e, err
}

func (s *Store) CommitStateEvent(state TaskState, eventType string, payload map[string]any, expectedVersion, expectedSequence *int) (Event, error) {
	state.Normalize()
	stateJSON, err := encodeState(state)
	if err != nil {
		return Event{}, err
	}
	payload = eventPayload(state, payload)
	var result Event
	err = s.withTx(func(tx *sql.Tx) error {
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
		nextSeq := seq + 1
		eventHash, err := hashEvent(previous, eventType, payload, nextSeq)
		if err != nil {
			return err
		}
		now := UTCNow()
		payloadJSON, _ := canonical(payload)
		nextVersion := currentVersion + 1
		if _, err := tx.Exec(`INSERT INTO harness_events(task_id,sequence,event_type,payload_json,previous_hash,event_hash,created_at,task_version) VALUES(?,?,?,?,?,?,?,?)`, state.TaskID, nextSeq, eventType, string(payloadJSON), previous, eventHash, now, nextVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE harness_tasks SET state_json=?,version=?,updated_at=? WHERE task_id=?`, stateJSON, nextVersion, now, state.TaskID); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO harness_checkpoints(task_id,sequence,state_json,created_at) VALUES(?,?,?,?)`, state.TaskID, nextSeq, stateJSON, now); err != nil {
			return err
		}
		result = Event{TaskID: state.TaskID, Sequence: nextSeq, Type: eventType, Payload: payload, PreviousHash: previous, EventHash: eventHash, CreatedAt: now, TaskVersion: nextVersion}
		return nil
	})
	return result, err
}

func (s *Store) GetTask(taskID string) (TaskState, int, error) {
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
func (s *Store) ListTasks() ([]TaskState, error) {
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

func (s *Store) Events(taskID string) ([]Event, error) {
	return s.eventsFrom(taskID, 0)
}

// EventsSince returns the append-only journal suffix after sequence. It is
// used by reconnecting clients and the SSE projection so a browser can resume
// from Last-Event-ID without downloading the whole trajectory again.
func (s *Store) EventsSince(taskID string, sequence int) ([]Event, error) {
	if sequence < 0 {
		sequence = 0
	}
	return s.eventsFrom(taskID, sequence)
}

func (s *Store) eventsFrom(taskID string, sequence int) ([]Event, error) {
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

func (s *Store) Replay(taskID string) ([]Event, error) {
	events, err := s.Events(taskID)
	if err != nil {
		return nil, err
	}
	previous := "GENESIS"
	for i, event := range events {
		if event.Sequence != i+1 || event.PreviousHash != previous {
			return nil, fmt.Errorf("%w: broken event link at %d", ErrStoreConflict, event.Sequence)
		}
		h, err := hashEvent(previous, event.Type, event.Payload, event.Sequence)
		if err != nil {
			return nil, err
		}
		if h != event.EventHash {
			return nil, fmt.Errorf("%w: tampered event at %d", ErrStoreConflict, event.Sequence)
		}
		previous = h
	}
	return events, nil
}

// ReplayState verifies the journal and the state snapshot written alongside
// every event. A caller can therefore distinguish a stale materialized row
// from a valid replay without trusting the row alone.
func (s *Store) ReplayState(taskID string) (TaskState, error) {
	events, err := s.Replay(taskID)
	if err != nil {
		return TaskState{}, err
	}
	if len(events) == 0 {
		return TaskState{}, ErrNotFound
	}
	for _, event := range events {
		digest, ok := event.Payload["_state_digest"].(string)
		if !ok || digest == "" {
			// Legacy rows written before state digests remain replayable; new
			// rows always take the verification path below.
			continue
		}
		var stateJSON string
		if err := s.db.QueryRow(`SELECT state_json FROM harness_checkpoints WHERE task_id=? AND sequence=?`, taskID, event.Sequence).Scan(&stateJSON); err != nil {
			return TaskState{}, fmt.Errorf("%w: missing checkpoint at %d", ErrStoreConflict, event.Sequence)
		}
		checkpointState, err := decodeState(stateJSON)
		if err != nil {
			return TaskState{}, err
		}
		if got := stateDigest(checkpointState); got != digest {
			return TaskState{}, fmt.Errorf("%w: state digest mismatch at %d", ErrStoreConflict, event.Sequence)
		}
	}
	state, _, err := s.GetTask(taskID)
	if err != nil {
		return TaskState{}, err
	}
	last, err := s.LatestCheckpoint(taskID)
	if err != nil {
		return TaskState{}, err
	}
	if last.Sequence != events[len(events)-1].Sequence || stateDigest(state) != stateDigest(last.State) {
		return TaskState{}, fmt.Errorf("%w: materialized state diverged from checkpoint", ErrStoreConflict)
	}
	return state, nil
}

func (s *Store) LatestCheckpoint(taskID string) (Checkpoint, error) {
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

func (s *Store) PutIdempotency(scope, key string, value any) (bool, error) {
	b, err := canonical(value)
	if err != nil {
		return false, err
	}
	now := UTCNow()
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`INSERT OR IGNORE INTO harness_idempotency(scope,idem_key,result_json,created_at) VALUES(?,?,?,?)`, scope, key, string(b), now)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *Store) GetIdempotency(scope, key string, out any) (bool, error) {
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

func (s *Store) PutMemory(auth AuthContext, record MemoryRecord) error {
	tags, _ := canonical(record.Tags)
	_, err := s.db.Exec(`INSERT INTO harness_memories(memory_id,tenant_id,subject_id,memory_type,namespace,memory_key,content,tags_json,importance,confidence,version,source,created_at,expires_at,supersedes,tombstone) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		record.MemoryID, auth.TenantID, auth.SubjectID, record.MemoryType, record.Namespace, record.Key, record.Content, string(tags), record.Importance, record.Confidence, record.Version, record.Source, record.CreatedAt, nullable(record.ExpiresAt), nullable(record.Supersedes), boolInt(record.Tombstone))
	return err
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

func (s *Store) MemoryRecords(auth AuthContext, namespace string) ([]MemoryRecord, error) {
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

func (s *Store) TombstoneMemory(auth AuthContext, id string) (bool, error) {
	result, err := s.db.Exec(`UPDATE harness_memories SET tombstone=1 WHERE memory_id=? AND tenant_id=? AND subject_id=?`, id, auth.TenantID, auth.SubjectID)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *Store) AcquireLease(resourceKey, ownerID, leaseID string, ttl time.Duration) (bool, error) {
	now := time.Now().UnixNano() / 1e9
	expires := float64(now) + ttl.Seconds()
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
	_, err = tx.Exec(`INSERT INTO harness_leases(lease_id,resource_key,owner_id,expires_at,acquired_at) VALUES(?,?,?,?,?)`, leaseID, resourceKey, ownerID, expires, UTCNow())
	if err != nil {
		_ = tx.Rollback()
		return false, nil
	}
	err = tx.Commit()
	return err == nil, err
}

func (s *Store) RenewLease(leaseID, ownerID string, ttl time.Duration) (bool, error) {
	result, err := s.db.Exec(`UPDATE harness_leases SET expires_at=? WHERE lease_id=? AND owner_id=? AND expires_at>?`, float64(time.Now().UnixNano()/1e9)+ttl.Seconds(), leaseID, ownerID, float64(time.Now().UnixNano()/1e9))
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *Store) ReleaseLease(leaseID, ownerID string) (bool, error) {
	result, err := s.db.Exec(`DELETE FROM harness_leases WHERE lease_id=? AND owner_id=?`, leaseID, ownerID)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

func (s *Store) ReleaseResourceLease(resourceKey, ownerID string) (bool, error) {
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
func (s *Store) PutArtifact(auth AuthContext, artifactID string, content map[string]any) (ArtifactRecord, error) {
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
	record := ArtifactRecord{ArtifactID: artifactID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Content: content, SHA256: digest, CreatedAt: UTCNow()}
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
func (s *Store) GetArtifact(auth AuthContext, artifactID string) (ArtifactRecord, bool, error) {
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

func (s *Store) PutFeedback(signal FeedbackSignal) error {
	if signal.FeedbackID == "" {
		signal.FeedbackID = NewID("FDBK")
	}
	if signal.CreatedAt == "" {
		signal.CreatedAt = UTCNow()
	}
	metadata, err := canonical(signal.Metadata)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO harness_feedback(feedback_id,task_id,tenant_id,subject_id,source,stage,outcome,category,rating,comment,variant,cohort,metadata_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, signal.FeedbackID, signal.TaskID, signal.TenantID, signal.SubjectID, signal.Source, signal.Stage, signal.Outcome, signal.Category, signal.Rating, signal.Comment, signal.Variant, signal.Cohort, string(metadata), signal.CreatedAt)
	return err
}

func (s *Store) Feedback(tenantID string) ([]FeedbackSignal, error) {
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
