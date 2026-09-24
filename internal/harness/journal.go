package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Journal owns the event hash chain.
//
// Every durable fact the runtime publishes goes through this type, and this
// type — not the storage backend — computes `previous_hash`, the monotonically
// increasing `sequence`, the `_state_digest` binding to the materialized state
// and the event hash itself. A backend receives a finished Event and is only
// allowed to store it atomically.
//
// The practical consequence: a third-party backend cannot silently corrupt the
// chain. If its conditional append is wrong it returns ErrStoreConflict, and
// Replay/ReplayState (also implemented here) will surface the damage.
type Journal struct {
	store CoreStore
	clock Clock
}

// NewJournal binds a journal to a store and a time source.
func NewJournal(store CoreStore, clock Clock) *Journal {
	return &Journal{store: store, clock: clockOrSystem(clock)}
}

// Store exposes the backing store for read-only projections.
func (j *Journal) Store() CoreStore { return j.store }

// Clock exposes the time source the journal stamps events with.
func (j *Journal) Clock() Clock { return j.clock }

// hashEvent computes the tamper-evident hash of one journal entry. It covers
// the link to the previous hash, the sequence, the type and the full payload
// (which already contains the bound state digest).
func hashEvent(previous, eventType string, payload map[string]any, sequence int) (string, error) {
	b, err := canonical(map[string]any{"previous_hash": previous, "sequence": sequence, "event_type": eventType, "payload": payload})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

// eventPayload binds the digest of the state being materialized into the
// hashed payload. Because the digest is inside the hash, a rewritten
// checkpoint or task row is detectable at replay time.
func eventPayload(state TaskState, payload map[string]any) map[string]any {
	result := make(map[string]any, len(payload)+1)
	for key, value := range payload {
		result[key] = value
	}
	result["_state_digest"] = stateDigest(state)
	return result
}

func stateDigest(state TaskState) string {
	digest, _ := Digest(state)
	return digest
}

// nextEvent computes the next journal entry. This is the only place the chain
// is built; backends are forbidden from recomputing these fields.
func nextEvent(prev *Event, state TaskState, eventType string, payload map[string]any, at time.Time) (Event, error) {
	sequence := 1
	previous := "GENESIS"
	if prev != nil {
		sequence = prev.Sequence + 1
		previous = prev.EventHash
	}
	bound := eventPayload(state, payload)
	hash, err := hashEvent(previous, eventType, bound, sequence)
	if err != nil {
		return Event{}, err
	}
	return Event{
		TaskID:       state.TaskID,
		Sequence:     sequence,
		Type:         eventType,
		Payload:      bound,
		PreviousHash: previous,
		EventHash:    hash,
		CreatedAt:    at.UTC().Format(time.RFC3339Nano),
	}, nil
}

// Create writes the genesis event and the initial task state.
//
// It fails with ErrStoreConflict when the task already exists, so callers can
// treat it as a create-once operation.
func (j *Journal) Create(state TaskState, eventType string, payload map[string]any) (Event, error) {
	state.Normalize()
	genesis, err := nextEvent(nil, state, eventType, payload, j.clock.Now())
	if err != nil {
		return Event{}, err
	}
	genesis.TaskVersion = 1
	if err := j.store.CreateTask(state, genesis); err != nil {
		return Event{}, err
	}
	return genesis, nil
}

// Commit appends the next event for an existing task.
//
// The expected version and sequence are read first and then enforced by the
// store inside the write transaction, so a concurrent writer produces
// ErrStoreConflict rather than a forked journal. Callers that need to win such
// a race should re-read and retry (see CommitWithRetry).
func (j *Journal) Commit(state TaskState, eventType string, payload map[string]any) (Event, error) {
	state.Normalize()
	_, version, err := j.store.GetTask(state.TaskID)
	if err != nil {
		return Event{}, err
	}
	tail, ok, err := j.store.LatestEvent(state.TaskID)
	if err != nil {
		return Event{}, err
	}
	var prev *Event
	sequence := 0
	if ok {
		prev = &tail
		sequence = tail.Sequence
	}
	next, err := nextEvent(prev, state, eventType, payload, j.clock.Now())
	if err != nil {
		return Event{}, err
	}
	next.TaskVersion = version + 1
	expectedVersion, expectedSequence := version, sequence
	if err := j.store.AppendEvent(state, &expectedVersion, &expectedSequence, next); err != nil {
		return Event{}, err
	}
	return next, nil
}

// CommitWithRetry retries Commit on ErrStoreConflict. The state to write is
// recomputed by the caller's function on every attempt so that a retry cannot
// replay a stale snapshot.
func (j *Journal) CommitWithRetry(taskID, eventType string, payload map[string]any, attempts int, state func() (TaskState, error)) (Event, error) {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		current, err := state()
		if err != nil {
			return Event{}, err
		}
		event, err := j.Commit(current, eventType, payload)
		if err == nil {
			return event, nil
		}
		if !errors.Is(err, ErrStoreConflict) {
			return Event{}, err
		}
		lastErr = err
	}
	return Event{}, lastErr
}

// Replay returns the whole journal after verifying it end to end: contiguous
// sequences, correct previous-hash links and matching event hashes.
func (j *Journal) Replay(taskID string) ([]Event, error) {
	events, err := j.store.Events(taskID)
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

// ReplayState verifies the journal and the state snapshots written alongside
// it, then returns the materialized state. A caller can therefore distinguish
// a stale materialized row from a valid replay without trusting the row alone.
func (j *Journal) ReplayState(taskID string) (TaskState, error) {
	events, err := j.Replay(taskID)
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
		checkpoint, found, err := j.store.CheckpointAt(taskID, event.Sequence)
		if err != nil {
			return TaskState{}, err
		}
		if !found {
			return TaskState{}, fmt.Errorf("%w: missing checkpoint at %d", ErrStoreConflict, event.Sequence)
		}
		if got := stateDigest(checkpoint.State); got != digest {
			return TaskState{}, fmt.Errorf("%w: state digest mismatch at %d", ErrStoreConflict, event.Sequence)
		}
	}
	state, _, err := j.store.GetTask(taskID)
	if err != nil {
		return TaskState{}, err
	}
	last, err := j.store.LatestCheckpoint(taskID)
	if err != nil {
		return TaskState{}, err
	}
	if last.Sequence != events[len(events)-1].Sequence || stateDigest(state) != stateDigest(last.State) {
		return TaskState{}, fmt.Errorf("%w: materialized state diverged from checkpoint", ErrStoreConflict)
	}
	return state, nil
}
