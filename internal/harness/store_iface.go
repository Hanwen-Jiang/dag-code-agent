package harness

import (
	"io"
	"time"
)

// Store is the full persistence surface: durable task/event history plus the
// optional side-effect projections (memory, leases, artifacts, feedback).
//
// It is composed from small capability interfaces so a backend only has to
// implement what it actually supports. The runtime itself depends on the
// narrower CoreStore; memory, leases, artifacts and feedback are declared
// separately and wired in with their own options.
//
// Correctness note: implementations do NOT own any invariant. The event hash
// chain, the state digest binding and replay verification all live in the core
// (see journal.go). A backend is only responsible for atomic conditional
// writes. A backend that implements the primitives below incorrectly can fail
// loudly with ErrStoreConflict, but it cannot silently break the chain.
type Store interface {
	TaskStore
	EventStore
	IdempotencyStore
	MemoryStore
	LeaseStore
	ArtifactStore
	FeedbackStore
	io.Closer
}

// CoreStore is the minimum a runtime needs: durable task state and the
// append-only event journal behind it.
type CoreStore interface {
	TaskStore
	EventStore
	io.Closer
}

// TaskStore owns the materialized task row and the atomic event append that
// keeps it in step with the journal.
type TaskStore interface {
	// CreateTask atomically creates the task row, its genesis event and the
	// genesis checkpoint. It must fail if the task already exists.
	CreateTask(state TaskState, genesis Event) error

	// GetTask returns the materialized state and its version. The version is
	// the optimistic-concurrency token passed back to AppendEvent.
	GetTask(taskID string) (TaskState, int, error)

	// ListTasks returns materialized state in update order (newest first).
	ListTasks() ([]TaskState, error)

	// AppendEvent atomically persists a pre-computed event and the state it
	// materializes, together with the matching checkpoint.
	//
	// The implementation MUST, inside a single transaction:
	//  1. reject the write with ErrStoreConflict when the task row is missing,
	//     or when expectedVersion/expectedSequence are non-nil and differ from
	//     the stored version and the journal tail,
	//  2. insert next exactly as given (never recompute its hashes),
	//  3. update the task row and the checkpoint to match.
	//
	// expectedVersion and expectedSequence are pointers so that "no
	// precondition" stays expressible, but the core always passes both.
	AppendEvent(state TaskState, expectedVersion, expectedSequence *int, next Event) error
}

// EventStore is the read side of the journal.
type EventStore interface {
	Events(taskID string) ([]Event, error)

	// EventsSince returns the journal suffix after the given sequence. It backs
	// SSE resume (Last-Event-ID) so a client does not refetch the trajectory.
	EventsSince(taskID string, sequence int) ([]Event, error)

	// LatestEvent returns the journal tail. ok is false when the task has no
	// events yet, which lets the core treat "first event" as a normal case.
	LatestEvent(taskID string) (Event, bool, error)

	// CheckpointAt returns the state snapshot written with a given event
	// sequence. ok is false when no checkpoint exists for that sequence.
	CheckpointAt(taskID string, sequence int) (Checkpoint, bool, error)

	// LatestCheckpoint returns the most recent checkpoint, or ErrNotFound.
	LatestCheckpoint(taskID string) (Checkpoint, error)
}

// IdempotencyStore deduplicates retried side effects.
type IdempotencyStore interface {
	// PutIdempotency records a result under (scope, key). ok is false when the
	// key already existed; the first writer wins and later writers must read
	// the stored value instead of re-executing the effect.
	PutIdempotency(scope, key string, value any) (bool, error)

	// GetIdempotency reads a previously stored result. ok is false when absent.
	GetIdempotency(scope, key string, out any) (bool, error)
}

// MemoryStore is the tenant-scoped long-term memory projection.
type MemoryStore interface {
	// PutMemory stores a record unconditionally.
	PutMemory(auth AuthContext, record MemoryRecord) error

	// PutMemoryVersioned stores a record only when the current latest version
	// in the (tenant, subject, namespace, key) chain equals expectedVersion.
	// expectedVersion 0 means "no live record expected".
	//
	// This is the memory analogue of AppendEvent: the core computes the next
	// version, the backend enforces it atomically. ok is false when the
	// precondition did not hold.
	PutMemoryVersioned(auth AuthContext, record MemoryRecord, expectedVersion int) (bool, error)

	MemoryRecords(auth AuthContext, namespace string) ([]MemoryRecord, error)

	// TombstoneMemory hides a record without deleting its history.
	TombstoneMemory(auth AuthContext, id string) (bool, error)
}

// LeaseStore provides mutual exclusion for DAG nodes and other resources.
type LeaseStore interface {
	// AcquireLease takes resourceKey for ownerID until now+ttl. ok is false
	// when a live lease is already held. An expired lease is reclaimable.
	AcquireLease(resourceKey, ownerID, leaseID string, ttl time.Duration) (bool, error)

	// RenewLease extends a live lease owned by ownerID.
	RenewLease(leaseID, ownerID string, ttl time.Duration) (bool, error)

	// ReleaseLease deletes a lease by id, scoped to its owner.
	ReleaseLease(leaseID, ownerID string) (bool, error)

	// ReleaseResourceLease deletes whichever lease currently holds resourceKey
	// for ownerID, without the caller having to remember the lease id.
	ReleaseResourceLease(resourceKey, ownerID string) (bool, error)
}

// ArtifactStore keeps content-addressed side-effect evidence.
type ArtifactStore interface {
	// PutArtifact is idempotent by content: rewriting the same artifact id with
	// different content fails with ErrStoreConflict rather than replacing
	// evidence silently.
	PutArtifact(auth AuthContext, artifactID string, content map[string]any) (ArtifactRecord, error)

	// GetArtifact returns ok=false for a missing artifact so callers can turn
	// it into a model-visible observation instead of an infrastructure error.
	GetArtifact(auth AuthContext, artifactID string) (ArtifactRecord, bool, error)
}

// FeedbackStore collects human/automated outcome signals for evaluation.
type FeedbackStore interface {
	PutFeedback(signal FeedbackSignal) error
	Feedback(tenantID string) ([]FeedbackSignal, error)
}
