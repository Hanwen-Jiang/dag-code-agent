package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// InMemoryStore is a pure-Go, dependency-free Store implementation.
//
// It exists for two reasons: it makes `go test` and embedded use fast and
// deterministic (no CGO, no files), and it is the second backend that proves
// the Store abstraction is real. It passes the same conformance suite
// (storetest) as SQLiteStore.
//
// It implements the primitives, never the invariants: CreateTask and
// AppendEvent store the events they are given and enforce only the
// preconditions documented on TaskStore. The hash chain is still computed and
// verified by Journal.
//
// A zero InMemoryStore is not usable; call NewInMemoryStore.
type InMemoryStore struct {
	mu          sync.Mutex
	clock       Clock
	tasks       map[string]*memTask
	order       []string // task ids in update order (oldest first)
	idempotency map[string]memIdem
	memories    []MemoryRecord
	leases      map[string]memLease
	artifacts   map[string]memArtifact
	feedback    []FeedbackSignal
	workspaces  map[string]Workspace
	topics      map[string]TopicValue
	mailbox     []MailboxMessage
	// mailboxAcks tracks broadcast acknowledgements per recipient, mirroring
	// harness_mailbox_acks in the SQLite backend.
	mailboxAcks map[string]map[string]bool
	closed      bool
}

type memTask struct {
	state       TaskState
	stateJSON   string
	version     int
	updatedAt   string
	events      []Event
	checkpoints map[int]Checkpoint
}

type memIdem struct {
	value any
}

type memLease struct {
	leaseID     string
	resourceKey string
	ownerID     string
	expiresAt   float64
	acquiredAt  string
}

type memArtifact struct {
	content   map[string]any
	sha256    string
	createdAt string
}

// NewInMemoryStore returns an empty in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		clock:       SystemClock(),
		tasks:       map[string]*memTask{},
		idempotency: map[string]memIdem{},
		leases:      map[string]memLease{},
		artifacts:   map[string]memArtifact{},
		workspaces:  map[string]Workspace{},
		topics:      map[string]TopicValue{},
		mailbox:     []MailboxMessage{},
		mailboxAcks: map[string]map[string]bool{},
	}
}

// SetClock replaces the time source used for lease expiry and timestamps.
func (s *InMemoryStore) SetClock(clock Clock) { s.clock = clockOrSystem(clock) }

// Close marks the store closed. Later calls fail with ErrStoreClosed.
func (s *InMemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// ErrStoreClosed is returned by a closed in-memory store.
var ErrStoreClosed = errors.New("store closed")

func (s *InMemoryStore) checkOpen() error {
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}

func (s *InMemoryStore) now() string { return utcNow(s.clock) }

func cloneState(state TaskState) (string, error) { return encodeState(state) }

// --- TaskStore -------------------------------------------------------------

func (s *InMemoryStore) CreateTask(state TaskState, genesis Event) error {
	state.Normalize()
	stateJSON, err := cloneState(state)
	if err != nil {
		return err
	}
	if genesis.Sequence != 1 {
		return fmt.Errorf("%w: genesis event must be sequence 1", ErrStoreConflict)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	if _, exists := s.tasks[state.TaskID]; exists {
		return fmt.Errorf("%w: task already exists", ErrStoreConflict)
	}
	createdAt := genesis.CreatedAt
	if createdAt == "" {
		createdAt = s.now()
	}
	genesis.CreatedAt = createdAt
	task := &memTask{
		state:       state,
		stateJSON:   stateJSON,
		version:     genesis.TaskVersion,
		updatedAt:   createdAt,
		events:      []Event{genesis},
		checkpoints: map[int]Checkpoint{genesis.Sequence: {TaskID: state.TaskID, Sequence: genesis.Sequence, StateJSON: stateJSON, State: state, CreatedAt: createdAt}},
	}
	s.tasks[state.TaskID] = task
	s.order = append(s.order, state.TaskID)
	return nil
}

func (s *InMemoryStore) GetTask(taskID string) (TaskState, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return TaskState{}, 0, err
	}
	task, ok := s.tasks[taskID]
	if !ok {
		return TaskState{}, 0, ErrNotFound
	}
	return task.state, task.version, nil
}

func (s *InMemoryStore) ListTasks() ([]TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	out := make([]TaskState, 0, len(s.tasks))
	for i := len(s.order) - 1; i >= 0; i-- {
		if task, ok := s.tasks[s.order[i]]; ok {
			out = append(out, task.state)
		}
	}
	return out, nil
}

func (s *InMemoryStore) AppendEvent(state TaskState, expectedVersion, expectedSequence *int, next Event) error {
	state.Normalize()
	stateJSON, err := cloneState(state)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	task, ok := s.tasks[state.TaskID]
	if !ok {
		return fmt.Errorf("%w: unknown task", ErrStoreConflict)
	}
	if expectedVersion != nil && task.version != *expectedVersion {
		return fmt.Errorf("%w: version mismatch", ErrStoreConflict)
	}
	tailSequence := 0
	previousHash := "GENESIS"
	if len(task.events) > 0 {
		tail := task.events[len(task.events)-1]
		tailSequence = tail.Sequence
		previousHash = tail.EventHash
	}
	if expectedSequence != nil && tailSequence != *expectedSequence {
		return fmt.Errorf("%w: sequence mismatch", ErrStoreConflict)
	}
	if next.Sequence != tailSequence+1 || next.PreviousHash != previousHash {
		return fmt.Errorf("%w: event does not extend the journal tail", ErrStoreConflict)
	}
	if next.TaskVersion != task.version+1 {
		return fmt.Errorf("%w: version mismatch", ErrStoreConflict)
	}
	createdAt := next.CreatedAt
	if createdAt == "" {
		createdAt = s.now()
	}
	next.CreatedAt = createdAt
	task.events = append(task.events, next)
	task.state = state
	task.stateJSON = stateJSON
	task.version = next.TaskVersion
	task.updatedAt = createdAt
	task.checkpoints[next.Sequence] = Checkpoint{TaskID: state.TaskID, Sequence: next.Sequence, StateJSON: stateJSON, State: state, CreatedAt: createdAt}
	return nil
}

// --- EventStore ------------------------------------------------------------

func (s *InMemoryStore) Events(taskID string) ([]Event, error) { return s.EventsSince(taskID, 0) }

func (s *InMemoryStore) EventsSince(taskID string, sequence int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	task, ok := s.tasks[taskID]
	if !ok {
		return nil, nil
	}
	out := []Event{}
	for _, event := range task.events {
		if event.Sequence > sequence {
			out = append(out, event)
		}
	}
	return out, nil
}

func (s *InMemoryStore) LatestEvent(taskID string) (Event, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return Event{}, false, err
	}
	task, ok := s.tasks[taskID]
	if !ok || len(task.events) == 0 {
		return Event{}, false, nil
	}
	return task.events[len(task.events)-1], true, nil
}

func (s *InMemoryStore) CheckpointAt(taskID string, sequence int) (Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return Checkpoint{}, false, err
	}
	task, ok := s.tasks[taskID]
	if !ok {
		return Checkpoint{}, false, nil
	}
	checkpoint, ok := task.checkpoints[sequence]
	return checkpoint, ok, nil
}

func (s *InMemoryStore) LatestCheckpoint(taskID string) (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return Checkpoint{}, err
	}
	task, ok := s.tasks[taskID]
	if !ok || len(task.checkpoints) == 0 {
		return Checkpoint{}, ErrNotFound
	}
	latest := 0
	for sequence := range task.checkpoints {
		if sequence > latest {
			latest = sequence
		}
	}
	return task.checkpoints[latest], nil
}

// --- IdempotencyStore ------------------------------------------------------

func (s *InMemoryStore) PutIdempotency(scope, key string, value any) (bool, error) {
	raw, err := CanonicalJSON(value)
	if err != nil {
		return false, err
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	composite := scope + "\x00" + key
	if _, exists := s.idempotency[composite]; exists {
		return false, nil
	}
	s.idempotency[composite] = memIdem{value: decoded}
	return true, nil
}

func (s *InMemoryStore) GetIdempotency(scope, key string, out any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	entry, ok := s.idempotency[scope+"\x00"+key]
	if !ok {
		return false, nil
	}
	if out == nil {
		return true, nil
	}
	raw, err := CanonicalJSON(entry.value)
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return false, err
	}
	return true, nil
}

// --- MemoryStore -----------------------------------------------------------

func (s *InMemoryStore) PutMemory(auth AuthContext, record MemoryRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.memories = append(s.memories, scopeMemory(auth, record))
	return nil
}

func (s *InMemoryStore) PutMemoryVersioned(auth AuthContext, record MemoryRecord, expectedVersion int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	current := 0
	for _, existing := range s.memories {
		if existing.TenantID != auth.TenantID || existing.SubjectID != auth.SubjectID {
			continue
		}
		if existing.Namespace != record.Namespace || existing.Key != record.Key || existing.Tombstone {
			continue
		}
		if existing.Version > current {
			current = existing.Version
		}
	}
	if current != expectedVersion {
		return false, nil
	}
	s.memories = append(s.memories, scopeMemory(auth, record))
	return true, nil
}

func scopeMemory(auth AuthContext, record MemoryRecord) MemoryRecord {
	record.TenantID = auth.TenantID
	record.SubjectID = auth.SubjectID
	return record
}

func (s *InMemoryStore) MemoryRecords(auth AuthContext, namespace string) ([]MemoryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	out := []MemoryRecord{}
	for _, record := range s.memories {
		if record.TenantID != auth.TenantID || record.SubjectID != auth.SubjectID {
			continue
		}
		if namespace != "" && record.Namespace != namespace {
			continue
		}
		out = append(out, record)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Version != out[j].Version {
			return out[i].Version > out[j].Version
		}
		return out[i].CreatedAt > out[j].CreatedAt
	})
	return out, nil
}

func (s *InMemoryStore) TombstoneMemory(auth AuthContext, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	for i := range s.memories {
		if s.memories[i].MemoryID == id && s.memories[i].TenantID == auth.TenantID && s.memories[i].SubjectID == auth.SubjectID {
			s.memories[i].Tombstone = true
			return true, nil
		}
	}
	return false, nil
}

// --- LeaseStore ------------------------------------------------------------

func (s *InMemoryStore) AcquireLease(resourceKey, ownerID, leaseID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	now := float64(s.clock.Now().UnixNano()) / 1e9
	for id, lease := range s.leases {
		if lease.resourceKey == resourceKey && lease.expiresAt <= now {
			delete(s.leases, id)
		}
	}
	for _, lease := range s.leases {
		if lease.resourceKey == resourceKey {
			return false, nil
		}
	}
	s.leases[leaseID] = memLease{leaseID: leaseID, resourceKey: resourceKey, ownerID: ownerID, expiresAt: now + ttl.Seconds(), acquiredAt: s.now()}
	return true, nil
}

func (s *InMemoryStore) RenewLease(leaseID, ownerID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	lease, ok := s.leases[leaseID]
	now := float64(s.clock.Now().UnixNano()) / 1e9
	if !ok || lease.ownerID != ownerID || lease.expiresAt <= now {
		return false, nil
	}
	lease.expiresAt = now + ttl.Seconds()
	s.leases[leaseID] = lease
	return true, nil
}

func (s *InMemoryStore) ReleaseLease(leaseID, ownerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	lease, ok := s.leases[leaseID]
	if !ok || lease.ownerID != ownerID {
		return false, nil
	}
	delete(s.leases, leaseID)
	return true, nil
}

func (s *InMemoryStore) ReleaseResourceLease(resourceKey, ownerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	for id, lease := range s.leases {
		if lease.resourceKey == resourceKey && lease.ownerID == ownerID {
			delete(s.leases, id)
			return true, nil
		}
	}
	return false, nil
}

// --- ArtifactStore ---------------------------------------------------------

func (s *InMemoryStore) PutArtifact(auth AuthContext, artifactID string, content map[string]any) (ArtifactRecord, error) {
	if artifactID == "" {
		return ArtifactRecord{}, errors.New("ARTIFACT_ID_REQUIRED")
	}
	if content == nil {
		content = map[string]any{}
	}
	digest, err := Digest(content)
	if err != nil {
		return ArtifactRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return ArtifactRecord{}, err
	}
	composite := auth.TenantID + "\x00" + auth.SubjectID + "\x00" + artifactID
	if existing, ok := s.artifacts[composite]; ok {
		if existing.sha256 != digest {
			return ArtifactRecord{}, fmt.Errorf("%w: artifact content changed", ErrStoreConflict)
		}
		return ArtifactRecord{ArtifactID: artifactID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Content: existing.content, SHA256: existing.sha256, CreatedAt: existing.createdAt}, nil
	}
	createdAt := s.now()
	s.artifacts[composite] = memArtifact{content: content, sha256: digest, createdAt: createdAt}
	return ArtifactRecord{ArtifactID: artifactID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Content: content, SHA256: digest, CreatedAt: createdAt}, nil
}

func (s *InMemoryStore) GetArtifact(auth AuthContext, artifactID string) (ArtifactRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return ArtifactRecord{}, false, err
	}
	existing, ok := s.artifacts[auth.TenantID+"\x00"+auth.SubjectID+"\x00"+artifactID]
	if !ok {
		return ArtifactRecord{}, false, nil
	}
	return ArtifactRecord{ArtifactID: artifactID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Content: existing.content, SHA256: existing.sha256, CreatedAt: existing.createdAt}, true, nil
}

func workspaceKey(auth AuthContext, workspaceID string) string {
	return auth.TenantID + "\x00" + auth.SubjectID + "\x00" + workspaceID
}

func topicKey(auth AuthContext, workspaceID, topic, key string) string {
	return workspaceKey(auth, workspaceID) + "\x00" + topic + "\x00" + key
}

func (s *InMemoryStore) EnsureWorkspace(auth AuthContext, workspaceID string, metadata map[string]any) (Workspace, error) {
	if workspaceID == "" {
		return Workspace{}, errors.New("WORKSPACE_ID_REQUIRED")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return Workspace{}, err
	}
	key := workspaceKey(auth, workspaceID)
	if existing, ok := s.workspaces[key]; ok {
		return existing, nil
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	now := s.now()
	workspace := Workspace{WorkspaceID: workspaceID, TenantID: auth.TenantID, SubjectID: auth.SubjectID, Metadata: metadata, CreatedAt: now, UpdatedAt: now}
	s.workspaces[key] = workspace
	return workspace, nil
}

func (s *InMemoryStore) PutTopicValue(auth AuthContext, workspaceID, topic, key string, value map[string]any, expectedVersion int) (TopicValue, bool, error) {
	if workspaceID == "" || topic == "" || key == "" {
		return TopicValue{}, false, errors.New("TOPIC_SCOPE_REQUIRED")
	}
	if value == nil {
		value = map[string]any{}
	}
	digest, err := Digest(value)
	if err != nil {
		return TopicValue{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return TopicValue{}, false, err
	}
	if _, ok := s.workspaces[workspaceKey(auth, workspaceID)]; !ok {
		return TopicValue{}, false, ErrWorkspaceNotFound
	}
	lookup := topicKey(auth, workspaceID, topic, key)
	current := s.topics[lookup]
	if current.Version != expectedVersion {
		return TopicValue{}, false, nil
	}
	item := TopicValue{WorkspaceID: workspaceID, Topic: topic, Key: key, Value: value, Digest: digest, Version: expectedVersion + 1, UpdatedAt: s.now()}
	s.topics[lookup] = item
	return item, true, nil
}

func (s *InMemoryStore) TopicValues(auth AuthContext, workspaceID, topic string) ([]TopicValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	values := []TopicValue{}
	prefix := workspaceKey(auth, workspaceID) + "\x00" + topic + "\x00"
	for key, value := range s.topics {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			values = append(values, value)
		}
	}
	normalizeTopicValues(values)
	return values, nil
}

func (s *InMemoryStore) AppendMailbox(auth AuthContext, message MailboxMessage) (MailboxMessage, error) {
	if message.WorkspaceID == "" || message.Topic == "" || message.FromAgent == "" {
		return MailboxMessage{}, errors.New("MAILBOX_SCOPE_REQUIRED")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return MailboxMessage{}, err
	}
	if _, ok := s.workspaces[workspaceKey(auth, message.WorkspaceID)]; !ok {
		return MailboxMessage{}, ErrWorkspaceNotFound
	}
	if message.MessageID == "" {
		message.MessageID = NewID("MSG")
	}
	if message.Payload == nil {
		message.Payload = map[string]any{}
	}
	if message.IdempotencyKey != "" {
		for _, existing := range s.mailbox {
			if existing.TenantID == auth.TenantID && existing.SubjectID == auth.SubjectID && existing.WorkspaceID == message.WorkspaceID && existing.Topic == message.Topic && existing.FromAgent == message.FromAgent && existing.IdempotencyKey == message.IdempotencyKey {
				return existing, nil
			}
		}
	}
	var sequence int64 = 1
	for _, existing := range s.mailbox {
		if existing.TenantID == auth.TenantID && existing.SubjectID == auth.SubjectID && existing.WorkspaceID == message.WorkspaceID && existing.Topic == message.Topic && existing.Sequence >= sequence {
			sequence = existing.Sequence + 1
		}
	}
	message.Sequence, message.CreatedAt = sequence, s.now()
	message.TenantID, message.SubjectID = auth.TenantID, auth.SubjectID
	s.mailbox = append(s.mailbox, message)
	return message, nil
}

// MailboxMessages returns unacked messages for one recipient, mirroring the
// SQLite backend: direct messages use the row's acked flag, broadcasts are
// acknowledged per recipient.
func (s *InMemoryStore) MailboxMessages(auth AuthContext, workspaceID, topic, recipient string, limit int) ([]MailboxMessage, error) {
	if limit <= 0 {
		limit = 64
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	messages := []MailboxMessage{}
	for _, message := range s.mailbox {
		if message.TenantID != auth.TenantID || message.SubjectID != auth.SubjectID || message.WorkspaceID != workspaceID || message.Topic != topic {
			continue
		}
		if message.ToAgent == "" {
			if s.mailboxAcks[message.MessageID][recipient] {
				continue
			}
		} else if message.Acked || message.ToAgent != recipient {
			continue
		}
		messages = append(messages, message)
		if len(messages) == limit {
			break
		}
	}
	return messages, nil
}

func (s *InMemoryStore) AckMailbox(auth AuthContext, messageID, recipient string) (bool, error) {
	if recipient == "" {
		return false, errors.New("MAILBOX_RECIPIENT_REQUIRED")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	for i := range s.mailbox {
		if s.mailbox[i].TenantID != auth.TenantID || s.mailbox[i].SubjectID != auth.SubjectID || s.mailbox[i].MessageID != messageID {
			continue
		}
		if s.mailbox[i].ToAgent != "" {
			if s.mailbox[i].ToAgent != recipient {
				// Only the addressed recipient may ack a direct message.
				return false, nil
			}
			s.mailbox[i].Acked = true
			return true, nil
		}
		// Broadcast: acknowledge for this recipient only, idempotently.
		if s.mailboxAcks[messageID] == nil {
			s.mailboxAcks[messageID] = map[string]bool{}
		}
		s.mailboxAcks[messageID][recipient] = true
		return true, nil
	}
	return false, nil
}

// --- FeedbackStore ---------------------------------------------------------

func (s *InMemoryStore) PutFeedback(signal FeedbackSignal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return err
	}
	if signal.FeedbackID == "" {
		signal.FeedbackID = NewID("FDBK")
	}
	if signal.CreatedAt == "" {
		signal.CreatedAt = s.now()
	}
	s.feedback = append(s.feedback, signal)
	return nil
}

func (s *InMemoryStore) Feedback(tenantID string) ([]FeedbackSignal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	out := []FeedbackSignal{}
	for _, signal := range s.feedback {
		if signal.TenantID == tenantID {
			out = append(out, signal)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].FeedbackID < out[j].FeedbackID
	})
	return out, nil
}

// Compile-time assertions: the in-memory backend satisfies both the narrow
// core contract and the full store surface.
var (
	_ CoreStore = (*InMemoryStore)(nil)
	_ Store     = (*InMemoryStore)(nil)
)
