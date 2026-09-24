package harness

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// PreviousResult is an explicit hand-off from an earlier step or agent.
// Keeping it separate from an untyped metadata map makes dependency edges
// visible to the context compiler and replayable from the task request.
//
// Note on immutability: the struct is passed by value, but Value is a map, so a
// caller must not mutate it after handing it off. What the runtime enforces is
// integrity, not aliasing — see Digest below.
//
// Digest is optional but binding: when it is set, ContextBuilder verifies it
// against Value via Verify and refuses the hand-off on a mismatch. Callers that
// leave it empty make no integrity claim.
type PreviousResult struct {
	ResultID string         `json:"result_id"`
	SourceID string         `json:"source_id,omitempty"`
	Kind     string         `json:"kind,omitempty"`
	Summary  string         `json:"summary,omitempty"`
	Digest   string         `json:"digest,omitempty"`
	Value    map[string]any `json:"value,omitempty"`
}

// ErrDigestMismatch reports a hand-off whose claimed digest does not match the
// value it carries.
var ErrDigestMismatch = errors.New("PREVIOUS_RESULT_DIGEST_MISMATCH")

// Verify checks the optional digest claim. A result with no digest passes: it
// simply makes no claim. A result whose digest does not match its value is
// rejected rather than compiled into the model context.
func (p PreviousResult) Verify() error {
	claimed := strings.TrimSpace(p.Digest)
	if claimed == "" {
		return nil
	}
	actual, err := Digest(p.Value)
	if err != nil {
		return err
	}
	if actual != claimed {
		return fmt.Errorf("%w: result_id=%s claimed=%s actual=%s", ErrDigestMismatch, p.ResultID, claimed, actual)
	}
	return nil
}

// Workspace is the identity boundary for shared agent state. A workspace is
// scoped to one tenant and subject; agents sharing it still communicate only
// through named topics and the mailbox protocol.
type Workspace struct {
	WorkspaceID string         `json:"workspace_id"`
	TenantID    string         `json:"tenant_id"`
	SubjectID   string         `json:"subject_id"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	CreatedAt   string         `json:"created_at"`
	UpdatedAt   string         `json:"updated_at"`
}

// TopicValue is a versioned key/value projection within a workspace topic.
// Writers use expectedVersion for compare-and-swap updates.
type TopicValue struct {
	WorkspaceID string         `json:"workspace_id"`
	Topic       string         `json:"topic"`
	Key         string         `json:"key"`
	Value       map[string]any `json:"value"`
	Digest      string         `json:"digest"`
	Version     int            `json:"version"`
	UpdatedAt   string         `json:"updated_at"`
}

// WorkspaceStore is optional so existing external CoreStore implementations
// remain source-compatible. The built-in SQLite and in-memory stores both
// implement it.
type WorkspaceStore interface {
	EnsureWorkspace(auth AuthContext, workspaceID string, metadata map[string]any) (Workspace, error)
	PutTopicValue(auth AuthContext, workspaceID, topic, key string, value map[string]any, expectedVersion int) (TopicValue, bool, error)
	TopicValues(auth AuthContext, workspaceID, topic string) ([]TopicValue, error)
}

// MailboxMessage is a durable point-to-point or broadcast envelope. Sequence
// is assigned by the store per workspace/topic; MessageID and IdempotencyKey
// make retries observable and deduplicated.
type MailboxMessage struct {
	MessageID      string         `json:"message_id"`
	TenantID       string         `json:"tenant_id,omitempty"`
	SubjectID      string         `json:"subject_id,omitempty"`
	WorkspaceID    string         `json:"workspace_id"`
	Topic          string         `json:"topic"`
	FromAgent      string         `json:"from_agent"`
	ToAgent        string         `json:"to_agent,omitempty"`
	Protocol       string         `json:"protocol"`
	MessageType    string         `json:"message_type"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Payload        map[string]any `json:"payload"`
	Sequence       int64          `json:"sequence"`
	CreatedAt      string         `json:"created_at"`
	Acked          bool           `json:"acked"`
}

// MailboxStore is the durable delivery primitive. Receive is ordered and
// returns unacked messages for recipient (or broadcast messages with an empty
// ToAgent). Ack is idempotent.
type MailboxStore interface {
	AppendMailbox(auth AuthContext, message MailboxMessage) (MailboxMessage, error)
	MailboxMessages(auth AuthContext, workspaceID, topic, recipient string, limit int) ([]MailboxMessage, error)
	AckMailbox(auth AuthContext, messageID, recipient string) (bool, error)
}

// MailboxProtocol validates and names an envelope protocol. Protocols are
// intentionally small: routing, ordering and persistence stay in MailboxStore.
type MailboxProtocol interface {
	Name() string
	Validate(MailboxMessage) error
}

// JSONMailboxProtocol is the default deterministic protocol for structured
// collaboration messages. It validates the envelope while leaving payload
// schema specialization to higher-level protocols.
type JSONMailboxProtocol struct{ Version string }

func (p JSONMailboxProtocol) Name() string {
	if strings.TrimSpace(p.Version) == "" {
		return "json/v1"
	}
	return "json/" + strings.TrimSpace(p.Version)
}

func (p JSONMailboxProtocol) Validate(message MailboxMessage) error {
	if strings.TrimSpace(message.MessageType) == "" {
		return errors.New("MESSAGE_TYPE_REQUIRED")
	}
	if message.Payload == nil {
		return errors.New("MESSAGE_PAYLOAD_REQUIRED")
	}
	return nil
}

// ProtocolRegistry keeps protocol selection explicit and deterministic.
type ProtocolRegistry struct{ protocols map[string]MailboxProtocol }

func NewProtocolRegistry(protocols ...MailboxProtocol) *ProtocolRegistry {
	r := &ProtocolRegistry{protocols: map[string]MailboxProtocol{}}
	for _, protocol := range protocols {
		if protocol != nil {
			r.protocols[protocol.Name()] = protocol
		}
	}
	return r
}

func (r *ProtocolRegistry) Register(protocol MailboxProtocol) error {
	if r == nil || protocol == nil || strings.TrimSpace(protocol.Name()) == "" {
		return errors.New("INVALID_MAILBOX_PROTOCOL")
	}
	if r.protocols == nil {
		r.protocols = map[string]MailboxProtocol{}
	}
	if _, exists := r.protocols[protocol.Name()]; exists {
		return fmt.Errorf("MAILBOX_PROTOCOL_EXISTS: %s", protocol.Name())
	}
	r.protocols[protocol.Name()] = protocol
	return nil
}

func (r *ProtocolRegistry) Get(name string) (MailboxProtocol, bool) {
	if r == nil {
		return nil, false
	}
	protocol, ok := r.protocols[name]
	return protocol, ok
}

// Mailbox is the protocol-aware facade used by runtimes and orchestrators.
type Mailbox struct {
	Store     MailboxStore
	Protocols *ProtocolRegistry
}

func NewMailbox(store MailboxStore) *Mailbox {
	return &Mailbox{Store: store, Protocols: NewProtocolRegistry(JSONMailboxProtocol{})}
}

// NewMailboxWithProtocols builds a mailbox whose registry contains exactly the
// given protocols. Use it to replace the built-in "json/v1" protocol: the
// default registry already holds it, so Register cannot override it.
func NewMailboxWithProtocols(store MailboxStore, protocols ...MailboxProtocol) *Mailbox {
	if len(protocols) == 0 {
		return NewMailbox(store)
	}
	return &Mailbox{Store: store, Protocols: NewProtocolRegistry(protocols...)}
}

// protocols returns the registry to use. It must not lazily assign to
// m.Protocols: a concurrent Send on a zero-value Mailbox would race on that
// field, and the race detector is part of this project's gate.
func (m *Mailbox) protocols() *ProtocolRegistry {
	if m.Protocols != nil {
		return m.Protocols
	}
	return NewProtocolRegistry(JSONMailboxProtocol{})
}

func (m *Mailbox) Send(auth AuthContext, message MailboxMessage) (MailboxMessage, error) {
	if m == nil || m.Store == nil {
		return MailboxMessage{}, errors.New("MAILBOX_STORE_REQUIRED")
	}
	if strings.TrimSpace(message.WorkspaceID) == "" || strings.TrimSpace(message.Topic) == "" {
		return MailboxMessage{}, errors.New("MAILBOX_SCOPE_REQUIRED")
	}
	if strings.TrimSpace(message.FromAgent) == "" {
		return MailboxMessage{}, errors.New("MAILBOX_SENDER_REQUIRED")
	}
	if strings.TrimSpace(message.Protocol) == "" {
		message.Protocol = JSONMailboxProtocol{}.Name()
	}
	protocol, ok := m.protocols().Get(message.Protocol)
	if !ok {
		return MailboxMessage{}, fmt.Errorf("MAILBOX_PROTOCOL_NOT_FOUND: %s", message.Protocol)
	}
	if err := protocol.Validate(message); err != nil {
		return MailboxMessage{}, err
	}
	return m.Store.AppendMailbox(auth, message)
}

func (m *Mailbox) Receive(auth AuthContext, workspaceID, topic, recipient string, limit int) ([]MailboxMessage, error) {
	if m == nil || m.Store == nil {
		return nil, errors.New("MAILBOX_STORE_REQUIRED")
	}
	return m.Store.MailboxMessages(auth, workspaceID, topic, recipient, limit)
}

func (m *Mailbox) Ack(auth AuthContext, messageID, recipient string) (bool, error) {
	if m == nil || m.Store == nil {
		return false, errors.New("MAILBOX_STORE_REQUIRED")
	}
	return m.Store.AckMailbox(auth, messageID, recipient)
}

func normalizeTopicValues(values []TopicValue) {
	sort.Slice(values, func(i, j int) bool {
		if values[i].Topic != values[j].Topic {
			return values[i].Topic < values[j].Topic
		}
		return values[i].Key < values[j].Key
	})
}
