package harness

import (
	"strings"
	"testing"
)

func TestPreviousResultsAreExplicitlyCompiled(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	registry := NewToolRegistry()
	builder := NewContextBuilder(nil, NewSkillRegistry(), registry, NewPrefixCache(4))
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject"}
	request := TaskRequest{TaskID: "task-previous", Goal: "synthesize", TenantID: auth.TenantID, SubjectID: auth.SubjectID, PreviousResults: []PreviousResult{{ResultID: "r1", SourceID: "agent-a", Kind: "research", Summary: "evidence", Value: map[string]any{"answer": "42"}}}}
	state := TaskState{TaskID: request.TaskID, Goal: request.Goal, TenantID: auth.TenantID, SubjectID: auth.SubjectID}
	packet, err := builder.Build(request, auth, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(packet.PreviousResults) != 1 || !strings.Contains(providerText(packet.PreviousResults[0]), "42") {
		t.Fatalf("previous results missing from packet: %#v", packet.PreviousResults)
	}
	joined := providerText(packet.ProviderMessages())
	if !strings.Contains(joined, "previous_results") || !strings.Contains(joined, "agent-a") {
		t.Fatalf("previous results missing from provider context: %s", joined)
	}
}

func testCollaborationStore(t *testing.T, store interface {
	CoreStore
	WorkspaceStore
	MailboxStore
}) {
	t.Helper()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject"}
	other := AuthContext{TenantID: "other", SubjectID: "subject"}
	if _, err := store.EnsureWorkspace(auth, "ws-1", map[string]any{"name": "shared"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.PutTopicValue(auth, "ws-1", "results", "a", map[string]any{"value": 1}, 0); err != nil || !ok {
		t.Fatalf("first topic write ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.PutTopicValue(auth, "ws-1", "results", "a", map[string]any{"value": 2}, 0); err != nil || ok {
		t.Fatalf("stale topic write ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.PutTopicValue(auth, "ws-1", "results", "a", map[string]any{"value": 2}, 1); err != nil || !ok {
		t.Fatalf("cas topic write ok=%v err=%v", ok, err)
	}
	values, err := store.TopicValues(auth, "ws-1", "results")
	if err != nil || len(values) != 1 || values[0].Version != 2 {
		t.Fatalf("topic values=%+v err=%v", values, err)
	}
	if _, err := store.EnsureWorkspace(other, "ws-1", nil); err != nil {
		t.Fatal(err)
	}
	if values, err := store.TopicValues(other, "ws-1", "results"); err != nil || len(values) != 0 {
		t.Fatalf("cross-tenant topic visibility values=%+v err=%v", values, err)
	}
	mailbox := &Mailbox{Store: store, Protocols: NewProtocolRegistry(JSONMailboxProtocol{Version: "v1"})}
	sent, err := mailbox.Send(auth, MailboxMessage{WorkspaceID: "ws-1", Topic: "work", FromAgent: "a", ToAgent: "b", Protocol: "json/v1", MessageType: "result", IdempotencyKey: "idem-1", Payload: map[string]any{"answer": 42}})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := mailbox.Send(auth, MailboxMessage{WorkspaceID: "ws-1", Topic: "work", FromAgent: "a", ToAgent: "b", Protocol: "json/v1", MessageType: "result", IdempotencyKey: "idem-1", Payload: map[string]any{"answer": 42}})
	if err != nil || replayed.MessageID != sent.MessageID {
		t.Fatalf("idempotent send sent=%+v replay=%+v err=%v", sent, replayed, err)
	}
	messages, err := mailbox.Receive(auth, "ws-1", "work", "b", 10)
	if err != nil || len(messages) != 1 || messages[0].Sequence != 1 {
		t.Fatalf("mailbox messages=%+v err=%v", messages, err)
	}
	acked, err := mailbox.Ack(auth, sent.MessageID, "b")
	if err != nil || !acked {
		t.Fatalf("ack=%v err=%v", acked, err)
	}
	messages, err = mailbox.Receive(auth, "ws-1", "work", "b", 10)
	if err != nil || len(messages) != 0 {
		t.Fatalf("acked mailbox messages=%+v err=%v", messages, err)
	}
	if _, err := mailbox.Send(auth, MailboxMessage{WorkspaceID: "ws-1", Topic: "work", FromAgent: "a", Protocol: "unknown", MessageType: "result", Payload: map[string]any{}}); err == nil {
		t.Fatal("unknown mailbox protocol was accepted")
	}
}

func TestCollaborationStoreInMemory(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	testCollaborationStore(t, store)
}

func TestCollaborationStoreSQLite(t *testing.T) {
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	testCollaborationStore(t, store)
}
