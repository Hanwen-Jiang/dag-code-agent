package harness

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The seam exists so an embedder can change a string the compiler used to
// hardcode. What it must NOT change is also part of the contract.

type fixedPrompt struct {
	text string
	err  error
}

func (p fixedPrompt) SystemPrompt(context.Context, TaskRequest, AuthContext) (string, error) {
	return p.text, p.err
}

func promptRuntime(t *testing.T, provider SystemPromptProvider) (*Runtime, *ContextBuilder, AuthContext) {
	t.Helper()
	store := NewInMemoryStore()
	t.Cleanup(func() { store.Close() })
	runtime, err := New(WithStore(store),
		WithModel(NewScriptedModel([]ModelResponse{{Text: "ok", FinishReason: "stop"}})),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()), WithSystemPromptProvider(provider))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { runtime.Close() })
	builder := NewContextBuilder(nil, nil, nil, nil)
	if provider != nil {
		builder.SystemPrompt = provider
	}
	return runtime, builder, AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}
}

func compileWith(t *testing.T, builder *ContextBuilder, auth AuthContext) ContextPacket {
	t.Helper()
	packet, err := builder.BuildWithContext(context.Background(), TaskRequest{
		TaskID: "prompted", Goal: "do the thing", TenantID: auth.TenantID, SubjectID: auth.SubjectID,
	}, auth, TaskState{TaskID: "prompted", Status: TaskRunning}, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return packet
}

func firstSystemMessage(packet ContextPacket) string {
	for _, message := range packet.Messages {
		if message["role"] == "system" {
			if content, ok := message["content"].(string); ok {
				return content
			}
		}
	}
	return ""
}

func TestSystemPromptProviderReplacesTheBuiltIn(t *testing.T) {
	_, builder, auth := promptRuntime(t, fixedPrompt{text: "You are a domain-specific reviewer."})
	packet := compileWith(t, builder, auth)
	if got := firstSystemMessage(packet); got != "You are a domain-specific reviewer." {
		t.Fatalf("system message=%q, want the provider's", got)
	}
}

// The control-plane contract is not a style choice: a provider must not be able
// to remove it by supplying its own system prompt.
func TestControlPlaneContractSurvivesACustomPrompt(t *testing.T) {
	_, builder, auth := promptRuntime(t, fixedPrompt{text: "anything"})
	packet := compileWith(t, builder, auth)
	found := false
	for _, message := range packet.Messages {
		if message["role"] == "developer" {
			if content, ok := message["content"].(string); ok && content != "" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("the developer message stating the control-plane contract was replaced or removed")
	}
}

// A provider that fails must degrade the wording, not the run.
func TestSystemPromptFailureFallsBackAndIsVisible(t *testing.T) {
	_, builder, auth := promptRuntime(t, fixedPrompt{err: errors.New("template store unreachable")})
	packet := compileWith(t, builder, auth)
	if got := firstSystemMessage(packet); got != defaultSystemPrompt {
		t.Fatalf("system message=%q, want the built-in fallback", got)
	}
	// Visible rather than silent: an operator can see which prompt was used.
	degraded := false
	for _, constraint := range packet.ExactInvariants {
		if constraint == "system_prompt_degraded" {
			degraded = true
		}
	}
	if !degraded {
		t.Fatal("the fallback was not recorded, so a degraded prompt is indistinguishable from the intended one")
	}
}

// An empty prompt means "nothing to say", not "send an empty system message".
func TestEmptySystemPromptKeepsTheBuiltIn(t *testing.T) {
	_, builder, auth := promptRuntime(t, fixedPrompt{text: "   "})
	packet := compileWith(t, builder, auth)
	if got := firstSystemMessage(packet); got != defaultSystemPrompt {
		t.Fatalf("system message=%q, want the built-in prompt", got)
	}
}

// With no provider the compiler behaves exactly as before.
func TestWithoutAProviderTheBuiltInIsUsed(t *testing.T) {
	_, builder, auth := promptRuntime(t, nil)
	packet := compileWith(t, builder, auth)
	if got := firstSystemMessage(packet); got != defaultSystemPrompt {
		t.Fatalf("system message=%q, want the built-in prompt", got)
	}
}
