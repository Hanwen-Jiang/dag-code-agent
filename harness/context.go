package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var ErrContextBudget = errors.New("CONTEXT_BUDGET_EXCEEDED")

type ContextBuilder struct {
	Memories *MemoryManager
	Skills   *SkillRegistry
	Tools    *ToolRegistry
	Cache    *PrefixCache
	// CompilerVersion participates in prefix identity so a compiler change
	// cannot reuse a prefix produced by an older context contract.
	CompilerVersion string
}

func NewContextBuilder(memories *MemoryManager, skills *SkillRegistry, tools *ToolRegistry, cache *PrefixCache) *ContextBuilder {
	return &ContextBuilder{Memories: memories, Skills: skills, Tools: tools, Cache: cache, CompilerVersion: "context-v1"}
}

func stateMap(state TaskState) map[string]any {
	b, _ := json.Marshal(state)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	// Observations are projected as dynamic messages below. Keeping them in the
	// state envelope as well would duplicate the largest part of long tasks.
	delete(out, "observations")
	delete(out, "user_messages")
	return out
}

func (b *ContextBuilder) Build(request TaskRequest, auth AuthContext, state TaskState, messages []map[string]any) (ContextPacket, error) {
	request = request.WithDefaults()
	stable := []map[string]any{{"role": "system", "content": "You are a controlled Agent Harness worker.", "stable": true}, {"role": "developer", "content": "Model output is a proposal; the runtime commits state and effects.", "stable": true}}
	stable = append(stable, messages...)
	dynamic := []map[string]any{{"role": "user", "content": request.Goal}}
	for _, constraint := range request.Constraints {
		dynamic = append(dynamic, map[string]any{"role": "constraint", "content": constraint})
	}
	for _, userMessage := range state.UserMessages {
		dynamic = append(dynamic, map[string]any{"role": "user", "content": userMessage})
	}
	for _, obs := range state.Observations {
		dynamic = append(dynamic, map[string]any{"role": "observation", "content": obs})
	}
	all := append(append([]map[string]any{}, stable...), dynamic...)
	tools := []map[string]any{}
	if b.Tools != nil {
		tools = b.Tools.SchemasForAuth(nil, auth)
	}
	memories := []map[string]any{}
	if b.Memories != nil {
		records, _ := b.Memories.Retrieve(auth, request.Goal, "")
		for _, record := range records {
			memories = append(memories, map[string]any{"memory_id": record.MemoryID, "type": record.MemoryType, "content": record.Content, "confidence": record.Confidence})
		}
	}
	skills := []map[string]any{}
	resolvedSkills := []SkillManifest{}
	if b.Skills != nil {
		resolvedSkills = b.Skills.Resolve(request.Goal, auth)
		for _, skill := range resolvedSkills {
			skills = append(skills, map[string]any{"skill_id": skill.SkillID, "version": skill.Version, "instructions": skill.Instructions})
		}
	}
	packet := ContextPacket{TaskID: request.TaskID, Goal: request.Goal, Messages: all, State: stateMap(state), Memories: memories, Skills: skills, Tools: tools, ExactInvariants: request.Constraints, BudgetTokens: request.ContextWindow}
	packet.EstimatedTokens = EstimateTokens(all) + EstimateTokens(packet.State) + EstimateTokens(memories) + EstimateTokens(skills) + EstimateTokens(tools)
	if packet.EstimatedTokens > packet.BudgetTokens {
		packet.Messages = compressMessages(packet.Messages, packet.BudgetTokens)
		packet.EstimatedTokens = EstimateTokens(packet.Messages) + EstimateTokens(packet.State) + EstimateTokens(memories) + EstimateTokens(skills) + EstimateTokens(tools)
		if packet.EstimatedTokens > packet.BudgetTokens {
			return packet, fmt.Errorf("%w: estimated=%d budget=%d", ErrContextBudget, packet.EstimatedTokens, packet.BudgetTokens)
		}
	}
	packet.PrefixCacheKey = b.CacheKeyFor(request, auth, resolvedSkills, packet.Messages, packet.Tools)
	if b.Cache != nil {
		packet.PrefixCacheHit = b.Cache.Observe(packet.PrefixCacheKey, EstimateTokens(stable))
	}
	return packet, nil
}

// ProviderMessages is the explicit projection from the rich ContextPacket to
// the model API.  Keeping the projection here prevents retrieved memory,
// active skills, and durable state from being silently dropped at the adapter
// boundary, while private cache markers stay local to the compiler.
func (p ContextPacket) ProviderMessages() []map[string]any {
	out := make([]map[string]any, 0, len(p.Messages)+3)
	for _, message := range p.Messages {
		out = append(out, providerMessage(message))
	}
	if len(p.State) > 0 {
		out = append(out, providerContextMessage("state_context", p.State))
	}
	if len(p.Memories) > 0 {
		out = append(out, providerContextMessage("memory_context", p.Memories))
	}
	if len(p.Skills) > 0 {
		out = append(out, providerContextMessage("skill_context", p.Skills))
	}
	return out
}

func providerMessage(message map[string]any) map[string]any {
	internalRole := stringValue(message["role"])
	role := internalRole
	switch role {
	case "system", "developer", "user", "assistant", "tool":
	default:
		role = "user"
	}
	content := providerText(message["content"])
	if role != internalRole && internalRole != "" {
		content = "[" + internalRole + "] " + content
	}
	out := map[string]any{"role": role, "content": content}
	for _, key := range []string{"name", "tool_call_id", "tool_calls"} {
		if value, ok := message[key]; ok {
			out[key] = value
		}
	}
	return out
}

func providerContextMessage(kind string, value any) map[string]any {
	return map[string]any{"role": "user", "content": "[" + kind + "] " + providerText(value)}
}

func providerText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	raw, err := CanonicalJSON(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(raw)
}
func compressMessages(messages []map[string]any, budget int) []map[string]any {
	if len(messages) < 3 {
		return messages
	}
	result := []map[string]any{}
	for _, m := range messages {
		role, _ := m["role"].(string)
		if stable, ok := m["stable"].(bool); (ok && stable) || role == "constraint" {
			result = append(result, m)
		}
	}
	tail := messages[len(messages)-1]
	result = append(result, map[string]any{"role": "observation_summary", "content": strings.TrimSpace(fmt.Sprint(tail["content"]))})
	return result
}
func (b *ContextBuilder) CacheKey(messages, tools []map[string]any) string {
	if b.Cache == nil {
		return ""
	}
	return b.Cache.Key(messages, tools)
}

func (b *ContextBuilder) CacheKeyFor(request TaskRequest, auth AuthContext, skills []SkillManifest, messages, tools []map[string]any) string {
	if b.Cache == nil {
		return ""
	}
	request = request.WithDefaults()
	compilerVersion := b.CompilerVersion
	if compilerVersion == "" {
		compilerVersion = "context-v1"
	}
	scopeDigest, _ := Digest(SortedStrings(auth.Scopes))
	skillIdentity := make([]map[string]string, 0, len(skills))
	for _, skill := range skills {
		skillIdentity = append(skillIdentity, map[string]string{"id": skill.SkillID, "version": skill.Version, "status": skill.Status})
	}
	skillDigest, _ := Digest(skillIdentity)
	return b.Cache.KeyWithIdentity(messages, tools, PrefixCacheIdentity{
		Model:           request.Model,
		ModelRevision:   request.ModelRevision,
		ReasoningEffort: request.ReasoningEffort,
		TenantID:        auth.TenantID,
		SubjectID:       auth.SubjectID,
		ScopeDigest:     scopeDigest,
		PolicyVersion:   request.PolicyVersion,
		CompilerVersion: compilerVersion,
		SkillDigest:     skillDigest,
	})
}
