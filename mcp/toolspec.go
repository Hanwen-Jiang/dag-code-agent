package mcp

import (
	"github.com/Hanwen-Jiang/dag-code-agent/harness"
)

// Side-effect classifications used by harness.ToolSpec.SideEffect.
const (
	sideEffectNone         = "NONE"
	sideEffectUnknown      = "UNKNOWN"
	sideEffectIrreversible = "IRREVERSIBLE"
)

// rawTool is the part of an MCP tool description the client consumes
// (design 6.4).
type rawTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema map[string]any  `json:"inputSchema"`
	Annotations *toolAnnotation `json:"annotations"`
}

// toolAnnotation mirrors MCP ToolAnnotations. Every hint is a pointer so that
// "absent" is distinguishable from "explicitly false": the mapping must be
// conservative when a server does not annotate a tool.
type toolAnnotation struct {
	Title           string `json:"title,omitempty"`
	ReadOnlyHint    *bool  `json:"readOnlyHint,omitempty"`
	DestructiveHint *bool  `json:"destructiveHint,omitempty"`
	IdempotentHint  *bool  `json:"idempotentHint,omitempty"`
	OpenWorldHint   *bool  `json:"openWorldHint,omitempty"`
}

// mapTool converts one MCP tool description into a harness.ToolSpec. The name
// is namespaced as "<server>__<tool>"; the input schema is already JSON Schema
// and is reused as-is by the core validator.
func mapTool(server string, tool rawTool) harness.ToolSpec {
	schema := tool.InputSchema
	if schema == nil {
		schema = map[string]any{"type": "object"}
	}
	sideEffect, idempotencyRequired := mapAnnotations(tool.Annotations)
	return harness.ToolSpec{
		Name:                qualifiedToolName(server, tool.Name),
		Description:         tool.Description,
		InputSchema:         schema,
		SideEffect:          sideEffect,
		IdempotencyRequired: idempotencyRequired,
	}
}

// mapAnnotations implements the annotation mapping of design 6.4:
//
//	readOnlyHint=true     -> SideEffect NONE
//	destructiveHint=true  -> SideEffect IRREVERSIBLE
//	idempotentHint=true   -> IdempotencyRequired false
//	no annotations        -> SideEffect UNKNOWN, IdempotencyRequired true
//
// An MCP server may claim anything, so only an explicit readOnlyHint=true
// earns no-side-effect treatment, and a contradictory
// destructiveHint=true/readOnlyHint=true pair resolves to IRREVERSIBLE. An
// unannotated (or merely non-idempotent) tool keeps requiring an idempotency
// key. Neither UNKNOWN nor IRREVERSIBLE bypasses the Authorizer or HookBus:
// that stays the core pipeline's decision.
func mapAnnotations(annotation *toolAnnotation) (string, bool) {
	sideEffect := sideEffectUnknown
	idempotencyRequired := true
	if annotation == nil {
		return sideEffect, idempotencyRequired
	}
	if annotation.DestructiveHint != nil && *annotation.DestructiveHint {
		sideEffect = sideEffectIrreversible
	} else if annotation.ReadOnlyHint != nil && *annotation.ReadOnlyHint {
		sideEffect = sideEffectNone
	}
	if annotation.IdempotentHint != nil && *annotation.IdempotentHint {
		idempotencyRequired = false
	}
	return sideEffect, idempotencyRequired
}

// qualifiedToolName namespaces a server tool as "<server>__<tool>".
func qualifiedToolName(server, tool string) string {
	return server + "__" + tool
}
