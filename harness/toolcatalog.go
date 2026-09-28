package harness

import (
	"fmt"
	"sort"
	"strings"
)

// Deferred tool loading.
//
// Every tool's full JSON Schema used to be injected into every model call. That
// is fine for a handful of tools and ruinous for a real catalogue: schemas are
// verbose, they are re-sent on every step, and they crowd out the actual work.
//
// Above a threshold the compiler now sends a compact catalogue instead — names
// and one-line descriptions — together with a tool_search tool that resolves a
// query to full schemas on demand. The model pays for the schema it asks for
// rather than for every schema that exists.
//
// The threshold is opt-in (zero disables it) because the right value depends on
// the embedder's context window and catalogue size; guessing a default would
// silently change the prompt of every existing deployment.

// toolCatalogThresholdKey is unused as state; the threshold lives on the builder.
const toolSearchToolName = "tool_search"

// ToolSearchToolName is the built-in resolver's name.
func ToolSearchToolName() string { return toolSearchToolName }

// WithToolCatalogThreshold defers full schemas once the visible catalogue exceeds
// the threshold, and registers the tool_search resolver that replaces them.
func WithToolCatalogThreshold(tools int) Option {
	return func(c *runtimeConfig) error {
		if tools < 0 {
			return fmt.Errorf("harness: WithToolCatalogThreshold requires a non-negative threshold")
		}
		c.toolCatalogThreshold = tools
		return nil
	}
}

// NewToolSearchTool builds the resolver that returns full schemas for a query.
//
// It is a normal tool, so it goes through validation, scoping, timeouts and the
// audit trail like any other. That matters: a resolver with a private path into
// the registry would be a way to bypass tool policy.
func NewToolSearchTool(registry *ToolRegistry) ToolSpec {
	return ToolSpec{
		Name:        toolSearchToolName,
		Description: "Look up tools by name or purpose and return their full JSON schemas. The catalogue lists which tools exist; call this before using one whose schema you were not given.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string", "description": "what you want to do, or part of a tool name"},
				"limit": map[string]any{"type": "number", "description": "maximum tools to return (default 5)"},
			},
			"required": []string{"query"},
		},
		SideEffect:      "NONE",
		ConcurrencySafe: true,
		Handler: func(arguments map[string]any, auth AuthContext) (any, error) {
			if registry == nil {
				return map[string]any{"tools": []any{}}, nil
			}
			query := strings.ToLower(strings.TrimSpace(stringValue(arguments["query"])))
			limit := 5
			if requested, ok := arguments["limit"].(float64); ok && requested > 0 {
				limit = int(requested)
			}
			if limit > 25 {
				limit = 25
			}
			scored := make([]scoredTool, 0)
			for _, spec := range registry.List() {
				if !authorizedForTool(spec, auth) {
					continue
				}
				if score := toolMatchScore(spec, query); score > 0 || query == "" {
					scored = append(scored, scoredTool{spec: spec, score: score})
				}
			}
			// Ties are broken by name so the same query returns the same order
			// on every call, which keeps a replayed run's tool choices stable.
			sort.SliceStable(scored, func(i, j int) bool {
				if scored[i].score != scored[j].score {
					return scored[i].score > scored[j].score
				}
				return scored[i].spec.Name < scored[j].spec.Name
			})
			if len(scored) > limit {
				scored = scored[:limit]
			}
			tools := make([]map[string]any, 0, len(scored))
			names := make([]string, 0, len(scored))
			for _, entry := range scored {
				tools = append(tools, entry.spec.SchemaForModel())
				names = append(names, entry.spec.Name)
			}
			return map[string]any{"tools": tools, "names": names}, nil
		},
	}
}

type scoredTool struct {
	spec  ToolSpec
	score int
}

// toolMatchScore ranks a tool against a query.
//
// A name match outweighs a description match, and a whole-word match outweighs a
// substring: a query of "read" should prefer workspace_read over a tool that
// merely mentions reading in its description.
func toolMatchScore(spec ToolSpec, query string) int {
	if query == "" {
		return 1
	}
	name := strings.ToLower(spec.Name)
	score := 0
	if name == query {
		score += 100
	}
	if strings.Contains(name, query) {
		score += 30
	}
	for _, word := range strings.FieldsFunc(query, func(r rune) bool { return r == ' ' || r == '_' || r == '-' }) {
		if len(word) < 2 {
			continue
		}
		if strings.Contains(name, word) {
			score += 12
		}
		if strings.Contains(strings.ToLower(spec.Description), word) {
			score += 5
		}
		for _, tag := range spec.Tags {
			if strings.Contains(strings.ToLower(tag), word) {
				score += 4
			}
		}
	}
	return score
}

// authorizedForTool reports whether the auth context may see a tool at all.
func authorizedForTool(spec ToolSpec, auth AuthContext) bool {
	if len(spec.RequiredScopes) == 0 {
		return true
	}
	granted := make(map[string]bool, len(auth.Scopes))
	for _, scope := range auth.Scopes {
		granted[scope] = true
	}
	for _, required := range spec.RequiredScopes {
		if !granted[required] {
			return false
		}
	}
	return true
}

// renderToolCatalog renders the compact catalogue injected in place of schemas.
func renderToolCatalog(schemas []map[string]any) string {
	var b strings.Builder
	b.WriteString("Available tools (schemas omitted to conserve context; call ")
	b.WriteString(toolSearchToolName)
	b.WriteString(" with a query to get the full schema before using one):\n")
	for _, schema := range schemas {
		function, _ := schema["function"].(map[string]any)
		name := stringValue(function["name"])
		if name == "" {
			continue
		}
		b.WriteString("- ")
		b.WriteString(name)
		if summary := firstSentence(stringValue(function["description"])); summary != "" {
			b.WriteString(": ")
			b.WriteString(summary)
		}
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// firstSentence keeps a catalogue entry to one line.
func firstSentence(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if index := strings.IndexAny(text, ".\n"); index > 0 {
		text = text[:index]
	}
	const maxRunes = 120
	if runes := []rune(text); len(runes) > maxRunes {
		text = string(runes[:maxRunes]) + "…"
	}
	return strings.TrimSpace(text)
}

// toolSchemasFor returns the tool schemas a model call should carry, plus an
// optional catalogue note to inject in their place.
func (b *ContextBuilder) toolSchemasFor(auth AuthContext) ([]map[string]any, string) {
	if b == nil || b.Tools == nil {
		return []map[string]any{}, ""
	}
	full := b.Tools.SchemasForAuth(nil, auth)
	if b.ToolCatalogThreshold <= 0 || len(full) <= b.ToolCatalogThreshold {
		return full, ""
	}
	// Deferred: keep only the resolver's own schema, so the model can always
	// reach the rest.
	deferred := make([]map[string]any, 0, 1)
	for _, schema := range full {
		function, _ := schema["function"].(map[string]any)
		if stringValue(function["name"]) == toolSearchToolName {
			deferred = append(deferred, schema)
		}
	}
	if len(deferred) == 0 {
		// Without the resolver the catalogue would be a list of names the model
		// cannot act on, so deferral is refused rather than left half-applied.
		return full, ""
	}
	return deferred, renderToolCatalog(full)
}

// compile-time assertion that the search tool is an ordinary tool.
var _ ToolHandler = NewToolSearchTool(nil).Handler
