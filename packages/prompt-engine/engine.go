// Package prompt_engine provides a deterministic template engine for prompt
// management with versioning. Templates are Go text/template with the full
// sprig function library plus a small "scoped" helper for variable scoping.
package prompt_engine

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
)

// Engine compiles and renders versioned prompt templates.
type Engine struct {
	templates map[string]*TemplateVersion // key: "name@version"
	index     map[string][]string         // name -> versions in insertion order
}

// NewEngine creates an empty engine.
func NewEngine() *Engine {
	return &Engine{
		templates: make(map[string]*TemplateVersion),
		index:     make(map[string][]string),
	}
}

// TemplateVersion represents a single version of a prompt template.
type TemplateVersion struct {
	Version   string
	Name      string
	Content   string
	Template  *template.Template
	Metadata  map[string]any
	CreatedAt time.Time
	Author    string
}

// funcMap returns the template function library: sprig plus scoped helpers.
func funcMap() template.FuncMap {
	fns := sprig.FuncMap()
	// scopedGet looks up a key in the data and returns its value, or nil when
	// the key is absent. Templates use it for conditional sections:
	//   {{ $note := scopedGet . "note" }}{{ if $note }}...{{ end }}
	fns["scopedGet"] = func(data map[string]any, key string) any {
		if data == nil {
			return nil
		}
		return data[key]
	}
	return fns
}

// Register adds a new template version after parsing.
func (e *Engine) Register(name, version, content string, metadata map[string]any) error {
	if name == "" {
		return &PromptError{Code: "INVALID_NAME", Message: "template name is required"}
	}
	if version == "" {
		return &PromptError{Code: "INVALID_VERSION", Message: "template version is required"}
	}

	tmpl, err := template.New(name).Funcs(funcMap()).Parse(content)
	if err != nil {
		return &PromptError{Code: "PARSE_FAILED", Message: "failed to parse template", Details: err.Error()}
	}

	key := name + "@" + version
	if _, exists := e.templates[key]; exists {
		return &PromptError{Code: "VERSION_EXISTS", Message: fmt.Sprintf("template %s@%s already registered", name, version)}
	}

	e.templates[key] = &TemplateVersion{
		Version:   version,
		Name:      name,
		Content:   content,
		Template:  tmpl,
		Metadata:  metadata,
		CreatedAt: time.Now(),
	}
	e.index[name] = append(e.index[name], version)
	return nil
}

// ResolveVersion returns the version string for a template reference:
// "name@version" resolves exactly; "name" resolves to the latest (semver).
func (e *Engine) ResolveVersion(ref string) (name, version string, err error) {
	at := indexOf(ref, '@')
	if at >= 0 {
		name = ref[:at]
		version = ref[at+1:]
		if version == "" {
			return "", "", &PromptError{Code: "BAD_REF", Message: "empty version in reference " + ref}
		}
		if _, ok := e.templates[name+"@"+version]; !ok {
			return "", "", &PromptError{Code: "TEMPLATE_NOT_FOUND", Message: "template " + ref + " not found"}
		}
		return name, version, nil
	}

	name = ref
	versions, ok := e.index[name]
	if !ok || len(versions) == 0 {
		return "", "", &PromptError{Code: "TEMPLATE_NOT_FOUND", Message: "template " + ref + " not found"}
	}
	best := versions[0]
	for _, v := range versions[1:] {
		if compareSemver(v, best) > 0 {
			best = v
		}
	}
	return name, best, nil
}

// Render fills in a template with data. The reference may be "name" (latest
// version) or "name@version". Rendering is deterministic for a given data
// value (no time/random functions are invoked by the engine itself).
func (e *Engine) Render(ctx context.Context, templateRef string, data any) (string, error) {
	name, version, err := e.ResolveVersion(templateRef)
	if err != nil {
		return "", err
	}
	v := e.templates[name+"@"+version]

	var buf bytes.Buffer
	if err := v.Template.Execute(&buf, data); err != nil {
		return "", &PromptError{Code: "EXECUTE_FAILED", Message: "failed to execute template", Details: err.Error()}
	}
	return buf.String(), nil
}

// ListVersions returns the registered versions of a template in insertion
// order.
func (e *Engine) ListVersions(name string) []string {
	return append([]string(nil), e.index[name]...)
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// compareSemver compares two semantic versions ("major.minor.patch", with an
// optional "v" prefix). Returns -1, 0, or 1. Non-numeric segments compare as
// 0; missing segments are treated as 0.
func compareSemver(a, b string) int {
	pa := splitVersion(a)
	pb := splitVersion(b)
	for i := 0; i < 3; i++ {
		if pa[i] != pb[i] {
			if pa[i] < pb[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func splitVersion(v string) [3]int {
	if v != "" && (v[0] == 'v' || v[0] == 'V') {
		v = v[1:]
	}
	out := [3]int{}
	part := 0
	for _, c := range v {
		if c >= '0' && c <= '9' {
			out[part] = out[part]*10 + int(c-'0')
			continue
		}
		if part < 2 {
			part++
		} else {
			// Stop at first non-digit beyond patch.
			break
		}
	}
	return out
}

// stableKeys returns sorted keys of a map (deterministic iteration).
func stableKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
