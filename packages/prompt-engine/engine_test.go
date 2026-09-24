package prompt_engine

import (
	"context"
	"strings"
	"testing"
)

func TestDeterministicRendering(t *testing.T) {
	e := NewEngine()
	tmpl := "Hello, {{ .Name }}! You have {{ .Count }} tasks ({{ upper .Status }})."
	if err := e.Register("greeter", "1.0.0", tmpl, nil); err != nil {
		t.Fatal(err)
	}
	data := map[string]any{"Name": "World", "Count": 3, "Status": "open"}

	ctx := context.Background()
	out1, err := e.Render(ctx, "greeter", data)
	if err != nil {
		t.Fatal(err)
	}
	want := "Hello, World! You have 3 tasks (OPEN)."
	if out1 != want {
		t.Fatalf("render = %q, want %q", out1, want)
	}

	// Same input renders identically (determinism).
	out2, err := e.Render(ctx, "greeter", data)
	if err != nil {
		t.Fatal(err)
	}
	if out1 != out2 {
		t.Fatalf("nondeterministic: %q vs %q", out1, out2)
	}
}

func TestSprigFunctions(t *testing.T) {
	e := NewEngine()
	// sprig's default + trimall + ternary prove the sprig function library
	// is wired into the engine.
	tmpl := `{{ default "fallback" .Missing }}|{{ trimall " " "  hi  " }}|{{ ternary "yes" "no" (gt .N 5) }}`
	if err := e.Register("sprigtest", "0.1.0", tmpl, nil); err != nil {
		t.Fatal(err)
	}
	out, err := e.Render(context.Background(), "sprigtest", map[string]any{"N": 10})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(out, "fallback") {
		t.Fatalf("sprig default not applied: %q", out)
	}
	if !strings.Contains(out, "hi") || strings.Contains(out, "  hi") {
		t.Fatalf("sprig trimall not applied: %q", out)
	}
	if !strings.Contains(out, "yes") {
		t.Fatalf("sprig ternary not applied: %q", out)
	}
}

func TestScopedConditionalSection(t *testing.T) {
	e := NewEngine()
	tmpl := `Base prompt.
{{- $note := scopedGet . "note" }}
{{- if $note }}
Note: {{ $note }}
{{- end }}`
	if err := e.Register("scoped", "1.0.0", tmpl, nil); err != nil {
		t.Fatal(err)
	}

	withNote, err := e.Render(context.Background(), "scoped", map[string]any{"note": "careful"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(withNote, "Note: careful") {
		t.Fatalf("scoped section missing: %q", withNote)
	}

	withoutNote, err := e.Render(context.Background(), "scoped", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(withoutNote, "Note:") {
		t.Fatalf("scoped section should be empty without the key: %q", withoutNote)
	}
}

func TestVersionResolutionSemver(t *testing.T) {
	m := NewPromptManager()
	if _, err := m.Create("prompt", "1.9.0", "v-nine", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create("prompt", "1.10.0", "v-ten", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create("prompt", "0.9.0", "v-old", nil); err != nil {
		t.Fatal(err)
	}

	// Semver comparison: 1.10.0 > 1.9.0 (lexicographic would be wrong).
	latest, err := m.GetLatest("prompt")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != "1.10.0" {
		t.Fatalf("latest = %s, want 1.10.0", latest.Version)
	}

	// Bare name resolves to latest at render time.
	out, err := m.Engine().Render(context.Background(), "prompt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != "v-ten" {
		t.Fatalf("bare name rendered %q, want latest v-ten", out)
	}

	// Explicit version reference.
	out, err = m.Engine().Render(context.Background(), "prompt@1.9.0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != "v-nine" {
		t.Fatalf("explicit version rendered %q", out)
	}

	// Duplicate version registration is rejected.
	if _, err := m.Create("prompt", "1.10.0", "dup", nil); err == nil {
		t.Fatal("duplicate version accepted")
	}

	// Missing template errors.
	if _, err := m.Engine().Render(context.Background(), "nope", nil); err == nil {
		t.Fatal("expected TEMPLATE_NOT_FOUND")
	}
}

func TestDiffVersions(t *testing.T) {
	m := NewPromptManager()
	if _, err := m.Create("doc", "1.0.0", "line one\nline two\nline three", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Create("doc", "1.1.0", "line one\nline two changed\nline four", nil); err != nil {
		t.Fatal(err)
	}
	diff, err := m.Diff("doc", "1.0.0", "1.1.0")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff.Added) != 2 {
		t.Fatalf("added = %v, want 2 lines", diff.Added)
	}
	if len(diff.Removed) != 2 {
		t.Fatalf("removed = %v, want 2 lines", diff.Removed)
	}
	found := false
	for _, l := range diff.Added {
		if l == "line two changed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("added lines missing 'line two changed': %v", diff.Added)
	}
}

func TestParseFailure(t *testing.T) {
	e := NewEngine()
	if err := e.Register("bad", "1.0.0", "{{ .Unbalanced", nil); err == nil {
		t.Fatal("expected parse failure for unbalanced template")
	}
}

func TestFewShotSelectorRanking(t *testing.T) {
	s := NewFewShotSelector(3)
	examples := []Example{
		{ID: "e-go-test", Input: "how to run go tests in a module"},
		{ID: "e-python", Input: "how to install python packages with pip"},
		{ID: "e-go-build", Input: "how to build a go binary"},
		{ID: "e-rust", Input: "how to compile rust code"},
	}

	ctx := context.Background()
	// Query shares the most vocabulary with the go-test example.
	sel, err := s.SelectSimilarExamples(ctx, "running go tests", examples, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(sel) != 2 {
		t.Fatalf("got %d examples, want 2", len(sel))
	}
	if sel[0].ID != "e-go-test" {
		t.Fatalf("top example = %s, want e-go-test", sel[0].ID)
	}
	// The go-build example should outrank the unrelated python/rust ones.
	if sel[1].ID != "e-go-build" {
		t.Fatalf("second example = %s, want e-go-build", sel[1].ID)
	}

	// Determinism: same inputs twice => identical ordering.
	sel2, err := s.SelectSimilarExamples(ctx, "running go tests", examples, 2)
	if err != nil {
		t.Fatal(err)
	}
	if sel[0].ID != sel2[0].ID || sel[1].ID != sel2[1].ID {
		t.Fatalf("nondeterministic selection")
	}

	// limit larger than the pool is clamped.
	all, err := s.SelectSimilarExamples(ctx, "go", examples, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != len(examples) {
		t.Fatalf("clamped = %d, want %d", len(all), len(examples))
	}
}

func TestFewShotEmpty(t *testing.T) {
	s := NewFewShotSelector(5)
	out, err := s.SelectSimilarExamples(context.Background(), "anything", nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty result, got %v", out)
	}
}
