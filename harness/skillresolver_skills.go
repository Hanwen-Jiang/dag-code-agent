package harness

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/Hanwen-Jiang/dag-code-agent/packages/skills"
)

// The only file in the core that names packages/skills.
//
// packages/skills has no third-party dependencies, which is what makes a direct
// import acceptable here: the core's dependency budget (guard G7) counts
// third-party imports and this adds none. It is isolated to one file so the
// choice is visible and reversible.

// ManagedSkillResolver resolves skills through the managed lifecycle in
// packages/skills: mined patterns with versions, status and success metrics,
// selected against hard constraints.
//
// The registry-based resolver answers "which skill's text overlaps the goal";
// this answers "which proven pattern best fits the goal and the budget", which
// is the question that actually matters once skills are learned rather than
// hand-written.
type ManagedSkillResolver struct {
	mu      sync.Mutex
	manager *skills.SkillManager
	store   skills.SkillStore
	loaded  bool
	loadErr error
}

// NewManagedSkillResolver builds a resolver over an in-memory skill store.
//
// In-memory is the right default for an embeddable control plane: the caller
// owns persistence, and a resolver that silently wrote to disk would violate
// the rule that library packages do not touch the filesystem. Provide a store
// through NewManagedSkillResolverWithStore to persist patterns.
func NewManagedSkillResolver() *ManagedSkillResolver {
	return NewManagedSkillResolverWithStore(skills.NewMemoryStore())
}

// NewManagedSkillResolverWithStore builds a resolver over a caller-supplied store.
func NewManagedSkillResolverWithStore(store skills.SkillStore) *ManagedSkillResolver {
	config := skills.DefaultRecognizerConfig()
	// Mined patterns go to review rather than straight into the active pool:
	// a learned skill changing production behaviour without a human decision is
	// exactly the kind of silent drift the rest of this runtime avoids.
	config.RequireHumanApproval = true
	return &ManagedSkillResolver{manager: skills.NewSkillManager(store, config), store: store}
}

// Manager exposes the underlying manager, so a host can mine, approve and export
// patterns. It is intentionally not used by the hot path.
func (r *ManagedSkillResolver) Manager() *skills.SkillManager { return r.manager }

// Register makes a hand-authored pattern available, which is how a caller seeds
// the resolver before any learning has happened.
//
// It writes to the store rather than to a recognizer: patterns are selected out
// of storage, so persisting one is exactly what makes it selectable. The next
// Resolve reloads and sees it.
func (r *ManagedSkillResolver) Register(manifest SkillManifest) error {
	if r == nil || r.store == nil {
		return errNilSkillResolver
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.store.Save(managedPatternFromManifest(manifest)); err != nil {
		return err
	}
	// Force a reload so the new pattern participates in the next selection.
	r.loaded = false
	return nil
}

// Resolve selects at most one pattern for the goal.
//
// One, not many: the compiler injects every resolved skill's instructions into
// the prompt, so resolving a list would silently multiply the context budget.
// Selecting the single best fit is what the constraint-aware selector is for.
func (r *ManagedSkillResolver) Resolve(goal string, _ AuthContext) []SkillManifest {
	if r == nil || r.manager == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.loaded && r.loadErr == nil {
		// Patterns live in the store; the manager must read them before it can
		// select anything. A load failure is remembered and reported as "no
		// skill applies" rather than failing the run: context compilation must
		// not become a hard dependency on the skill store.
		if err := r.manager.LoadAllSkills(context.Background()); err != nil {
			r.loadErr = err
		} else {
			r.loaded = true
		}
	}
	pattern := r.manager.GetBestSkill(context.Background(), goal, skills.SkillConstraints{})
	if pattern == nil {
		return nil
	}
	manifest := manifestFromManagedPattern(pattern)
	if manifest.Instructions == "" {
		// A pattern with nothing to say would consume budget for no benefit.
		return nil
	}
	return []SkillManifest{manifest}
}

// managedPatternFromManifest converts a harness skill into a managed pattern.
func managedPatternFromManifest(manifest SkillManifest) *skills.PatternDefinition {
	status := skills.StatusActive
	if strings.EqualFold(manifest.Status, "DEPRECATED") {
		status = skills.StatusDeprecated
	}
	version := 1
	if parsed, err := strconv.Atoi(strings.TrimSpace(manifest.Version)); err == nil && parsed > 0 {
		version = parsed
	}
	return &skills.PatternDefinition{
		ID:          manifest.SkillID,
		Name:        manifest.Title,
		Description: manifest.Instructions,
		Status:      status,
		Version:     version,
	}
}

// manifestFromManagedPattern converts a selected pattern into the shape the
// context compiler injects.
func manifestFromManagedPattern(pattern *skills.PatternDefinition) SkillManifest {
	if pattern == nil {
		return SkillManifest{}
	}
	instructions := strings.TrimSpace(pattern.Description)
	// The execution plan is the actionable part of a mined pattern, so it is
	// appended rather than dropped: a skill that only describes itself gives the
	// model nothing to follow.
	if steps := describeManagedPlan(pattern.ExecutionPlan); steps != "" {
		if instructions != "" {
			instructions += "\n"
		}
		instructions += steps
	}
	return SkillManifest{
		SkillID:      pattern.ID,
		Version:      strconv.Itoa(pattern.Version),
		Title:        pattern.Name,
		Instructions: instructions,
		Tags:         append([]string{}, pattern.ContextRequirements...),
		Status:       string(pattern.Status),
	}
}

// describeManagedPlan renders a pattern's execution plan as prompt text.
func describeManagedPlan(plan []skills.SkillAction) string {
	if len(plan) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Execution plan:")
	for i, action := range plan {
		b.WriteString("\n")
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString(". ")
		description := strings.TrimSpace(action.Prompt)
		if description == "" {
			description = strings.TrimSpace(action.TargetTool)
		}
		if description == "" {
			description = strings.TrimSpace(action.ActionType)
		}
		if condition := strings.TrimSpace(action.Condition); condition != "" {
			description += " (when " + condition + ")"
		}
		if expectation := strings.TrimSpace(action.Expectation); expectation != "" {
			description += " -> " + expectation
		}
		b.WriteString(description)
	}
	return b.String()
}

// compile-time assertion
var _ SkillResolver = (*ManagedSkillResolver)(nil)
