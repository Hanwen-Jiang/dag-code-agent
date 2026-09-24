package harness

import (
	"sort"
	"strings"
)

type SkillRegistry struct{ skills map[string][]SkillManifest }

func NewSkillRegistry() *SkillRegistry { return &SkillRegistry{skills: map[string][]SkillManifest{}} }
func (r *SkillRegistry) Register(skill SkillManifest) {
	if skill.Status == "" {
		skill.Status = "ACTIVE"
	}
	r.skills[skill.SkillID] = append(r.skills[skill.SkillID], skill)
}
func versionParts(v string) []int {
	parts := []int{}
	for _, piece := range strings.FieldsFunc(v, func(r rune) bool { return r == '.' || r == '-' || r == '_' }) {
		n := 0
		for _, c := range piece {
			if c < '0' || c > '9' {
				break
			}
			n = n*10 + int(c-'0')
		}
		parts = append(parts, n)
	}
	return parts
}
func compareVersion(a, b string) int {
	pa, pb := versionParts(a), versionParts(b)
	for i := 0; i < len(pa) || i < len(pb); i++ {
		av, bv := 0, 0
		if i < len(pa) {
			av = pa[i]
		}
		if i < len(pb) {
			bv = pb[i]
		}
		if av > bv {
			return 1
		}
		if av < bv {
			return -1
		}
	}
	return strings.Compare(a, b)
}
func (r *SkillRegistry) Activate(id string) (SkillManifest, bool) {
	items := append([]SkillManifest(nil), r.skills[id]...)
	sort.Slice(items, func(i, j int) bool { return compareVersion(items[i].Version, items[j].Version) > 0 })
	for _, s := range items {
		if s.Status == "ACTIVE" {
			return s, true
		}
	}
	return SkillManifest{}, false
}
func (r *SkillRegistry) Deprecate(id, version string) bool {
	items := r.skills[id]
	for i := range items {
		if items[i].Version == version {
			items[i].Status = "DEPRECATED"
			r.skills[id] = items
			return true
		}
	}
	return false
}
func (r *SkillRegistry) Resolve(goal string, auth AuthContext) []SkillManifest {
	result := []SkillManifest{}
	for id := range r.skills {
		skill, ok := r.Activate(id)
		if !ok {
			continue
		}
		document := strings.Join(append([]string{skill.SkillID, skill.Title}, skill.Tags...), " ")
		relevant := strings.TrimSpace(goal) == "" || lexicalOverlap(goal, document) > 0
		if !relevant {
			continue
		}
		allowed := true
		for _, scope := range skill.RequiredScopes {
			if !auth.HasScope(scope) {
				allowed = false
				break
			}
		}
		if allowed {
			result = append(result, skill)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SkillID < result[j].SkillID })
	return result
}
