package multiagent

import (
	"sort"
	"strings"
)

// domainKeywords maps vocabulary to domains for requirement analysis.
var domainKeywords = map[string]string{
	"python":     "python",
	"golang":     "go",
	"go":         "go",
	"rust":       "rust",
	"database":   "database",
	"db":         "database",
	"sql":        "database",
	"postgres":   "database",
	"frontend":   "frontend",
	"react":      "frontend",
	"css":        "frontend",
	"html":       "frontend",
	"api":        "backend",
	"grpc":       "backend",
	"auth":       "backend",
	"security":   "security",
	"test":       "testing",
	"tests":      "testing",
	"deploy":     "devops",
	"k8s":        "devops",
	"docker":     "devops",
	"ml":         "ml",
	"model":      "ml",
}

// taskTypeKeywords classifies the task type.
var taskTypeKeywords = map[string][]string{
	"research":   {"research", "investigate", "analyze", "find", "look", "compare", "survey"},
	"implement":  {"implement", "build", "write", "create", "add", "develop", "code", "refactor"},
	"review":     {"review", "audit", "check", "verify", "inspect"},
	"test":       {"test", "verify", "validate"},
	"document":   {"document", "explain", "summarize", "write docs"},
}

var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "is": true, "are": true, "to": true,
	"of": true, "in": true, "for": true, "and": true, "or": true, "on": true,
	"with": true, "that": true, "this": true, "please": true, "me": true, "i": true,
}

// tokenize splits lowercased text into alphanumeric words.
func tokenize(text string) []string {
	text = strings.ToLower(text)
	var words []string
	var sb strings.Builder
	flush := func() {
		if sb.Len() > 0 {
			words = append(words, sb.String())
			sb.Reset()
		}
	}
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return words
}

// analyzeQueryRequirements deterministically classifies a task query.
func analyzeQueryRequirements(query string) QueryRequirements {
	words := tokenize(query)
	seen := make(map[string]bool)
	var keywords []string
	for _, w := range words {
		if len(w) < 2 || stopWords[w] || seen[w] {
			continue
		}
		seen[w] = true
		keywords = append(keywords, w)
	}

	// Required domains.
	domainSet := make(map[string]bool)
	for _, w := range words {
		if d, ok := domainKeywords[w]; ok {
			domainSet[d] = true
		}
	}
	domains := make([]string, 0, len(domainSet))
	for d := range domainSet {
		domains = append(domains, d)
	}
	sort.Strings(domains)

	// Task type: first keyword match wins, deterministic order of the map.
	taskType := "general"
	for _, w := range words {
		matched := false
		for tt, kws := range taskTypeKeywords {
			for _, kw := range kws {
				if w == kw {
					taskType = tt
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if matched {
			break
		}
	}

	// Complexity: 1..5 from length and diversity.
	complexity := 1 + minInt(len(domains), 2)
	if len(words) > 12 {
		complexity++
	}
	if complexity > 5 {
		complexity = 5
	}

	// Urgency signals.
	urgent := false
	for _, w := range words {
		if w == "urgent" || w == "asap" || w == "critical" || w == "immediately" {
			urgent = true
			break
		}
	}

	return QueryRequirements{
		TaskType:        taskType,
		RequiredDomains: domains,
		Complexity:      complexity,
		Urgent:          urgent,
		Keywords:        keywords,
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// selectBestAgents scores the pool against the requirements and selects the
// top agents that add capability coverage, capped at DefaultTeamSize.
// Ties break deterministically by agent ID.
func (o *TeamOrchestrator) selectBestAgents(req QueryRequirements) []*Agent {
	o.mu.Lock()
	type scored struct {
		agent *Agent
		score float64
	}
	var candidates []scored
	for id, a := range o.agents {
		_ = id
		score := computeAgentFitScore(a, req)
		if score >= o.config.MinFitScore {
			candidates = append(candidates, scored{agent: a, score: score})
		}
	}
	o.mu.Unlock()

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].agent.ID < candidates[j].agent.ID
	})

	var selected []*Agent
	coveredCaps := make(map[string]bool)
	coveredDomains := make(map[string]bool)

	addAgent := func(a *Agent) {
		selected = append(selected, a)
		for _, cap := range a.Capabilities {
			coveredCaps[cap.Name] = true
		}
		if a.Domain != "" {
			coveredDomains[a.Domain] = true
		}
	}
	addsCoverage := func(a *Agent) bool {
		for _, cap := range a.Capabilities {
			if !coveredCaps[cap.Name] {
				return true
			}
		}
		for _, d := range req.RequiredDomains {
			if a.Domain == d && !coveredDomains[d] {
				return true
			}
		}
		return false
	}

	// First pass: greedily take agents that add new coverage, in score order.
	for _, c := range candidates {
		if len(selected) >= o.config.DefaultTeamSize {
			break
		}
		if len(selected) == 0 || addsCoverage(c.agent) {
			addAgent(c.agent)
		}
	}
	// Second pass: fill remaining slots with the next-best agents regardless
	// of coverage so the team reaches its target size.
	for _, c := range candidates {
		if len(selected) >= o.config.DefaultTeamSize {
			break
		}
		if !containsAgent(selected, c.agent) {
			addAgent(c.agent)
		}
	}
	return selected
}

func containsAgent(list []*Agent, a *Agent) bool {
	for _, x := range list {
		if x.ID == a.ID {
			return true
		}
	}
	return false
}

// computeAgentFitScore blends role suitability, domain match, capability
// strength, and reliability into a 0..1 score.
func computeAgentFitScore(a *Agent, req QueryRequirements) float64 {
	score := 0.0

	// Role suitability for the task type (0.4 weight).
	roleScore := 0.0
	switch req.TaskType {
	case "research":
		roleScore = roleScoreFor(a.Role, RoleResearcher)
	case "implement":
		roleScore = roleScoreFor(a.Role, RoleCoder)
	case "review":
		roleScore = roleScoreFor(a.Role, RoleReviewer)
	case "test":
		roleScore = roleScoreFor(a.Role, RoleReviewer)
	case "document":
		roleScore = roleScoreFor(a.Role, RoleResearcher)
	default:
		roleScore = 0.5 // general task: any role is fine
	}
	score += 0.4 * roleScore

	// Domain match (0.3 weight).
	domainScore := 0.0
	if len(req.RequiredDomains) > 0 {
		for _, d := range req.RequiredDomains {
			if a.Domain == d {
				domainScore = 1.0
				break
			}
		}
	} else {
		domainScore = 0.5 // no specific domain required
	}
	score += 0.3 * domainScore

	// Capability strength (0.2 weight): average verified capability level /5.
	verified := 0
	sum := 0
	for _, c := range a.Capabilities {
		if c.Verified {
			verified++
			sum += c.Level
		}
	}
	var capScore float64
	if verified > 0 {
		capScore = float64(sum) / (5.0 * float64(verified))
	} else {
		capScore = 0.3
	}
	score += 0.2 * capScore

	// Reliability (0.1 weight): success rate if measured, else neutral 0.8.
	relScore := 0.8
	if n := a.TotalExecutions(); n > 0 {
		relScore = a.SuccessRate()
	}
	score += 0.1 * relScore

	return score
}

func roleScoreFor(actual, desired Role) float64 {
	switch actual {
	case desired:
		return 1.0
	case RoleGeneralist:
		return 0.7
	default:
		return 0.3
	}
}

// AnalyzeQueryRequirements is the exported form of query analysis.
func AnalyzeQueryRequirements(query string) QueryRequirements {
	return analyzeQueryRequirements(query)
}

// ComputeAgentFitScore is the exported form of agent fit scoring.
func ComputeAgentFitScore(agent *Agent, req QueryRequirements) float64 {
	return computeAgentFitScore(agent, req)
}

// SelectBestAgents is the exported form of team selection.
func (o *TeamOrchestrator) SelectBestAgents(req QueryRequirements) []*Agent {
	return o.selectBestAgents(req)
}
