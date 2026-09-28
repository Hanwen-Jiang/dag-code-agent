package harness

import (
	"github.com/Hanwen-Jiang/dag-code-agent/packages/multiagent"
)

// Team fit: who should do the work, as opposed to how it is run.
//
// Everything else in a fan-out concerns execution — durability, ordering,
// budget. Nothing concerned selection: a model could propose any agent for any
// task, the runtime spawned it, and a specialist matched to nothing spent real
// budget producing an answer nobody needed.
//
// packages/multiagent already scored that fit and reached no runtime path. It has
// no third-party dependencies, which is what allows the core to import it; the
// dependency is confined to this file behind a conversion, so replacing it, or
// removing it, is a local change.

// teamRequirements derives what the task needs from its goal.
func teamRequirements(goal string) multiagent.QueryRequirements {
	return multiagent.AnalyzeQueryRequirements(goal)
}

// agentFitScore scores how well a proposed subagent matches the task.
//
// The spec is the model's own proposal, so this is a check on untrusted input
// rather than a property the runtime controls: a role or tag the model invented
// simply scores poorly instead of being trusted.
func agentFitScore(spec SubagentSpec, requirements multiagent.QueryRequirements) float64 {
	agent := &multiagent.Agent{
		ID:           spec.AgentID,
		Role:         multiagent.Role(spec.Role),
		Domain:       primaryTag(spec.Tags),
		Capabilities: capabilitiesFromTags(spec.Tags),
	}
	return multiagent.ComputeAgentFitScore(agent, requirements)
}

// primaryTag picks the first tag as the agent's domain.
func primaryTag(tags []string) string {
	for _, tag := range tags {
		if tag != "" {
			return tag
		}
	}
	return ""
}

// capabilitiesFromTags turns a spec's tags into declared capabilities.
//
// Tags are what a model can express about a proposed agent, so they are the only
// signal available for capability matching. An agent that declares nothing
// simply matches nothing, which is the honest outcome: the score should not
// invent competence the proposal did not claim.
func capabilitiesFromTags(tags []string) []multiagent.Capability {
	if len(tags) == 0 {
		return nil
	}
	capabilities := make([]multiagent.Capability, 0, len(tags))
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		capabilities = append(capabilities, multiagent.Capability{Name: tag, Level: 3})
	}
	return capabilities
}

// filterByFit drops proposals whose match is too poor to be worth running.
//
// The threshold is opt-in: zero keeps every proposal, because a model's role
// names rarely line up with a fixed vocabulary and a default cut-off would
// silently discard work the embedder expected to run.
func filterByFit(specs []SubagentSpec, requirements multiagent.QueryRequirements, minimum float64) (kept []SubagentSpec, rejected []map[string]any) {
	kept = make([]SubagentSpec, 0, len(specs))
	for _, spec := range specs {
		score := agentFitScore(spec, requirements)
		if minimum > 0 && score < minimum {
			rejected = append(rejected, map[string]any{
				"agent_id": spec.AgentID, "role": spec.Role, "fit_score": score, "min_fit": minimum,
			})
			continue
		}
		kept = append(kept, spec)
	}
	return kept, rejected
}
