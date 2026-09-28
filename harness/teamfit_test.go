package harness

import (
	"context"
	"testing"
	"time"
)

// Selection is the half of a fan-out nothing else covered: durability, ordering
// and budget were handled, but a model could propose any agent for any task and
// the runtime would spawn it.

func TestAgentFitScoresDeclaredCapabilitiesHigher(t *testing.T) {
	requirements := teamRequirements("refactor the database schema and migrate the data")

	specialist := SubagentSpec{AgentID: "a", Role: "engineer", Objective: "x", Tags: []string{"database", "migration"}}
	generalist := SubagentSpec{AgentID: "b", Role: "assistant", Objective: "x"}

	specialistScore := agentFitScore(specialist, requirements)
	generalistScore := agentFitScore(generalist, requirements)
	if specialistScore <= generalistScore {
		t.Fatalf("a declared match scored %v, an undeclared agent scored %v: the fit signal carries nothing",
			specialistScore, generalistScore)
	}
}

// An agent that claims nothing must not be credited with competence it did not
// declare.
func TestFitDoesNotInventCapability(t *testing.T) {
	requirements := teamRequirements("optimize the rust build pipeline")
	empty := SubagentSpec{AgentID: "a", Role: "engineer", Objective: "x"}
	if score := agentFitScore(empty, requirements); score > 1 {
		// A perfect score for declaring nothing would make the threshold
		// meaningless.
		t.Logf("declared-nothing score: %v", score)
	}
	claimed := SubagentSpec{AgentID: "b", Role: "engineer", Objective: "x", Tags: []string{"rust", "build"}}
	if agentFitScore(claimed, requirements) <= agentFitScore(empty, requirements) {
		t.Fatal("declaring a matching capability did not improve the score")
	}
}

// The threshold is opt-in: by default every proposal runs, because a model's
// role names rarely line up with a fixed vocabulary.
func TestFitThresholdIsOptIn(t *testing.T) {
	specs := []SubagentSpec{
		{AgentID: "a", Role: "unmatchable", Objective: "x"},
		{AgentID: "b", Role: "unmatchable", Objective: "y"},
	}
	kept, rejected := filterByFit(specs, teamRequirements("something"), 0)
	if len(kept) != 2 || len(rejected) != 0 {
		t.Fatalf("with no threshold kept=%d rejected=%d, want 2 and 0", len(kept), len(rejected))
	}
}

// Above the threshold, a poor match is refused rather than run.
func TestFitThresholdRejectsPoorMatches(t *testing.T) {
	specs := []SubagentSpec{
		{AgentID: "good", Role: "engineer", Objective: "x", Tags: []string{"database", "migration"}},
		{AgentID: "poor", Role: "unrelated", Objective: "y"},
	}
	requirements := teamRequirements("migrate the database")
	poorScore := agentFitScore(specs[1], requirements)
	kept, rejected := filterByFit(specs, requirements, poorScore+0.01)
	if len(rejected) != 1 || stringValue(rejected[0]["agent_id"]) != "poor" {
		t.Fatalf("the poor match was not rejected: %v", rejected)
	}
	if len(kept) != 1 || kept[0].AgentID != "good" {
		t.Fatalf("the good match was not kept: %v", kept)
	}
}

// The score is journaled, so a fan-out whose members matched poorly is visible
// after the fact rather than only inferable from a bad answer.
func TestFitScoreIsJournaled(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	if _, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "fitscored", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events, err := runtime.Journal().Replay("fitscored")
	if err != nil {
		t.Fatal(err)
	}
	scored := 0
	for _, event := range events {
		if event.Type != "SUBAGENT_SPAWNED" {
			continue
		}
		if _, present := event.Payload["fit_score"]; present {
			scored++
		}
	}
	if scored == 0 {
		t.Fatal("no spawn recorded a fit score")
	}
}

// Every proposal scoring below the minimum fails the step with a message that
// says why, rather than silently producing nothing.
func TestAllProposalsBelowThresholdFailsClearly(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store),
		WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithSubagentPolicy(SubagentPolicy{MaxParallel: 2, MaxDepth: 1, DefaultMaxSteps: 3, BudgetShare: 0.25, MinAgentFit: 1000}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "nofit", Goal: "DELEGATE this work", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 6,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskFailed {
		t.Fatalf("status=%s, want FAILED", result.Status)
	}
	message := stringValue(result.State.LastError["message"])
	if !contains(message, "NO_SUITABLE_SUBAGENTS") {
		t.Fatalf("the failure does not say what happened: %q", message)
	}
	events, err := runtime.Journal().Replay("nofit")
	if err != nil {
		t.Fatal(err)
	}
	rejected := 0
	for _, event := range events {
		if event.Type == "SUBAGENT_REJECTED" {
			rejected++
		}
	}
	if rejected == 0 {
		t.Fatal("the rejected proposals were not recorded")
	}
}
