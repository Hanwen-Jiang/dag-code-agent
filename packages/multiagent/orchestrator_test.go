package multiagent

import (
	"context"
	"strings"
	"testing"
)

// makeAgent builds an agent with a scripted LLM.
func makeAgent(id string, role Role, domain string, level int, llm *ScriptedLLM) *Agent {
	return &Agent{
		ID:           id,
		Role:         role,
		Domain:       domain,
		Capabilities: []Capability{{Name: id + "-cap", Level: level, Verified: true}},
		LLM:          llm,
	}
}

func registerPool(t *testing.T, o *TeamOrchestrator, agents ...*Agent) {
	t.Helper()
	for _, a := range agents {
		if err := o.RegisterAgent(a); err != nil {
			t.Fatalf("RegisterAgent(%s): %v", a.ID, err)
		}
	}
}

func TestRegistrationValidation(t *testing.T) {
	o := NewTeamOrchestrator(DefaultTeamConfig())
	llm := NewScriptedLLM("x").When("t", "ok")

	if err := o.RegisterAgent(&Agent{ID: "a", Role: RoleCoder, LLM: llm}); err != nil {
		t.Fatalf("valid agent rejected: %v", err)
	}
	if err := o.RegisterAgent(&Agent{ID: "a", Role: RoleCoder, LLM: llm}); err == nil {
		t.Fatal("duplicate agent accepted")
	}
	if err := o.RegisterAgent(&Agent{ID: "b", LLM: llm}); err == nil {
		t.Fatal("agent without role accepted")
	}
	if err := o.RegisterAgent(&Agent{ID: "", Role: RoleCoder, LLM: llm}); err == nil {
		t.Fatal("agent without ID accepted")
	}

	// Agent without its own LLM fails when no default is configured.
	if err := o.RegisterAgent(&Agent{ID: "c", Role: RoleCoder}); err == nil {
		t.Fatal("agent without LLM accepted despite no default LLM")
	}

	// With a default LLM configured, a bare agent is fine.
	o2 := NewTeamOrchestrator(DefaultTeamConfig())
	o2.config.DefaultLLM = NewScriptedLLM("default").When("t", "ok")
	if err := o2.RegisterAgent(&Agent{ID: "c", Role: RoleCoder}); err != nil {
		t.Fatalf("bare agent with default LLM rejected: %v", err)
	}
}

func TestTeamFormationDeterministicSelection(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = SequentialStrategy
	cfg.DefaultTeamSize = 3
	o := NewTeamOrchestrator(cfg)

	// Distinct capabilities so coverage-based selection is unambiguous.
	res := makeAgent("res-1", RoleResearcher, "go", 4, NewScriptedLLM("res").When("task", "RESEARCH: findings"))
	rev := makeAgent("rev-1", RoleReviewer, "go", 4, NewScriptedLLM("rev").When("task", "REVIEW: looks good"))
	cod := makeAgent("cod-1", RoleCoder, "go", 4, NewScriptedLLM("cod").When("task", "IMPL: cache done"))
	registerPool(t, o, res, rev, cod)

	team, err := o.FormTeam(context.Background(), "implement a go cache")
	if err != nil {
		t.Fatalf("FormTeam: %v", err)
	}
	// Task type "implement" => coder scores highest; the tied
	// researcher/reviewer break by ID (res-1 < rev-1).
	wantOrder := []string{"cod-1", "res-1", "rev-1"}
	if len(team.AssignedAgents) != 3 {
		t.Fatalf("expected 3 agents, got %d", len(team.AssignedAgents))
	}
	for i, a := range team.AssignedAgents {
		if a.ID != wantOrder[i] {
			t.Fatalf("agent %d = %s, want %s", i, a.ID, wantOrder[i])
		}
	}
	if team.Status != TeamPending {
		t.Fatalf("team status = %s, want pending", team.Status)
	}
}

func TestSequentialExecution(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = SequentialStrategy
	o := NewTeamOrchestrator(cfg)
	registerPool(t, o,
		makeAgent("res-1", RoleResearcher, "go", 4, NewScriptedLLM("res").When("task", "RESEARCH: findings")),
		makeAgent("rev-1", RoleReviewer, "go", 4, NewScriptedLLM("rev").When("task", "REVIEW: looks good")),
		makeAgent("cod-1", RoleCoder, "go", 4, NewScriptedLLM("cod").When("task", "IMPL: cache done")),
	)

	result, err := o.Execute(context.Background(), "implement a go cache")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != TeamSuccess {
		t.Fatalf("status = %s", result.Status)
	}
	// Execution follows selection order cod-1 -> res-1 -> rev-1, so the
	// final output is the last agent's.
	if result.Output != "REVIEW: looks good" {
		t.Fatalf("output = %q, want last agent's output", result.Output)
	}
	if len(result.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(result.Steps))
	}
	// Agent stats recorded.
	if a, ok := o.Agent("cod-1"); !ok || a.TotalExecutions() != 1 || a.SuccessRate() != 1.0 {
		t.Fatalf("agent stats not recorded: %+v", a)
	}
	if len(result.Conflicts) != 0 {
		t.Fatalf("unexpected conflicts: %+v", result.Conflicts)
	}
}

func TestPipelineExecution(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = PipelineStrategy
	o := NewTeamOrchestrator(cfg)
	registerPool(t, o,
		makeAgent("res-1", RoleResearcher, "go", 4, NewScriptedLLM("res").When("task", "S0: research")),
		makeAgent("cod-1", RoleCoder, "go", 4, NewScriptedLLM("cod").When("task", "S1: code")),
		makeAgent("rev-1", RoleReviewer, "go", 4, NewScriptedLLM("rev").When("task", "S2: review")),
	)

	result, err := o.Execute(context.Background(), "implement a go service")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	want := "S0: research\n---\nS1: code\n---\nS2: review"
	if result.Output != want {
		t.Fatalf("output = %q, want %q", result.Output, want)
	}
	// Stages follow the role order: researcher, coder, reviewer.
	wantOrder := []string{"res-1", "cod-1", "rev-1"}
	if len(result.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(result.Steps))
	}
	for i, step := range result.Steps {
		if step.AgentID != wantOrder[i] {
			t.Fatalf("step %d = %s, want %s", i, step.AgentID, wantOrder[i])
		}
	}
}

func TestMasterWorkerExecution(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = MasterWorkerStrategy
	o := NewTeamOrchestrator(cfg)

	mgr := &Agent{ID: "a-mgr", Role: RoleManager,
		Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}},
		LLM: NewScriptedLLM("mgr").When("worker_outputs", "SUMMARY: alpha+beta")}
	w1 := &Agent{ID: "a-w1", Role: RoleCoder,
		Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}},
		LLM: NewScriptedLLM("w1").When("task", "frag alpha")}
	w2 := &Agent{ID: "a-w2", Role: RoleCoder,
		Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}},
		LLM: NewScriptedLLM("w2").When("task", "frag beta")}
	registerPool(t, o, mgr, w1, w2)

	result, err := o.Execute(context.Background(), "general task")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output != "SUMMARY: alpha+beta" {
		t.Fatalf("output = %q, want master aggregation", result.Output)
	}
	if len(result.Steps) != 3 {
		t.Fatalf("steps = %d, want 3 (2 workers + master)", len(result.Steps))
	}
	// Master runs last.
	if result.Steps[2].AgentID != "a-mgr" {
		t.Fatalf("master should be last step, got %s", result.Steps[2].AgentID)
	}
}

func TestConsensusMajority(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = ConsensusStrategy
	o := NewTeamOrchestrator(cfg)
	registerPool(t, o,
		&Agent{ID: "a1", Role: RoleGeneralist, Capabilities: []Capability{{Name: "c", Level: 3, Verified: true}}, LLM: NewScriptedLLM("a1").When("task", "yes")},
		&Agent{ID: "a2", Role: RoleGeneralist, Capabilities: []Capability{{Name: "c", Level: 3, Verified: true}}, LLM: NewScriptedLLM("a2").When("task", "YES")},
		&Agent{ID: "a3", Role: RoleGeneralist, Capabilities: []Capability{{Name: "c", Level: 3, Verified: true}}, LLM: NewScriptedLLM("a3").When("task", "no")},
	)

	result, err := o.Execute(context.Background(), "should we ship it")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output != "yes" {
		t.Fatalf("output = %q, want majority vote 'yes'", result.Output)
	}
	for _, c := range result.Conflicts {
		if c.Type == "no_majority" {
			t.Fatalf("unexpected no_majority conflict: %+v", c)
		}
	}
}

func TestConsensusNoMajority(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = ConsensusStrategy
	o := NewTeamOrchestrator(cfg)
	registerPool(t, o,
		&Agent{ID: "a1", Role: RoleGeneralist, Capabilities: []Capability{{Name: "c", Level: 3, Verified: true}}, LLM: NewScriptedLLM("a1").When("task", "yes")},
		&Agent{ID: "a2", Role: RoleGeneralist, Capabilities: []Capability{{Name: "c", Level: 3, Verified: true}}, LLM: NewScriptedLLM("a2").When("task", "no")},
	)

	result, err := o.Execute(context.Background(), "a 50/50 call")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	found := false
	for _, c := range result.Conflicts {
		if c.Type == "no_majority" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected no_majority conflict, got %+v", result.Conflicts)
	}
	// Deterministic fallback: first agent by ID order ("a1").
	if result.Output != "yes" {
		t.Fatalf("fallback output = %q, want first agent's output", result.Output)
	}
}

func TestConflictDetectionContradiction(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = MasterWorkerStrategy
	o := NewTeamOrchestrator(cfg)
	registerPool(t, o,
		&Agent{ID: "a-mgr", Role: RoleManager, Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}}, LLM: NewScriptedLLM("mgr").When("worker_outputs", "AGG")},
		&Agent{ID: "a-w1", Role: RoleCoder, Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}}, LLM: NewScriptedLLM("w1").When("task", "CONCLUSION: use redis")},
		&Agent{ID: "a-w2", Role: RoleCoder, Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}}, LLM: NewScriptedLLM("w2").When("task", "CONCLUSION: use memcached")},
	)

	result, err := o.Execute(context.Background(), "choose a cache")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	found := false
	for _, c := range result.Conflicts {
		if c.Type == "contradiction" {
			found = true
			if len(c.Agents) != 2 || c.Agents[0] != "a-w1" || c.Agents[1] != "a-w2" {
				t.Fatalf("contradiction agents = %v", c.Agents)
			}
		}
	}
	if !found {
		t.Fatalf("expected contradiction conflict, got %+v", result.Conflicts)
	}
}

func TestConflictDetectionDuplicate(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = MasterWorkerStrategy
	o := NewTeamOrchestrator(cfg)
	registerPool(t, o,
		&Agent{ID: "a-mgr", Role: RoleManager, Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}}, LLM: NewScriptedLLM("mgr").When("worker_outputs", "AGG")},
		&Agent{ID: "a-w1", Role: RoleCoder, Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}}, LLM: NewScriptedLLM("w1").When("task", "SAME OUTPUT")},
		&Agent{ID: "a-w2", Role: RoleCoder, Capabilities: []Capability{{Name: "m", Level: 3, Verified: true}}, LLM: NewScriptedLLM("w2").When("task", "SAME OUTPUT")},
	)

	result, err := o.Execute(context.Background(), "redundant task")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	found := false
	for _, c := range result.Conflicts {
		if c.Type == "duplicate" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected duplicate conflict, got %+v", result.Conflicts)
	}
}

func TestAgentFailurePropagates(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = SequentialStrategy
	o := NewTeamOrchestrator(cfg)
	bad := makeAgent("bad-1", RoleCoder, "go", 4, NewScriptedLLM("bad").FailOn("boom"))
	good := makeAgent("good-1", RoleReviewer, "go", 4, NewScriptedLLM("good").When("task", "ok"))
	registerPool(t, o, bad, good)

	result, err := o.Execute(context.Background(), "boom build")
	if err == nil {
		t.Fatal("expected error from failing agent")
	}
	// The team must be marked failed and the result reflects the failure.
	if result == nil || result.Status != TeamFailure {
		t.Fatalf("result status = %+v, want failure", result)
	}
	if team, ok := o.Team(result.TeamID); !ok || team.Status != TeamFailure {
		t.Fatalf("team %s status = %s, want failure", result.TeamID, team.Status)
	}
}

func TestFallbackToSingleAgent(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.CoordinationStrategy = SequentialStrategy
	cfg.MinFitScore = 0.99 // nobody reaches this
	cfg.FallbackToSingle = true
	o := NewTeamOrchestrator(cfg)
	llm := NewScriptedLLM("g").When("task", "generalist output")
	registerPool(t, o, makeAgent("gen-1", RoleGeneralist, "", 5, llm))

	result, err := o.Execute(context.Background(), "mysterious task")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Output != "generalist output" {
		t.Fatalf("output = %q", result.Output)
	}
	team, _ := o.Team(result.TeamID)
	if len(team.AssignedAgents) != 1 || team.AssignedAgents[0].ID != "gen-1" {
		t.Fatalf("fallback agent not used: %+v", team.AssignedAgents)
	}
}

func TestTeamSizeCap(t *testing.T) {
	cfg := DefaultTeamConfig()
	cfg.DefaultTeamSize = 2
	o := NewTeamOrchestrator(cfg)
	for i, id := range []string{"a", "b", "c"} {
		_ = i
		llm := NewScriptedLLM("x")
		a := &Agent{ID: id, Role: RoleGeneralist,
			Capabilities: []Capability{{Name: id + "cap", Level: 3, Verified: true}}, LLM: llm}
		if err := o.RegisterAgent(a); err != nil {
			t.Fatal(err)
		}
	}
	team, err := o.FormTeam(context.Background(), "do three things")
	if err != nil {
		t.Fatal(err)
	}
	if len(team.AssignedAgents) != 2 {
		t.Fatalf("team size = %d, want 2", len(team.AssignedAgents))
	}
}

func TestScriptedLLMDeterministic(t *testing.T) {
	llm := NewScriptedLLM("s").When("alpha", "ONE")
	if out, err := llm.Complete(context.Background(), "hello ALPHA world"); err != nil || out != "ONE" {
		t.Fatalf("matched case: %q %v", out, err)
	}
	// No matcher matches => deterministic echo of the prompt's last line.
	if out, err := llm.Complete(context.Background(), "something else\nlast line"); err != nil || out != "last line" {
		t.Fatalf("default echo: %q %v", out, err)
	}
	if llm.CallCount() != 2 {
		t.Fatalf("call count = %d", llm.CallCount())
	}
	if !strings.Contains(strings.Join(llm.Prompts(), "|"), "hello ALPHA world") {
		t.Fatalf("prompt log missing: %v", llm.Prompts())
	}

	// An empty matcher acts as a catch-all.
	catchAll := NewScriptedLLM("t").When("", "DEFAULT")
	if out, err := catchAll.Complete(context.Background(), "anything at all"); err != nil || out != "DEFAULT" {
		t.Fatalf("catch-all: %q %v", out, err)
	}

	// Simulated failure.
	failing := NewScriptedLLM("f").FailOn("boom")
	if _, err := failing.Complete(context.Background(), "here be boom"); err == nil {
		t.Fatal("expected simulated failure")
	}
}
