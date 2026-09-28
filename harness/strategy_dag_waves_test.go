package harness

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Evidence for a claim that was previously only asserted: a run-DAG action is
// not needed because a strategy can express a DAG as sequential spawn waves.
//
// The shape below is the smallest graph that distinguishes a DAG from a fan-out:
//
//	A
//	├── B
//	└── C
//	    └── D   (depends on B and C)
//
// A fan-out cannot express it, because B and C must not start until A finished and
// D must not start until both did. A strategy can, because the kernel returns to
// the strategy after each wave completes — which is the whole reason the loop is
// a strategy seam and not a fixed pipeline.
//
// This test is the evidence. If it stopped compiling or passing, the claim that
// ActionRunDAG is a convenience gap rather than a capability gap would be false,
// and the decision recorded in the design doc would need revisiting.

// dagWaveStrategy runs the graph above, one dependency wave per step.
type dagWaveStrategy struct{}

const dagWavePlanKey = "dag_wave"

func (dagWaveStrategy) Name() string    { return "dag-waves-test" }
func (dagWaveStrategy) Version() string { return "1.0.0" }

func (s dagWaveStrategy) Next(_ context.Context, st LoopState) (Action, error) {
	switch scratchInt(st, dagWavePlanKey) {
	case 0:
		return Action{Kind: ActionSpawn, Subagents: []SubagentSpec{{AgentID: "a", Objective: "ANSWER-FOR a"}}}, nil
	case 1:
		// B and C depend on A but not on each other, so they are one wave.
		return Action{Kind: ActionSpawn, Subagents: []SubagentSpec{
			{AgentID: "b", Objective: "ANSWER-FOR b"},
			{AgentID: "c", Objective: "ANSWER-FOR c"},
		}}, nil
	case 2:
		return Action{Kind: ActionSpawn, Subagents: []SubagentSpec{{AgentID: "d", Objective: "ANSWER-FOR d"}}}, nil
	default:
		return ReActStrategy{}.Next(nil, st)
	}
}

func (s dagWaveStrategy) OnResult(st LoopState, result StepResult) LoopState {
	next := ReActStrategy{}.OnResult(st, result)
	if next.Scratch == nil {
		next.Scratch = map[string]any{}
	}
	// A wave is complete once its spawn produced results. Advancing on anything
	// else would start the next wave before its dependencies finished.
	if len(result.SubagentResults) > 0 {
		next.Scratch[dagWavePlanKey] = scratchInt(st, dagWavePlanKey) + 1
	}
	return next
}

func TestRunDAGIsExpressibleAsSpawnWaves(t *testing.T) {
	store := NewInMemoryStore()
	defer store.Close()
	runtime, err := New(
		WithStore(store), WithModel(newDelegatingModel()),
		WithClock(FixedClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))),
		WithIDSource(sequentialIDs()),
		WithStrategyInstance(dagWaveStrategy{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	auth := AuthContext{TenantID: "tenant", SubjectID: "subject", Scopes: []string{"read", "write"}}

	result, err := runtime.Run(context.Background(), TaskRequest{
		TaskID: "dag", Goal: "run the graph", TenantID: auth.TenantID, SubjectID: auth.SubjectID, MaxSteps: 8,
	}, auth)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != TaskCompleted {
		t.Fatalf("status=%s category=%s error=%v", result.Status, result.FailureCategory, result.State.LastError)
	}

	// Every node exists and produced its own answer.
	for _, node := range []string{"a", "b", "c", "d"} {
		child, _, err := store.GetTask("dag:" + node)
		if err != nil {
			t.Fatalf("node %s was not created: %v", node, err)
		}
		if child.Status != TaskCompleted || child.FinalAnswer != "answer from "+node {
			t.Fatalf("node %s status=%s answer=%q", node, child.Status, child.FinalAnswer)
		}
	}

	// The ordering is what makes it a DAG rather than a fan-out: D's spawn event
	// must come after both B and C completed, and B and C after A.
	events, err := runtime.Journal().Replay("dag")
	if err != nil {
		t.Fatal(err)
	}
	sequence := make([]string, 0, len(events))
	for _, event := range events {
		switch event.Type {
		case "SUBAGENT_SPAWNED":
			sequence = append(sequence, "spawn:"+stringValue(event.Payload["agent_id"]))
		case "SUBAGENT_COMPLETED":
			sequence = append(sequence, "done:"+stringValue(event.Payload["agent_id"]))
		}
	}
	position := func(entry string) int {
		for i, candidate := range sequence {
			if candidate == entry {
				return i
			}
		}
		return -1
	}
	dependencies := []struct{ before, after string }{
		{"done:a", "spawn:b"},
		{"done:a", "spawn:c"},
		{"done:b", "spawn:d"},
		{"done:c", "spawn:d"},
	}
	for _, dependency := range dependencies {
		before, after := position(dependency.before), position(dependency.after)
		if before < 0 || after < 0 {
			t.Fatalf("missing event: %s or %s (sequence=%v)", dependency.before, dependency.after, sequence)
		}
		if before >= after {
			t.Fatalf("%s did not precede %s (sequence=%v)", dependency.before, dependency.after, sequence)
		}
	}
	// B and C were one wave, so neither preceded the other's spawn.
	spawnB, spawnC := position("spawn:b"), position("spawn:c")
	if spawnB != spawnC-1 && spawnC != spawnB-1 {
		t.Fatalf("B and C were not one wave: spawn:b at %d, spawn:c at %d", spawnB, spawnC)
	}
	_ = fmt.Sprint
}
