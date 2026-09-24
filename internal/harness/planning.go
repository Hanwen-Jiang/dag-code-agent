package harness

import "sort"

type Planner interface {
	Plan(TaskRequest, TaskState) []PlanStep
}
type RulePlanner struct{}

func (RulePlanner) Plan(request TaskRequest, _ TaskState) []PlanStep {
	return []PlanStep{{StepID: "understand", Objective: "understand the goal", Status: "READY"}, {StepID: "act", Objective: request.Goal, Dependencies: []string{"understand"}, Status: "PENDING"}, {StepID: "verify", Objective: "verify evidence and answer", Dependencies: []string{"act"}, Status: "PENDING"}}
}
func ReadySteps(plan []PlanStep) []PlanStep {
	result := []PlanStep{}
	states := map[string]string{}
	for _, step := range plan {
		states[step.StepID] = step.Status
	}
	for _, step := range plan {
		if step.Status != "PENDING" {
			continue
		}
		ready := true
		for _, dep := range step.Dependencies {
			if states[dep] != "SUCCEEDED" {
				ready = false
				break
			}
		}
		if ready {
			result = append(result, step)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StepID < result[j].StepID })
	return result
}
func Replan(plan []PlanStep, failedID string) []PlanStep {
	out := append([]PlanStep(nil), plan...)
	for i := range out {
		if out[i].StepID == failedID {
			out[i].Status = "READY"
			out[i].Attempts++
		}
	}
	return out
}
