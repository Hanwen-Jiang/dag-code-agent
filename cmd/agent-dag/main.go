package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"agent-harness/internal/dag"
	"agent-harness/internal/harness"
)

func main() {
	manifestPath := flag.String("manifest", "configs/software-feature-with-rework-and-uat.json", "canonical manifest")
	runID := flag.String("run", "dag-demo", "run id")
	rework := flag.Bool("rework", false, "fail both v1 verification attempts and exercise diagnosis/v2")
	crash := flag.Int("crash-after", 0, "simulate a process crash after N committed events")
	flag.Parse()
	manifest, err := dag.LoadManifest(*manifestPath)
	if err != nil {
		log.Fatal(err)
	}
	store, err := harness.NewStore(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	outcomes := map[string][]string{}
	if *rework {
		outcomes["N-VERIFY-V1"] = []string{"FAILED", "FAILED"}
	}
	executor, err := dag.NewExecutor(manifest, store, dag.NewDeterministicRunnerFactory(outcomes))
	if err != nil {
		log.Fatal(err)
	}
	report, err := executor.Execute(*runID, dag.Options{CrashAfterCommits: *crash, Context: context.Background()})
	if err != nil && err != dag.ErrSimulatedCrash {
		log.Fatal(err)
	}
	if err == dag.ErrSimulatedCrash {
		// A restart gets a fresh executor/worker identity while retaining the
		// durable store.  This is the recovery path used by the tests.
		report, err = executor.Execute(*runID, dag.Options{Context: context.Background()})
		if err != nil {
			log.Fatal(err)
		}
	}
	if report.Status == "WAITING_USER" {
		report, err = executor.ResumeUser(*runID, "PASS", "demo-user")
		if err != nil {
			log.Fatal(err)
		}
	}
	output := map[string]any{"status": report.Status, "run_id": report.RunID, "node_states": report.NodeStates, "completed_order": report.CompletedOrder, "attempt_count": len(report.Attempts), "artifact_count": len(report.Artifacts), "event_count": len(report.Events), "metrics": report.Metrics()}
	b, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(b))
	if report.Status != "COMPLETED" {
		os.Exit(1)
	}
}
