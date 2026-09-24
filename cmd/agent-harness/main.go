package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Hanwen-Jiang/dag-code-agent/internal/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/internal/httpapi"
)

func main() {
	defaultMode := os.Getenv("HARNESS_MODE")
	if defaultMode == "" {
		defaultMode = "demo"
	}
	defaultAddr := os.Getenv("HARNESS_ADDR")
	if defaultAddr == "" {
		defaultAddr = "127.0.0.1:8080"
	}
	mode := flag.String("mode", defaultMode, "demo or server")
	addr := flag.String("addr", defaultAddr, "HTTP listen address")
	flag.Parse()
	switch *mode {
	case "demo":
		environment, err := harness.BuildDemoEnvironment()
		if err != nil {
			log.Fatal(err)
		}
		result := environment.Run(context.Background())
		encoded, _ := json.MarshalIndent(map[string]any{
			"status": result.Status, "task_id": result.TaskID,
			"final_answer": result.FinalAnswer, "metrics": result.Metrics,
			"event_count": len(result.Events), "cache": environment.Prefix.Stats(),
		}, "", "  ")
		fmt.Println(string(encoded))
		if result.Status != harness.TaskCompleted {
			os.Exit(1)
		}
	case "server":
		handler, closeFn, err := httpapi.NewHandler()
		if err != nil {
			log.Fatal(err)
		}
		defer closeFn()
		log.Printf("agent harness listening on http://%s", *addr)
		if err := http.ListenAndServe(*addr, handler); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}
