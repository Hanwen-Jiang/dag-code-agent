package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Hanwen-Jiang/dag-code-agent/internal/dag"
	"github.com/Hanwen-Jiang/dag-code-agent/internal/harness"
	"github.com/Hanwen-Jiang/dag-code-agent/internal/httpapi"
	"github.com/Hanwen-Jiang/dag-code-agent/internal/modelenv"
)

// main is a composition root, not a library: reading the environment, probing
// for files and starting a server all belong here. The packages it calls take
// explicit configuration so they can be embedded elsewhere.
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
		defer func() { _ = environment.Store.Close() }()
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
		runServer(*addr)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

func runServer(addr string) {
	controlPath := os.Getenv("HARNESS_DB_PATH")
	if controlPath == "" {
		controlPath = ":memory:"
	}
	store, err := harness.NewSQLiteStore(controlPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	settings := modelenv.FromEnv()
	manifest, manifestErr := loadDAGManifest()
	if manifestErr != nil {
		log.Printf("dag manifest unavailable: %v", manifestErr)
	}

	handler, err := httpapi.New(httpapi.Config{
		Store:          store,
		Provider:       settings.Describe(),
		DAGManifest:    manifest,
		DAGLoadError:   manifestErr,
		CORSOrigins:    corsOrigins(),
		SessionFactory: settings.SessionFactory(store),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("agent harness listening on http://%s", addr)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatal(err)
	}
}

// corsOrigins reads HARNESS_CORS_ORIGINS (comma separated). Development
// defaults to allowing any origin; production deployments should set it.
func corsOrigins() []string {
	raw := strings.TrimSpace(os.Getenv("HARNESS_CORS_ORIGINS"))
	if raw == "" {
		return []string{"*"}
	}
	origins := []string{}
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			origins = append(origins, trimmed)
		}
	}
	return origins
}

// loadDAGManifest resolves the canonical DAG. Path probing is deliberately
// kept out of the library packages so embedding never depends on the working
// directory of a parent process.
func loadDAGManifest() (dag.Manifest, error) {
	candidates := []string{}
	if configured := os.Getenv("DAG_MANIFEST_PATH"); configured != "" {
		candidates = append(candidates, configured)
	}
	candidates = append(candidates,
		"configs/software-feature-with-rework-and-uat.json",
		"../configs/software-feature-with-rework-and-uat.json",
		"../../configs/software-feature-with-rework-and-uat.json",
		filepath.Join("..", "..", "..", "configs", "software-feature-with-rework-and-uat.json"),
	)
	for _, path := range candidates {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		return dag.LoadManifest(path)
	}
	return dag.Manifest{}, errors.New("DAG_MANIFEST_NOT_FOUND")
}
