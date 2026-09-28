package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// WorkspaceTools exposes a small, inspectable code-agent surface. The root is
// explicit so a production deployment can bind it to a sandbox or checkout.
type WorkspaceTools struct {
	Root string
}

func NewWorkspaceTools(root string) *WorkspaceTools {
	if strings.TrimSpace(root) == "" {
		root, _ = os.Getwd()
	}
	abs, err := filepath.Abs(root)
	if err == nil {
		root = abs
	}
	return &WorkspaceTools{Root: filepath.Clean(root)}
}

func (w *WorkspaceTools) Register(registry *ToolRegistry) error {
	if w == nil || registry == nil {
		return errors.New("WORKSPACE_TOOLS_REQUIRED")
	}
	if err := registry.Register(ToolSpec{
		Name: "workspace_list", ConcurrencySafe: true, SideEffect: "NONE", Description: "list files in the code workspace", RequiredScopes: []string{"workspace:read"},
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}, "depth": map[string]any{"type": "integer"}}, "additionalProperties": false},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return w.list(args) },
	}); err != nil {
		return err
	}
	if err := registry.Register(ToolSpec{
		Name: "workspace_read", ConcurrencySafe: true, SideEffect: "NONE", Description: "read a text file from the code workspace", RequiredScopes: []string{"workspace:read"},
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}, "max_bytes": map[string]any{"type": "integer"}}, "required": []string{"path"}, "additionalProperties": false},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return w.read(args) },
	}); err != nil {
		return err
	}
	if err := registry.Register(ToolSpec{
		Name: "workspace_search", ConcurrencySafe: true, SideEffect: "NONE", Description: "search text in the code workspace", RequiredScopes: []string{"workspace:read"},
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"query": map[string]any{"type": "string"}, "glob": map[string]any{"type": "string"}}, "required": []string{"query"}, "additionalProperties": false},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return w.search(args) },
	}); err != nil {
		return err
	}
	return registry.Register(ToolSpec{
		Name: "workspace_test", Description: "run a bounded repository test command", RequiredScopes: []string{"workspace:read"},
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}, "required": []string{"command"}, "additionalProperties": false},
		Handler:     func(args map[string]any, _ AuthContext) (any, error) { return w.test(context.Background(), args) },
	})
}

func (w *WorkspaceTools) safePath(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		value = "."
	}
	path, err := filepath.Abs(filepath.Join(w.Root, value))
	if err != nil {
		return "", err
	}
	root, err := filepath.Abs(w.Root)
	if err != nil {
		return "", err
	}
	if path != root && !strings.HasPrefix(path, root+string(os.PathSeparator)) {
		return "", errors.New("WORKSPACE_PATH_OUTSIDE_ROOT")
	}
	return path, nil
}

func (w *WorkspaceTools) list(args map[string]any) (any, error) {
	root, err := w.safePath(stringValue(args["path"]))
	if err != nil {
		return nil, err
	}
	depth := intNumber(args["depth"])
	if depth <= 0 || depth > 4 {
		depth = 2
	}
	entries := []string{}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		if ignoredWorkspacePath(rel) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		level := strings.Count(filepath.ToSlash(rel), "/") + 1
		if level > depth {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() {
			entries = append(entries, filepath.ToSlash(rel)+"/")
		} else {
			entries = append(entries, filepath.ToSlash(rel))
		}
		if len(entries) >= 200 {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(entries)
	return map[string]any{"root": w.Root, "path": stringValue(args["path"]), "entries": entries, "truncated": len(entries) >= 200}, nil
}

func (w *WorkspaceTools) read(args map[string]any) (any, error) {
	path, err := w.safePath(stringValue(args["path"]))
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	maxBytes := intNumber(args["max_bytes"])
	if maxBytes <= 0 || maxBytes > 100_000 {
		maxBytes = 30_000
	}
	truncated := len(raw) > maxBytes
	if truncated {
		raw = raw[:maxBytes]
	}
	return map[string]any{"path": filepath.ToSlash(filepath.Clean(stringValue(args["path"]))), "content": string(raw), "bytes": len(raw), "truncated": truncated}, nil
}

func (w *WorkspaceTools) search(args map[string]any) (any, error) {
	query := strings.TrimSpace(stringValue(args["query"]))
	if query == "" {
		return nil, errors.New("SEARCH_QUERY_REQUIRED")
	}
	glob := stringValue(args["glob"])
	matches := []map[string]any{}
	err := filepath.Walk(w.Root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(w.Root, path)
		if path != w.Root && ignoredWorkspacePath(rel) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() || len(matches) >= 100 || (glob != "" && !globMatch(glob, filepath.ToSlash(rel))) {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil || len(raw) > 1_000_000 || !strings.Contains(strings.ToLower(string(raw)), strings.ToLower(query)) {
			return nil
		}
		lines := strings.Split(string(raw), "\n")
		for index, line := range lines {
			if strings.Contains(strings.ToLower(line), strings.ToLower(query)) {
				matches = append(matches, map[string]any{"path": filepath.ToSlash(rel), "line": index + 1, "text": strings.TrimSpace(line)})
				if len(matches) >= 100 {
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"query": query, "matches": matches, "truncated": len(matches) >= 100}, nil
}

func (w *WorkspaceTools) test(ctx context.Context, args map[string]any) (any, error) {
	command := strings.TrimSpace(stringValue(args["command"]))
	allowed := map[string][]string{"go test ./...": {"go", "test", "./..."}, "go vet ./...": {"go", "vet", "./..."}, "npm run typecheck": {"npm", "run", "typecheck"}}
	argv, ok := allowed[command]
	if !ok {
		return nil, fmt.Errorf("TEST_COMMAND_NOT_ALLOWED: %s", command)
	}
	process := exec.CommandContext(ctx, argv[0], argv[1:]...)
	process.Dir = w.Root
	// A workspace test must not inherit the harness server's durable control
	// database. Otherwise the checked repository can observe the agent's live
	// sessions and produce order-dependent results.
	process.Env = withoutEnvironment(os.Environ(), "HARNESS_DB_PATH")
	raw, err := process.CombinedOutput()
	if len(raw) > 20_000 {
		raw = raw[:20_000]
	}
	result := map[string]any{"command": command, "output": string(raw), "passed": err == nil}
	if err != nil {
		result["error"] = err.Error()
	}
	return result, nil
}

func withoutEnvironment(environment []string, excluded ...string) []string {
	blocked := make(map[string]struct{}, len(excluded))
	for _, key := range excluded {
		blocked[key] = struct{}{}
	}
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if ok {
			if _, skip := blocked[key]; skip {
				continue
			}
		}
		result = append(result, entry)
	}
	return result
}

func ignoredWorkspacePath(path string) bool {
	first := strings.Split(filepath.ToSlash(path), "/")[0]
	for _, name := range []string{".git", "node_modules", "dist", ".data", ".idea"} {
		if first == name {
			return true
		}
	}
	return strings.HasPrefix(filepath.Base(path), ".") && filepath.Base(path) != ".env.example"
}

func globMatch(glob, path string) bool {
	matched, err := filepath.Match(glob, path)
	return err == nil && matched
}
