package harness

import (
	"errors"
	"path/filepath"
	"strings"
)

type ExecutionPolicy struct {
	WorkspaceRoot  string
	AllowedPaths   []string
	ForbiddenPaths []string
	AllowedTools   []string
	NetworkProfile string
	SecretProfile  string
}

func (p ExecutionPolicy) AuthorizeTool(name string) error {
	if len(p.AllowedTools) > 0 && !ContainsString(p.AllowedTools, name) {
		return errors.New("TOOL_NOT_ALLOWED")
	}
	return nil
}
func (p ExecutionPolicy) AuthorizePath(path string) error {
	root, err := filepath.Abs(p.WorkspaceRoot)
	if err != nil {
		return err
	}
	candidate, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(root, candidate)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("PATH_OUTSIDE_WORKSPACE")
	}
	normalized := filepath.ToSlash(relative)
	for _, pattern := range p.ForbiddenPaths {
		matched, _ := filepath.Match(pattern, normalized)
		if matched || normalized == pattern {
			return errors.New("PATH_FORBIDDEN")
		}
	}
	if len(p.AllowedPaths) == 0 {
		return nil
	}
	for _, pattern := range p.AllowedPaths {
		matched, _ := filepath.Match(pattern, normalized)
		prefix := strings.TrimSuffix(pattern, "/**")
		if matched || normalized == prefix || strings.HasPrefix(normalized, prefix+"/") {
			return nil
		}
	}
	return errors.New("PATH_NOT_ALLOWED")
}
