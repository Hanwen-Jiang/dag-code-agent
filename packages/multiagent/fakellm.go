package multiagent

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// ScriptedLLM is a deterministic, dependency-free LLM implementation for
// tests and offline runs. It resolves prompts to canned responses using a set
// of matchers; when nothing matches it returns a stable echo. It also records
// every prompt it receives for assertions.
type ScriptedLLM struct {
	name string

	mu        sync.Mutex
	responses []scriptedResponse
	log       []string
	failOn    []string // prompts containing any of these substrings error out
	callCount int
}

type scriptedResponse struct {
	match     string // substring to match in the prompt (case-insensitive)
	output    string
}

// NewScriptedLLM creates an empty scripted LLM.
func NewScriptedLLM(name string) *ScriptedLLM {
	return &ScriptedLLM{name: name}
}

// When registers a response that is returned when the prompt contains match.
// Later registrations take precedence over earlier ones.
func (s *ScriptedLLM) When(match, output string) *ScriptedLLM {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, scriptedResponse{match: match, output: output})
	return s
}

// FailOn makes any prompt containing any of the given substrings return an
// error (to simulate failures).
func (s *ScriptedLLM) FailOn(substrings ...string) *ScriptedLLM {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failOn = append(s.failOn, substrings...)
	return s
}

// Prompts returns the prompts received so far, in order.
func (s *ScriptedLLM) Prompts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.log))
	copy(out, s.log)
	return out
}

// CallCount returns how many times Complete was invoked.
func (s *ScriptedLLM) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callCount
}

func (s *ScriptedLLM) Name() string { return s.name }

func (s *ScriptedLLM) Complete(ctx context.Context, prompt string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCount++
	s.log = append(s.log, prompt)

	lower := strings.ToLower(prompt)
	for _, sub := range s.failOn {
		if sub != "" && strings.Contains(lower, strings.ToLower(sub)) {
			return "", fmt.Errorf("scripted LLM: simulated failure (matched %q)", sub)
		}
	}

	// Most-recent non-empty matcher first, then the most-recent empty
	// (catch-all) matcher.
	fallbackIdx := -1
	for i := len(s.responses) - 1; i >= 0; i-- {
		r := s.responses[i]
		if r.match == "" {
			if fallbackIdx == -1 {
				fallbackIdx = i
			}
			continue
		}
		if strings.Contains(lower, strings.ToLower(r.match)) {
			return r.output, nil
		}
	}
	if fallbackIdx >= 0 {
		return s.responses[fallbackIdx].output, nil
	}
	// Default: deterministic echo of the last line of the prompt.
	lines := strings.Split(strings.TrimSpace(prompt), "\n")
	if len(lines) == 0 {
		return "OK", nil
	}
	return lines[len(lines)-1], nil
}
