package harness

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Branch scoring, the piece that makes a fan-out more than a spawn.
//
// ActionSpawn runs subagents and takes every result. A fan-out samples several
// attempts at the same objective, SCORES them, and keeps the best few — which is
// what FanoutSpec's Prune field describes and what no runtime path could do,
// because there was nothing to score with.
//
// There is deliberately no default scorer. A built-in heuristic would have to
// rank answers without understanding them, and the honest ones available are
// proxies for the wrong thing: length rewards verbosity, status rewards
// confident failure, and a keyword count rewards restating the question. An
// embedder who wants best-of-N supplies the judgement, or does not get to prune.

// Scorer rates one branch of a fan-out.
//
// It must be safe for concurrent use: branches are scored as they finish.
type Scorer interface {
	// Score returns a higher-is-better rating. The scale is the scorer's own,
	// because it is only ever compared against other scores from the same
	// scorer within one fan-out.
	Score(ctx context.Context, objective string, result SubagentResult) (float64, error)
}

// ScorerFunc adapts a function to the interface.
type ScorerFunc func(ctx context.Context, objective string, result SubagentResult) (float64, error)

func (f ScorerFunc) Score(ctx context.Context, objective string, result SubagentResult) (float64, error) {
	return f(ctx, objective, result)
}

// WithScorer registers a named scorer, resolvable from FanoutSpec.Scorer.
func WithScorer(name string, scorer Scorer) Option {
	return func(c *runtimeConfig) error {
		if name == "" {
			return errors.New("harness: WithScorer requires a name")
		}
		if scorer == nil {
			return fmt.Errorf("harness: WithScorer(%q) requires a non-nil scorer", name)
		}
		if c.scorers == nil {
			c.scorers = map[string]Scorer{}
		}
		c.scorers[name] = scorer
		return nil
	}
}

// scorerFor resolves a name, or reports what is available.
//
// Naming the registered scorers matters: a typo in a configuration string is the
// likeliest reason a name does not resolve, and "not found" alone leaves the
// author guessing which names exist.
func (r *Runtime) scorerFor(name string) (Scorer, error) {
	if name == "" {
		return nil, errors.New("FANOUT_SCORER_REQUIRED: FanoutSpec.Scorer names the scorer to rank branches with, and no default is assumed")
	}
	scorer, ok := r.scorers[name]
	if !ok {
		available := make([]string, 0, len(r.scorers))
		for registered := range r.scorers {
			available = append(available, registered)
		}
		sort.Strings(available)
		return nil, fmt.Errorf("FANOUT_SCORER_UNKNOWN: %q is not registered (available: %v)", name, available)
	}
	return scorer, nil
}

// scoredBranch pairs a result with its rating.
type scoredBranch struct {
	result SubagentResult
	score  float64
	err    error
}

// scoreBranches rates every branch, concurrently, preserving request order for
// equal scores.
//
// A branch that cannot be scored is kept with its error rather than dropped: a
// scorer failure is not evidence that the answer was bad, and silently discarding
// it would let a broken scorer look like a successful pruning decision.
func scoreBranches(ctx context.Context, scorer Scorer, objective string, results []SubagentResult) []scoredBranch {
	scored := make([]scoredBranch, len(results))
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			score, err := scorer.Score(ctx, objective, results[index])
			scored[index] = scoredBranch{result: results[index], score: score, err: err}
		}(i)
	}
	wg.Wait()
	return scored
}

// pruneBranches orders branches best-first and keeps the best `keep`.
//
// A keep of zero or less keeps everything, which is the FanoutSpec default and
// means "sample without selecting". Ties break on the existing request order, so
// the outcome does not depend on which branch happened to be scored first.
func pruneBranches(scored []scoredBranch, keep int) []SubagentResult {
	ordered := make([]scoredBranch, len(scored))
	copy(ordered, scored)
	sort.SliceStable(ordered, func(i, j int) bool {
		// Unscorable branches sort last: an error is not a rating.
		if (ordered[i].err == nil) != (ordered[j].err == nil) {
			return ordered[i].err == nil
		}
		return ordered[i].score > ordered[j].score
	})
	if keep > 0 && keep < len(ordered) {
		ordered = ordered[:keep]
	}
	out := make([]SubagentResult, 0, len(ordered))
	for _, branch := range ordered {
		out = append(out, branch.result)
	}
	return out
}
