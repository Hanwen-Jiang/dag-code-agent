package xai

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ModelTier describes one selectable model offering.
type ModelTier struct {
	ID       string
	Name     string
	Provider string
	// Capabilities this tier provides (e.g. "vision", "code", "long_context").
	Capabilities []string
	// MinComplexity is the minimum task complexity this tier should handle
	// (below this the tier is wasteful, not ineligible).
	MinComplexity float64
	// MaxComplexity is the highest task complexity this tier can reliably handle.
	MaxComplexity float64
	// CostPer1KTokens is the blended input+output price per 1K tokens in USD.
	CostPer1KTokens float64
	// AvgLatencyMS is the typical end-to-end latency of one call.
	AvgLatencyMS float64
}

// RoutingContext carries the constraints for one routing decision.
type RoutingContext struct {
	// BudgetUSD caps the estimated cost of the routed call. <= 0 means unlimited.
	BudgetUSD float64
	// RequiredCapabilities must all be present on the chosen tier.
	RequiredCapabilities []string
	// DeadlineMS, if > 0, bounds the acceptable average latency.
	DeadlineMS float64
}

// RoutingDecision is the outcome of a routing call.
type RoutingDecision struct {
	Tier             *ModelTier   `json:"tier"`
	FallbackChain    []*ModelTier `json:"fallback_chain"`
	Complexity       float64      `json:"complexity"`
	EstimatedTokens  int          `json:"estimated_tokens"`
	EstimatedCostUSD float64      `json:"estimated_cost_usd"`
	Reasoning        string       `json:"reasoning"`
}

// TierTradeoff is one row of the cost/latency tradeoff table.
type TierTradeoff struct {
	TierID          string  `json:"tier_id"`
	Provider        string  `json:"provider"`
	CostPer1KTokens float64 `json:"cost_per_1k_tokens"`
	AvgLatencyMS    float64 `json:"avg_latency_ms"`
	MinComplexity   float64 `json:"min_complexity"`
	MaxComplexity   float64 `json:"max_complexity"`
}

// ModelRouter deterministically routes queries to model tiers based on query
// complexity heuristics, budget and capability requirements, with a fallback
// chain for provider failures. No randomness is involved.
type ModelRouter struct {
	mu              sync.RWMutex
	tiers           []*ModelTier
	failedProviders map[string]bool
}

// NewModelRouter creates a router over the given tiers. Tiers are stored in
// the order given; ordering only affects deterministic tie-breaks.
func NewModelRouter(tiers []*ModelTier) *ModelRouter {
	return &ModelRouter{
		tiers:           tiers,
		failedProviders: make(map[string]bool),
	}
}

// DefaultTiers returns a three-tier catalog (small/medium/large) used as a
// sensible baseline.
func DefaultTiers() []*ModelTier {
	return []*ModelTier{
		{
			ID: "tier-small", Name: "Small", Provider: "local-host",
			Capabilities:  []string{"text", "code"},
			MinComplexity: 0, MaxComplexity: 4,
			CostPer1KTokens: 0.0003, AvgLatencyMS: 350,
		},
		{
			ID: "tier-medium", Name: "Medium", Provider: "cloud-primary",
			Capabilities:  []string{"text", "code", "vision", "long_context"},
			MinComplexity: 0, MaxComplexity: 7,
			CostPer1KTokens: 0.003, AvgLatencyMS: 1200,
		},
		{
			ID: "tier-large", Name: "Large", Provider: "cloud-frontier",
			Capabilities:  []string{"text", "code", "vision", "long_context", "reasoning"},
			MinComplexity: 0, MaxComplexity: 10,
			CostPer1KTokens: 0.02, AvgLatencyMS: 3500,
		},
	}
}

// MarkProviderFailed flags a provider as unavailable; routing will skip its
// tiers and fall back to the next eligible tier.
func (r *ModelRouter) MarkProviderFailed(provider string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failedProviders[provider] = true
}

// ResetProvider clears a provider's failure flag.
func (r *ModelRouter) ResetProvider(provider string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failedProviders, provider)
}

// TradeoffTable returns the cost/latency tradeoff table, sorted by cost
// ascending (ties broken by tier ID).
func (r *ModelRouter) TradeoffTable() []TierTradeoff {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TierTradeoff, 0, len(r.tiers))
	for _, t := range r.tiers {
		out = append(out, TierTradeoff{
			TierID:          t.ID,
			Provider:        t.Provider,
			CostPer1KTokens: t.CostPer1KTokens,
			AvgLatencyMS:    t.AvgLatencyMS,
			MinComplexity:   t.MinComplexity,
			MaxComplexity:   t.MaxComplexity,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CostPer1KTokens != out[j].CostPer1KTokens {
			return out[i].CostPer1KTokens < out[j].CostPer1KTokens
		}
		return out[i].TierID < out[j].TierID
	})
	return out
}

// queryComplexity scores a query in [0, 10] from deterministic heuristics:
// word count, line count, code markers, list/step markers and question count.
func queryComplexity(query string) float64 {
	words := len(strings.Fields(query))
	lines := strings.Count(query, "\n") + 1
	score := 0.0

	// Base from length: 0.5 per 25 words, capped at 4.
	score += minF(4.0, float64(words)/25.0)

	// Multi-line queries are harder to hold in working context.
	if lines > 1 {
		score += minF(1.5, 0.5*float64(lines-1))
	}

	codeMarkers := []string{"def ", "func ", "class ", "import ", "=>", "{", "}", "SELECT ", "SELECT\n", "#include", "public ", "private ", ";", "&&", "||"}
	codeHits := 0
	lower := strings.ToLower(query)
	for _, m := range codeMarkers {
		if strings.Contains(lower, strings.ToLower(m)) {
			codeHits++
		}
	}
	if codeHits > 0 {
		score += minF(2.5, 0.5*float64(codeHits))
	}

	// Bulleted/numbered steps indicate multi-step tasks.
	steps := 0
	for _, line := range strings.Split(query, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "- ") || strings.HasPrefix(t, "* ") || strings.HasPrefix(t, "• ") {
			steps++
		} else if len(t) > 2 && t[0] >= '0' && t[0] <= '9' && (t[1] == '.' || t[1] == ')') {
			steps++
		}
	}
	if steps > 0 {
		score += minF(1.5, 0.3*float64(steps))
	}

	if strings.Contains(query, "?") {
		score += 0.5
	}

	if score > 10 {
		score = 10
	}
	return score
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// estimateTokens models the routed call as input + output tokens.
// Output is estimated at 3x input; input is the classic ~4 chars/token.
func estimateTokens(query string) int {
	input := len(query)/4 + 1
	return input + 3*input
}

func hasAllCapabilities(tier *ModelTier, required []string) bool {
	have := make(map[string]bool, len(tier.Capabilities))
	for _, c := range tier.Capabilities {
		have[strings.ToLower(c)] = true
	}
	for _, req := range required {
		if !have[strings.ToLower(req)] {
			return false
		}
	}
	return true
}

// Route picks the model tier for a query. The selection is:
//  1. score query complexity,
//  2. filter tiers that can handle it under budget/capability/deadline,
//  3. pick the cheapest eligible tier (cost-first; ties broken by latency,
//     then ID) — cheapest tier that is not below its MinComplexity floor is
//     preferred for trivial queries,
//  4. if that tier's provider is marked failed, walk the fallback chain.
func (r *ModelRouter) Route(query string, ctx RoutingContext) (*RoutingDecision, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.tiers) == 0 {
		return nil, fmt.Errorf("router has no tiers")
	}

	complexity := queryComplexity(query)
	tokens := estimateTokens(query)

	// Candidate tiers: handle complexity, have capabilities, meet deadline.
	var band []*ModelTier    // MinComplexity <= complexity <= MaxComplexity
	var capable []*ModelTier // MaxComplexity >= complexity (band is empty -> fallback)
	for _, t := range r.tiers {
		if t.MaxComplexity < complexity {
			continue
		}
		if !hasAllCapabilities(t, ctx.RequiredCapabilities) {
			continue
		}
		if ctx.DeadlineMS > 0 && t.AvgLatencyMS > ctx.DeadlineMS {
			continue
		}
		capable = append(capable, t)
		if t.MinComplexity <= complexity {
			band = append(band, t)
		}
	}
	// Prefer tiers whose complexity band covers the query; if none does (all
	// bands sit above the query), fall back to the capable set.
	eligible := band
	if len(eligible) == 0 {
		eligible = capable
	}
	if len(eligible) == 0 {
		return nil, fmt.Errorf("no tier can handle complexity %.1f under the given constraints", complexity)
	}

	// Budget filter (estimated cost = tokens/1000 * per-1K price).
	var inBudget []*ModelTier
	for _, t := range eligible {
		cost := float64(tokens) / 1000.0 * t.CostPer1KTokens
		if ctx.BudgetUSD <= 0 || cost <= ctx.BudgetUSD {
			inBudget = append(inBudget, t)
		}
	}
	if len(inBudget) == 0 {
		return nil, fmt.Errorf("no eligible tier fits budget $%.4f (cheapest eligible is $%.4f)",
			ctx.BudgetUSD, cheapestEstimate(eligible, tokens))
	}

	// Cheapest first; ties by latency then ID.
	sort.Slice(inBudget, func(i, j int) bool {
		ci := float64(tokens) / 1000.0 * inBudget[i].CostPer1KTokens
		cj := float64(tokens) / 1000.0 * inBudget[j].CostPer1KTokens
		if ci != cj {
			return ci < cj
		}
		if inBudget[i].AvgLatencyMS != inBudget[j].AvgLatencyMS {
			return inBudget[i].AvgLatencyMS < inBudget[j].AvgLatencyMS
		}
		return inBudget[i].ID < inBudget[j].ID
	})

	// Walk the chain, skipping tiers whose provider is marked failed.
	var chosen *ModelTier
	var skipped []string
	for _, t := range inBudget {
		if r.failedProviders[t.Provider] {
			skipped = append(skipped, fmt.Sprintf("%s (provider %s down)", t.ID, t.Provider))
			continue
		}
		chosen = t
		break
	}

	if chosen == nil {
		return nil, fmt.Errorf("all eligible tiers' providers are failed: %s", strings.Join(skipped, ", "))
	}

	// Fallback chain: remaining cheaper-or-equal alternatives in the same
	// order (they are, by construction, the next-best choices).
	chain := make([]*ModelTier, 0, len(inBudget))
	for _, t := range inBudget {
		if t.ID != chosen.ID {
			chain = append(chain, t)
		}
	}

	estCost := float64(tokens) / 1000.0 * chosen.CostPer1KTokens
	reason := fmt.Sprintf(
		"complexity %.1f/10; chose %s ($%.4f est, ~%.0f ms): cheapest tier handling complexity %.1f with capabilities [%s]",
		complexity, chosen.ID, estCost, chosen.AvgLatencyMS, complexity,
		strings.Join(chosen.Capabilities, ","))
	if len(skipped) > 0 {
		reason += "; skipped: " + strings.Join(skipped, "; ")
	}

	return &RoutingDecision{
		Tier:             chosen,
		FallbackChain:    chain,
		Complexity:       complexity,
		EstimatedTokens:  tokens,
		EstimatedCostUSD: estCost,
		Reasoning:        reason,
	}, nil
}

func cheapestEstimate(tiers []*ModelTier, tokens int) float64 {
	best := 0.0
	for i, t := range tiers {
		c := float64(tokens) / 1000.0 * t.CostPer1KTokens
		if i == 0 || c < best {
			best = c
		}
	}
	return best
}
