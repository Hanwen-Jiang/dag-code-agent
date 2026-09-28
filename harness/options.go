package harness

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// Option configures a Runtime built by New.
//
// Options are validated eagerly: a bad option fails New instead of producing a
// runtime that misbehaves on the first task.
type Option func(*runtimeConfig) error

type runtimeConfig struct {
	store          CoreStore
	ownsStore      bool
	journal        *Journal
	clock          Clock
	model          ModelAdapter
	tools          *ToolRuntime
	contextBuilder *ContextBuilder
	memory         *MemoryManager
	registry       *ToolRegistry
	skills         *SkillRegistry
	cache          *PrefixCache
	planner        Planner
	verifier       VerifyFunc
	budget         *BudgetLedger
	logger         *slog.Logger
	workspaceStore WorkspaceStore
	mailboxStore   MailboxStore
	strategies     *StrategyRegistry
	strategy       Strategy
	strategyName   string
	newID          func(string) string
	// emitter is the runtime-level observation channel installed by
	// WithEmitter (declared in stream.go).
	emitter Emitter
	// loopGuard detects stuck runs; nil selects the detector-backed default.
	loopGuard LoopGuard
	// skillResolver overrides the context compiler's skill source.
	skillResolver SkillResolver
	// streamBuffer overrides the per-run observation buffer.
	streamBuffer int
	// subagentPolicy bounds fan-out.
	subagentPolicy SubagentPolicy
	// toolCatalogThreshold defers tool schemas above this catalogue size.
	toolCatalogThreshold int
	// watchdog bounds live runs.
	watchdog WatchdogPolicy
	// memoryRanker orders memory retrieval when set.
	memoryRanker MemoryRanker
}

// WithStreamBuffer sets how many observation frames a streamed run may queue
// ahead of its consumer. Frames beyond it are dropped and counted rather than
// blocking the run (design §3.3), so a consumer that keeps up can raise this,
// and a test that needs to observe the drop path can lower it.
func WithStreamBuffer(frames int) Option {
	return func(c *runtimeConfig) error {
		if frames < 1 {
			return errors.New("harness: WithStreamBuffer requires a positive frame count")
		}
		c.streamBuffer = frames
		return nil
	}
}

// WithStore injects the persistence backend. Without it, New creates a
// private in-memory SQLite store that the runtime also closes.
func WithStore(store CoreStore) Option {
	return func(c *runtimeConfig) error {
		if store == nil {
			return errors.New("harness: WithStore requires a non-nil store")
		}
		c.store = store
		return nil
	}
}

// WithJournal injects a pre-built journal. It is rarely needed: supplying a
// store is enough, and the journal is derived from it.
func WithJournal(journal *Journal) Option {
	return func(c *runtimeConfig) error {
		if journal == nil {
			return errors.New("harness: WithJournal requires a non-nil journal")
		}
		c.journal = journal
		return nil
	}
}

// WithClock sets the time source for event stamps, lease expiry and budgets.
func WithClock(clock Clock) Option {
	return func(c *runtimeConfig) error {
		if clock == nil {
			return errors.New("harness: WithClock requires a non-nil clock")
		}
		c.clock = clock
		return nil
	}
}

// WithModel sets the model adapter. The default is a deterministic offline
// conversation adapter, so New() works with no credentials at all.
func WithModel(model ModelAdapter) Option {
	return func(c *runtimeConfig) error {
		if model == nil {
			return errors.New("harness: WithModel requires a non-nil adapter")
		}
		c.model = model
		return nil
	}
}

// WithTools injects a pre-built tool runtime.
func WithTools(tools *ToolRuntime) Option {
	return func(c *runtimeConfig) error {
		c.tools = tools
		return nil
	}
}

// WithToolRegistry supplies the tool registry used to build the default tool
// runtime and context builder.
func WithToolRegistry(registry *ToolRegistry) Option {
	return func(c *runtimeConfig) error {
		c.registry = registry
		return nil
	}
}

// WithSkills supplies the skill registry used to compile context.
func WithSkills(skills *SkillRegistry) Option {
	return func(c *runtimeConfig) error {
		c.skills = skills
		return nil
	}
}

// WithPrefixCache supplies the prompt prefix cache used by the context builder.
func WithPrefixCache(cache *PrefixCache) Option {
	return func(c *runtimeConfig) error {
		c.cache = cache
		return nil
	}
}

// WithContextBuilder injects a fully custom context compiler.
func WithContextBuilder(builder *ContextBuilder) Option {
	return func(c *runtimeConfig) error {
		c.contextBuilder = builder
		return nil
	}
}

// WithMemoryManager injects a memory manager. Without it, New derives one from
// the store when the store also implements MemoryStore.
func WithMemoryManager(memory *MemoryManager) Option {
	return func(c *runtimeConfig) error {
		c.memory = memory
		return nil
	}
}

// WithPlanner replaces the default rule planner.
func WithPlanner(planner Planner) Option {
	return func(c *runtimeConfig) error {
		c.planner = planner
		return nil
	}
}

// WithVerifier installs the output verifier consulted before a task completes.
func WithVerifier(verifier VerifyFunc) Option {
	return func(c *runtimeConfig) error {
		c.verifier = verifier
		return nil
	}
}

// WithBudget sets the budget ledger shared by runs of this runtime.
func WithBudget(budget *BudgetLedger) Option {
	return func(c *runtimeConfig) error {
		c.budget = budget
		return nil
	}
}

// WithStrategyRegistry supplies the registry used to resolve WithStrategy.
func WithStrategyRegistry(registry *StrategyRegistry) Option {
	return func(c *runtimeConfig) error {
		if registry == nil {
			return errors.New("harness: WithStrategyRegistry requires a non-nil registry")
		}
		c.strategies = registry
		return nil
	}
}

// WithStrategy selects the loop topology by name from the registry. The
// built-in "react" strategy is always available.
func WithStrategy(name string) Option {
	return func(c *runtimeConfig) error {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return errors.New("harness: WithStrategy requires a name")
		}
		c.strategyName = trimmed
		return nil
	}
}

// WithStrategyInstance injects a strategy value directly, without registering
// it. Use it for one-off topologies.
func WithStrategyInstance(strategy Strategy) Option {
	return func(c *runtimeConfig) error {
		if strategy == nil {
			return errors.New("harness: WithStrategyInstance requires a non-nil strategy")
		}
		c.strategy = strategy
		return nil
	}
}

// WithIDSource replaces the generator used for identifiers created during a
// run (turn ids today, and any id an extension asks for). The default is
// crypto-random, which makes two runs of the same task differ; supplying a
// deterministic source is what lets a test assert a byte-identical journal.
func WithIDSource(newID func(prefix string) string) Option {
	return func(c *runtimeConfig) error {
		if newID == nil {
			return errors.New("harness: WithIDSource requires a non-nil generator")
		}
		c.newID = newID
		return nil
	}
}

// WithLogger sets the structured logger used for lifecycle diagnostics.
func WithLogger(logger *slog.Logger) Option {
	return func(c *runtimeConfig) error {
		c.logger = logger
		return nil
	}
}

// WithWorkspaceStore injects the durable Workspace/Topic projection. When
// omitted, New uses the supplied CoreStore if it implements WorkspaceStore.
func WithWorkspaceStore(store WorkspaceStore) Option {
	return func(c *runtimeConfig) error {
		if store == nil {
			return errors.New("harness: WithWorkspaceStore requires a non-nil store")
		}
		c.workspaceStore = store
		return nil
	}
}

// WithMailboxStore injects the durable mailbox projection. When omitted, New
// uses the supplied CoreStore if it implements MailboxStore.
func WithMailboxStore(store MailboxStore) Option {
	return func(c *runtimeConfig) error {
		if store == nil {
			return errors.New("harness: WithMailboxStore requires a non-nil store")
		}
		c.mailboxStore = store
		return nil
	}
}

// newMailboxOrNil avoids handing back a Mailbox whose Store is nil: such an
// object is non-nil but fails every call, which hides a missing capability
// behind an error instead of letting the caller check for it.
func newMailboxOrNil(store MailboxStore) *Mailbox {
	if store == nil {
		return nil
	}
	return NewMailbox(store)
}

// New builds a runtime.
//
// New() with no options is a complete, working, offline runtime: an in-memory
// SQLite control-plane store, a deterministic model adapter, an empty tool
// registry and a system clock. Every collaborator can be replaced by an
// option, which is what makes the control plane embeddable.
func New(opts ...Option) (*Runtime, error) {
	cfg := &runtimeConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	if cfg.clock == nil {
		cfg.clock = SystemClock()
	}
	if cfg.store == nil {
		store, err := NewSQLiteStore(":memory:")
		if err != nil {
			return nil, fmt.Errorf("harness: default store: %w", err)
		}
		store.SetClock(cfg.clock)
		cfg.store = store
		cfg.ownsStore = true
	}
	if setter, ok := cfg.store.(interface{ SetClock(Clock) }); ok {
		setter.SetClock(cfg.clock)
	}
	journal := cfg.journal
	if journal == nil {
		journal = NewJournal(cfg.store, cfg.clock)
	}
	if cfg.model == nil {
		cfg.model = NewConversationModel()
	}
	if cfg.memory == nil {
		if memStore, ok := cfg.store.(MemoryStore); ok {
			cfg.memory = NewMemoryManager(memStore)
		}
	}
	if cfg.memory != nil && cfg.memory.clock == nil {
		cfg.memory.SetClock(cfg.clock)
	}
	if cfg.registry == nil {
		cfg.registry = NewToolRegistry()
	}
	if cfg.skills == nil {
		cfg.skills = NewSkillRegistry()
	}
	if cfg.cache == nil {
		cfg.cache = NewPrefixCache(8)
	}
	if cfg.tools == nil {
		var idempotency IdempotencyStore
		if candidate, ok := cfg.store.(IdempotencyStore); ok {
			idempotency = candidate
		}
		cfg.tools = NewToolRuntime(cfg.registry, idempotency)
	}
	if cfg.contextBuilder == nil {
		cfg.contextBuilder = NewContextBuilder(cfg.memory, cfg.skills, cfg.registry, cfg.cache)
	}
	// Reuse the artifact projection for context spill: it is already
	// content-addressed, idempotent and tenant-scoped, which is everything the
	// spill level needs.
	if cfg.memoryRanker != nil && cfg.memory != nil {
		cfg.memory.SetRanker(cfg.memoryRanker)
	}
	if cfg.skillResolver != nil {
		cfg.contextBuilder.Skills = cfg.skillResolver
	}
	if cfg.toolCatalogThreshold > 0 {
		cfg.contextBuilder.ToolCatalogThreshold = cfg.toolCatalogThreshold
		// The resolver is registered here rather than left to the embedder: a
		// deferred catalogue without it would list tools the model cannot use.
		if cfg.registry != nil {
			if err := cfg.registry.Register(NewToolSearchTool(cfg.registry)); err != nil {
				return nil, err
			}
		}
	}
	if cfg.contextBuilder.Spill == nil {
		if artifacts, ok := cfg.store.(ArtifactStore); ok {
			cfg.contextBuilder.Spill = artifactSpillStore{artifacts: artifacts}
		}
	}
	if cfg.planner == nil {
		cfg.planner = RulePlanner{}
	}
	if cfg.budget == nil {
		cfg.budget = &BudgetLedger{}
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.newID == nil {
		cfg.newID = NewID
	}
	if cfg.subagentPolicy.MaxParallel <= 0 {
		cfg.subagentPolicy = DefaultSubagentPolicy()
	}
	if cfg.streamBuffer <= 0 {
		cfg.streamBuffer = defaultStreamBuffer
	}
	if cfg.loopGuard == nil {
		// The detector-backed guard is the default because loop detection is a
		// correctness concern, not an optional enhancement: the run has to be
		// able to stop itself, and the detectors also know how to warn first.
		cfg.loopGuard = newDetectorLoopGuard()
	}
	if cfg.strategies == nil {
		cfg.strategies = NewStrategyRegistry(ReActStrategy{}, ReflectionStrategy{}, CoTStrategy{}, DebateStrategy{}, TreeOfThoughtsStrategy{})
	}
	if cfg.strategy == nil {
		name := cfg.strategyName
		if name == "" {
			name = reactStrategyName
		}
		resolved, ok := cfg.strategies.Get(name)
		if !ok {
			return nil, fmt.Errorf("harness: strategy %q is not registered", name)
		}
		cfg.strategy = resolved
	}
	if cfg.workspaceStore == nil {
		if candidate, ok := cfg.store.(WorkspaceStore); ok {
			cfg.workspaceStore = candidate
		}
	}
	if cfg.mailboxStore == nil {
		if candidate, ok := cfg.store.(MailboxStore); ok {
			cfg.mailboxStore = candidate
		}
	}
	// Background tasks hang off a context the runtime owns, so Close can stop
	// them. context.Background() would make them outlive the runtime.
	baseCtx, baseCancel := context.WithCancelCause(context.Background())
	return &Runtime{
		store:          cfg.store,
		journal:        journal,
		model:          cfg.model,
		tools:          cfg.tools,
		contextBuilder: cfg.contextBuilder,
		memory:         cfg.memory,
		planner:        cfg.planner,
		verifier:       cfg.verifier,
		budget:         cfg.budget,
		clock:          cfg.clock,
		logger:         cfg.logger,
		workspaceStore: cfg.workspaceStore,
		mailbox:        newMailboxOrNil(cfg.mailboxStore),
		ownsStore:      cfg.ownsStore,
		strategies:     cfg.strategies,
		strategy:       cfg.strategy,
		newID:          cfg.newID,
		emitter:        cfg.emitter,
		loopGuard:      cfg.loopGuard,
		streamBuffer:   cfg.streamBuffer,
		subagentPolicy: cfg.subagentPolicy,
		watchdog:       cfg.watchdog,
		baseCtx:        baseCtx,
		baseCancel:     baseCancel,
	}, nil
}
