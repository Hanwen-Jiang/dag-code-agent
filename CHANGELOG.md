# Changelog

All notable changes to this project are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Pre-1.0 note: per the Go module rules, `v0.x` minor releases may contain
breaking changes. The API stability tiers below say which packages are meant to
be safe to build on.

| Tier | Packages | Promise |
|---|---|---|
| Stable | `harness`, `harness/dag`, `harness/storetest`, `harness/looptest` | Breaking changes need a major version after v1.0 |
| Experimental | `httpapi`, `backends/postgres`, `modelenv`, `packages/*` | May change in a minor release; changes are recorded here |

## [Unreleased]

### Added
- Context lifecycle: staged compaction (pin / summarize / spill / drop) replacing
  the amputation that discarded every earlier turn and tool result; every
  decision is recorded in a `ContextPlan` on the `CONTEXT_COMPILED` event.
- Oversized tool results spill to artifact storage and the context carries a
  `read_artifact` hint, so the bulk leaves the prompt but stays recoverable.
- Loop composability: a `Strategy` seam (`Strategy`, `LoopState`, `Action`) with
  the kernel executing actions; `ReActStrategy` is the default and reference
  topology; strategies can be registered and are pinned per task.
- `harness/looptest`: conformance suite every topology must pass (vocabulary,
  determinism, boundedness, journal integrity, identity pinning, scratch
  persistence, tool-policy convergence, budget termination).
- `WithIDSource` for reproducible identifiers, which is what makes the
  determinism check meaningful.

## [0.1.0] - 2026-09-28

The embeddability milestone: the control plane became a library other Go
programs can import, not only an HTTP service.

### Added
- Public packages `harness`, `harness/dag`, `httpapi` and `modelenv`; the
  former `internal/` tree is gone.
- Capability-split storage interfaces (`TaskStore`, `EventStore`,
  `IdempotencyStore`, `MemoryStore`, `LeaseStore`, `ArtifactStore`,
  `FeedbackStore`) and a `CoreStore` the runtime actually needs.
- `Journal`: the core now owns the event hash chain. Backends only perform
  atomic conditional appends, so a third-party backend cannot silently corrupt
  the chain.
- `Clock` abstraction (`SystemClock`, `FixedClock`, `SteppedClock`,
  `ManualClock`) so lease expiry and timestamps are deterministic in tests.
- `New(opts ...Option)` with `WithStore`/`WithModel`/`WithClock`/`WithLogger`
  and a working zero-configuration default.
- `harness/storetest`: conformance suite for storage backends, plus
  `harness/InMemoryStore` as a dependency-free second implementation.
- `backends/postgres`: an independent backend (kept out of `harness/` so the
  core never depends on pgx), verified against PostgreSQL 17.
- `examples/`: `embed_offline`, `embed_custom_store`, `embed_custom_model` and
  `embed_external` — the last is a separate Go module that proves the public
  packages are importable.
- `scripts/embed-guards.sh`: machine-enforced acceptance criteria (CGO-free
  core, no heavy dependencies in the core closure, no environment reads or
  relative-path probing in libraries, external module builds, conformance
  suites, dependency budget, multi-instance lifecycle) with a `--self-test`
  mode that plants a violation to prove the guards can fail.
- `Run`/`Resume`/`SendMessage`/`Cancel` return `(RunResult, error)`, with
  `ErrInfrastructure` separating control-plane failures from task outcomes.

### Changed
- Module path is `github.com/Hanwen-Jiang/dag-code-agent`.
- `Runtime` fields are unexported with read-only accessors; `Close` releases
  only a store the runtime created, never an injected one.
- Library packages no longer read the environment; that lives in `modelenv`
  and `cmd/*`.

[Unreleased]: https://github.com/Hanwen-Jiang/dag-code-agent/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Hanwen-Jiang/dag-code-agent/releases/tag/v0.1.0
