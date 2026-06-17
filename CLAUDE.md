# CLAUDE.md — contract for AI helpers

A Hanzo Base-native Go service in the console tRPC→ZAP migration. It carries the
Langfuse-style observability OLTP model (traces, observations, sessions, scores,
score-configs, events), replacing 8 console tRPC routers. It clones the
[ui-customization](../ui-customization) reference pattern.

## The one rule

**The `.zap` schema is the source of truth.** `proto/observability.zap` defines
the data structs; `gen/` is its Go projection via `make zap-gen`. Never hand-edit
`gen/`. Change the schema, regenerate, then update `server/`.

## Two schema dialects (do not conflate)

- `proto/observability.zap` is the **zap-spec dialect** (`package … / Field Type
  @off`) that `github.com/zap-proto/go/cmd/zapgen` compiles to Go. This is what
  THIS repo (a Go service) consumes.
- The console keeps a copy compiled to TS via `zapgen --target=ts`. The field set
  is the shared byte-for-byte contract; each toolchain owns the encoding it
  parses.

## Build / test

- Pure-Go always: `CGO_ENABLED=0`. CGO pulls blst/accel C deps that need a full
  toolchain and break reproducibility. `make` sets this + `GOWORK=off`.
- `GOWORK=off`: this repo is self-contained via go.mod `replace`s; a parent
  `go.work` must not capture it.
- `make zap-gen` and `make build` are **idempotent** (byte-identical regen,
  reproducible binary). Keep them so — no timestamps/paths in generated output.
- `make test` runs the in-process per-method CRUD + capability + tenant-isolation
  + pipelining suite. Show it passing; don't claim "done" without it.

## Architecture invariants (DRY, orthogonal, decomplected)

- **Three wire layers, separated:** transport (`luxfi/zap` Node, msgType **203**)
  / envelope (`server/wire.go`) / payload (`gen/` typed views). The capability is
  carried as OPAQUE bytes through all three — auth is a value, not a place.
- **One auth chokepoint:** `Server.authorize`. Kind + the per-method
  `ObsPermissions` bit are always enforced (table `methodPermission`, fail closed
  on an unknown method); signature verify is gated on a wired issuer registry
  (TODO → SPEC.md §2.3). Do not scatter permission checks into the handlers.
- **One backend:** Hanzo Base. No Prisma, Postgres/ClickHouse-as-source-of-truth,
  Mongo, Redis, tRPC, nginx. OLTP rows live in the `obs_*` Base collections
  (encrypted SQLite via the vault plugin when `--vaultDir` is set), every query
  scoped to `projectId`.
- **Honest about the gap:** the ClickHouse columnar aggregations
  (`*All`/`*CountAll`/`metrics`/`filterOptions`/score-comparison) are NOT faked on
  OLTP rows — they return `Empty{Stubbed:true}` with the exact upstream query to
  port enumerated in `server/handlers.go` `handleStub`. Wiring the analytics shim
  is additive; don't fabricate aggregates.
- **One way to nest a message:** as pre-encoded `bytes`, `Wrap*`-ed on read
  (`TraceWithDetail.Trace`, `SessionWithScores.Session`, every `list<…>` element).
  zapgen's inline-struct embed does NOT round-trip a `Finish()`'d sub-message.
- **Pipelining is real:** `observationById` can target `traceById`'s promise; the
  server's promise table (`await`/`resolve`) joins them. Genuine in-flight
  pipelining needs the two calls on SEPARATE connections (the transport is FIFO
  per connection) — see `Client.PipelineTraceObservation` and its test.

## Do not

- Build Docker images locally (CI does, multi-arch → ghcr.io/hanzoai).
- Push to GitHub from here unless asked.
- Add plugins this one service doesn't need. Lean binary.
- Touch the console — wiring is documented in README, not done here.
