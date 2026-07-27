# CLAUDE.md — contract for AI helpers

A Hanzo Base-native Go service in the console tRPC→ZAP migration. It carries the
Langfuse-style observability OLTP model (traces, observations, sessions, scores,
score-configs, events) AND the presentation layer over it (dashboards, widgets,
table batch-actions, view presets, monitors), replacing 13 console tRPC routers.
It clones the [ui-customization](../ui-customization) reference pattern.

The dashboards half shipped first as its own binary (hanzoai/dashboards, msgType
206) and was folded in here. It was never a separate concern: a dashboard is a
saved QUERY over the traces/observations/scores this service already owns, and
its analytics methods are the same Datastore aggregations the trace/score
analytics need. Two binaries meant writing that shim twice against two copies of
one OLTP model. The dashboards ordinals live at 200+ (their original 0–29 offset
by 200); its files are the `dash_*.go` set.

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
- **Two gates, deliberately separate:** `Server.authorize` answers "may this
  caller invoke this method" (Kind + the per-method `ObsPermissions` bit, table
  `methodPermission`, fail closed on an unknown method; signature verify gated on
  a wired issuer registry — TODO → SPEC.md §2.3). `Server.scope` answers "whose
  rows may it touch" and returns the project every handler is passed. Braiding
  them is how a valid capability ends up reading another tenant's data. Do not
  scatter permission checks into the handlers, and do not re-read projectId there
  — take the passed `project`.
- **One tenant boundary:** `scope()` reads the payload's `ProjectId` (text @0 in
  every request struct) or, for a pipelined call, INHERITS the target's resolved
  project — so a dependent call can never name a different tenant than the call
  it pipelines off. The @0 assumption is pinned by
  `TestEveryRequestStructCarriesProjectIdAtZero`; a new request struct that puts
  another text field first fails that test instead of silently mis-scoping.
- **One backend:** Hanzo Base. No Prisma, Postgres/Datastore-as-source-of-truth,
  Mongo, Redis, tRPC, nginx. OLTP rows live in the `obs_*` Base collections
  (encrypted SQLite via the vault plugin when `--vaultDir` is set), every query
  scoped to `projectId`.
- **Honest about the gap — and loud about it:** the Datastore columnar
  aggregations (`*All`/`*CountAll`/`metrics`/`filterOptions`/score-comparison,
  plus `chart`/`scoreHistogram`/`executeQuery`) and the BullMQ
  `isBatchActionInProgress` are NOT faked on OLTP rows. They answer **501** via
  the single `Server.stub` helper, carrying the typed zero body and a log naming
  the shim. 501, never a 200: a stubbed 200 with an empty result is
  indistinguishable at the call site from a real query that matched nothing, so
  the UI renders "no data" and the missing backend never surfaces. The status is
  the one field a caller cannot skip reading. Wiring a shim is additive; don't
  fabricate aggregates and don't downgrade the status.
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
