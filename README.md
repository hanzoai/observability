# observability

A Hanzo Base-native Go service binary in the console tRPC→ZAP migration. It
serves the **Observability** capability — the Langfuse-style trace / observation
/ score OLTP backbone, plus the dashboards/widgets/tables/presets/monitors
presentation layer over it — that the Next.js console previously ran in-process
as 13 tRPC routers hitting ClickHouse + Prisma.

The dashboards half shipped first as a standalone binary (`hanzoai/dashboards`,
msgType 206) and was folded in: a dashboard is a saved query over the very
traces/observations/scores this service owns, and both halves were waiting on the
same ClickHouse shim. One service, one shim, one capability gate. Those methods
keep their original ordinals offset by 200, and live in the `dash_*.go` files.

Replaces these console routers:

| tRPC router | source |
|---|---|
| `traceRouter` | `console/web/src/server/api/routers/traces.ts` |
| `observationsRouter` | `…/routers/observations.ts` |
| `sessionRouter` | `…/routers/sessions.ts` |
| `generationsRouter` | (a typed view over observations where `type=GENERATION`) |
| `scoresRouter` | `…/routers/scores.ts` |
| `scoreConfigsRouter` | `…/routers/scoreConfigs.ts` |
| `scoreAnalyticsRouter` | `…/features/score-analytics/server/scoreAnalyticsRouter.ts` |
| `eventsRouter` | `…/features/events/server/eventsRouter.ts` |
| `dashboardRouter` | `…/routers/dashboards.ts` |
| `dashboardWidgetRouter` | `…/routers/dashboardWidgets.ts` |
| `tableRouter` | `…/routers/tables.ts` (batch-action progress) |
| `TableViewPresetsRouter` | `…/routers/tableViewPresets.ts` |
| `monitorsRouter` | `…/routers/monitors.ts` |

**Pattern:** Go binary on [Hanzo Base](../base) (embedded encrypted SQLite +
plugins) exposing a typed [ZAP](../zap) capability-RPC interface. No Prisma, no
Postgres-as-source-of-truth, no Mongo, no tRPC in the backend. ClickHouse is
**not** the source of truth here — Base is the OLTP store; the heavy columnar
analytics aggregations are documented stubs awaiting a Go analytics shim (see
below).

```
.zap schema (source of truth)  ──zapgen──▶  gen/ (Go views)
        │                                        │
        └──zapgen --target=ts──▶ console TS      ▼
                                   server/  ─ ZAP RPC handlers (cap-gated)
                                   main.go  ─ base.New() + ZAP router :9992
                                              Base HTTP (health/metrics) :8090
                                              vault → per-org encrypted SQLite
```

## Run

```bash
make build
./observability serve --http=127.0.0.1:8090 --zap=127.0.0.1:9992
```

Optional per-org encrypted SQLite (vault plugin): add `--vaultDir=/data/vaults`.
The OLTP collections (`obs_trace`, `obs_observation`, `obs_session`, `obs_score`,
`obs_score_config`) are created at bootstrap and populated by ingestion — there
are no env-seeded rows.

## Smoke test

In-process suite (per-method CRUD + capability gate + tenant isolation +
pipelining proof):

```bash
make test
```

Out-of-process probe against a live binary:

```bash
./observability serve --zap=127.0.0.1:9992 &
go run ./cmd/probe --addr 127.0.0.1:9992 --peer observability
```

## The interface

ZAP msgType **203**. Wire envelope: `(method:u32, promiseID:u32, target:u32,
cap:bytes, payload:bytes)`; responses `(status:u32, promiseID:u32, body:bytes)`.
Each method is gated on exactly one `ObsPermissions` bit at the single chokepoint
`Server.authorize` (Wrap cap → enforce `Kind==CapKindIAMSession` → enforce the
bit → verify signature when an issuer registry is wired, per
[zap-spec SPEC.md §2.3](../../zap-proto/zap-spec/SPEC.md)).

### Honest methods (OLTP on Base)

| method | @ord | permission | one-liner |
|---|---|---|---|
| `traceById` | 0 | TraceRead | one trace by (project, id) |
| `traceWithDetail` | 1 | TraceRead | trace + its observations + scores + latency |
| `traceBookmark` | 2 | TraceWrite | set the bookmark flag |
| `tracePublish` | 3 | TraceWrite | set the public flag |
| `traceUpdateTags` | 4 | TraceWrite | replace the tag list |
| `traceDeleteMany` | 5 | TraceWrite | delete N traces, returns count |
| `observationById` | 6 | ObservationRead | one observation/generation by id |
| `eventBatchIO` | 81 | ObservationRead | bulk (input,output,metadata) for N observations |
| `sessionHasAny` | 20 | SessionRead | does the project have any session |
| `sessionById` | 21 | SessionRead | session + its scores + trace count |
| `sessionBookmark` | 22 | SessionWrite | materialize+set bookmark |
| `sessionPublish` | 23 | SessionWrite | materialize+set public |
| `scoreById` | 40 | ScoreRead | one score by id |
| `scoreHasAny` | 41 | ScoreRead | does the project have any score |
| `eventScoresForTrace` | 80 | ScoreRead | every score on a trace |
| `scoreCreateAnnotation` | 42 | ScoreWrite | create an annotation score |
| `scoreUpdateAnnotation` | 43 | ScoreWrite | update an annotation score |
| `scoreDeleteAnnotation` | 44 | ScoreWrite | delete an annotation score |
| `scoreConfigAll` | 60 | ScoreConfigRead | paginated config list + total |
| `scoreConfigById` | 61 | ScoreConfigRead | one config by id |
| `scoreConfigCreate` | 62 | ScoreConfigWrite | create a score config |
| `scoreConfigUpdate` | 63 | ScoreConfigWrite | patch a score config |

### Stubbed methods (ClickHouse aggregations — follow-up)

These are columnar table-scans / group-bys / cross-tabulations that cannot be
honestly served from Base OLTP rows without either fabricating numbers or
silently degrading to a full scan per request. They return `Empty{Stubbed:true}`
(still capability-gated on `AnalyticsRead`, still parameter-validated). The exact
upstream ClickHouse queries to port are enumerated in `server/handlers.go`
`handleStub` — wiring a Go analytics reader (`server/analytics.go`) is purely
additive, no API/auth change.

| method | @ord | replaces (ClickHouse helper) |
|---|---|---|
| `traceAll` / `traceCountAll` | 100 / 101 | `getTracesTable` / `…Count` |
| `traceMetrics` | 102 | `getTracesTableMetrics` (quantiles, token/cost sums) |
| `traceFilterOptions` | 103 | `getTracesGroupedByName/Tags/Users` |
| `sessionAll` / `sessionCountAll` | 120 / 121 | `getSessionsTable` / `…Count` |
| `scoreAll` / `scoreCountAll` | 140 / 141 | `getScoresUiTable` / `…Count` |
| `eventAll` | 160 | `getEventList` |
| `analyticsScoreComparison` | 180 | `getScoreComparisonAnalytics` (heaviest) |

Nested messages (the `Trace` inside `TraceWithDetail`, the `Session` inside
`SessionWithScores`, and every `list<…>` element) are carried as pre-encoded
`bytes` and `Wrap*`-ed on read — the one way to nest a ZAP message, identical to
how the envelope carries the payload/body.

## Wiring the console to this service (handoff)

The console keeps its TS client (same `.zap` contract, compiled to TS by
`zapgen --target=ts`). Its ZAP bridge substitutes the in-process tRPC routers for
a ZAP client to this service at msgType **203**; method ordinals match the schema
header. The cap is the opaque ZAP capability buffer; the body is the
zapgen-compiled struct bytes. This repo does **not** edit console.

## Layout

| Path | What |
|------|------|
| `proto/observability.zap` | Canonical schema (zap-spec dialect → Go + TS). |
| `gen/` | zapgen output (`make zap-gen`). Generated; do not edit. |
| `server/wire.go` | Transport envelope codec + msgType/method/status consts. |
| `server/server.go` | Cap auth (`ObsPermissions`), dispatch, promise pipelining. |
| `server/handlers.go` | Per-method bodies + record↔view mapping + stubs. |
| `server/collections.go` | Base collection schema (fields, indexes, FK by value). |
| `server/client.go` | Typed client (one method per procedure) + pipelining + `SyntheticCap`. |
| `server/ids.go` | External-id minting (Base id generator). |
| `main.go` | Binary: `base.New()` + vault + ZAP router :9992. |
| `cmd/probe/` | Out-of-process smoke probe. |

Container registry: `ghcr.io/hanzoai/observability` (CI-built, multi-arch).
