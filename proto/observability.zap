# observability.zap — canonical wire schema for the Observability service.
#
# Dialect: zap-spec (the `package … / Field Type @off` grammar that
# github.com/zap-proto/go/cmd/zapgen consumes) — the SAME dialect the capability
# schema (zap-spec/capabilities.zap) is written in. NOT capnp. ONE schema, two
# code targets: this Go service binary compiles it with `zapgen` (default → Go
# views); the console copies it verbatim and compiles `--target=ts`. The field
# set below is the single byte-for-byte contract both speak.
#
# This service replaces 8 console tRPC routers — the Langfuse-style OLTP backbone
# (traces + observations + scores) plus their satellites (sessions, scoreConfigs,
# events, scoreAnalytics). Reference: console/web/src/server/api/routers/{traces,
# observations,sessions,scores,scoreConfigs}.ts and features/{score-analytics,
# events}/server/*.ts.
#
# RPC surface (hand-dispatched in server/, exactly as cap/ hand-writes Verify on
# top of zapgen'd views — zapgen emits DATA views, never method stubs). The
# numeric @n ordinals are the method ids in server/wire.go:
#
#   interface Observability @ MsgTypeRouterBase (203) {
#     # --- traces (OLTP, honest) ----------------------------------------
#     traceById                  @0  (TraceByIdParams)        -> (Trace)
#     traceWithDetail            @1  (TraceByIdParams)        -> (TraceWithDetail)
#     traceBookmark              @2  (TraceBookmarkParams)    -> (Trace)
#     tracePublish               @3  (TracePublishParams)     -> (Trace)
#     traceUpdateTags            @4  (TraceUpdateTagsParams)  -> (Trace)
#     traceDeleteMany            @5  (TraceDeleteManyParams)  -> (MutationCount)
#     # --- observations (OLTP, honest) ----------------------------------
#     observationById            @6  (ObservationByIdParams)  -> (Observation)
#     # --- sessions (OLTP, honest) --------------------------------------
#     sessionHasAny              @20 (ProjectScope)           -> (BoolResult)
#     sessionById                @21 (SessionByIdParams)      -> (SessionWithScores)
#     sessionBookmark            @22 (SessionBookmarkParams)  -> (Session)
#     sessionPublish             @23 (SessionPublishParams)   -> (Session)
#     # --- scores (OLTP, honest) ----------------------------------------
#     scoreById                  @40 (ScoreByIdParams)        -> (Score)
#     scoreHasAny                @41 (ProjectScope)           -> (BoolResult)
#     scoreCreateAnnotation      @42 (AnnotationScoreParams)  -> (Score)
#     scoreUpdateAnnotation      @43 (AnnotationScoreParams)  -> (Score)
#     scoreDeleteAnnotation      @44 (ScoreByIdParams)        -> (MutationCount)
#     # --- scoreConfigs (OLTP, honest — pure Prisma upstream) -----------
#     scoreConfigAll             @60 (ScoreConfigAllParams)   -> (ScoreConfigList)
#     scoreConfigById            @61 (ScoreConfigByIdParams)  -> (ScoreConfig)
#     scoreConfigCreate          @62 (ScoreConfigWriteParams) -> (ScoreConfig)
#     scoreConfigUpdate          @63 (ScoreConfigWriteParams) -> (ScoreConfig)
#     # --- events (OLTP read by FK, honest) -----------------------------
#     eventScoresForTrace        @80 (EventScoresParams)      -> (ScoreList)
#     eventBatchIO               @81 (EventBatchIOParams)     -> (ObservationIOList)
#     # --- analytics (ClickHouse aggregations — STUBBED w/ TODO) --------
#     traceAll                   @100 (TableQueryParams)      -> (Empty)   [STUB]
#     traceCountAll              @101 (TableQueryParams)      -> (Empty)   [STUB]
#     traceMetrics               @102 (TableQueryParams)      -> (Empty)   [STUB]
#     traceFilterOptions         @103 (ProjectScope)          -> (Empty)   [STUB]
#     sessionAll                 @120 (TableQueryParams)      -> (Empty)   [STUB]
#     sessionCountAll            @121 (TableQueryParams)      -> (Empty)   [STUB]
#     scoreAll                   @140 (TableQueryParams)      -> (Empty)   [STUB]
#     scoreCountAll              @141 (TableQueryParams)      -> (Empty)   [STUB]
#     eventAll                   @160 (TableQueryParams)      -> (Empty)   [STUB]
#     analyticsScoreComparison   @180 (AnalyticsParams)       -> (Empty)   [STUB]
#   }
#
# Permission model: the caller's verified Capability (CapKindIAMSession = 0x01)
# carries a u64 Permissions bitmask. Each method gates on exactly one bit (the
# ObsPermissions consts in server/server.go) at the single chokepoint
# requirePermission(cap, bit). Bits are grouped by procedure category (read vs
# write per resource) so a capability is mintable least-privilege. See
# zap-spec/capabilities_kinds.md.

package obs

# ============================ shared envelopes ==============================

# ProjectScope is the bare project-id every procedure carries (the tRPC
# `projectId` that protectedProjectProcedure requires). Used directly by the
# parameter-less reads (hasAny, filterOptions).
struct ProjectScope {
    ProjectId text @0
}

# BoolResult wraps the boolean returns (hasAny). Distinct, pipeline-able.
struct BoolResult {
    Value bool @0
}

# MutationCount is the affected-row count returned by deletes/counts.
struct MutationCount {
    Count u64 @0
}

# ============================== traces =====================================

# TraceByIdParams mirrors traceRouter.byId / byIdWithObservationsAndScores
# input. Timestamp/FromTimestamp are unix-seconds (0 = unset); upstream they are
# a ClickHouse partition hint, so they are advisory here.
struct TraceByIdParams {
    ProjectId     text @0
    TraceId       text @8
    Timestamp     i64  @16
    FromTimestamp i64  @24
}

# Trace is the OLTP trace row. Mirrors the Prisma/ClickHouse Trace the tRPC
# `byId` returned, with metadata/input/output already stringified. Tags is a
# JSON-encoded []string carried as text.
struct Trace {
    Id         text @0
    ProjectId  text @8
    Name       text @16
    UserId     text @24
    SessionId  text @32
    Release    text @40
    Version    text @48
    Input      text @56
    Output     text @64
    Metadata   text @72
    Tags       text @80   # JSON []string
    Bookmarked bool @88
    Public     bool @89
    Timestamp  i64  @90
    CreatedAt  i64  @98
    UpdatedAt  i64  @106
    Present    bool @114  # false => no such trace (wire model of tRPC NOT_FOUND)
}

# TraceWithDetail is byIdWithObservationsAndScores: the trace plus its child
# observations and scores. Sub-messages are carried as pre-encoded `bytes`
# (Wrap* on read), the SAME way the list<…> elements and the top-level envelope
# Payload/Body carry messages — one way to nest a message: as opaque bytes,
# never an inline struct (zapgen's inline-struct embed does not round-trip a
# Finish()'d sub-message, since that includes the zap header). Latency is seconds
# (float) computed from observation start/end spans server-side.
struct TraceWithDetail {
    Trace        bytes             @0    # encoded Trace; WrapTrace on read
    Observations list<Observation> @8
    Scores       list<Score>       @16
    Latency      f64               @24
    Present      bool              @32
}

struct TraceBookmarkParams {
    ProjectId  text @0
    TraceId    text @8
    Bookmarked bool @16
}

struct TracePublishParams {
    ProjectId text @0
    TraceId   text @8
    Public    bool @16
}

struct TraceUpdateTagsParams {
    ProjectId text @0
    TraceId   text @8
    Tags      text @16  # JSON []string
}

struct TraceDeleteManyParams {
    ProjectId text       @0
    TraceIds  list<text> @8
}

# ============================ observations =================================

# ObservationByIdParams mirrors observationsRouter.byId. Verbosity is 0=compact,
# 1=truncated, 2=full (default 2) — controls IO rendering.
struct ObservationByIdParams {
    ProjectId     text @0
    ObservationId text @8
    TraceId       text @16
    StartTime     i64  @24
    Verbosity     u8   @32
}

# Observation is the OLTP observation/span row (a generation is an observation
# with type=GENERATION — generationsRouter is a typed view over the same table).
struct Observation {
    Id                  text @0
    TraceId             text @8
    ProjectId           text @16
    Type                text @24   # SPAN | GENERATION | EVENT
    Name                text @32
    ParentObservationId text @40
    StartTime           i64  @48
    EndTime             i64  @56
    Input               text @64
    Output              text @72
    Metadata            text @80
    Level               text @88   # DEBUG | DEFAULT | WARNING | ERROR
    StatusMessage       text @96
    Model               text @104
    InternalModelId     text @112
    PromptTokens        i64  @120
    CompletionTokens    i64  @128
    TotalTokens         i64  @136
    Present             bool @144
}

# ObservationIO is the trimmed (id,input,output,metadata) shape eventBatchIO
# returns — IO fetched in bulk for a set of observations.
struct ObservationIO {
    Id       text @0
    Input    text @8
    Output   text @16
    Metadata text @24
}

struct ObservationIOList {
    Items list<ObservationIO> @0
}

struct ObservationRef {
    Id      text @0
    TraceId text @8
}

struct EventBatchIOParams {
    ProjectId    text                 @0
    Observations list<ObservationRef> @8
    MinStartTime i64                  @16
    MaxStartTime i64                  @24
    Truncated    bool                 @32
}

# ============================== sessions ===================================

struct SessionByIdParams {
    ProjectId text @0
    SessionId text @8
}

# Session is the OLTP session row. CountTraces is the number of traces sharing
# this SessionId (events FK rollup).
struct Session {
    Id          text @0
    ProjectId   text @8
    Bookmarked  bool @16
    Public      bool @17
    CreatedAt   i64  @18
    CountTraces u64  @26
    Present     bool @34
}

# SessionWithScores is byIdWithScores: the session plus the scores attached to
# its traces. Session is carried as pre-encoded `bytes` (WrapSession on read) —
# see the TraceWithDetail note on why nested messages are bytes, not inline
# structs.
struct SessionWithScores {
    Session bytes       @0    # encoded Session; WrapSession on read
    Scores  list<Score> @8
    Present bool        @16
}

struct SessionBookmarkParams {
    ProjectId  text @0
    SessionId  text @8
    Bookmarked bool @16
}

struct SessionPublishParams {
    ProjectId text @0
    SessionId text @8
    Public    bool @16
}

# =============================== scores ====================================

struct ScoreByIdParams {
    ProjectId text @0
    ScoreId   text @8
}

# Score is the OLTP score row. DataType ∈ {NUMERIC, CATEGORICAL, BOOLEAN, TEXT,
# CORRECTION}; Source ∈ {ANNOTATION, API, EVAL}. Value is the numeric value;
# StringValue the categorical/text value. Exactly one trace/observation/session
# target is set (the rest empty). ScoreConfigId is named (not ConfigId) so its
# generated offset const does not collide with the ScoreConfig struct's own
# field consts — zapgen uses a flat const namespace keyed by lowerFirst(struct)+
# field, and `Score.Config*` would clash with `ScoreConfig.*`.
struct Score {
    Id            text @0
    ProjectId     text @8
    Name          text @16
    Value         f64  @24
    StringValue   text @32
    DataType      text @40
    Source        text @48
    Comment       text @56
    TraceId       text @64
    ObservationId text @72
    SessionId     text @80
    ScoreConfigId text @88   # FK → ScoreConfig
    QueueId       text @96
    AuthorUserId  text @104
    Environment   text @112
    Metadata      text @120
    Timestamp     i64  @128
    CreatedAt     i64  @136
    UpdatedAt     i64  @144
    Present       bool @152
}

struct ScoreList {
    Items list<Score> @0
}

# AnnotationScoreParams mirrors CreateAnnotationScoreData / UpdateAnnotationScoreData
# (console/packages/shared/src/features/annotation/types.ts). Id empty on create.
# ScoreTargetType ∈ {trace, session}; for trace targets TraceId (and optional
# ObservationId) is set, for session targets SessionId is set.
struct AnnotationScoreParams {
    ProjectId       text @0
    Id              text @8
    Name            text @16
    Value           f64  @24
    StringValue     text @32
    DataType        text @40
    Comment         text @48
    ConfigId        text @56
    QueueId         text @64
    Environment     text @72
    ScoreTargetType text @80
    TraceId         text @88
    ObservationId   text @96
    SessionId       text @104
    Timestamp       i64  @112
    AuthorUserId    text @120
}

struct EventScoresParams {
    ProjectId text @0
    TraceId   text @8
    Timestamp i64  @16
}

# ============================ score configs ================================

struct ScoreConfigByIdParams {
    ProjectId text @0
    Id        text @8
}

# ScoreConfigAllParams: paginated list. Limit/Page are advisory (0 = all).
struct ScoreConfigAllParams {
    ProjectId text @0
    Limit     u32  @8
    Page      u32  @12
}

# ScoreConfig is the OLTP score-config row (pure Prisma upstream → maps cleanly).
# Categories is a JSON-encoded array of {label,value} objects carried as text.
struct ScoreConfig {
    Id          text @0
    ProjectId   text @8
    Name        text @16
    DataType    text @24    # NUMERIC | CATEGORICAL | BOOLEAN
    MinValue    f64  @32
    MaxValue    f64  @40
    HasMin      bool @48
    HasMax      bool @49
    Categories  text @50    # JSON [{label,value}]
    Description text @58
    IsArchived  bool @66
    CreatedAt   i64  @74
    UpdatedAt   i64  @82
    Present     bool @90
}

struct ScoreConfigList {
    Items      list<ScoreConfig> @0
    TotalCount u64               @8
}

# ScoreConfigWriteParams covers both create and update. On update Id is set; the
# Has*/Set* flags distinguish "field omitted" from "field set to zero" (the tRPC
# layer used optional()/nullish() for this).
struct ScoreConfigWriteParams {
    ProjectId     text @0
    Id            text @8
    Name          text @16
    DataType      text @24
    MinValue      f64  @32
    MaxValue      f64  @40
    HasMin        bool @48
    HasMax        bool @49
    Categories    text @50
    Description   text @58
    IsArchived    bool @66
    SetIsArchived bool @67   # update: was isArchived present in the patch?
    SetName       bool @68
    SetCategories bool @69
}

# ====================== analytics query envelopes (STUBS) ==================

# TableQueryParams is the generic paginated/filtered table query the analytics
# list/count procedures take (traceRouter.all/countAll/metrics,
# sessionRouter.all/countAll, scoresRouter.all/countAll, eventsRouter.all). Filter
# and OrderBy are carried as opaque JSON so the schema need not mirror the full
# console filter AST. These hit ClickHouse upstream → STUBBED.
struct TableQueryParams {
    ProjectId   text @0
    Filter      text @8    # JSON filter AST
    OrderBy     text @16   # JSON orderBy
    SearchQuery text @24
    Limit       u32  @32
    Page        u32  @36
}

# AnalyticsParams covers the score-analytics router (getScoreIdentifiers,
# estimateScoreComparisonSize, getScoreComparisonAnalytics) — a heavy ClickHouse
# cross-tabulation → STUBBED.
struct AnalyticsParams {
    ProjectId  text @0
    ScoreNames text @8    # JSON []string
    Filter     text @16   # JSON filter AST
    FromTime   i64  @24
    ToTime     i64  @32
}

# Empty is the placeholder result for STUBBED analytics methods: they return an
# Empty body with Stubbed=true so a caller distinguishes "honestly empty" from
# "analytics shim not yet wired". See server.go handleStub.
struct Empty {
    Stubbed bool @0
}
