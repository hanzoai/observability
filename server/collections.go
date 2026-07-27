package server

import (
	"github.com/hanzoai/base/core"
	"github.com/hanzoai/base/tools/hook"
)

// Base collections backing the observability OLTP data model. One collection per
// Langfuse entity. Every collection carries `projectId` (the tenant scope the
// verified capability binds to) plus a stable external `extId` (the
// caller-supplied trace/observation/score id — Base's own record id is internal).
// Reads scope to projectId; the service NEVER returns a row outside the
// capability's project.
//
// This mirrors the Prisma/ClickHouse schema the 8 tRPC routers read. ClickHouse
// is the analytics store upstream; here Base is the OLTP source of truth (per
// the migration directive "use Base for OLTP; TODO ClickHouse shim"). The
// analytics aggregations that genuinely need columnar scans are stubbed in
// handlers.go — they are NOT backed by these collections.
const (
	ColTrace       = "obs_trace"
	ColObservation = "obs_observation"
	ColSession     = "obs_session"
	ColScore       = "obs_score"
	ColScoreConfig = "obs_score_config"
)

// Field names — shared across collections.go and handlers.go so the read/write
// paths reference one source of truth (no string literals scattered around).
const (
	fProjectId = "projectId"
	fExtId     = "extId" // caller-facing id (trace/observation/score/session/config id)

	// trace
	fName       = "name"
	fUserId     = "userId"
	fSessionId  = "sessionId"
	fRelease    = "release"
	fVersion    = "version"
	fInput      = "input"
	fOutput     = "output"
	fMetadata   = "metadata"
	fTags       = "tags" // JSON []string
	fBookmarked = "bookmarked"
	fPublic     = "public"
	fTimestamp  = "timestamp"

	// observation
	fTraceId             = "traceId"
	fType                = "type"
	fParentObservationId = "parentObservationId"
	fStartTime           = "startTime"
	fEndTime             = "endTime"
	fLevel               = "level"
	fStatusMessage       = "statusMessage"
	fModel               = "model"
	fInternalModelId     = "internalModelId"
	fPromptTokens        = "promptTokens"
	fCompletionTokens    = "completionTokens"
	fTotalTokens         = "totalTokens"

	// score
	fValue         = "value"
	fStringValue   = "stringValue"
	fDataType      = "dataType"
	fSource        = "source"
	fComment       = "comment"
	fObservationId = "observationId"
	fConfigId      = "configId"
	fQueueId       = "queueId"
	fAuthorUserId  = "authorUserId"
	fEnvironment   = "environment"

	// score config
	fMinValue    = "minValue"
	fMaxValue    = "maxValue"
	fHasMin      = "hasMin"
	fHasMax      = "hasMax"
	fCategories  = "categories" // JSON [{label,value}]
	fDescription = "description"
	fIsArchived  = "isArchived"
)

// RegisterCollections provisions every observability collection at bootstrap.
// Idempotent: re-running finds existing collections and no-ops. Wired via
// OnBootstrap so it runs once before the ZAP listener accepts calls. Unlike the
// ui-customization template there is no env-seeded default row — these are OLTP
// tables populated by ingestion, empty until written.
func RegisterCollections(app core.App) {
	app.OnBootstrap().Bind(&hook.Handler[*core.BootstrapEvent]{
		Id: "observabilityCollections",
		Func: func(e *core.BootstrapEvent) error {
			if err := e.Next(); err != nil {
				return err
			}
			return EnsureCollections(app)
		},
	})
}

// EnsureCollections creates all collections if absent, in FK order (parents
// before children so relations resolve by value). Exported so tests and one-shot
// migrations can provision directly. Idempotent.
func EnsureCollections(app core.App) error {
	if err := ensureTrace(app); err != nil {
		return err
	}
	if err := ensureObservation(app); err != nil {
		return err
	}
	if err := ensureSession(app); err != nil {
		return err
	}
	if err := ensureScoreConfig(app); err != nil {
		return err
	}
	if err := ensureScore(app); err != nil {
		return err
	}
	// The dashboards surface (dash_collections.go) — saved queries over the
	// tables above, provisioned in the same app so the analytics shim can join
	// them without leaving the process.
	return ensureDashboardCollections(app)
}

func has(app core.App, name string) bool {
	_, err := app.FindCollectionByNameOrId(name)
	return err == nil
}

// text builds a long-text field (URLs, IO blobs, JSON-as-text). 1 MiB cap covers
// large IO payloads without unbounding the row.
func text(name string) *core.TextField {
	return &core.TextField{Name: name, Max: 1 << 20}
}

func ensureTrace(app core.App) error {
	if has(app, ColTrace) {
		return nil
	}
	c := core.NewBaseCollection(ColTrace)
	c.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fExtId, Required: true, Max: 255})
	c.Fields.Add(text(fName), text(fUserId), text(fSessionId), text(fRelease), text(fVersion))
	c.Fields.Add(text(fInput), text(fOutput), text(fMetadata))
	c.Fields.Add(&core.JSONField{Name: fTags, MaxSize: 1 << 16})
	c.Fields.Add(&core.BoolField{Name: fBookmarked}, &core.BoolField{Name: fPublic})
	c.Fields.Add(&core.NumberField{Name: fTimestamp})
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})
	// One trace per (project, extId). Lookups are always project-scoped by extId.
	c.AddIndex("idx_trace_project_ext", true, fProjectId+","+fExtId, "")
	c.AddIndex("idx_trace_project_session", false, fProjectId+","+fSessionId, "")
	c.AddIndex("idx_trace_project_ts", false, fProjectId+","+fTimestamp, "")
	return app.Save(c)
}

func ensureObservation(app core.App) error {
	if has(app, ColObservation) {
		return nil
	}
	c := core.NewBaseCollection(ColObservation)
	c.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fExtId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fTraceId, Max: 255}) // FK → trace.extId (by value, cross-store safe)
	c.Fields.Add(text(fType), text(fName), text(fParentObservationId))
	c.Fields.Add(&core.NumberField{Name: fStartTime}, &core.NumberField{Name: fEndTime})
	c.Fields.Add(text(fInput), text(fOutput), text(fMetadata))
	c.Fields.Add(text(fLevel), text(fStatusMessage), text(fModel), text(fInternalModelId))
	c.Fields.Add(&core.NumberField{Name: fPromptTokens}, &core.NumberField{Name: fCompletionTokens}, &core.NumberField{Name: fTotalTokens})
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})
	c.AddIndex("idx_obs_project_ext", true, fProjectId+","+fExtId, "")
	// Child lookup: every observation for a trace (the trace-detail join).
	c.AddIndex("idx_obs_project_trace", false, fProjectId+","+fTraceId, "")
	return app.Save(c)
}

func ensureSession(app core.App) error {
	if has(app, ColSession) {
		return nil
	}
	c := core.NewBaseCollection(ColSession)
	c.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fExtId, Required: true, Max: 255})
	c.Fields.Add(&core.BoolField{Name: fBookmarked}, &core.BoolField{Name: fPublic})
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})
	c.AddIndex("idx_session_project_ext", true, fProjectId+","+fExtId, "")
	return app.Save(c)
}

func ensureScoreConfig(app core.App) error {
	if has(app, ColScoreConfig) {
		return nil
	}
	c := core.NewBaseCollection(ColScoreConfig)
	c.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fExtId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fName, Max: 1024})
	c.Fields.Add(text(fDataType))
	c.Fields.Add(&core.NumberField{Name: fMinValue}, &core.NumberField{Name: fMaxValue})
	c.Fields.Add(&core.BoolField{Name: fHasMin}, &core.BoolField{Name: fHasMax})
	c.Fields.Add(&core.JSONField{Name: fCategories, MaxSize: 1 << 16})
	c.Fields.Add(text(fDescription))
	c.Fields.Add(&core.BoolField{Name: fIsArchived})
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})
	c.AddIndex("idx_scfg_project_ext", true, fProjectId+","+fExtId, "")
	c.AddIndex("idx_scfg_project", false, fProjectId, "")
	return app.Save(c)
}

func ensureScore(app core.App) error {
	if has(app, ColScore) {
		return nil
	}
	c := core.NewBaseCollection(ColScore)
	c.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fExtId, Required: true, Max: 255})
	c.Fields.Add(&core.TextField{Name: fName, Max: 1024})
	c.Fields.Add(&core.NumberField{Name: fValue})
	c.Fields.Add(text(fStringValue), text(fDataType), text(fSource), text(fComment))
	c.Fields.Add(&core.TextField{Name: fTraceId, Max: 255})       // FK → trace.extId
	c.Fields.Add(&core.TextField{Name: fObservationId, Max: 255}) // FK → observation.extId
	c.Fields.Add(&core.TextField{Name: fSessionId, Max: 255})     // FK → session.extId
	c.Fields.Add(&core.TextField{Name: fConfigId, Max: 255})      // FK → score_config.extId
	c.Fields.Add(text(fQueueId), text(fAuthorUserId), text(fEnvironment), text(fMetadata))
	c.Fields.Add(&core.NumberField{Name: fTimestamp})
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})
	c.AddIndex("idx_score_project_ext", true, fProjectId+","+fExtId, "")
	// Trace-detail / events.scoresForTrace join: every score on a trace.
	c.AddIndex("idx_score_project_trace", false, fProjectId+","+fTraceId, "")
	c.AddIndex("idx_score_project_session", false, fProjectId+","+fSessionId, "")
	return app.Save(c)
}
