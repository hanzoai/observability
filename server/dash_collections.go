package server

import (
	"github.com/hanzoai/base/core"
)

// dash_collections.go — the five Base collections behind the dashboards surface,
// one per model the migrated tRPC routers persisted. Every row is scoped by
// projectId; the service reads and writes ONLY within the project scope() has
// resolved, which is the multi-tenant boundary (mirrors the console's
// throwIfNoProjectAccess(projectId)).
//
// These live alongside the obs_* OLTP tables rather than in their own store: a
// dashboard is a saved query over those tables, so keeping both in one Base app
// is what lets the analytics shim join them without a network hop.
const (
	CollectionDashboard = "dashboards"         // dashboardRouter
	CollectionWidget    = "dashboard_widgets"  // dashboardWidgetRouter
	CollectionTable     = "table_batch_jobs"   // tableRouter (batch-action job tracking)
	CollectionPreset    = "table_view_presets" // TableViewPresetsRouter
	CollectionMonitor   = "monitors"           // monitorsRouter
)

// Column names for the dashboards surface. ONE constant per COLUMN NAME, shared
// by every collection that uses it — `description` is the same column name
// whether it sits on a dashboard or a widget, so it is one constant, not three.
// (fProjectId, fName, fDescription and fTags are declared in collections.go with
// the obs_* tables and reused here.)
const (
	fCreatedBy = "createdBy"
	fOwner     = "owner"
	fView      = "view"
	fFilters   = "filters"

	// dashboard
	fDefinition = "definition"

	// widget
	fDimensions  = "dimensions"
	fMetrics     = "metrics"
	fChartType   = "chartType"
	fChartConfig = "chartConfig"

	// table batch job
	fTableName = "tableName"
	fActionId  = "actionId"
	fState     = "state"

	// table view preset
	fColumnOrder      = "columnOrder"
	fColumnVisibility = "columnVisibility"
	fSearchQuery      = "searchQuery"
	fOrderByColumn    = "orderByColumn"
	fOrderByOrder     = "orderByOrder"

	// monitor
	fMetric            = "metric"
	fWindow            = "window"
	fThresholdOperator = "thresholdOperator"
	fAlertThreshold    = "alertThreshold"
	fWarningThreshold  = "warningThreshold"
	fNoData            = "noData"
	fRenotify          = "renotify"
	fStatus            = "status"
	fSeverity          = "severity"
	fSeverityChangedAt = "severityChangedAt"
	fAlertedAt         = "alertedAt"
	fNextRunAt         = "nextRunAt"
)

// dashText adds TextFields sized for short scalars. The obs_* tables use text()
// (1 MiB) because they hold IO blobs; these columns are names, enums and
// timestamps, so they take a tighter cap.
func dashText(col *core.Collection, names ...string) {
	for _, n := range names {
		col.Fields.Add(&core.TextField{Name: n, Max: 4096})
	}
}

// jsonField adds JSONFields sized for structured blobs (definitions, filters).
func jsonField(col *core.Collection, names ...string) {
	for _, n := range names {
		col.Fields.Add(&core.JSONField{Name: n, MaxSize: 262144})
	}
}

// timestamps adds the created/updated autodate pair every collection carries.
func timestamps(col *core.Collection) {
	col.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true})
	col.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true})
}

// ensureDashboardCollections creates any missing dashboards-surface collection.
// Called from EnsureCollections alongside the obs_* tables; idempotent.
func ensureDashboardCollections(app core.App) error {
	for _, build := range []func(core.App) error{
		ensureDashboard, ensureWidget, ensureTable, ensurePreset, ensureMonitor,
	} {
		if err := build(app); err != nil {
			return err
		}
	}
	return nil
}

func ensureDashboard(app core.App) error {
	if has(app, CollectionDashboard) {
		return nil
	}
	col := core.NewBaseCollection(CollectionDashboard)
	col.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	dashText(col, fName, fDescription, fOwner, fCreatedBy)
	jsonField(col, fDefinition, fFilters)
	timestamps(col)
	col.AddIndex("idx_dashboards_project", false, fProjectId, "")
	return app.Save(col)
}

func ensureWidget(app core.App) error {
	if has(app, CollectionWidget) {
		return nil
	}
	col := core.NewBaseCollection(CollectionWidget)
	col.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	dashText(col, fName, fDescription, fView, fOwner, fChartType, fCreatedBy)
	jsonField(col, fDimensions, fMetrics, fFilters, fChartConfig)
	timestamps(col)
	col.AddIndex("idx_widgets_project", false, fProjectId, "")
	return app.Save(col)
}

func ensureTable(app core.App) error {
	if has(app, CollectionTable) {
		return nil
	}
	col := core.NewBaseCollection(CollectionTable)
	col.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	dashText(col, fTableName, fActionId, fState, fCreatedBy)
	timestamps(col)
	col.AddIndex("idx_table_jobs_project", false, fProjectId, "")
	return app.Save(col)
}

func ensurePreset(app core.App) error {
	if has(app, CollectionPreset) {
		return nil
	}
	col := core.NewBaseCollection(CollectionPreset)
	col.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	dashText(col, fName, fTableName, fSearchQuery, fOrderByColumn, fOrderByOrder, fCreatedBy)
	jsonField(col, fFilters, fColumnOrder, fColumnVisibility)
	timestamps(col)
	// Unique (projectId, tableName, name): mirrors the console P2002 conflict the
	// TableViewPresetsRouter maps to HanzoConflictError.
	col.AddIndex("idx_presets_unique", true, fProjectId+", "+fTableName+", "+fName, "")
	return app.Save(col)
}

func ensureMonitor(app core.App) error {
	if has(app, CollectionMonitor) {
		return nil
	}
	col := core.NewBaseCollection(CollectionMonitor)
	col.Fields.Add(&core.TextField{Name: fProjectId, Required: true, Max: 255})
	dashText(col, fName, fView, fThresholdOperator, fStatus,
		fSeverity, fSeverityChangedAt, fAlertedAt, fNextRunAt, fCreatedBy)
	col.Fields.Add(&core.NumberField{Name: fAlertThreshold})
	// warningThreshold is z.number().nullable() upstream. Base's NumberField is
	// NUMERIC DEFAULT 0 NOT NULL — it cannot hold SQL NULL — so the nullable
	// number is stored as text: a decimal string when set, "" when null. This
	// preserves the source's three-state (set / null) faithfully; the wire still
	// carries an f64 with NaN as the null sentinel (see dash_encode.go).
	dashText(col, fWarningThreshold)
	jsonField(col, fFilters, fMetric, fWindow, fNoData, fRenotify, fTags)
	timestamps(col)
	col.AddIndex("idx_monitors_project", false, fProjectId, "")
	return app.Save(col)
}
