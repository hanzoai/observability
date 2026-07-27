package server

import (
	"strconv"

	"github.com/hanzoai/base/core"

	gen "github.com/hanzoai/observability/gen"
)

// encode.go is the single place a Base record is projected onto its generated
// wire struct. One encoder per model; handlers call these so the record→view
// mapping lives in exactly one location (no per-handler field plucking).
//
// JSON columns (definition, filters, dimensions, …) are emitted verbatim as the
// text they were stored as — the UI parses them, the transport never does. The
// created/updated columns are emitted as their stored autodate strings.

func encodeDashboard(r *core.Record) []byte {
	return gen.NewDashboard(gen.DashboardInput{
		Id:          r.Id,
		ProjectId:   r.GetString(fProjectId),
		Name:        r.GetString(fName),
		Description: r.GetString(fDescription),
		Owner:       r.GetString(fOwner),
		Definition:  r.GetString(fDefinition),
		Filters:     r.GetString(fFilters),
		CreatedBy:   r.GetString(fCreatedBy),
		CreatedAt:   r.GetString("created"),
		UpdatedAt:   r.GetString("updated"),
	})
}

func encodeWidget(r *core.Record) []byte {
	return gen.NewWidget(gen.WidgetInput{
		Id:          r.Id,
		ProjectId:   r.GetString(fProjectId),
		Name:        r.GetString(fName),
		Description: r.GetString(fDescription),
		View:        r.GetString(fView),
		Owner:       r.GetString(fOwner),
		Dimensions:  r.GetString(fDimensions),
		Metrics:     r.GetString(fMetrics),
		Filters:     r.GetString(fFilters),
		ChartType:   r.GetString(fChartType),
		ChartConfig: r.GetString(fChartConfig),
		CreatedAt:   r.GetString("created"),
		UpdatedAt:   r.GetString("updated"),
	})
}

func encodePreset(r *core.Record) []byte {
	return gen.NewPreset(gen.PresetInput{
		Id:               r.Id,
		ProjectId:        r.GetString(fProjectId),
		Name:             r.GetString(fName),
		TableName:        r.GetString(fTableName),
		Filters:          r.GetString(fFilters),
		ColumnOrder:      r.GetString(fColumnOrder),
		ColumnVisibility: r.GetString(fColumnVisibility),
		SearchQuery:      r.GetString(fSearchQuery),
		OrderByColumn:    r.GetString(fOrderByColumn),
		OrderByOrder:     r.GetString(fOrderByOrder),
		CreatedBy:        r.GetString(fCreatedBy),
		CreatedAt:        r.GetString("created"),
		UpdatedAt:        r.GetString("updated"),
	})
}

func encodeMonitor(r *core.Record) []byte {
	return gen.NewMonitor(gen.MonitorInput{
		Id:                r.Id,
		ProjectId:         r.GetString(fProjectId),
		Name:              r.GetString(fName),
		View:              r.GetString(fView),
		Filters:           r.GetString(fFilters),
		Metric:            r.GetString(fMetric),
		Window:            r.GetString(fWindow),
		ThresholdOperator: r.GetString(fThresholdOperator),
		AlertThreshold:    r.GetFloat(fAlertThreshold),
		WarningThreshold:  nullableTextToFloat(r.GetString(fWarningThreshold)),
		NoData:            r.GetString(fNoData),
		Renotify:          r.GetString(fRenotify),
		Tags:              r.GetString(fTags),
		Status:            r.GetString(fStatus),
		Severity:          r.GetString(fSeverity),
		SeverityChangedAt: r.GetString(fSeverityChangedAt),
		AlertedAt:         r.GetString(fAlertedAt),
		NextRunAt:         r.GetString(fNextRunAt),
		CreatedBy:         r.GetString(fCreatedBy),
		CreatedAt:         r.GetString("created"),
		UpdatedAt:         r.GetString("updated"),
	})
}

// nullableTextToFloat decodes the warningThreshold storage column: "" → the NaN
// null sentinel, else the parsed decimal. Inverse of floatToNullableText.
func nullableTextToFloat(s string) float64 {
	if s == "" {
		return nullThreshold()
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nullThreshold()
	}
	return f
}
