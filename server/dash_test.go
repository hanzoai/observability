package server_test

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	zaplib "github.com/luxfi/zap"

	gen "github.com/hanzoai/observability/gen"
	"github.com/hanzoai/observability/server"
)

// dash_test.go — the dashboards-surface suite, ported unchanged when the
// standalone binary was folded in. The bodies are byte-identical to the ones
// that shipped with it so they can be diffed against the original; only the
// setup is adapted, via the three shims below, onto the SAME newHarness the
// obs suite uses. One server construction path, two suites.

const testProject = "proj-test"

// dashPortSeq allocates listener ports above the block the obs suite occupies
// (19700+), so the two suites never race for a port when run together.
var dashPortSeq int32 = 19800

func dashPort() int { return int(atomic.AddInt32(&dashPortSeq, 1)) }

// newService starts the merged service and reports the (addr, peer) pair the
// ported tests were written against. The harness registers its own t.Cleanup,
// so the returned stop is a no-op kept for call-shape compatibility.
func newService(t *testing.T) (addr, peer string, stop func()) {
	t.Helper()
	h := newHarness(t, dashPort())
	return h.addr, h.peer, func() {}
}

// dialClient is the one dial path both suites use.
func dialClient(t *testing.T, addr, peer string, perms uint64) (*server.Client, func()) {
	t.Helper()
	capBuf, err := server.SyntheticCap(perms)
	if err != nil {
		t.Fatalf("synthetic cap: %v", err)
	}
	port := dashPort()
	cli := zaplib.NewNode(zaplib.NodeConfig{NodeID: "dash-cli-" + itoa(port), Port: port, NoDiscovery: true})
	if err := cli.Start(); err != nil {
		t.Fatalf("client node start: %v", err)
	}
	t.Cleanup(func() { cli.Stop() })
	c, err := server.Dial(cli, addr, peer, capBuf)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // handshake settle
	return c, func() {}
}

// fullClient holds every bit — the honest-path default.
func fullClient(t *testing.T, addr, peer string) (*server.Client, func()) {
	return dialClient(t, addr, peer, allPerms)
}

// newClient holds exactly the bits given — the negative-path default.
func newClient(t *testing.T, addr, peer string, perms uint64) (*server.Client, func()) {
	return dialClient(t, addr, peer, perms)
}

func TestDashboardCRUD(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	c, stopC := fullClient(t, addr, peer)
	defer stopC()
	ctx, cancel := ctx5()
	defer cancel()

	// Create
	d, err := c.CreateDashboard(ctx, gen.CreateDashReqInput{ProjectId: testProject, Name: "Ops", Description: "ops board"})
	if err != nil {
		t.Fatalf("CreateDashboard: %v", err)
	}
	if d.Id() == "" || d.Name() != "Ops" || d.Owner() != "PROJECT" {
		t.Fatalf("unexpected created dashboard: id=%q name=%q owner=%q", d.Id(), d.Name(), d.Owner())
	}

	// Get
	got, err := c.GetDashboard(ctx, gen.IdReqInput{ProjectId: testProject, Id: d.Id()})
	if err != nil {
		t.Fatalf("GetDashboard: %v", err)
	}
	if got.Name() != "Ops" {
		t.Fatalf("GetDashboard name = %q, want Ops", got.Name())
	}

	// Update metadata
	if _, err := c.UpdateDashboardMetadata(ctx, gen.UpdateDashReqInput{
		ProjectId: testProject, DashboardId: d.Id(), Name: "Ops v2", Description: "renamed",
	}); err != nil {
		t.Fatalf("UpdateDashboardMetadata: %v", err)
	}

	// Update definition + filters (JSON blobs round-trip verbatim)
	const def = `{"widgets":[{"type":"widget","id":"w1","widgetId":"x","x":0,"y":0,"x_size":3,"y_size":3}]}`
	dd, err := c.UpdateDashboardDefinition(ctx, gen.DashDefReqInput{ProjectId: testProject, DashboardId: d.Id(), Definition: def})
	if err != nil {
		t.Fatalf("UpdateDashboardDefinition: %v", err)
	}
	if dd.Definition() != def {
		t.Fatalf("definition not round-tripped:\n got %q\nwant %q", dd.Definition(), def)
	}
	const filt = `[{"column":"name","operator":"=","value":"x","type":"string"}]`
	df, err := c.UpdateDashboardFilters(ctx, gen.DashFiltersReqInput{ProjectId: testProject, DashboardId: d.Id(), Filters: filt})
	if err != nil {
		t.Fatalf("UpdateDashboardFilters: %v", err)
	}
	if df.Filters() != filt {
		t.Fatalf("filters not round-tripped: got %q", df.Filters())
	}

	// Clone
	cl, err := c.CloneDashboard(ctx, gen.IdReqInput{ProjectId: testProject, Id: d.Id()})
	if err != nil {
		t.Fatalf("CloneDashboard: %v", err)
	}
	if cl.Name() != "Ops v2 (Clone)" {
		t.Fatalf("clone name = %q, want 'Ops v2 (Clone)'", cl.Name())
	}
	if cl.Definition() != def {
		t.Fatalf("clone did not copy definition")
	}

	// List: original + clone = 2
	list, err := c.AllDashboards(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("AllDashboards: %v", err)
	}
	if list.TotalCount() != 2 || list.Dashboards().Len() != 2 {
		t.Fatalf("AllDashboards total=%d len=%d, want 2/2", list.TotalCount(), list.Dashboards().Len())
	}

	// Delete the clone
	del, err := c.DeleteDashboard(ctx, gen.IdReqInput{ProjectId: testProject, Id: cl.Id()})
	if err != nil {
		t.Fatalf("DeleteDashboard: %v", err)
	}
	if !del.Success() {
		t.Fatalf("delete success=false")
	}
	if _, err := c.GetDashboard(ctx, gen.IdReqInput{ProjectId: testProject, Id: cl.Id()}); err == nil {
		t.Fatalf("expected deleted dashboard to be gone")
	}
	t.Logf("dashboard CRUD OK: created, got, updated, cloned, listed(2), deleted")
}

func TestWidgetCRUD(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	c, stopC := fullClient(t, addr, peer)
	defer stopC()
	ctx, cancel := ctx5()
	defer cancel()

	w, err := c.CreateWidget(ctx, gen.WidgetReqInput{
		ProjectId: testProject, Name: "Latency", Description: "p95", View: "observations",
		Dimensions: `[{"field":"model"}]`, Metrics: `[{"measure":"latency","agg":"p95"}]`,
		Filters: `[]`, ChartType: "LINE_TIME_SERIES", ChartConfig: `{"type":"LINE_TIME_SERIES"}`,
	})
	if err != nil {
		t.Fatalf("CreateWidget: %v", err)
	}
	if w.Id() == "" || w.View() != "observations" || w.Owner() != "PROJECT" {
		t.Fatalf("unexpected widget: %+v", w)
	}

	got, err := c.GetWidget(ctx, gen.IdReqInput{ProjectId: testProject, Id: w.Id()})
	if err != nil {
		t.Fatalf("GetWidget: %v", err)
	}
	if got.Metrics() != `[{"measure":"latency","agg":"p95"}]` {
		t.Fatalf("widget metrics not round-tripped: %q", got.Metrics())
	}

	upd, err := c.UpdateWidget(ctx, gen.WidgetReqInput{
		ProjectId: testProject, WidgetId: w.Id(), Name: "Latency p99", Description: "p99",
		View: "observations", Dimensions: `[]`, Metrics: `[{"measure":"latency","agg":"p99"}]`,
		Filters: `[]`, ChartType: "BAR_TIME_SERIES", ChartConfig: `{"type":"BAR_TIME_SERIES"}`,
	})
	if err != nil {
		t.Fatalf("UpdateWidget: %v", err)
	}
	if upd.Name() != "Latency p99" || upd.ChartType() != "BAR_TIME_SERIES" {
		t.Fatalf("widget update not applied: %+v", upd)
	}

	cp, err := c.CopyWidgetToProject(ctx, gen.CopyWidgetReqInput{
		ProjectId: testProject, WidgetId: w.Id(), DashboardId: "d1", PlacementId: "p1",
	})
	if err != nil {
		t.Fatalf("CopyWidgetToProject: %v", err)
	}
	if !cp.Success() || cp.Id() == "" {
		t.Fatalf("copy widget failed: %+v", cp)
	}

	list, err := c.AllWidgets(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("AllWidgets: %v", err)
	}
	if list.TotalCount() != 2 {
		t.Fatalf("AllWidgets total=%d, want 2 (original + copy)", list.TotalCount())
	}

	if _, err := c.DeleteWidget(ctx, gen.IdReqInput{ProjectId: testProject, Id: w.Id()}); err != nil {
		t.Fatalf("DeleteWidget: %v", err)
	}
	t.Logf("widget CRUD OK: created, got, updated, copied, listed(2), deleted")
}

func TestPresetCRUD(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	c, stopC := fullClient(t, addr, peer)
	defer stopC()
	ctx, cancel := ctx5()
	defer cancel()

	p, err := c.CreatePreset(ctx, gen.PresetReqInput{
		ProjectId: testProject, Name: "My traces", TableName: "traces",
		Filters: `[]`, ColumnOrder: `["id","name"]`, ColumnVisibility: `{"id":true}`,
		SearchQuery: "err", OrderByColumn: "timestamp", OrderByOrder: "DESC",
	})
	if err != nil {
		t.Fatalf("CreatePreset: %v", err)
	}
	if p.Id() == "" || p.TableName() != "traces" || p.ColumnOrder() != `["id","name"]` {
		t.Fatalf("unexpected preset: %+v", p)
	}

	// Duplicate (projectId, tableName, name) → conflict.
	if _, err := c.CreatePreset(ctx, gen.PresetReqInput{
		ProjectId: testProject, Name: "My traces", TableName: "traces",
		Filters: `[]`, ColumnOrder: `[]`, ColumnVisibility: `{}`,
	}); err == nil {
		t.Fatalf("expected duplicate preset to conflict")
	}

	got, err := c.GetPresetById(ctx, gen.IdReqInput{ProjectId: testProject, Id: p.Id()})
	if err != nil {
		t.Fatalf("GetPresetById: %v", err)
	}
	if got.SearchQuery() != "err" {
		t.Fatalf("preset searchQuery = %q, want err", got.SearchQuery())
	}

	if _, err := c.UpdatePresetName(ctx, gen.PresetNameReqInput{
		ProjectId: testProject, PresetId: p.Id(), Name: "My traces v2", TableName: "traces",
	}); err != nil {
		t.Fatalf("UpdatePresetName: %v", err)
	}

	byTable, err := c.GetPresetsByTableName(ctx, gen.PresetListReqInput{ProjectId: testProject, TableName: "traces"})
	if err != nil {
		t.Fatalf("GetPresetsByTableName: %v", err)
	}
	if byTable.Presets().Len() != 1 {
		t.Fatalf("GetPresetsByTableName len=%d, want 1", byTable.Presets().Len())
	}

	link, err := c.GeneratePermalink(ctx, gen.PermalinkReqInput{
		ProjectId: testProject, PresetId: p.Id(), TableName: "traces", BaseUrl: "https://app.hanzo.ai",
	})
	if err != nil {
		t.Fatalf("GeneratePermalink: %v", err)
	}
	want := "https://app.hanzo.ai/project/" + testProject + "/traces?viewId=" + p.Id()
	if link.Value() != want {
		t.Fatalf("permalink = %q, want %q", link.Value(), want)
	}

	if _, err := c.DeletePreset(ctx, gen.IdReqInput{ProjectId: testProject, Id: p.Id()}); err != nil {
		t.Fatalf("DeletePreset: %v", err)
	}
	t.Logf("preset CRUD OK: created, conflict-rejected, got, renamed, by-table(1), permalink, deleted")
}

func TestMonitorCRUD(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	c, stopC := fullClient(t, addr, peer)
	defer stopC()
	ctx, cancel := ctx5()
	defer cancel()

	m, err := c.CreateMonitor(ctx, gen.MonitorReqInput{
		ProjectId: testProject, Name: "Error rate", View: "traces",
		Filters: `[]`, Metric: `{"measure":"count","agg":"count"}`, Window: `{"value":5,"unit":"minutes"}`,
		ThresholdOperator: ">", AlertThreshold: 100, WarningThreshold: 50,
		NoData: `{"mode":"SILENT"}`, Renotify: `{"mode":"OFF"}`, Tags: `["prod"]`, Status: "active",
	})
	if err != nil {
		t.Fatalf("CreateMonitor: %v", err)
	}
	if m.Id() == "" || m.AlertThreshold() != 100 || m.WarningThreshold() != 50 || m.Severity() != "unknown" {
		t.Fatalf("unexpected monitor: id=%q alert=%v warn=%v sev=%q", m.Id(), m.AlertThreshold(), m.WarningThreshold(), m.Severity())
	}

	got, err := c.GetMonitor(ctx, gen.IdReqInput{ProjectId: testProject, Id: m.Id()})
	if err != nil {
		t.Fatalf("GetMonitor: %v", err)
	}
	if got.Metric() != `{"measure":"count","agg":"count"}` || got.Status() != "active" {
		t.Fatalf("monitor not round-tripped: metric=%q status=%q", got.Metric(), got.Status())
	}

	upd, err := c.UpdateMonitor(ctx, gen.MonitorReqInput{
		ProjectId: testProject, MonitorId: m.Id(), Name: "Error rate v2", View: "traces",
		Filters: `[]`, Metric: `{"measure":"count","agg":"count"}`, Window: `{"value":10,"unit":"minutes"}`,
		ThresholdOperator: ">", AlertThreshold: 200, WarningThreshold: nan(),
		NoData: `{"mode":"SILENT"}`, Renotify: `{"mode":"OFF"}`, Tags: `[]`, Status: "paused",
	})
	if err != nil {
		t.Fatalf("UpdateMonitor: %v", err)
	}
	if upd.AlertThreshold() != 200 || upd.Status() != "paused" {
		t.Fatalf("monitor update not applied: %+v", upd)
	}
	// A null warning threshold must come back as NaN, not 0.
	if !nanEq(upd.WarningThreshold()) {
		t.Fatalf("expected null warningThreshold (NaN), got %v", upd.WarningThreshold())
	}

	list, err := c.AllMonitors(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("AllMonitors: %v", err)
	}
	if list.TotalCount() != 1 {
		t.Fatalf("AllMonitors total=%d, want 1", list.TotalCount())
	}

	if _, err := c.DeleteMonitor(ctx, gen.IdReqInput{ProjectId: testProject, Id: m.Id()}); err != nil {
		t.Fatalf("DeleteMonitor: %v", err)
	}
	t.Logf("monitor CRUD OK: created, got, updated(null-warn), listed(1), deleted")
}

// TestPermissionGatingPerCategory proves each method's DashPermissions bit is
// the single chokepoint: a cap holding ONLY one category's read bit can read
// that category but is forbidden everything else (write of the same category,
// other categories).
func TestPermissionGatingPerCategory(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	ctx, cancel := ctx5()
	defer cancel()

	// Dashboard-read-only cap: AllDashboards ok, CreateDashboard forbidden,
	// AllMonitors forbidden, AllWidgets ok (widgets share the dashboard bit).
	cRead, stopR := newClient(t, addr, peer, server.PermDashboardRead)
	defer stopR()
	if _, err := cRead.AllDashboards(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 10}); err != nil {
		t.Fatalf("read-only cap should read dashboards: %v", err)
	}
	if _, err := cRead.AllWidgets(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 10}); err != nil {
		t.Fatalf("dashboard-read cap should read widgets (shared bit): %v", err)
	}
	if _, err := cRead.CreateDashboard(ctx, gen.CreateDashReqInput{ProjectId: testProject, Name: "x"}); err == nil {
		t.Fatalf("read-only cap must NOT create dashboards")
	}
	if _, err := cRead.AllMonitors(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 10}); err == nil {
		t.Fatalf("dashboard cap must NOT read monitors")
	}

	// Monitor-write cap: CreateMonitor ok, CreatePreset forbidden.
	cMon, stopM := newClient(t, addr, peer, server.PermMonitorWrite|server.PermMonitorRead)
	defer stopM()
	if _, err := cMon.CreateMonitor(ctx, gen.MonitorReqInput{
		ProjectId: testProject, Name: "m", View: "traces", Filters: `[]`,
		Metric: `{}`, Window: `{}`, ThresholdOperator: ">", AlertThreshold: 1, WarningThreshold: nan(),
		NoData: `{}`, Renotify: `{}`, Tags: `[]`, Status: "active",
	}); err != nil {
		t.Fatalf("monitor-write cap should create monitors: %v", err)
	}
	if _, err := cMon.CreatePreset(ctx, gen.PresetReqInput{
		ProjectId: testProject, Name: "p", TableName: "traces", Filters: `[]`, ColumnOrder: `[]`, ColumnVisibility: `{}`,
	}); err == nil {
		t.Fatalf("monitor cap must NOT create presets")
	}
	t.Logf("capability gating OK: per-category read/write bits enforced as the single chokepoint")
}

// TestNoCapDenied proves a cap of the WRONG kind / zero permissions is rejected
// before any data access.
func TestZeroPermissionDenied(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	c, stopC := newClient(t, addr, peer, 0) // no bits
	defer stopC()
	ctx, cancel := ctx5()
	defer cancel()
	if _, err := c.AllDashboards(ctx, gen.ListReqInput{ProjectId: testProject, Page: 1, Limit: 10}); err == nil {
		t.Fatalf("zero-permission cap must be denied")
	} else {
		t.Logf("correctly denied: %v", err)
	}
}

// TestAnalyticsNotImplemented locks the contract: analytics methods are wired to
// 501 until the Datastore shim lands — they must NOT fabricate data.
func TestAnalyticsNotImplemented(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	c, stopC := fullClient(t, addr, peer)
	defer stopC()
	ctx, cancel := ctx5()
	defer cancel()
	for _, m := range []struct {
		name   string
		method uint32
	}{
		{"chart", server.MethodChart},
		{"scoreHistogram", server.MethodScoreHistogram},
		{"executeQuery", server.MethodExecuteQuery},
		{"isBatchActionInProgress", server.MethodIsBatchActionInProgress},
	} {
		payload := gen.NewAnalyticsReq(gen.AnalyticsReqInput{ProjectId: testProject})
		if m.method == server.MethodIsBatchActionInProgress {
			payload = gen.NewBatchActionReq(gen.BatchActionReqInput{ProjectId: testProject, TableName: "traces", ActionId: "a1"})
		}
		status := c.Probe(ctx, m.method, payload)
		if status != server.StatusNotImpl {
			t.Fatalf("%s: status=%d, want 501 (not-implemented TODO)", m.name, status)
		}
	}
	t.Logf("analytics + queue methods correctly return 501 (Datastore/BullMQ shims pending)")
}

// TestPipelineDashboardThenWidget is the load-bearing pipelining proof: the
// dependent CreateWidget call (pipelined off CreateDashboard's promise) must ship
// before CreateDashboard's answer resolves. We assert on the instrumented send
// log that BOTH "send" events precede the FIRST "recv" event.
func TestPipelineDashboardThenWidget(t *testing.T) {
	addr, peer, stop := newService(t)
	defer stop()
	ctx, cancel := ctx5()
	defer cancel()

	var log []server.SendEvent
	c, stopC := fullClient(t, addr, peer)
	defer stopC()
	c.WithSendLog(&log)
	dep, stopD := fullClient(t, addr, peer)
	defer stopD()
	dep.WithSendLog(&log)

	dash, widget, err := c.PipelineCreateDashboardThenWidget(ctx, dep,
		gen.CreateDashReqInput{ProjectId: testProject, Name: "Pipelined", Description: ""},
		gen.WidgetReqInput{
			ProjectId: testProject, Name: "On board", View: "traces",
			Dimensions: `[]`, Metrics: `[]`, Filters: `[]`, ChartType: "NUMBER", ChartConfig: `{"type":"NUMBER"}`,
		})
	if err != nil {
		t.Fatalf("Pipeline: %v", err)
	}
	if dash.Id() == "" || widget.Id() == "" {
		t.Fatalf("pipeline produced empty ids: dash=%q widget=%q", dash.Id(), widget.Id())
	}

	firstRecv, sendsBefore := -1, 0
	for i, e := range log {
		if e.Kind == "recv" {
			firstRecv = i
			break
		}
		if e.Kind == "send" {
			sendsBefore++
		}
	}
	t.Logf("send log: %s", formatLog(log))
	if firstRecv == -1 {
		t.Fatalf("no recv events recorded")
	}
	if sendsBefore < 2 {
		t.Fatalf("pipelining violated: only %d sends before first answer; want 2", sendsBefore)
	}
	t.Logf("PIPELINING PROVEN: %d calls (dashboard+widget) shipped before the first answer resolved", sendsBefore)
}

// nan is the wire sentinel for a null float (a null Monitor warningThreshold).
func nan() float64 { return math.NaN() }

// nanEq reports whether f carries the null sentinel.
func nanEq(f float64) bool { return math.IsNaN(f) }
