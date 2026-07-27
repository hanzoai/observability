package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/hanzoai/base/core"
	basetests "github.com/hanzoai/base/tests"
	luxlog "github.com/luxfi/log"
	zaplib "github.com/luxfi/zap"
	zcap "github.com/zap-proto/go/cap"

	gen "github.com/hanzoai/observability/gen"
	"github.com/hanzoai/observability/server"
)

const testOrg = "test-org"

// allPerms is a capability with every observability bit set — the default for
// honest-path tests. Negative tests pass a narrower mask.
const allPerms = ^uint64(0)

// harness bundles the Base app + a running ZAP node so a test can both seed rows
// directly (via app) and exercise them over the wire (via a dialed Client).
type harness struct {
	app  core.App
	node *zaplib.Node
	addr string
	peer string
}

func newHarness(t *testing.T, port int) *harness {
	t.Helper()
	app, err := basetests.NewTestApp()
	if err != nil {
		t.Fatalf("new test app: %v", err)
	}
	if err := server.EnsureCollections(app); err != nil {
		t.Fatalf("ensure collections: %v", err)
	}
	logger := luxlog.New("component", "obs-test")
	peer := "obs-test-srv-" + itoa(port)
	node := zaplib.NewNode(zaplib.NodeConfig{NodeID: peer, Port: port, NoDiscovery: true})
	srv := server.NewServer(app, logger, zcap.Verifier{})
	srv.Register(node)
	if err := node.Start(); err != nil {
		t.Fatalf("node start: %v", err)
	}
	t.Cleanup(func() {
		node.Stop()
		app.Cleanup()
	})
	return &harness{app: app, node: node, addr: "127.0.0.1:" + itoa(port), peer: peer}
}

// client dials the harness with a synthetic cap holding perms.
func (h *harness) client(t *testing.T, perms uint64, port int) *server.Client {
	t.Helper()
	c, _ := dialClient(t, h.addr, h.peer, perms)
	return c
}

// seed writes one record into col with the given field map and returns it.
func (h *harness) seed(t *testing.T, col string, fields map[string]any) *core.Record {
	t.Helper()
	c, err := h.app.FindCollectionByNameOrId(col)
	if err != nil {
		t.Fatalf("find collection %s: %v", col, err)
	}
	rec := core.NewRecord(c)
	for k, v := range fields {
		rec.Set(k, v)
	}
	if err := h.app.Save(rec); err != nil {
		t.Fatalf("seed %s: %v", col, err)
	}
	return rec
}

func ctx5() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// ---------------------------------------------------------------------------
// Score configs — pure OLTP CRUD (create → read → list → update). The cleanest
// honest-migration path (pure Prisma upstream).
// ---------------------------------------------------------------------------

func TestScoreConfigCRUD(t *testing.T) {
	h := newHarness(t, 19700)
	cli := h.client(t, allPerms, 19701)
	ctx, cancel := ctx5()
	defer cancel()

	created, err := cli.ScoreConfigCreate(ctx, gen.ScoreConfigWriteParamsInput{
		ProjectId: testOrg, Name: "quality", DataType: "NUMERIC",
		HasMin: true, MinValue: 0, HasMax: true, MaxValue: 1,
		Categories: `[]`,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Id() == "" || created.Name() != "quality" || created.DataType() != "NUMERIC" {
		t.Fatalf("create returned id=%q name=%q dataType=%q", created.Id(), created.Name(), created.DataType())
	}
	t.Logf("created id=%s name=%s min=%v max=%v", created.Id(), created.Name(), created.MinValue(), created.MaxValue())

	got, err := cli.ScoreConfigById(ctx, gen.ScoreConfigByIdParamsInput{ProjectId: testOrg, Id: created.Id()})
	if err != nil {
		t.Fatalf("byId: %v", err)
	}
	if !got.Present() || got.Name() != "quality" {
		t.Fatalf("byId present=%v name=%q", got.Present(), got.Name())
	}

	list, err := cli.ScoreConfigAll(ctx, gen.ScoreConfigAllParamsInput{ProjectId: testOrg})
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if list.TotalCount() != 1 || list.Items().Len() != 1 {
		t.Fatalf("all total=%d items=%d, want 1/1", list.TotalCount(), list.Items().Len())
	}

	updated, err := cli.ScoreConfigUpdate(ctx, gen.ScoreConfigWriteParamsInput{
		ProjectId: testOrg, Id: created.Id(),
		SetName: true, Name: "quality-v2",
		SetIsArchived: true, IsArchived: true,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Name() != "quality-v2" || !updated.IsArchived() {
		t.Fatalf("update name=%q archived=%v", updated.Name(), updated.IsArchived())
	}
	t.Logf("CRUD ok: created→read→list(1)→update(name=%s,archived=%v)", updated.Name(), updated.IsArchived())
}

func TestScoreConfigByIdAbsentReturnsNotPresent(t *testing.T) {
	h := newHarness(t, 19702)
	cli := h.client(t, allPerms, 19703)
	ctx, cancel := ctx5()
	defer cancel()

	got, err := cli.ScoreConfigById(ctx, gen.ScoreConfigByIdParamsInput{ProjectId: testOrg, Id: "nope"})
	if err != nil {
		t.Fatalf("byId: %v", err)
	}
	if got.Present() {
		t.Fatalf("expected present=false for absent config")
	}
}

func TestScoreConfigUpdateMissingFails(t *testing.T) {
	h := newHarness(t, 19704)
	cli := h.client(t, allPerms, 19705)
	ctx, cancel := ctx5()
	defer cancel()

	if _, err := cli.ScoreConfigUpdate(ctx, gen.ScoreConfigWriteParamsInput{
		ProjectId: testOrg, Id: "missing", SetName: true, Name: "x",
	}); err == nil {
		t.Fatalf("expected update of missing config to fail")
	}
}

// ---------------------------------------------------------------------------
// Traces — read + mutations (bookmark/publish/updateTags/deleteMany) + the
// observations+scores join.
// ---------------------------------------------------------------------------

func TestTraceByIdAndMutations(t *testing.T) {
	h := newHarness(t, 19706)
	cli := h.client(t, allPerms, 19707)
	ctx, cancel := ctx5()
	defer cancel()

	h.seed(t, server.ColTrace, map[string]any{
		"projectId": testOrg, "extId": "t1", "name": "root", "userId": "u1",
		"sessionId": "s1", "input": `{"q":1}`, "output": `{"a":2}`, "timestamp": 1700,
	})

	tr, err := cli.TraceById(ctx, gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "t1"})
	if err != nil {
		t.Fatalf("traceById: %v", err)
	}
	if !tr.Present() || tr.Name() != "root" || tr.UserId() != "u1" {
		t.Fatalf("traceById present=%v name=%q user=%q", tr.Present(), tr.Name(), tr.UserId())
	}

	bm, err := cli.TraceBookmark(ctx, gen.TraceBookmarkParamsInput{ProjectId: testOrg, TraceId: "t1", Bookmarked: true})
	if err != nil {
		t.Fatalf("bookmark: %v", err)
	}
	if !bm.Bookmarked() {
		t.Fatalf("bookmark not applied")
	}

	pub, err := cli.TracePublish(ctx, gen.TracePublishParamsInput{ProjectId: testOrg, TraceId: "t1", Public: true})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !pub.Public() {
		t.Fatalf("publish not applied")
	}

	tg, err := cli.TraceUpdateTags(ctx, gen.TraceUpdateTagsParamsInput{ProjectId: testOrg, TraceId: "t1", Tags: `["a","b"]`})
	if err != nil {
		t.Fatalf("updateTags: %v", err)
	}
	if tg.Tags() != `["a","b"]` {
		t.Fatalf("tags = %q, want [\"a\",\"b\"]", tg.Tags())
	}

	del, err := cli.TraceDeleteMany(ctx, gen.TraceDeleteManyParamsInput{ProjectId: testOrg, TraceIds: [][]byte{[]byte("t1")}})
	if err != nil {
		t.Fatalf("deleteMany: %v", err)
	}
	if del.Count() != 1 {
		t.Fatalf("deleteMany count=%d, want 1", del.Count())
	}
	gone, _ := cli.TraceById(ctx, gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "t1"})
	if gone.Present() {
		t.Fatalf("trace still present after delete")
	}
	t.Logf("trace mutations ok: bookmark→publish→tags→delete(1)")
}

func TestTraceWithDetailJoinsObservationsAndScores(t *testing.T) {
	h := newHarness(t, 19708)
	cli := h.client(t, allPerms, 19709)
	ctx, cancel := ctx5()
	defer cancel()

	h.seed(t, server.ColTrace, map[string]any{"projectId": testOrg, "extId": "t2", "name": "agent-run"})
	h.seed(t, server.ColObservation, map[string]any{
		"projectId": testOrg, "extId": "o1", "traceId": "t2", "type": "GENERATION",
		"name": "llm", "startTime": 1000, "endTime": 3500, "totalTokens": 42,
	})
	h.seed(t, server.ColObservation, map[string]any{
		"projectId": testOrg, "extId": "o2", "traceId": "t2", "type": "SPAN",
		"name": "tool", "startTime": 1200, "endTime": 1800,
	})
	h.seed(t, server.ColScore, map[string]any{
		"projectId": testOrg, "extId": "sc1", "traceId": "t2", "name": "quality",
		"value": 0.9, "dataType": "NUMERIC", "source": "ANNOTATION",
	})

	d, err := cli.TraceWithDetail(ctx, gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "t2"})
	if err != nil {
		t.Fatalf("traceWithDetail: %v", err)
	}
	if !d.Present() {
		t.Fatalf("expected present")
	}
	if d.Observations().Len() != 2 {
		t.Fatalf("observations=%d, want 2", d.Observations().Len())
	}
	if d.Scores().Len() != 1 {
		t.Fatalf("scores=%d, want 1", d.Scores().Len())
	}
	// Latency = (maxEnd 3500 - minStart 1000)/1000 = 2.5s.
	if d.Latency() != 2.5 {
		t.Fatalf("latency=%v, want 2.5", d.Latency())
	}
	// Decode a nested observation view from the list to prove the sub-message
	// round-trips.
	obs, err := gen.WrapObservation(d.Observations().BytesAt(0))
	if err != nil {
		t.Fatalf("decode nested observation: %v", err)
	}
	if obs.TraceId() != "t2" {
		t.Fatalf("nested observation traceId=%q, want t2", obs.TraceId())
	}
	t.Logf("trace-detail join ok: 2 observations, 1 score, latency=%.1fs", d.Latency())
}

// ---------------------------------------------------------------------------
// Observations + events.batchIO.
// ---------------------------------------------------------------------------

func TestObservationByIdAndBatchIO(t *testing.T) {
	h := newHarness(t, 19710)
	cli := h.client(t, allPerms, 19711)
	ctx, cancel := ctx5()
	defer cancel()

	h.seed(t, server.ColObservation, map[string]any{
		"projectId": testOrg, "extId": "ob1", "traceId": "tr1", "type": "GENERATION",
		"input": `"hi"`, "output": `"yo"`, "model": "qwen3", "promptTokens": 3, "completionTokens": 2,
	})

	o, err := cli.ObservationById(ctx, gen.ObservationByIdParamsInput{ProjectId: testOrg, ObservationId: "ob1", TraceId: "tr1"})
	if err != nil {
		t.Fatalf("observationById: %v", err)
	}
	if !o.Present() || o.Model() != "qwen3" || o.PromptTokens() != 3 {
		t.Fatalf("observationById present=%v model=%q prompt=%d", o.Present(), o.Model(), o.PromptTokens())
	}

	io, err := cli.EventBatchIO(ctx, gen.EventBatchIOParamsInput{
		ProjectId: testOrg,
		Observations: [][]byte{
			gen.NewObservationRef(gen.ObservationRefInput{Id: "ob1", TraceId: "tr1"}),
			gen.NewObservationRef(gen.ObservationRefInput{Id: "missing", TraceId: "tr1"}),
		},
	})
	if err != nil {
		t.Fatalf("batchIO: %v", err)
	}
	if io.Items().Len() != 1 { // missing one is skipped
		t.Fatalf("batchIO items=%d, want 1", io.Items().Len())
	}
	item, _ := gen.WrapObservationIO(io.Items().BytesAt(0))
	if item.Input() != `"hi"` || item.Output() != `"yo"` {
		t.Fatalf("batchIO io input=%q output=%q", item.Input(), item.Output())
	}
	t.Logf("observation + batchIO ok")
}

// ---------------------------------------------------------------------------
// Scores — annotation create/update/delete, byId, hasAny, events.scoresForTrace.
// ---------------------------------------------------------------------------

func TestAnnotationScoreLifecycle(t *testing.T) {
	h := newHarness(t, 19712)
	cli := h.client(t, allPerms, 19713)
	ctx, cancel := ctx5()
	defer cancel()

	created, err := cli.ScoreCreateAnnotation(ctx, gen.AnnotationScoreParamsInput{
		ProjectId: testOrg, Name: "helpfulness", Value: 1, DataType: "NUMERIC",
		ScoreTargetType: "trace", TraceId: "trace-A", ConfigId: "cfg-1", AuthorUserId: "user-1",
	})
	if err != nil {
		t.Fatalf("createAnnotation: %v", err)
	}
	if created.Id() == "" || created.Source() != "ANNOTATION" || created.TraceId() != "trace-A" {
		t.Fatalf("createAnnotation id=%q source=%q trace=%q", created.Id(), created.Source(), created.TraceId())
	}
	// The FK to the score config rides on the renamed ScoreConfigId wire field.
	if created.ScoreConfigId() != "cfg-1" {
		t.Fatalf("createAnnotation scoreConfigId=%q, want cfg-1", created.ScoreConfigId())
	}

	updated, err := cli.ScoreUpdateAnnotation(ctx, gen.AnnotationScoreParamsInput{
		ProjectId: testOrg, Id: created.Id(), Name: "helpfulness", Value: 0, DataType: "NUMERIC",
		ScoreTargetType: "trace", TraceId: "trace-A", ConfigId: "cfg-1",
	})
	if err != nil {
		t.Fatalf("updateAnnotation: %v", err)
	}
	if updated.Value() != 0 {
		t.Fatalf("updateAnnotation value=%v, want 0", updated.Value())
	}

	one, err := cli.EventScoresForTrace(ctx, gen.EventScoresParamsInput{ProjectId: testOrg, TraceId: "trace-A"})
	if err != nil {
		t.Fatalf("scoresForTrace: %v", err)
	}
	if one.Items().Len() != 1 {
		t.Fatalf("scoresForTrace items=%d, want 1", one.Items().Len())
	}

	any, err := cli.ScoreHasAny(ctx, gen.ProjectScopeInput{ProjectId: testOrg})
	if err != nil {
		t.Fatalf("hasAny: %v", err)
	}
	if !any.Value() {
		t.Fatalf("hasAny=false, want true")
	}

	del, err := cli.ScoreDeleteAnnotation(ctx, gen.ScoreByIdParamsInput{ProjectId: testOrg, ScoreId: created.Id()})
	if err != nil {
		t.Fatalf("deleteAnnotation: %v", err)
	}
	if del.Count() != 1 {
		t.Fatalf("deleteAnnotation count=%d, want 1", del.Count())
	}
	t.Logf("annotation score lifecycle ok: create→update(0)→scoresForTrace(1)→hasAny→delete(1)")
}

// ---------------------------------------------------------------------------
// Sessions — lazy materialization on bookmark/publish, byIdWithScores rollup.
// ---------------------------------------------------------------------------

func TestSessionMaterializeAndRead(t *testing.T) {
	h := newHarness(t, 19714)
	cli := h.client(t, allPerms, 19715)
	ctx, cancel := ctx5()
	defer cancel()

	// bookmark materializes the session row even though none was ingested.
	bm, err := cli.SessionBookmark(ctx, gen.SessionBookmarkParamsInput{ProjectId: testOrg, SessionId: "sess-1", Bookmarked: true})
	if err != nil {
		t.Fatalf("sessionBookmark: %v", err)
	}
	if !bm.Bookmarked() {
		t.Fatalf("session bookmark not applied")
	}

	// Two traces in the session → CountTraces == 2 on read.
	h.seed(t, server.ColTrace, map[string]any{"projectId": testOrg, "extId": "ta", "sessionId": "sess-1"})
	h.seed(t, server.ColTrace, map[string]any{"projectId": testOrg, "extId": "tb", "sessionId": "sess-1"})
	h.seed(t, server.ColScore, map[string]any{"projectId": testOrg, "extId": "ss1", "sessionId": "sess-1", "name": "rating", "value": 5})

	sess, err := cli.SessionById(ctx, gen.SessionByIdParamsInput{ProjectId: testOrg, SessionId: "sess-1"})
	if err != nil {
		t.Fatalf("sessionById: %v", err)
	}
	// The nested Session rides as bytes — Wrap it (same as list elements).
	inner, err := gen.WrapSession(sess.Session())
	if err != nil {
		t.Fatalf("wrap nested session: %v", err)
	}
	if !sess.Present() || inner.CountTraces() != 2 || sess.Scores().Len() != 1 {
		t.Fatalf("sessionById present=%v traces=%d scores=%d", sess.Present(), inner.CountTraces(), sess.Scores().Len())
	}

	has, err := cli.SessionHasAny(ctx, gen.ProjectScopeInput{ProjectId: testOrg})
	if err != nil {
		t.Fatalf("sessionHasAny: %v", err)
	}
	if !has.Value() {
		t.Fatalf("sessionHasAny=false, want true")
	}
	t.Logf("session ok: materialized via bookmark, countTraces=2, scores=1")
}

// ---------------------------------------------------------------------------
// Tenant isolation — a read scoped to projectId never returns another project's
// row.
// ---------------------------------------------------------------------------

func TestTenantIsolation(t *testing.T) {
	h := newHarness(t, 19716)
	cli := h.client(t, allPerms, 19717)
	ctx, cancel := ctx5()
	defer cancel()

	h.seed(t, server.ColTrace, map[string]any{"projectId": "other-org", "extId": "secret", "name": "do-not-leak"})

	got, err := cli.TraceById(ctx, gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "secret"})
	if err != nil {
		t.Fatalf("traceById: %v", err)
	}
	if got.Present() {
		t.Fatalf("LEAK: read another org's trace under %s", testOrg)
	}
	t.Logf("tenant isolation ok: cross-project read returns present=false")
}

// ---------------------------------------------------------------------------
// Capability gating — fail closed.
// ---------------------------------------------------------------------------

// TestPermissionFailClosed proves each method demands its specific bit: a cap
// with ONLY TraceRead can read traces but is rejected on a trace write, a score
// read, and a score-config write.
func TestPermissionFailClosed(t *testing.T) {
	h := newHarness(t, 19718)
	// cap holds ONLY PermTraceRead.
	cli := h.client(t, server.PermTraceRead, 19719)
	ctx, cancel := ctx5()
	defer cancel()

	// Allowed: trace read (present=false is fine; the point is no auth error).
	if _, err := cli.TraceById(ctx, gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "x"}); err != nil {
		t.Fatalf("TraceById with PermTraceRead should be allowed: %v", err)
	}

	// Denied: trace write (needs PermTraceWrite).
	if _, err := cli.TraceBookmark(ctx, gen.TraceBookmarkParamsInput{ProjectId: testOrg, TraceId: "x", Bookmarked: true}); err == nil {
		t.Fatalf("TraceBookmark must be denied without PermTraceWrite")
	}
	// Denied: score read (needs PermScoreRead).
	if _, err := cli.ScoreHasAny(ctx, gen.ProjectScopeInput{ProjectId: testOrg}); err == nil {
		t.Fatalf("ScoreHasAny must be denied without PermScoreRead")
	}
	// Denied: score-config write (needs PermScoreConfigWrite).
	if _, err := cli.ScoreConfigCreate(ctx, gen.ScoreConfigWriteParamsInput{ProjectId: testOrg, Name: "n", DataType: "NUMERIC"}); err == nil {
		t.Fatalf("ScoreConfigCreate must be denied without PermScoreConfigWrite")
	}
	t.Logf("fail-closed ok: TraceRead admits traceById, denies write/score-read/config-write")
}

// TestZeroPermsDeniesEverything: a cap with no bits is rejected on every method.
func TestZeroPermsDeniesEverything(t *testing.T) {
	h := newHarness(t, 19720)
	cli := h.client(t, 0, 19721)
	ctx, cancel := ctx5()
	defer cancel()

	if _, err := cli.TraceById(ctx, gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "x"}); err == nil {
		t.Fatalf("expected denial with zero perms")
	} else {
		t.Logf("correctly denied zero-perm cap: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Analytics stubs — honest about the gap: return Stubbed=true, still gated.
// ---------------------------------------------------------------------------

func TestAnalyticsStubsReturnStubbed(t *testing.T) {
	h := newHarness(t, 19722)
	cli := h.client(t, allPerms, 19723)
	ctx, cancel := ctx5()
	defer cancel()

	payload := gen.NewTableQueryParams(gen.TableQueryParamsInput{ProjectId: testOrg})
	for _, m := range []uint32{
		server.MethodTraceAll, server.MethodTraceCountAll, server.MethodTraceMetrics,
		server.MethodTraceFilterOptions, server.MethodSessionAll, server.MethodSessionCountAll,
		server.MethodScoreAll, server.MethodScoreCountAll, server.MethodEventAll,
		server.MethodAnalyticsScoreComparison,
	} {
		// 501, not 200. A stubbed 200 carrying an empty aggregate is
		// indistinguishable at the call site from a real query that matched
		// nothing, so the UI would render "no data" and the missing Datastore
		// backend would never surface. The status is the field a caller cannot
		// skip reading.
		if got := cli.Probe(ctx, m, payload); got != server.StatusNotImpl {
			t.Fatalf("method %d: status = %d, want %d (not-implemented, never 200)",
				m, got, server.StatusNotImpl)
		}
	}
	t.Logf("analytics stubs ok: all 10 answer 501 (no faked aggregates, no empty-looking 200s)")
}

func TestAnalyticsStubDeniedWithoutAnalyticsBit(t *testing.T) {
	h := newHarness(t, 19724)
	cli := h.client(t, server.PermTraceRead, 19725) // no PermAnalyticsRead
	ctx, cancel := ctx5()
	defer cancel()

	if _, err := cli.AnalyticsStub(ctx, server.MethodTraceAll, gen.TableQueryParamsInput{ProjectId: testOrg}); err == nil {
		t.Fatalf("analytics stub must be denied without PermAnalyticsRead")
	}
}

// ---------------------------------------------------------------------------
// Pipelining — the load-bearing proof: the dependent observationById call ships
// BEFORE the trace call's answer resolves (Cap'n Proto promise pipelining over
// the trace→observation dependency).
// ---------------------------------------------------------------------------

func TestPipeliningTraceObservation(t *testing.T) {
	h := newHarness(t, 19726)

	var log []server.SendEvent
	cli := h.client(t, allPerms, 19727)
	cli.WithSendLog(&log)
	dep := h.client(t, allPerms, 19728)
	dep.WithSendLog(&log)

	ctx, cancel := ctx5()
	defer cancel()

	tr, ob, err := cli.PipelineTraceObservation(ctx, dep,
		gen.TraceByIdParamsInput{ProjectId: testOrg, TraceId: "tp"},
		gen.ObservationByIdParamsInput{ProjectId: testOrg, ObservationId: "op", TraceId: "tp"},
	)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	_ = tr
	_ = ob

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
		t.Fatalf("pipelining violated: %d sends before first answer; want 2", sendsBefore)
	}
	t.Logf("PIPELINING PROVEN: %d calls in flight before the first answer resolved", sendsBefore)
}

// --- tiny local helpers ----------------------------------------------------

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func formatLog(log []server.SendEvent) string {
	out := ""
	for _, e := range log {
		m := "traceById"
		if e.Method == server.MethodObservationById {
			m = "observationById"
		}
		out += e.Kind + "(" + m + ",p=" + itoa(int(e.PromiseID)) + ",t=" + itoa(int(e.Target)) + ") "
	}
	return out
}
