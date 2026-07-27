package server

import (
	"encoding/json"

	"github.com/hanzoai/base/core"
	zaplib "github.com/luxfi/zap"

	gen "github.com/hanzoai/observability/gen"
)

// handlers.go — one method body per tRPC procedure. Honest handlers read/write
// the Base collections in collections.go and scope every query to the
// capability's project. The analytics handlers (handleStub) return an explicit
// Stubbed result with a TODO pointing at the Datastore shim that must replace
// them — they do NOT fabricate aggregate data.
//
// Each handler decodes its typed params view from req.Payload, does its work,
// and encodes a typed result view into the response body. The wire envelope
// (status/promise) is added by buildResponse.

// reply is the common tail: wrap a status + pre-encoded body into a response.
func (s *Server) reply(req Call, status uint32, body []byte) (*zaplib.Message, error) {
	return buildResponse(status, req.PromiseID, body)
}

// =============================== traces ====================================

func (s *Server) handleTraceById(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapTraceByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad TraceByIdParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColTrace, project, p.TraceId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewTrace(gen.TraceInput{Present: false}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, traceToBytes(rec))
}

func (s *Server) handleTraceWithDetail(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapTraceByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad TraceByIdParams: "+err.Error()))
	}
	trace, err := s.findByExt(ColTrace, project, p.TraceId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewTraceWithDetail(gen.TraceWithDetailInput{
				Trace: gen.NewTrace(gen.TraceInput{Present: false}), Present: false,
			}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}

	// Child observations + scores for this trace (the join the tRPC byIdWith…
	// procedure assembled from Datastore; here a project-scoped FK lookup).
	obsRecs, err := s.app.FindRecordsByFilter(ColObservation,
		"projectId = {:p} && traceId = {:t}", "startTime", 0, 0,
		map[string]any{"p": project, "t": p.TraceId()})
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	scoreRecs, err := s.app.FindRecordsByFilter(ColScore,
		"projectId = {:p} && traceId = {:t}", "timestamp", 0, 0,
		map[string]any{"p": project, "t": p.TraceId()})
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}

	obs := make([][]byte, len(obsRecs))
	for i, r := range obsRecs {
		obs[i] = observationToBytes(r)
	}
	scores := make([][]byte, len(scoreRecs))
	for i, r := range scoreRecs {
		scores[i] = scoreToBytes(r)
	}

	// Latency (seconds): last observation end minus first observation start.
	latency := latencySeconds(obsRecs)

	return s.reply(req, StatusOK, gen.NewTraceWithDetail(gen.TraceWithDetailInput{
		Trace:        traceToBytes(trace),
		Observations: obs,
		Scores:       scores,
		Latency:      latency,
		Present:      true,
	}))
}

func (s *Server) handleTraceBookmark(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapTraceBookmarkParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad TraceBookmarkParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColTrace, project, p.TraceId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusNotFound, errorBody("trace not found"))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	rec.Set(fBookmarked, p.Bookmarked())
	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, traceToBytes(rec))
}

func (s *Server) handleTracePublish(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapTracePublishParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad TracePublishParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColTrace, project, p.TraceId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusNotFound, errorBody("trace not found"))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	rec.Set(fPublic, p.Public())
	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, traceToBytes(rec))
}

func (s *Server) handleTraceUpdateTags(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapTraceUpdateTagsParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad TraceUpdateTagsParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColTrace, project, p.TraceId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusNotFound, errorBody("trace not found"))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	// Tags arrive as a JSON []string (text on the wire); store as JSON column.
	rec.Set(fTags, jsonArray(p.Tags()))
	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, traceToBytes(rec))
}

func (s *Server) handleTraceDeleteMany(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapTraceDeleteManyParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad TraceDeleteManyParams: "+err.Error()))
	}
	ids := p.TraceIds()
	var deleted uint64
	for i := 0; i < ids.Len(); i++ {
		ext := string(ids.BytesAt(i))
		rec, err := s.findByExt(ColTrace, project, ext)
		if err != nil {
			if notFound(err) {
				continue
			}
			return s.reply(req, StatusInternal, errorBody(err.Error()))
		}
		if err := s.app.Delete(rec); err != nil {
			return s.reply(req, StatusInternal, errorBody(err.Error()))
		}
		deleted++
	}
	return s.reply(req, StatusOK, gen.NewMutationCount(gen.MutationCountInput{Count: deleted}))
}

// ============================ observations =================================

func (s *Server) handleObservationById(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapObservationByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ObservationByIdParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColObservation, project, p.ObservationId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewObservation(gen.ObservationInput{Present: false}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, observationToBytes(rec))
}

func (s *Server) handleEventBatchIO(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapEventBatchIOParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad EventBatchIOParams: "+err.Error()))
	}
	refs := p.Observations()
	items := make([][]byte, 0, refs.Len())
	for i := 0; i < refs.Len(); i++ {
		ref, err := gen.WrapObservationRef(refs.BytesAt(i))
		if err != nil {
			return s.reply(req, StatusBadRequest, errorBody("bad ObservationRef: "+err.Error()))
		}
		rec, err := s.findByExt(ColObservation, project, ref.Id())
		if err != nil {
			if notFound(err) {
				continue
			}
			return s.reply(req, StatusInternal, errorBody(err.Error()))
		}
		items = append(items, gen.NewObservationIO(gen.ObservationIOInput{
			Id:       rec.GetString(fExtId),
			Input:    rec.GetString(fInput),
			Output:   rec.GetString(fOutput),
			Metadata: rec.GetString(fMetadata),
		}))
	}
	return s.reply(req, StatusOK, gen.NewObservationIOList(gen.ObservationIOListInput{Items: items}))
}

// ============================== sessions ===================================

func (s *Server) handleSessionHasAny(req Call, project string) (*zaplib.Message, error) {
	_, err := gen.WrapProjectScope(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ProjectScope: "+err.Error()))
	}
	_, err = s.app.FindFirstRecordByFilter(ColSession, "projectId = {:p}", map[string]any{"p": project})
	has := err == nil
	if err != nil && !notFound(err) {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, gen.NewBoolResult(gen.BoolResultInput{Value: has}))
}

func (s *Server) handleSessionById(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapSessionByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad SessionByIdParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColSession, project, p.SessionId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewSessionWithScores(gen.SessionWithScoresInput{
				Session: gen.NewSession(gen.SessionInput{Present: false}), Present: false,
			}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	scoreRecs, err := s.app.FindRecordsByFilter(ColScore,
		"projectId = {:p} && sessionId = {:s}", "timestamp", 0, 0,
		map[string]any{"p": project, "s": p.SessionId()})
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	scores := make([][]byte, len(scoreRecs))
	for i, r := range scoreRecs {
		scores[i] = scoreToBytes(r)
	}
	return s.reply(req, StatusOK, gen.NewSessionWithScores(gen.SessionWithScoresInput{
		Session: s.sessionToBytes(rec), Scores: scores, Present: true,
	}))
}

func (s *Server) handleSessionBookmark(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapSessionBookmarkParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad SessionBookmarkParams: "+err.Error()))
	}
	rec, err := s.upsertSession(project, p.SessionId())
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	rec.Set(fBookmarked, p.Bookmarked())
	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, s.sessionToBytes(rec))
}

func (s *Server) handleSessionPublish(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapSessionPublishParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad SessionPublishParams: "+err.Error()))
	}
	rec, err := s.upsertSession(project, p.SessionId())
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	rec.Set(fPublic, p.Public())
	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, s.sessionToBytes(rec))
}

// upsertSession finds-or-creates a session row. Sessions are not ingested as
// first-class rows upstream (they are a rollup of traces sharing a sessionId);
// bookmark/publish are the only writes, so the row is materialized lazily on
// first flag write — matching the tRPC behavior of writing session state on
// demand.
func (s *Server) upsertSession(projectId, sessionId string) (*core.Record, error) {
	rec, err := s.findByExt(ColSession, projectId, sessionId)
	if err == nil {
		return rec, nil
	}
	if !notFound(err) {
		return nil, err
	}
	col, err := s.app.FindCollectionByNameOrId(ColSession)
	if err != nil {
		return nil, err
	}
	rec = core.NewRecord(col)
	rec.Set(fProjectId, projectId)
	rec.Set(fExtId, sessionId)
	return rec, nil
}

// =============================== scores ====================================

func (s *Server) handleScoreById(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapScoreByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ScoreByIdParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColScore, project, p.ScoreId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewScore(gen.ScoreInput{Present: false}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, scoreToBytes(rec))
}

func (s *Server) handleScoreHasAny(req Call, project string) (*zaplib.Message, error) {
	_, err := gen.WrapProjectScope(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ProjectScope: "+err.Error()))
	}
	_, err = s.app.FindFirstRecordByFilter(ColScore, "projectId = {:p}", map[string]any{"p": project})
	has := err == nil
	if err != nil && !notFound(err) {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, gen.NewBoolResult(gen.BoolResultInput{Value: has}))
}

// handleScoreUpsertAnnotation implements createAnnotationScore (update=false) and
// updateAnnotationScore (update=true). Both upsert by (project, extId): on
// create the id may be empty and one is generated; on update the id is required
// and must already exist.
func (s *Server) handleScoreUpsertAnnotation(req Call, project string, update bool) (*zaplib.Message, error) {
	p, err := gen.WrapAnnotationScoreParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad AnnotationScoreParams: "+err.Error()))
	}

	id := p.Id()
	var rec *core.Record
	if id != "" {
		rec, err = s.findByExt(ColScore, project, id)
		if err != nil && !notFound(err) {
			return s.reply(req, StatusInternal, errorBody(err.Error()))
		}
	}
	if rec == nil {
		if update {
			return s.reply(req, StatusNotFound, errorBody("score not found for update"))
		}
		col, cerr := s.app.FindCollectionByNameOrId(ColScore)
		if cerr != nil {
			return s.reply(req, StatusInternal, errorBody(cerr.Error()))
		}
		rec = core.NewRecord(col)
		rec.Set(fProjectId, project)
		if id == "" {
			id = newID()
		}
		rec.Set(fExtId, id)
	}

	rec.Set(fName, p.Name())
	rec.Set(fValue, p.Value())
	rec.Set(fStringValue, p.StringValue())
	rec.Set(fDataType, p.DataType())
	rec.Set(fSource, "ANNOTATION")
	rec.Set(fComment, p.Comment())
	rec.Set(fConfigId, p.ConfigId())
	rec.Set(fQueueId, p.QueueId())
	rec.Set(fAuthorUserId, p.AuthorUserId())
	rec.Set(fEnvironment, defaultStr(p.Environment(), "default"))
	rec.Set(fTimestamp, p.Timestamp())
	// Target: trace (+ optional observation) or session. Mirrors the discriminated
	// union ScoreTarget in the tRPC input — exactly one branch is populated.
	switch p.ScoreTargetType() {
	case "session":
		rec.Set(fSessionId, p.SessionId())
		rec.Set(fTraceId, "")
		rec.Set(fObservationId, "")
	default: // "trace"
		rec.Set(fTraceId, p.TraceId())
		rec.Set(fObservationId, p.ObservationId())
		rec.Set(fSessionId, "")
	}

	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, scoreToBytes(rec))
}

func (s *Server) handleScoreDeleteAnnotation(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapScoreByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ScoreByIdParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColScore, project, p.ScoreId())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewMutationCount(gen.MutationCountInput{Count: 0}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	if err := s.app.Delete(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, gen.NewMutationCount(gen.MutationCountInput{Count: 1}))
}

func (s *Server) handleEventScoresForTrace(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapEventScoresParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad EventScoresParams: "+err.Error()))
	}
	recs, err := s.app.FindRecordsByFilter(ColScore,
		"projectId = {:p} && traceId = {:t}", "timestamp", 0, 0,
		map[string]any{"p": project, "t": p.TraceId()})
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	items := make([][]byte, len(recs))
	for i, r := range recs {
		items[i] = scoreToBytes(r)
	}
	return s.reply(req, StatusOK, gen.NewScoreList(gen.ScoreListInput{Items: items}))
}

// ============================ score configs ================================

func (s *Server) handleScoreConfigAll(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapScoreConfigAllParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ScoreConfigAllParams: "+err.Error()))
	}
	limit, offset := 0, 0
	if p.Limit() > 0 {
		limit = int(p.Limit())
		offset = int(p.Page()) * int(p.Limit())
	}
	recs, err := s.app.FindRecordsByFilter(ColScoreConfig,
		"projectId = {:p}", "-created", limit, offset, map[string]any{"p": project})
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	total, err := s.app.CountRecords(ColScoreConfig)
	if err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	items := make([][]byte, len(recs))
	for i, r := range recs {
		items[i] = scoreConfigToBytes(r)
	}
	return s.reply(req, StatusOK, gen.NewScoreConfigList(gen.ScoreConfigListInput{
		Items: items, TotalCount: uint64(total),
	}))
}

func (s *Server) handleScoreConfigById(req Call, project string) (*zaplib.Message, error) {
	p, err := gen.WrapScoreConfigByIdParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ScoreConfigByIdParams: "+err.Error()))
	}
	rec, err := s.findByExt(ColScoreConfig, project, p.Id())
	if err != nil {
		if notFound(err) {
			return s.reply(req, StatusOK, gen.NewScoreConfig(gen.ScoreConfigInput{Present: false}))
		}
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, scoreConfigToBytes(rec))
}

// handleScoreConfigUpsert implements create (update=false) and update
// (update=true). create generates an id; update requires an existing row and
// applies only the fields the patch set (the Set* flags distinguish omitted
// from zero).
func (s *Server) handleScoreConfigUpsert(req Call, project string, update bool) (*zaplib.Message, error) {
	p, err := gen.WrapScoreConfigWriteParams(req.Payload)
	if err != nil {
		return s.reply(req, StatusBadRequest, errorBody("bad ScoreConfigWriteParams: "+err.Error()))
	}

	var rec *core.Record
	if update {
		rec, err = s.findByExt(ColScoreConfig, project, p.Id())
		if err != nil {
			if notFound(err) {
				return s.reply(req, StatusNotFound, errorBody("score config not found"))
			}
			return s.reply(req, StatusInternal, errorBody(err.Error()))
		}
	} else {
		col, cerr := s.app.FindCollectionByNameOrId(ColScoreConfig)
		if cerr != nil {
			return s.reply(req, StatusInternal, errorBody(cerr.Error()))
		}
		rec = core.NewRecord(col)
		rec.Set(fProjectId, project)
		rec.Set(fExtId, newID())
		rec.Set(fName, p.Name())
		rec.Set(fDataType, p.DataType())
		rec.Set(fIsArchived, false)
	}

	// Patchable fields. On update, only apply fields the caller actually set.
	if !update || p.SetName() {
		rec.Set(fName, p.Name())
	}
	if !update {
		rec.Set(fDataType, p.DataType())
	}
	rec.Set(fHasMin, p.HasMin())
	rec.Set(fHasMax, p.HasMax())
	rec.Set(fMinValue, p.MinValue())
	rec.Set(fMaxValue, p.MaxValue())
	rec.Set(fDescription, p.Description())
	if !update || p.SetCategories() {
		rec.Set(fCategories, jsonRaw(p.Categories()))
	}
	if !update || p.SetIsArchived() {
		rec.Set(fIsArchived, p.IsArchived())
	}

	if err := s.app.Save(rec); err != nil {
		return s.reply(req, StatusInternal, errorBody(err.Error()))
	}
	return s.reply(req, StatusOK, scoreConfigToBytes(rec))
}

// ====================== analytics (STUBBED with TODO) ======================

// handleStub answers the Datastore-backed analytics methods. These are
// columnar aggregations (paginated/filtered table scans, group-bys,
// cross-tabulations) that CANNOT be honestly served from Base OLTP rows — doing
// so would either fabricate numbers or silently degrade to a full table scan per
// request. Rather than fake the result, the method returns an explicit
// Empty{Stubbed:true} so callers can detect the gap.
//
// TODO(analytics-shim): implement a Go-side analytics reader that issues these
// queries against the columnar store and lives in server/analytics.go. The exact
// upstream queries to port (from console/web/src/server/api/routers + features):
//
//   - traceAll / traceCountAll  → getTracesTable / getTracesTableCount:
//     SELECT … FROM traces WHERE project_id = ? AND <filter>
//     ORDER BY <orderBy> LIMIT ? OFFSET ?   (+ count(*) variant)
//   - traceMetrics              → getTracesTableMetrics:
//     latency quantiles, token sums, cost sums, scores GROUP BY trace_id
//   - traceFilterOptions        → getTracesGroupedByName / …Tags / …Users:
//     SELECT name, count(*) FROM traces WHERE project_id = ? GROUP BY name
//   - sessionAll / sessionCountAll → getSessionsTable / …Count:
//     sessions rolled up from traces GROUP BY session_id (durations, counts)
//   - scoreAll / scoreCountAll  → getScoresUiTable / getScoresUiCount:
//     SELECT … FROM scores WHERE project_id = ? AND <filter> ORDER BY … LIMIT …
//   - eventAll                  → getEventList (events table scan + search)
//   - analyticsScoreComparison  → getScoreComparisonAnalytics:
//     per-score-name numeric/categorical distributions cross-tabulated over a
//     time window — the heaviest aggregation in the bundle.
//
// Until that shim exists these return Stubbed=true. They are still capability-
// gated (PermAnalyticsRead) and parameter-validated, so wiring the data path is
// purely additive — no API or auth change.
// stub answers a method whose data path is not wired yet. ONE policy in ONE
// place: status 501, a warn naming the method and where the shim goes, and the
// method's own typed zero body so a generated client still decodes the declared
// return type.
//
// 501, never 200. A stubbed 200 carrying an empty result is indistinguishable
// at the call site from a real query that matched nothing — the UI renders "no
// data" and the missing backend never surfaces. The status is the one field a
// caller cannot skip reading; a Stubbed flag in the body is one a caller can,
// and eventually does.
func (s *Server) stub(req Call, shim string, body []byte) (*zaplib.Message, error) {
	s.logger.Warn("obs: method stubbed", "method", req.Method, "shim", shim)
	return s.reply(req, StatusNotImpl, body)
}

// handleStub serves the trace/session/score/event analytics aggregations.
// handleStub serves the trace/session/score/event analytics aggregations. It
// takes project because the shim replacing it must scope its aggregation to the
// tenant — carrying it now makes wiring the shim a body change rather than a
// signature change threaded back through the dispatcher.
func (s *Server) handleStub(req Call, project string) (*zaplib.Message, error) {
	s.logger.Debug("obs: analytics stub hit", "project", project)
	return s.stub(req, "Datastore analytics", gen.NewEmpty(gen.EmptyInput{Stubbed: true}))
}

// =========================== record <-> view ===============================

func traceToBytes(r *core.Record) []byte {
	return gen.NewTrace(gen.TraceInput{
		Id:         r.GetString(fExtId),
		ProjectId:  r.GetString(fProjectId),
		Name:       r.GetString(fName),
		UserId:     r.GetString(fUserId),
		SessionId:  r.GetString(fSessionId),
		Release:    r.GetString(fRelease),
		Version:    r.GetString(fVersion),
		Input:      r.GetString(fInput),
		Output:     r.GetString(fOutput),
		Metadata:   r.GetString(fMetadata),
		Tags:       r.GetString(fTags),
		Bookmarked: r.GetBool(fBookmarked),
		Public:     r.GetBool(fPublic),
		Timestamp:  iv(r, fTimestamp),
		CreatedAt:  unixOf(r, "created"),
		UpdatedAt:  unixOf(r, "updated"),
		Present:    true,
	})
}

func observationToBytes(r *core.Record) []byte {
	return gen.NewObservation(gen.ObservationInput{
		Id:                  r.GetString(fExtId),
		TraceId:             r.GetString(fTraceId),
		ProjectId:           r.GetString(fProjectId),
		Type:                r.GetString(fType),
		Name:                r.GetString(fName),
		ParentObservationId: r.GetString(fParentObservationId),
		StartTime:           iv(r, fStartTime),
		EndTime:             iv(r, fEndTime),
		Input:               r.GetString(fInput),
		Output:              r.GetString(fOutput),
		Metadata:            r.GetString(fMetadata),
		Level:               r.GetString(fLevel),
		StatusMessage:       r.GetString(fStatusMessage),
		Model:               r.GetString(fModel),
		InternalModelId:     r.GetString(fInternalModelId),
		PromptTokens:        iv(r, fPromptTokens),
		CompletionTokens:    iv(r, fCompletionTokens),
		TotalTokens:         iv(r, fTotalTokens),
		Present:             true,
	})
}

func scoreToBytes(r *core.Record) []byte {
	return gen.NewScore(gen.ScoreInput{
		Id:            r.GetString(fExtId),
		ProjectId:     r.GetString(fProjectId),
		Name:          r.GetString(fName),
		Value:         r.GetFloat(fValue),
		StringValue:   r.GetString(fStringValue),
		DataType:      r.GetString(fDataType),
		Source:        r.GetString(fSource),
		Comment:       r.GetString(fComment),
		TraceId:       r.GetString(fTraceId),
		ObservationId: r.GetString(fObservationId),
		SessionId:     r.GetString(fSessionId),
		ScoreConfigId: r.GetString(fConfigId),
		QueueId:       r.GetString(fQueueId),
		AuthorUserId:  r.GetString(fAuthorUserId),
		Environment:   r.GetString(fEnvironment),
		Metadata:      r.GetString(fMetadata),
		Timestamp:     iv(r, fTimestamp),
		CreatedAt:     unixOf(r, "created"),
		UpdatedAt:     unixOf(r, "updated"),
		Present:       true,
	})
}

// sessionToBytes also rolls up the trace count for the session (the events FK
// aggregate the tRPC layer reported alongside the session). A precise columnar
// count belongs in the analytics shim (handleStub TODO); here it is a
// project-scoped row count, correct for the modest per-session trace volume.
func (s *Server) sessionToBytes(r *core.Record) []byte {
	var count int64
	if recs, err := s.app.FindRecordsByFilter(ColTrace,
		"projectId = {:p} && sessionId = {:s}", "", 0, 0,
		map[string]any{"p": r.GetString(fProjectId), "s": r.GetString(fExtId)}); err == nil {
		count = int64(len(recs))
	}
	return gen.NewSession(gen.SessionInput{
		Id:          r.GetString(fExtId),
		ProjectId:   r.GetString(fProjectId),
		Bookmarked:  r.GetBool(fBookmarked),
		Public:      r.GetBool(fPublic),
		CreatedAt:   unixOf(r, "created"),
		CountTraces: uint64(count),
		Present:     true,
	})
}

func scoreConfigToBytes(r *core.Record) []byte {
	return gen.NewScoreConfig(gen.ScoreConfigInput{
		Id:          r.GetString(fExtId),
		ProjectId:   r.GetString(fProjectId),
		Name:        r.GetString(fName),
		DataType:    r.GetString(fDataType),
		MinValue:    r.GetFloat(fMinValue),
		MaxValue:    r.GetFloat(fMaxValue),
		HasMin:      r.GetBool(fHasMin),
		HasMax:      r.GetBool(fHasMax),
		Categories:  r.GetString(fCategories),
		Description: r.GetString(fDescription),
		IsArchived:  r.GetBool(fIsArchived),
		CreatedAt:   unixOf(r, "created"),
		UpdatedAt:   unixOf(r, "updated"),
		Present:     true,
	})
}

// =============================== helpers ===================================

// iv reads a numeric field as int64. Base stores NumberField as a float in
// SQLite and exposes it via GetInt (which returns the platform int); we widen to
// the int64 the generated i64 views expect.
func iv(r *core.Record, field string) int64 { return int64(r.GetInt(field)) }

// unixOf reads an autodate field as unix seconds (0 if unset).
func unixOf(r *core.Record, field string) int64 {
	t := r.GetDateTime(field)
	if t.IsZero() {
		return 0
	}
	return t.Time().Unix()
}

// latencySeconds computes (max endTime - min startTime) over observation records,
// in seconds. Returns 0 when there are no observations or no end times — matching
// the tRPC byIdWith… latency rollup.
func latencySeconds(recs []*core.Record) float64 {
	if len(recs) == 0 {
		return 0
	}
	var minStart, maxEnd int64
	haveStart, haveEnd := false, false
	for _, r := range recs {
		st := iv(r, fStartTime)
		if st != 0 && (!haveStart || st < minStart) {
			minStart, haveStart = st, true
		}
		et := iv(r, fEndTime)
		if et != 0 && (!haveEnd || et > maxEnd) {
			maxEnd, haveEnd = et, true
		}
	}
	if !haveStart || !haveEnd || maxEnd <= minStart {
		return 0
	}
	return float64(maxEnd-minStart) / 1000.0
}

// jsonArray re-encodes a wire JSON []string into a canonical JSON array for
// storage. The wire already carries JSON text; we round-trip to validate and
// normalize, falling back to an empty array on malformed input.
func jsonArray(s string) any {
	if s == "" {
		return []string{}
	}
	var arr []string
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return []string{}
	}
	return arr
}

// jsonRaw validates a JSON blob (categories) and returns it as a normalized
// value for a JSON column, or null on malformed/empty input.
func jsonRaw(s string) any {
	if s == "" {
		return nil
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil
	}
	return v
}

func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
