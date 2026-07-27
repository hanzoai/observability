package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hanzoai/base/core"
	luxlog "github.com/luxfi/log"
	zaplib "github.com/luxfi/zap"
	zcap "github.com/zap-proto/go/cap"
)

// ObsPermissions — the per-procedure-category capability bits carried in the
// CapKindIAMSession Permissions u64. One bit per (resource, access) so a
// capability is minted least-privilege: a read-only dashboard cap holds the
// *Read bits; an ingestion/annotation cap holds the *Write bits. Each method
// gates on exactly ONE bit at the single chokepoint authorize.
//
// Layout (low bits = the OLTP backbone, high bits = satellites + analytics):
const (
	PermTraceRead        uint64 = 1 << 0  // traceById, traceWithDetail
	PermTraceWrite       uint64 = 1 << 1  // bookmark, publish, updateTags, deleteMany
	PermObservationRead  uint64 = 1 << 2  // observationById, eventBatchIO
	PermSessionRead      uint64 = 1 << 3  // sessionById, sessionHasAny
	PermSessionWrite     uint64 = 1 << 4  // sessionBookmark, sessionPublish
	PermScoreRead        uint64 = 1 << 5  // scoreById, scoreHasAny, eventScoresForTrace
	PermScoreWrite       uint64 = 1 << 6  // createAnnotation, updateAnnotation, deleteAnnotation
	PermScoreConfigRead  uint64 = 1 << 7  // scoreConfigAll, scoreConfigById
	PermScoreConfigWrite uint64 = 1 << 8  // scoreConfigCreate, scoreConfigUpdate
	PermEventWrite       uint64 = 1 << 9  // (reserved: event ingestion once writes land here)
	PermAnalyticsRead    uint64 = 1 << 10 // all/countAll/metrics/filterOptions + scoreAnalytics (STUBBED)

	// Dashboards surface (folded in). Widget and table-batch-action operations
	// live under the Dashboard category — the console scopes them as
	// dashboards:read / dashboards:CUD, so they reuse these bits rather than
	// minting redundant ones.
	//
	//	PermDashboardRead ⇄ dashboards:read        PermDashboardWrite ⇄ dashboards:CUD
	//	PermPresetRead    ⇄ TableViewPresets:read  PermPresetWrite    ⇄ TableViewPresets:CUD
	//	PermMonitorRead   ⇄ monitors:read          PermMonitorWrite   ⇄ monitors:CUD
	//
	// chart/scoreHistogram/executeQuery do NOT get a bit here: they gate on
	// PermAnalyticsRead above. The standalone binary carried its own duplicate
	// analytics bit; the same aggregations answering under one bit is the whole
	// reason these two services are one.
	PermDashboardRead  uint64 = 1 << 11
	PermDashboardWrite uint64 = 1 << 12
	PermPresetRead     uint64 = 1 << 13
	PermPresetWrite    uint64 = 1 << 14
	PermMonitorRead    uint64 = 1 << 15
	PermMonitorWrite   uint64 = 1 << 16
)

// methodPermission maps each method id to the single bit that admits it. A
// method absent from this table is unknown and rejected (fail closed).
var methodPermission = map[uint32]uint64{
	MethodTraceById:       PermTraceRead,
	MethodTraceWithDetail: PermTraceRead,
	MethodTraceBookmark:   PermTraceWrite,
	MethodTracePublish:    PermTraceWrite,
	MethodTraceUpdateTags: PermTraceWrite,
	MethodTraceDeleteMany: PermTraceWrite,

	MethodObservationById: PermObservationRead,
	MethodEventBatchIO:    PermObservationRead,

	MethodSessionHasAny:   PermSessionRead,
	MethodSessionById:     PermSessionRead,
	MethodSessionBookmark: PermSessionWrite,
	MethodSessionPublish:  PermSessionWrite,

	MethodScoreById:             PermScoreRead,
	MethodScoreHasAny:           PermScoreRead,
	MethodEventScoresForTrace:   PermScoreRead,
	MethodScoreCreateAnnotation: PermScoreWrite,
	MethodScoreUpdateAnnotation: PermScoreWrite,
	MethodScoreDeleteAnnotation: PermScoreWrite,

	MethodScoreConfigAll:    PermScoreConfigRead,
	MethodScoreConfigById:   PermScoreConfigRead,
	MethodScoreConfigCreate: PermScoreConfigWrite,
	MethodScoreConfigUpdate: PermScoreConfigWrite,

	// Analytics (STUBBED): still gated — a caller must hold AnalyticsRead even
	// though the body returns an Empty/Stubbed result. The gate is real; the
	// data path is a documented follow-up (handlers.go handleStub).
	MethodTraceAll:                 PermAnalyticsRead,
	MethodTraceCountAll:            PermAnalyticsRead,
	MethodTraceMetrics:             PermAnalyticsRead,
	MethodTraceFilterOptions:       PermAnalyticsRead,
	MethodSessionAll:               PermAnalyticsRead,
	MethodSessionCountAll:          PermAnalyticsRead,
	MethodScoreAll:                 PermAnalyticsRead,
	MethodScoreCountAll:            PermAnalyticsRead,
	MethodEventAll:                 PermAnalyticsRead,
	MethodAnalyticsScoreComparison: PermAnalyticsRead,

	// --- dashboards surface ---
	MethodAllDashboards:  PermDashboardRead,
	MethodGetDashboard:   PermDashboardRead,
	MethodAllWidgets:     PermDashboardRead,
	MethodGetWidget:      PermDashboardRead,
	// Table batch-action progress is a dashboards:read scope upstream, so it
	// gates on the Dashboard bit even though its data path is the queue.
	MethodIsBatchActionInProgress: PermDashboardRead,

	MethodCreateDashboard:        PermDashboardWrite,
	MethodUpdateDashboardMeta:    PermDashboardWrite,
	MethodUpdateDashboardDef:     PermDashboardWrite,
	MethodUpdateDashboardFilters: PermDashboardWrite,
	MethodCloneDashboard:         PermDashboardWrite,
	MethodDeleteDashboard:        PermDashboardWrite,
	MethodCreateWidget:           PermDashboardWrite,
	MethodUpdateWidget:           PermDashboardWrite,
	MethodCopyWidgetToProject:    PermDashboardWrite,
	MethodDeleteWidget:           PermDashboardWrite,

	MethodGetPresetsByTableName: PermPresetRead,
	MethodGetPresetById:         PermPresetRead,
	MethodGeneratePermalink:     PermPresetRead,
	MethodCreatePreset:          PermPresetWrite,
	MethodUpdatePreset:          PermPresetWrite,
	MethodUpdatePresetName:      PermPresetWrite,
	MethodDeletePreset:          PermPresetWrite,

	MethodAllMonitors:   PermMonitorRead,
	MethodGetMonitor:    PermMonitorRead,
	MethodCreateMonitor: PermMonitorWrite,
	MethodUpdateMonitor: PermMonitorWrite,
	MethodDeleteMonitor: PermMonitorWrite,

	// Dashboard analytics share the analytics bit with the trace/score
	// aggregations above — one bit, one shim, one gate.
	MethodChart:          PermAnalyticsRead,
	MethodScoreHistogram: PermAnalyticsRead,
	MethodExecuteQuery:   PermAnalyticsRead,
}

// Server implements the Observability ZAP capability-RPC interface on a Base
// app: one method per tRPC procedure, each gated on the caller's capability via
// the single chokepoint authorize, then reading/writing a Base collection. The
// Go peer of the console's ObservabilityTarget.
type Server struct {
	app    core.App
	logger luxlog.Logger

	// verifier validates capability buffers. Wired to ed25519 (bootstrap); a PQ
	// deployment swaps in an ML-DSA-65 SchemeVerify + the IAM pubkey registry for
	// IssuerKey. See SPEC.md §2.3.
	verifier zcap.Verifier

	// promises is the server-side pipelining table: a call may carry PromiseID,
	// and a later call may Target it. Promises are FUTURES — a dependent call
	// arriving before its target resolves WAITS, then dispatches against the
	// resolved answer (Cap'n Proto promise pipelining). Short-lived.
	mu       sync.Mutex
	promises map[uint32]*promiseSlot
}

// promiseSlot is a future for a pipelined call's answer. done is closed when the
// slot resolves; project is then readable. The pipelined value is the resolved
// PROJECT scope — that is what a dependent call inherits, and inheriting it is
// what stops a pipelined call naming a different tenant. resolvedAt drives reaping.
type promiseSlot struct {
	done       chan struct{}
	project    string
	resolvedAt time.Time
}

const promiseWaitTimeout = 5 * time.Second

func (s *Server) getOrCreate(id uint32) *promiseSlot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reapLocked()
	slot, ok := s.promises[id]
	if !ok {
		slot = &promiseSlot{done: make(chan struct{})}
		s.promises[id] = slot
	}
	return slot
}

// reapLocked drops slots resolved more than promiseWaitTimeout ago — by then any
// dependent call has consumed them or timed out. Caller holds s.mu.
func (s *Server) reapLocked() {
	cutoff := time.Now().Add(-promiseWaitTimeout)
	for id, slot := range s.promises {
		if !slot.resolvedAt.IsZero() && slot.resolvedAt.Before(cutoff) {
			delete(s.promises, id)
		}
	}
}

func (s *Server) resolve(id uint32, project string) {
	slot := s.getOrCreate(id)
	s.mu.Lock()
	select {
	case <-slot.done:
		// already resolved — leave as-is
	default:
		slot.project = project
		slot.resolvedAt = time.Now()
		close(slot.done)
	}
	s.mu.Unlock()
}

func (s *Server) await(target uint32) (string, bool) {
	slot := s.getOrCreate(target)
	select {
	case <-slot.done:
		s.mu.Lock()
		project := slot.project
		s.mu.Unlock()
		return project, true
	case <-time.After(promiseWaitTimeout):
		return "", false
	}
}

// NewServer builds an Observability server. verifier supplies the capability
// trust anchor; pass a Verifier whose IssuerKey resolves your IAM issuer key.
func NewServer(app core.App, logger luxlog.Logger, verifier zcap.Verifier) *Server {
	return &Server{
		app:      app,
		logger:   logger,
		verifier: verifier,
		promises: make(map[uint32]*promiseSlot),
	}
}

// Register wires the handler onto a luxfi/zap node at this service's slot.
func (s *Server) Register(node *zaplib.Node) {
	node.Handle(MsgTypeRouterBase, s.handle)
}

// handle is the ZAP dispatch entrypoint: decode envelope → authorize → scope →
// route. The two gates are deliberately separate questions: authorize answers
// "may this caller invoke this method at all" (capability + permission bit);
// scope answers "whose rows may it touch" (the project). Braiding them is how a
// service ends up with a valid capability reading another tenant's data.
func (s *Server) handle(ctx context.Context, from string, msg *zaplib.Message) (*zaplib.Message, error) {
	_ = ctx
	req := parseRequest(msg)

	if status, errMsg := s.authorize(req); status != StatusOK {
		s.logger.Debug("obs: auth rejected", "from", from, "method", req.Method, "status", status, "err", errMsg)
		return buildResponse(status, req.PromiseID, errorBody(errMsg))
	}

	project, status, errMsg := s.scope(req)
	if status != StatusOK {
		return buildResponse(status, req.PromiseID, errorBody(errMsg))
	}

	// Resolve this call's promise (its project scope) so any dependent call
	// WAITING on it can proceed.
	if req.PromiseID != NoTarget {
		s.resolve(req.PromiseID, project)
	}

	return s.dispatch(req, project)
}

// scope resolves the project this call operates within — the ONE tenant
// boundary. A pipelined call inherits its target's resolved project; otherwise
// the project is read from the request payload, where every request struct
// carries ProjectId as text @0 (pinned by TestEveryRequestStructCarriesProjectIdAtZero).
//
// Inheriting rather than re-reading the payload on a pipelined call is the
// point: a dependent call cannot name a different project than the call it
// pipelines off, so pipelining can never be used to hop tenants.
func (s *Server) scope(req Call) (project string, status uint32, errMsg string) {
	if req.Target != NoTarget {
		p, ok := s.await(req.Target)
		if !ok {
			return "", StatusBadRequest, fmt.Sprintf("pipelined target %d did not resolve in time", req.Target)
		}
		return p, StatusOK, ""
	}
	if len(req.Payload) == 0 {
		return "", StatusBadRequest, "missing request payload"
	}
	p, err := zaplib.Parse(req.Payload)
	if err != nil {
		return "", StatusBadRequest, "malformed payload: " + err.Error()
	}
	project = p.Root().Text(0) // ProjectId is text @0 in every request struct
	if project == "" {
		return "", StatusBadRequest, "missing projectId"
	}
	return project, StatusOK, ""
}

// dispatch routes an authorized, project-scoped call to its handler. Adding a
// method means one case here + one row in methodPermission — nowhere else.
func (s *Server) dispatch(req Call, project string) (*zaplib.Message, error) {
	switch req.Method {
	// --- traces (honest) ---
	case MethodTraceById:
		return s.handleTraceById(req, project)
	case MethodTraceWithDetail:
		return s.handleTraceWithDetail(req, project)
	case MethodTraceBookmark:
		return s.handleTraceBookmark(req, project)
	case MethodTracePublish:
		return s.handleTracePublish(req, project)
	case MethodTraceUpdateTags:
		return s.handleTraceUpdateTags(req, project)
	case MethodTraceDeleteMany:
		return s.handleTraceDeleteMany(req, project)
	// --- observations (honest) ---
	case MethodObservationById:
		return s.handleObservationById(req, project)
	case MethodEventBatchIO:
		return s.handleEventBatchIO(req, project)
	// --- sessions (honest) ---
	case MethodSessionHasAny:
		return s.handleSessionHasAny(req, project)
	case MethodSessionById:
		return s.handleSessionById(req, project)
	case MethodSessionBookmark:
		return s.handleSessionBookmark(req, project)
	case MethodSessionPublish:
		return s.handleSessionPublish(req, project)
	// --- scores (honest) ---
	case MethodScoreById:
		return s.handleScoreById(req, project)
	case MethodScoreHasAny:
		return s.handleScoreHasAny(req, project)
	case MethodScoreCreateAnnotation:
		return s.handleScoreUpsertAnnotation(req, project, false)
	case MethodScoreUpdateAnnotation:
		return s.handleScoreUpsertAnnotation(req, project, true)
	case MethodScoreDeleteAnnotation:
		return s.handleScoreDeleteAnnotation(req, project)
	case MethodEventScoresForTrace:
		return s.handleEventScoresForTrace(req, project)
	// --- score configs (honest) ---
	case MethodScoreConfigAll:
		return s.handleScoreConfigAll(req, project)
	case MethodScoreConfigById:
		return s.handleScoreConfigById(req, project)
	case MethodScoreConfigCreate:
		return s.handleScoreConfigUpsert(req, project, false)
	case MethodScoreConfigUpdate:
		return s.handleScoreConfigUpsert(req, project, true)
	// --- analytics (STUBBED with TODO) ---
	case MethodTraceAll, MethodTraceCountAll, MethodTraceMetrics, MethodTraceFilterOptions,
		MethodSessionAll, MethodSessionCountAll,
		MethodScoreAll, MethodScoreCountAll,
		MethodEventAll, MethodAnalyticsScoreComparison:
		return s.handleStub(req, project)

	// --- dashboards (honest) ---
	case MethodAllDashboards:
		return s.handleAllDashboards(req, project)
	case MethodGetDashboard:
		return s.handleGetDashboard(req, project)
	case MethodCreateDashboard:
		return s.handleCreateDashboard(req, project)
	case MethodUpdateDashboardMeta:
		return s.handleUpdateDashboardMeta(req, project)
	case MethodUpdateDashboardDef:
		return s.handleUpdateDashboardDef(req, project)
	case MethodUpdateDashboardFilters:
		return s.handleUpdateDashboardFilters(req, project)
	case MethodCloneDashboard:
		return s.handleCloneDashboard(req, project)
	case MethodDeleteDashboard:
		return s.handleDeleteDashboard(req, project)
	// --- widgets (honest) ---
	case MethodAllWidgets:
		return s.handleAllWidgets(req, project)
	case MethodGetWidget:
		return s.handleGetWidget(req, project)
	case MethodCreateWidget:
		return s.handleUpsertWidget(req, project, false)
	case MethodUpdateWidget:
		return s.handleUpsertWidget(req, project, true)
	case MethodCopyWidgetToProject:
		return s.handleCopyWidget(req, project)
	case MethodDeleteWidget:
		return s.handleDeleteWidget(req, project)
	// --- table view presets (honest) ---
	case MethodGetPresetsByTableName:
		return s.handleGetPresetsByTableName(req, project)
	case MethodGetPresetById:
		return s.handleGetPresetById(req, project)
	case MethodCreatePreset:
		return s.handleUpsertPreset(req, project, false)
	case MethodUpdatePreset:
		return s.handleUpsertPreset(req, project, true)
	case MethodUpdatePresetName:
		return s.handleUpdatePresetName(req, project)
	case MethodDeletePreset:
		return s.handleDeletePreset(req, project)
	case MethodGeneratePermalink:
		return s.handleGeneratePermalink(req, project)
	// --- monitors (honest) ---
	case MethodAllMonitors:
		return s.handleAllMonitors(req, project)
	case MethodGetMonitor:
		return s.handleGetMonitor(req, project)
	case MethodCreateMonitor:
		return s.handleUpsertMonitor(req, project, false)
	case MethodUpdateMonitor:
		return s.handleUpsertMonitor(req, project, true)
	case MethodDeleteMonitor:
		return s.handleDeleteMonitor(req, project)
	// --- table batch-action (BullMQ queue — STUBBED) ---
	case MethodIsBatchActionInProgress:
		return s.handleIsBatchActionInProgress(req, project)
	// --- dashboard analytics (Datastore — STUBBED, same shim as above) ---
	case MethodChart, MethodScoreHistogram, MethodExecuteQuery:
		return s.handleAnalytics(req, project)

	default:
		return buildResponse(StatusBadRequest, req.PromiseID, errorBody(fmt.Sprintf("unknown method %d", req.Method)))
	}
}

// authorize enforces the capability: it must be a CapKindIAMSession cap
// carrying the one permission bit the requested method requires. Returns
// StatusOK on success. It answers ONLY "may this caller invoke this method" —
// which tenant's rows the call may touch is scope()'s question, resolved
// separately.
//
// The capability is verified on EVERY call. Pipelining elides round trips,
// never authorization.
func (s *Server) authorize(req Call) (status uint32, errMsg string) {
	need, known := methodPermission[req.Method]
	if !known {
		return StatusBadRequest, fmt.Sprintf("unknown method %d", req.Method)
	}

	c, err := zcap.Wrap(req.Cap)
	if err != nil {
		return StatusBadRequest, "malformed capability: " + err.Error()
	}

	// Kind gate: these methods are defined on a CapKindIAMSession cap.
	if c.Kind() != uint32(zcap.KindIAMSession) {
		return StatusForbidden, "capability is not a CapKindIAMSession"
	}

	// Permission gate — the single chokepoint. Fail closed: a cap lacking the
	// method's bit is rejected before any data is touched.
	if c.Permissions()&need == 0 {
		return StatusForbidden, fmt.Sprintf("capability lacks permission bit %#x for method %d", need, req.Method)
	}

	// Cryptographic verification. The full chain check (signature, expiry,
	// revocation, chain links) lives in verifier.Verify; run whenever an issuer
	// registry is wired. With no registry (bootstrap/tests) the signature step is
	// skipped but Kind + Permissions above are STILL enforced.
	//
	// TODO(SPEC.md §2.3): bind the cap to the live session via the out-of-band
	// holderSig over a server nonce, and walk the parent chain with
	// verifier.VerifyChain once the IAM pubkey registry is wired here.
	if s.verifier.IssuerKey != nil {
		if err := s.verifier.Verify(c, time.Now().Unix()); err != nil {
			return StatusUnauthorized, "capability verify failed: " + err.Error()
		}
	}
	return StatusOK, ""
}

// notFound reports whether err is the no-such-row sentinel from a project-scoped
// lookup (so a handler returns a present=false view rather than a 500).
func notFound(err error) bool { return errors.Is(err, sql.ErrNoRows) }

// findByExt looks up the single record in col with extId in projectId. Scoping
// EVERY query by projectId is the multi-tenant isolation boundary.
func (s *Server) findByExt(col, projectId, extId string) (*core.Record, error) {
	return s.app.FindFirstRecordByFilter(col,
		"projectId = {:p} && extId = {:e}",
		map[string]any{"p": projectId, "e": extId})
}
