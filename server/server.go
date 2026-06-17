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
}

// Server implements the Observability ZAP capability-RPC interface on a Base
// app: one method per tRPC procedure, each gated on the caller's capability via
// the single chokepoint authorize, then reading/writing a Base collection. The
// Go peer of the console's ObservabilityTarget.
type Server struct {
	app        core.App
	logger     luxlog.Logger
	defaultOrg string

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
// slot resolves; org is then readable. The pipelined value is the authenticated
// org. resolvedAt drives reaping.
type promiseSlot struct {
	done       chan struct{}
	org        string
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

func (s *Server) resolve(id uint32, org string) {
	slot := s.getOrCreate(id)
	s.mu.Lock()
	select {
	case <-slot.done:
		// already resolved — leave as-is
	default:
		slot.org = org
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
		org := slot.org
		s.mu.Unlock()
		return org, true
	case <-time.After(promiseWaitTimeout):
		return "", false
	}
}

// NewServer builds an Observability server. verifier supplies the capability
// trust anchor; pass a Verifier whose IssuerKey resolves your IAM issuer key.
func NewServer(app core.App, logger luxlog.Logger, defaultOrg string, verifier zcap.Verifier) *Server {
	return &Server{
		app:        app,
		logger:     logger,
		defaultOrg: defaultOrg,
		verifier:   verifier,
		promises:   make(map[uint32]*promiseSlot),
	}
}

// Register wires the handler onto a luxfi/zap node at this service's slot.
func (s *Server) Register(node *zaplib.Node) {
	node.Handle(MsgTypeRouterBase, s.handle)
}

// handle is the ZAP dispatch entrypoint: decode envelope → authorize → route.
func (s *Server) handle(ctx context.Context, from string, msg *zaplib.Message) (*zaplib.Message, error) {
	req := parseRequest(msg)

	_, status, errMsg := s.authorize(req)
	if status != StatusOK {
		s.logger.Debug("obs: auth rejected", "from", from, "method", req.Method, "status", status, "err", errMsg)
		return buildResponse(status, req.PromiseID, errorBody(errMsg))
	}

	// Resolve this call's promise (the authenticated org) so any dependent call
	// WAITING on it can proceed. authorize() already awaited our own target if we
	// had one, so the scope is fully resolved by here.
	if req.PromiseID != NoTarget {
		s.resolve(req.PromiseID, s.defaultOrg)
	}

	switch req.Method {
	// --- traces (honest) ---
	case MethodTraceById:
		return s.handleTraceById(req)
	case MethodTraceWithDetail:
		return s.handleTraceWithDetail(req)
	case MethodTraceBookmark:
		return s.handleTraceBookmark(req)
	case MethodTracePublish:
		return s.handleTracePublish(req)
	case MethodTraceUpdateTags:
		return s.handleTraceUpdateTags(req)
	case MethodTraceDeleteMany:
		return s.handleTraceDeleteMany(req)
	// --- observations (honest) ---
	case MethodObservationById:
		return s.handleObservationById(req)
	case MethodEventBatchIO:
		return s.handleEventBatchIO(req)
	// --- sessions (honest) ---
	case MethodSessionHasAny:
		return s.handleSessionHasAny(req)
	case MethodSessionById:
		return s.handleSessionById(req)
	case MethodSessionBookmark:
		return s.handleSessionBookmark(req)
	case MethodSessionPublish:
		return s.handleSessionPublish(req)
	// --- scores (honest) ---
	case MethodScoreById:
		return s.handleScoreById(req)
	case MethodScoreHasAny:
		return s.handleScoreHasAny(req)
	case MethodScoreCreateAnnotation:
		return s.handleScoreUpsertAnnotation(req, false)
	case MethodScoreUpdateAnnotation:
		return s.handleScoreUpsertAnnotation(req, true)
	case MethodScoreDeleteAnnotation:
		return s.handleScoreDeleteAnnotation(req)
	case MethodEventScoresForTrace:
		return s.handleEventScoresForTrace(req)
	// --- score configs (honest) ---
	case MethodScoreConfigAll:
		return s.handleScoreConfigAll(req)
	case MethodScoreConfigById:
		return s.handleScoreConfigById(req)
	case MethodScoreConfigCreate:
		return s.handleScoreConfigUpsert(req, false)
	case MethodScoreConfigUpdate:
		return s.handleScoreConfigUpsert(req, true)
	// --- analytics (STUBBED with TODO) ---
	case MethodTraceAll, MethodTraceCountAll, MethodTraceMetrics, MethodTraceFilterOptions,
		MethodSessionAll, MethodSessionCountAll,
		MethodScoreAll, MethodScoreCountAll,
		MethodEventAll, MethodAnalyticsScoreComparison:
		return s.handleStub(req)
	default:
		return buildResponse(StatusBadRequest, req.PromiseID, errorBody(fmt.Sprintf("unknown method %d", req.Method)))
	}
}

// authorize resolves the call's effective org and enforces the per-method
// capability bit. Returns (org, StatusOK, "") on success.
//
// Pipelining: if the call Targets an earlier promise, its org is inherited from
// that promise's resolved answer. The capability is STILL verified on every call
// — pipelining elides round trips, never authorization.
func (s *Server) authorize(req Call) (org string, status uint32, errMsg string) {
	need, known := methodPermission[req.Method]
	if !known {
		return "", StatusBadRequest, fmt.Sprintf("unknown method %d", req.Method)
	}

	c, err := zcap.Wrap(req.Cap)
	if err != nil {
		return "", StatusBadRequest, "malformed capability: " + err.Error()
	}

	// Kind gate: these methods are defined on a CapKindIAMSession cap.
	if c.Kind() != uint32(zcap.KindIAMSession) {
		return "", StatusForbidden, "capability is not a CapKindIAMSession"
	}

	// Permission gate — the single chokepoint. Fail closed: a cap lacking the
	// method's bit is rejected before any data is touched.
	if c.Permissions()&need == 0 {
		return "", StatusForbidden, fmt.Sprintf("capability lacks permission bit %#x for method %d", need, req.Method)
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
			return "", StatusUnauthorized, "capability verify failed: " + err.Error()
		}
	}

	// Effective org: inherited from a targeted promise, else the service default.
	// (Holder→org is an IAM lookup; until wired, scope to the default org.) The
	// luxfi/zap transport is per-connection FIFO and the client ships the target
	// call before the dependent one, so await returns immediately in practice;
	// the timeout is a safety net against an out-of-order client.
	if req.Target != NoTarget {
		o, ok := s.await(req.Target)
		if !ok {
			return "", StatusBadRequest, fmt.Sprintf("pipelined target %d did not resolve in time", req.Target)
		}
		return o, StatusOK, ""
	}
	return s.defaultOrg, StatusOK, ""
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
