package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	zaplib "github.com/luxfi/zap"
	zcap "github.com/zap-proto/go/cap"

	gen "github.com/hanzoai/observability/gen"
)

// Client is an Observability ZAP capability-RPC client — what console's bridge
// substitutes for the in-process Server: a thin typed wrapper over a luxfi/zap
// connection that ships the verified capability with every call. One Go method
// per tRPC procedure, each shipping a typed params view and decoding a typed
// result view.
//
// Construct with Dial, then call the typed methods or Pipeline*.
type Client struct {
	node   *zaplib.Node
	peerID string
	capBuf []byte

	promiseSeq uint32 // monotonic PromiseID allocator

	// sendLog records, in order, every call shipped — used by the pipelining
	// proof to show a dependent call ships before the target's answer resolves.
	logMu   sync.Mutex
	sendLog *[]SendEvent
}

// SendEvent is one entry in the instrumentation log: a call left the client
// (send) or its answer arrived (recv), with a monotonic sequence number.
type SendEvent struct {
	Seq       uint64
	Kind      string // "send" or "recv"
	Method    uint32
	PromiseID uint32
	Target    uint32
	At        time.Time
}

var sendEventSeq uint64

// pipelineIDSeq hands out process-unique promise ids for pipelined call groups,
// starting high so a pipeline id never collides with a plain per-call PromiseID.
var pipelineIDSeq uint32 = 1 << 20

func nextPipelineID() uint32 { return atomic.AddUint32(&pipelineIDSeq, 1) }

// Dial constructs a Client over an already-started local node, connecting to the
// service at addr (e.g. "127.0.0.1:9992"). capBuf is the caller's opaque
// capability buffer (a zcap.Cap.Bytes()). peerID is the service's ZAP node id.
func Dial(node *zaplib.Node, addr, peerID string, capBuf []byte) (*Client, error) {
	if err := node.ConnectDirect(addr); err != nil {
		return nil, fmt.Errorf("obs client: connect %s: %w", addr, err)
	}
	return &Client{node: node, peerID: peerID, capBuf: capBuf}, nil
}

// WithSendLog attaches an instrumentation slice the client appends send/recv
// events to. Returns the client for chaining.
func (c *Client) WithSendLog(log *[]SendEvent) *Client {
	c.sendLog = log
	return c
}

func (c *Client) record(kind string, method, promiseID, target uint32) {
	if c.sendLog == nil {
		return
	}
	c.logMu.Lock()
	*c.sendLog = append(*c.sendLog, SendEvent{
		Seq:       atomic.AddUint64(&sendEventSeq, 1),
		Kind:      kind,
		Method:    method,
		PromiseID: promiseID,
		Target:    target,
		At:        time.Now(),
	})
	c.logMu.Unlock()
}

func (c *Client) nextPromise() uint32 { return atomic.AddUint32(&c.promiseSeq, 1) }

// call ships one request (with payload) and blocks for its correlated response.
func (c *Client) call(ctx context.Context, method, promiseID, target uint32, payload []byte) (Response, error) {
	msg, err := buildRequest(Call{
		Method:    method,
		PromiseID: promiseID,
		Target:    target,
		Cap:       c.capBuf,
		Payload:   payload,
	})
	if err != nil {
		return Response{}, err
	}
	c.record("send", method, promiseID, target)
	resp, err := c.node.Call(ctx, c.peerID, msg)
	if err != nil {
		return Response{}, err
	}
	c.record("recv", method, promiseID, target)
	return parseResponse(resp), nil
}

// do is the typed-call helper: ship method+payload, check status, return body.
func (c *Client) do(ctx context.Context, method uint32, payload []byte) ([]byte, error) {
	resp, err := c.call(ctx, method, c.nextPromise(), NoTarget, payload)
	if err != nil {
		return nil, err
	}
	if resp.Status != StatusOK {
		return nil, fmt.Errorf("method %d: status %d: %s", method, resp.Status, resp.Body)
	}
	return resp.Body, nil
}

// =============================== traces ====================================

func (c *Client) TraceById(ctx context.Context, p gen.TraceByIdParamsInput) (gen.Trace, error) {
	b, err := c.do(ctx, MethodTraceById, gen.NewTraceByIdParams(p))
	if err != nil {
		return gen.Trace{}, err
	}
	return gen.WrapTrace(b)
}

func (c *Client) TraceWithDetail(ctx context.Context, p gen.TraceByIdParamsInput) (gen.TraceWithDetail, error) {
	b, err := c.do(ctx, MethodTraceWithDetail, gen.NewTraceByIdParams(p))
	if err != nil {
		return gen.TraceWithDetail{}, err
	}
	return gen.WrapTraceWithDetail(b)
}

func (c *Client) TraceBookmark(ctx context.Context, p gen.TraceBookmarkParamsInput) (gen.Trace, error) {
	b, err := c.do(ctx, MethodTraceBookmark, gen.NewTraceBookmarkParams(p))
	if err != nil {
		return gen.Trace{}, err
	}
	return gen.WrapTrace(b)
}

func (c *Client) TracePublish(ctx context.Context, p gen.TracePublishParamsInput) (gen.Trace, error) {
	b, err := c.do(ctx, MethodTracePublish, gen.NewTracePublishParams(p))
	if err != nil {
		return gen.Trace{}, err
	}
	return gen.WrapTrace(b)
}

func (c *Client) TraceUpdateTags(ctx context.Context, p gen.TraceUpdateTagsParamsInput) (gen.Trace, error) {
	b, err := c.do(ctx, MethodTraceUpdateTags, gen.NewTraceUpdateTagsParams(p))
	if err != nil {
		return gen.Trace{}, err
	}
	return gen.WrapTrace(b)
}

func (c *Client) TraceDeleteMany(ctx context.Context, p gen.TraceDeleteManyParamsInput) (gen.MutationCount, error) {
	b, err := c.do(ctx, MethodTraceDeleteMany, gen.NewTraceDeleteManyParams(p))
	if err != nil {
		return gen.MutationCount{}, err
	}
	return gen.WrapMutationCount(b)
}

// ============================ observations =================================

func (c *Client) ObservationById(ctx context.Context, p gen.ObservationByIdParamsInput) (gen.Observation, error) {
	b, err := c.do(ctx, MethodObservationById, gen.NewObservationByIdParams(p))
	if err != nil {
		return gen.Observation{}, err
	}
	return gen.WrapObservation(b)
}

func (c *Client) EventBatchIO(ctx context.Context, p gen.EventBatchIOParamsInput) (gen.ObservationIOList, error) {
	b, err := c.do(ctx, MethodEventBatchIO, gen.NewEventBatchIOParams(p))
	if err != nil {
		return gen.ObservationIOList{}, err
	}
	return gen.WrapObservationIOList(b)
}

// ============================== sessions ===================================

func (c *Client) SessionHasAny(ctx context.Context, p gen.ProjectScopeInput) (gen.BoolResult, error) {
	b, err := c.do(ctx, MethodSessionHasAny, gen.NewProjectScope(p))
	if err != nil {
		return gen.BoolResult{}, err
	}
	return gen.WrapBoolResult(b)
}

func (c *Client) SessionById(ctx context.Context, p gen.SessionByIdParamsInput) (gen.SessionWithScores, error) {
	b, err := c.do(ctx, MethodSessionById, gen.NewSessionByIdParams(p))
	if err != nil {
		return gen.SessionWithScores{}, err
	}
	return gen.WrapSessionWithScores(b)
}

func (c *Client) SessionBookmark(ctx context.Context, p gen.SessionBookmarkParamsInput) (gen.Session, error) {
	b, err := c.do(ctx, MethodSessionBookmark, gen.NewSessionBookmarkParams(p))
	if err != nil {
		return gen.Session{}, err
	}
	return gen.WrapSession(b)
}

func (c *Client) SessionPublish(ctx context.Context, p gen.SessionPublishParamsInput) (gen.Session, error) {
	b, err := c.do(ctx, MethodSessionPublish, gen.NewSessionPublishParams(p))
	if err != nil {
		return gen.Session{}, err
	}
	return gen.WrapSession(b)
}

// =============================== scores ====================================

func (c *Client) ScoreById(ctx context.Context, p gen.ScoreByIdParamsInput) (gen.Score, error) {
	b, err := c.do(ctx, MethodScoreById, gen.NewScoreByIdParams(p))
	if err != nil {
		return gen.Score{}, err
	}
	return gen.WrapScore(b)
}

func (c *Client) ScoreHasAny(ctx context.Context, p gen.ProjectScopeInput) (gen.BoolResult, error) {
	b, err := c.do(ctx, MethodScoreHasAny, gen.NewProjectScope(p))
	if err != nil {
		return gen.BoolResult{}, err
	}
	return gen.WrapBoolResult(b)
}

func (c *Client) ScoreCreateAnnotation(ctx context.Context, p gen.AnnotationScoreParamsInput) (gen.Score, error) {
	b, err := c.do(ctx, MethodScoreCreateAnnotation, gen.NewAnnotationScoreParams(p))
	if err != nil {
		return gen.Score{}, err
	}
	return gen.WrapScore(b)
}

func (c *Client) ScoreUpdateAnnotation(ctx context.Context, p gen.AnnotationScoreParamsInput) (gen.Score, error) {
	b, err := c.do(ctx, MethodScoreUpdateAnnotation, gen.NewAnnotationScoreParams(p))
	if err != nil {
		return gen.Score{}, err
	}
	return gen.WrapScore(b)
}

func (c *Client) ScoreDeleteAnnotation(ctx context.Context, p gen.ScoreByIdParamsInput) (gen.MutationCount, error) {
	b, err := c.do(ctx, MethodScoreDeleteAnnotation, gen.NewScoreByIdParams(p))
	if err != nil {
		return gen.MutationCount{}, err
	}
	return gen.WrapMutationCount(b)
}

func (c *Client) EventScoresForTrace(ctx context.Context, p gen.EventScoresParamsInput) (gen.ScoreList, error) {
	b, err := c.do(ctx, MethodEventScoresForTrace, gen.NewEventScoresParams(p))
	if err != nil {
		return gen.ScoreList{}, err
	}
	return gen.WrapScoreList(b)
}

// ============================ score configs ================================

func (c *Client) ScoreConfigAll(ctx context.Context, p gen.ScoreConfigAllParamsInput) (gen.ScoreConfigList, error) {
	b, err := c.do(ctx, MethodScoreConfigAll, gen.NewScoreConfigAllParams(p))
	if err != nil {
		return gen.ScoreConfigList{}, err
	}
	return gen.WrapScoreConfigList(b)
}

func (c *Client) ScoreConfigById(ctx context.Context, p gen.ScoreConfigByIdParamsInput) (gen.ScoreConfig, error) {
	b, err := c.do(ctx, MethodScoreConfigById, gen.NewScoreConfigByIdParams(p))
	if err != nil {
		return gen.ScoreConfig{}, err
	}
	return gen.WrapScoreConfig(b)
}

func (c *Client) ScoreConfigCreate(ctx context.Context, p gen.ScoreConfigWriteParamsInput) (gen.ScoreConfig, error) {
	b, err := c.do(ctx, MethodScoreConfigCreate, gen.NewScoreConfigWriteParams(p))
	if err != nil {
		return gen.ScoreConfig{}, err
	}
	return gen.WrapScoreConfig(b)
}

func (c *Client) ScoreConfigUpdate(ctx context.Context, p gen.ScoreConfigWriteParamsInput) (gen.ScoreConfig, error) {
	b, err := c.do(ctx, MethodScoreConfigUpdate, gen.NewScoreConfigWriteParams(p))
	if err != nil {
		return gen.ScoreConfig{}, err
	}
	return gen.WrapScoreConfig(b)
}

// ====================== analytics (STUBBED with TODO) ======================

// AnalyticsStub calls any of the stubbed ClickHouse analytics methods and
// returns the Empty result (Stubbed=true until the shim lands). Exposed so a
// caller can probe whether the analytics path is wired without special-casing
// each method.
func (c *Client) AnalyticsStub(ctx context.Context, method uint32, p gen.TableQueryParamsInput) (gen.Empty, error) {
	b, err := c.do(ctx, method, gen.NewTableQueryParams(p))
	if err != nil {
		return gen.Empty{}, err
	}
	return gen.WrapEmpty(b)
}

// ============================== pipelining =================================

// PipelineTraceObservation proves Cap'n Proto promise pipelining over the
// trace→observation dependency: traceById on connection c, then observationById
// on connection dep pipelined off the trace call's promise. The dependent
// observation call is shipped BEFORE the trace call's answer resolves; the
// server holds it on its promise table (Server.await) until the trace call
// resolves the org, then dispatches it — no intermediate round trip.
//
// Transport note (load-bearing): luxfi/zap processes a single connection's
// frames strictly FIFO — one handler runs to completion before the next frame
// is read. So genuine concurrent in-flight calls require the two calls on
// SEPARATE connections (dep is a second client). The shared send log both
// clients append to is the proof surface.
func (c *Client) PipelineTraceObservation(
	ctx context.Context,
	dep *Client,
	trace gen.TraceByIdParamsInput,
	obs gen.ObservationByIdParamsInput,
) (gen.Trace, gen.Observation, error) {
	tracePromise := nextPipelineID()
	obsPromise := nextPipelineID()

	var (
		gotTrace gen.Trace
		gotObs   gen.Observation
		traceErr error
		obsErr   error
		wg       sync.WaitGroup
	)
	// barrier releases the dependent send only after the trace send is committed
	// to its wire, so the server resolves the trace promise before (or
	// concurrently with) the dependent call's await — never after a timeout.
	barrier := make(chan struct{})
	wg.Add(2)

	// Call #1: traceById on connection c — the promise the dependent call targets.
	go func() {
		defer wg.Done()
		close(barrier)
		resp, err := c.call(ctx, MethodTraceById, tracePromise, NoTarget, gen.NewTraceByIdParams(trace))
		if err != nil {
			traceErr = err
			return
		}
		if resp.Status != StatusOK {
			traceErr = fmt.Errorf("traceById: status %d: %s", resp.Status, resp.Body)
			return
		}
		gotTrace, traceErr = gen.WrapTrace(resp.Body)
	}()

	// Call #2: observationById on connection dep, pipelined off the trace promise.
	go func() {
		defer wg.Done()
		<-barrier
		resp, err := dep.call(ctx, MethodObservationById, obsPromise, tracePromise, gen.NewObservationByIdParams(obs))
		if err != nil {
			obsErr = err
			return
		}
		if resp.Status != StatusOK {
			obsErr = fmt.Errorf("observationById: status %d: %s", resp.Status, resp.Body)
			return
		}
		gotObs, obsErr = gen.WrapObservation(resp.Body)
	}()

	wg.Wait()
	if traceErr != nil {
		return gen.Trace{}, gen.Observation{}, traceErr
	}
	if obsErr != nil {
		return gen.Trace{}, gen.Observation{}, obsErr
	}
	return gotTrace, gotObs, nil
}

// SyntheticCap mints an in-memory CapKindIAMSession capability for tests and
// bootstrap: ed25519-signed (the SPEC bootstrap scheme), holding the given
// permission bits. The signature is real (ed25519); production wires an
// IAM-issued cap instead. Returns the opaque buffer to pass to Dial.
func SyntheticCap(perms uint64) ([]byte, error) {
	signer, err := zcap.NewEd25519Signer()
	if err != nil {
		return nil, err
	}
	c, err := zcap.Issue(zcap.Issuance{
		Kind:        uint32(zcap.KindIAMSession),
		Holder:      signer.Public(),
		Permissions: perms,
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
	}, signer)
	if err != nil {
		return nil, err
	}
	return c.Bytes(), nil
}
