// Package server implements the Observability ZAP capability-RPC service —
// the Langfuse-style trace/observation/score OLTP backbone, replacing 8 console
// tRPC routers (traces, observations, sessions, generations, scores,
// scoreConfigs, scoreAnalytics, events) with native ZAP capability RPC on Hanzo
// Base.
//
// Wire model — three concentric, decoupled layers (values, not places):
//
//  1. Transport: github.com/luxfi/zap Node. Frames are length-prefixed,
//     request/response-correlated, and routed by msgType = flags>>8. This
//     service owns MsgTypeRouterBase (203), disjoint from Base's generic
//     ORM plugin (100–103) and the other typed routers (200, 201, …). One TCP
//     listener, default port 9992.
//
//  2. Envelope: a luxfi/zap object carrying the call shape. Request =
//     (Method u32, PromiseID u32, Target u32, Cap bytes, Payload bytes);
//     response = (Status u32, PromiseID u32, Body bytes). The Cap field is
//     the OPAQUE zap-proto/go capability buffer — re-Wrapped server-side,
//     never decoded by the transport.
//
//  3. Payload/Body: zap-proto/go typed views generated from the .zap schema
//     (gen/). These are the contract both Go and the console's TS client
//     agree on. The transport carries them as opaque bytes — the contract
//     (data) is fully separate from the transport (place).
//
// Pipelining: PromiseID + Target let a caller reference the not-yet-resolved
// answer of an earlier call. A second call shipped with Target = the first
// call's PromiseID is dispatched against that promise's resolved value —
// Cap'n Proto-style promise pipelining (see client.go PipelineTraceObservation).
package server

import (
	"fmt"

	zaplib "github.com/luxfi/zap"
)

// MsgTypeRouterBase is this service's ZAP message-type slot. Base's generic
// ORM transport plugin uses 100–103; typed capability routers start at 200, one
// slot per service-binary in the console tRPC→ZAP migration. Observability owns
// 203.
const MsgTypeRouterBase uint16 = 203

// Method identifiers — one per .zap interface method (the `@n` ordinals in
// proto/observability.zap). Grouped by resource; ordinals leave gaps between
// groups so a resource can grow without renumbering. The numbering matches the
// schema header's interface block exactly.
const (
	// traces — OLTP CRUD (honest).
	MethodTraceById       uint32 = 0
	MethodTraceWithDetail uint32 = 1
	MethodTraceBookmark   uint32 = 2
	MethodTracePublish    uint32 = 3
	MethodTraceUpdateTags uint32 = 4
	MethodTraceDeleteMany uint32 = 5

	// observations — OLTP read (honest).
	MethodObservationById uint32 = 6

	// sessions — OLTP CRUD (honest).
	MethodSessionHasAny   uint32 = 20
	MethodSessionById     uint32 = 21
	MethodSessionBookmark uint32 = 22
	MethodSessionPublish  uint32 = 23

	// scores — OLTP CRUD (honest).
	MethodScoreById             uint32 = 40
	MethodScoreHasAny           uint32 = 41
	MethodScoreCreateAnnotation uint32 = 42
	MethodScoreUpdateAnnotation uint32 = 43
	MethodScoreDeleteAnnotation uint32 = 44

	// scoreConfigs — OLTP CRUD (honest; pure Prisma upstream).
	MethodScoreConfigAll    uint32 = 60
	MethodScoreConfigById   uint32 = 61
	MethodScoreConfigCreate uint32 = 62
	MethodScoreConfigUpdate uint32 = 63

	// events — OLTP read by FK (honest).
	MethodEventScoresForTrace uint32 = 80
	MethodEventBatchIO        uint32 = 81

	// analytics — ClickHouse aggregations (STUBBED; see handlers.go handleStub).
	MethodTraceAll                 uint32 = 100
	MethodTraceCountAll            uint32 = 101
	MethodTraceMetrics             uint32 = 102
	MethodTraceFilterOptions       uint32 = 103
	MethodSessionAll               uint32 = 120
	MethodSessionCountAll          uint32 = 121
	MethodScoreAll                 uint32 = 140
	MethodScoreCountAll            uint32 = 141
	MethodEventAll                 uint32 = 160
	MethodAnalyticsScoreComparison uint32 = 180
)

// NoTarget is the Target value for a call that does not pipeline off an
// earlier promise (i.e. it acts on the root capability directly).
const NoTarget uint32 = 0

// Request-frame field offsets within the envelope object's fixed section.
const (
	reqMethodOff    = 0  // u32: which interface method
	reqPromiseIDOff = 4  // u32: caller-assigned id this call's answer resolves to
	reqTargetOff    = 8  // u32: promise this call pipelines off (NoTarget = root)
	reqCapOff       = 12 // bytes: opaque zap-proto/go capability buffer
	reqPayloadOff   = 20 // bytes: zap-proto/go-encoded method params
	reqFixedSize    = 28
)

// Response-frame field offsets.
const (
	respStatusOff    = 0  // u32: 200 ok, else error
	respPromiseIDOff = 4  // u32: echoes the request's PromiseID
	respBodyOff      = 12 // bytes: zap-proto/go-encoded results (or error JSON)
	respFixedSize    = 20
)

// Status codes mirror the HTTP-ish convention Base's ZAP plugin already uses.
const (
	StatusOK           uint32 = 200
	StatusBadRequest   uint32 = 400
	StatusUnauthorized uint32 = 401
	StatusForbidden    uint32 = 403
	StatusNotFound     uint32 = 404
	StatusInternal     uint32 = 500
)

// Call is the decoded request envelope.
type Call struct {
	Method    uint32
	PromiseID uint32
	Target    uint32
	Cap       []byte // opaque capability buffer
	Payload   []byte // opaque zap-proto/go params
}

// buildRequest encodes a Call into a luxfi/zap message tagged for this router.
func buildRequest(c Call) (*zaplib.Message, error) {
	b := zaplib.NewBuilder(len(c.Cap) + len(c.Payload) + reqFixedSize + 64)
	ob := b.StartObject(reqFixedSize)
	ob.SetUint32(reqMethodOff, c.Method)
	ob.SetUint32(reqPromiseIDOff, c.PromiseID)
	ob.SetUint32(reqTargetOff, c.Target)
	ob.SetBytes(reqCapOff, c.Cap)
	ob.SetBytes(reqPayloadOff, c.Payload)
	ob.FinishAsRoot()
	data := b.FinishWithFlags(MsgTypeRouterBase << 8)
	return zaplib.Parse(data)
}

// parseRequest decodes a luxfi/zap message into a Call.
func parseRequest(msg *zaplib.Message) Call {
	root := msg.Root()
	return Call{
		Method:    root.Uint32(reqMethodOff),
		PromiseID: root.Uint32(reqPromiseIDOff),
		Target:    root.Uint32(reqTargetOff),
		Cap:       root.Bytes(reqCapOff),
		Payload:   root.Bytes(reqPayloadOff),
	}
}

// buildResponse encodes a status + body into a router-tagged luxfi/zap message.
func buildResponse(status, promiseID uint32, body []byte) (*zaplib.Message, error) {
	b := zaplib.NewBuilder(len(body) + respFixedSize + 64)
	ob := b.StartObject(respFixedSize)
	ob.SetUint32(respStatusOff, status)
	ob.SetUint32(respPromiseIDOff, promiseID)
	ob.SetBytes(respBodyOff, body)
	ob.FinishAsRoot()
	data := b.FinishWithFlags(MsgTypeRouterBase << 8)
	return zaplib.Parse(data)
}

// Response is the decoded response envelope.
type Response struct {
	Status    uint32
	PromiseID uint32
	Body      []byte
}

// parseResponse decodes a luxfi/zap response message.
func parseResponse(msg *zaplib.Message) Response {
	root := msg.Root()
	return Response{
		Status:    root.Uint32(respStatusOff),
		PromiseID: root.Uint32(respPromiseIDOff),
		Body:      root.Bytes(respBodyOff),
	}
}

// errorBody is a minimal JSON error body, matching the shape Base's plugin
// returns ({"error": "..."}) so a single client error path covers both.
func errorBody(msg string) []byte {
	return []byte(fmt.Sprintf(`{"error":%q}`, msg))
}
