package server

import (
	"math"

	"github.com/hanzoai/base/core"
	"github.com/hanzoai/dbx"
	zaplib "github.com/luxfi/zap"
)

// dash_helpers.go — the query + response helpers the dashboards surface uses.
//
// Two scoped lookups exist in this package and they are NOT duplicates:
//
//   - findByExt (collections.go) addresses an obs_* row by its EXTERNAL id — the
//     caller-facing trace/observation/score id, stored in its own extId column
//     because ingestion assigns it upstream.
//   - findScoped (here) addresses a dashboards-surface row by its BASE record id,
//     which is the caller-facing id for these models: nothing upstream assigns
//     them, so the store's own id is the identity.
//
// Both scope by projectId. That is the invariant; which id column identifies the
// row is a property of where the id comes from.

// ok answers a call with a successful body.
func (s *Server) ok(req Call, body []byte) (*zaplib.Message, error) {
	return buildResponse(StatusOK, req.PromiseID, body)
}

// fail answers a call with a status and a JSON error body.
func (s *Server) fail(req Call, status uint32, msg string) (*zaplib.Message, error) {
	return buildResponse(status, req.PromiseID, errorBody(msg))
}

// findScoped reads one dashboards-surface row by Base id within a project. A row
// in another project is not found — never read.
func (s *Server) findScoped(collection, project, id string) (*core.Record, error) {
	return s.app.FindFirstRecordByFilter(collection,
		"id = {:id} && projectId = {:p}", dbx.Params{"id": id, "p": project})
}

// listScoped returns one page of a project's rows plus the unpaginated total,
// which the list envelopes carry so the UI can render pagination.
func (s *Server) listScoped(collection, project, sort string, page, limit int) ([]*core.Record, int, error) {
	all, err := s.app.FindRecordsByFilter(collection,
		"projectId = {:p}", "", 0, 0, dbx.Params{"p": project})
	if err != nil {
		return nil, 0, err
	}
	total := len(all)
	rows, err := s.app.FindRecordsByFilter(collection,
		"projectId = {:p}", sort, limit, offsetOf(page, limit), dbx.Params{"p": project})
	if err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}

func offsetOf(page, limit int) int {
	if page <= 1 || limit <= 0 {
		return 0
	}
	return (page - 1) * limit
}

// sortExpr maps the tRPC orderBy object onto Base's sort syntax, defaulting to
// newest-first when the caller names no column.
func sortExpr(column, order string) string {
	if column == "" {
		return "-created"
	}
	if order == "ASC" {
		return "+" + column
	}
	return "-" + column
}

// nullThreshold is the wire sentinel for a nullable f64 that is null. The source
// column is z.number().nullable(); the wire carries f64, so NaN marks "absent"
// — the one float that is never a legitimate threshold.
func nullThreshold() float64 { return math.NaN() }

// isNull reports whether a float carries the null sentinel.
func isNull(f float64) bool { return math.IsNaN(f) }
