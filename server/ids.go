package server

import "github.com/hanzoai/base/core"

// newID mints a caller-facing external id for an entity created without one
// (e.g. an annotation score or score config created via the UI with no
// client-supplied id). Uses Base's canonical record-id generator so the format
// matches every other id in the system — one way to make an id.
func newID() string { return core.GenerateDefaultRandomId() }
