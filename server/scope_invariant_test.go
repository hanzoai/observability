package server_test

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

// scope() resolves the tenant by reading the payload's field at offset 0 as
// text, without knowing which struct it holds:
//
//	project = p.Root().Text(0) // ProjectId is text @0 in every request struct
//
// That is fast and type-agnostic, and it is silently wrong the moment a request
// struct puts some other text field first — the service would then scope the
// query to whatever that field contains. No test of a single method would catch
// it; only the method with the misplaced field would misbehave, and it would
// misbehave by reading ANOTHER TENANT'S ROWS rather than by failing.
//
// So the invariant is asserted against the schema itself, not against any one
// handler: every struct carrying a ProjectId must carry it at @0, except the
// entity structs below, which are RESULTS (never decoded by scope) and place
// ProjectId after their id.
func TestEveryRequestStructCarriesProjectIdAtZero(t *testing.T) {
	// Entity/result structs: returned to callers, never passed to scope().
	entities := map[string]bool{
		"Trace": true, "Observation": true, "Session": true, "Score": true,
		"ScoreConfig": true, "Dashboard": true, "Widget": true, "Preset": true,
		"Monitor": true,
	}

	src, err := os.ReadFile("../proto/observability.zap")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	// Strip comments so the documentation block (which mentions field names in
	// prose) cannot be mistaken for a declaration.
	clean := regexp.MustCompile(`#[^\n]*`).ReplaceAllString(string(src), "")

	structRe := regexp.MustCompile(`struct\s+(\w+)\s*\{([^}]*)\}`)
	fieldRe := regexp.MustCompile(`(\w+)\s+([\w<>]+)\s+@(\d+)`)

	var checked int
	for _, m := range structRe.FindAllStringSubmatch(clean, -1) {
		name, body := m[1], m[2]
		for _, f := range fieldRe.FindAllStringSubmatch(body, -1) {
			if f[1] != "ProjectId" {
				continue
			}
			off, err := strconv.Atoi(f[3])
			if err != nil {
				t.Fatalf("%s.ProjectId: bad offset %q", name, f[3])
			}
			checked++
			if entities[name] {
				continue // a result struct; scope() never sees it
			}
			if off != 0 {
				t.Errorf("%s.ProjectId is @%d, must be @0 — scope() reads text @0 as "+
					"the tenant key, so this struct would scope queries to the wrong "+
					"field. Move ProjectId to @0, or add %s to the entities set if it "+
					"is a result type.", name, off, name)
			}
			if f[2] != "text" {
				t.Errorf("%s.ProjectId is %q, must be text — scope() reads it as text", name, f[2])
			}
		}
	}
	if checked == 0 {
		t.Fatal("no ProjectId fields found — the schema path or parser is wrong, " +
			"so this test was proving nothing")
	}
	t.Logf("scope() invariant holds across %d ProjectId declarations", checked)
}
