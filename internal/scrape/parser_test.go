package scrape

import (
	"math"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	input := `# HELP http_requests_total Total requests.
# TYPE http_requests_total counter
http_requests_total{method="get",path="/a \"b\"",code="200"} 1027 1395066363000
up 1
weird_metric{msg="line1\nline2"} NaN
`
	pts, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pts) != 3 {
		t.Fatalf("want 3 points, got %d", len(pts))
	}

	p := pts[0]
	if p.Name != "http_requests_total" || p.Value != 1027 || p.Timestamp != 1395066363000 {
		t.Errorf("bad first point: %+v", p)
	}
	if p.Labels["path"] != `/a "b"` {
		t.Errorf("escaped quote not decoded: %q", p.Labels["path"])
	}
	if pts[1].Name != "up" || pts[1].Value != 1 || pts[1].Timestamp != 0 {
		t.Errorf("bad bare metric: %+v", pts[1])
	}
	if pts[2].Labels["msg"] != "line1\nline2" {
		t.Errorf("newline escape not decoded: %q", pts[2].Labels["msg"])
	}
	if !math.IsNaN(pts[2].Value) {
		t.Errorf("want NaN, got %v", pts[2].Value)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		`metric{a=1} 2`,
		`metric{a="x" 2`,
		`metric abc`,
		`{a="b"} 1`,
		`metric 1 2 3`,
	}
	for _, in := range bad {
		if _, err := Parse(strings.NewReader(in)); err == nil {
			t.Errorf("expected error for %q", in)
		}
	}
}
