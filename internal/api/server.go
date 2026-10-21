package api

import (
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/alert"
	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/scrape"
	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

var webFS embed.FS

const maxWriteBytes = 5 << 20

var (
	metricRe = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	// reserved query parameters are never treated as label matchers.
	reserved = map[string]bool{"name": true, "from": true, "to": true, "step": true}
)

// Server wires the HTTP handlers to the platform components.
type Server struct {
	Store *tsdb.Store
	Scraper *scrape.Scraper
	Alerts *alert.Engine

	ingested atomic.Int64
}

// Handler returns the HTTP router.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /metrics", s.handleSelfMetrics)
	mux.HandleFunc("POST /api/v1/write", s.handleWrite)
	mux.HandleFunc("GET /api/v1/query", s.handleQuery)
	mux.HandleFunc("GET /api/v1/series", s.handleSeries)
	mux.HandleFunc("GET /api/v1/alerts", s.handleAlerts)
	mux.HandleFunc("GET /api/v1/targets", s.handleTargets)
	return mux
}

type writeItem struct {
	Name string `json:"name"`
	Labels map[string]string `json:"labels"`
	Timestamp *int64 `json:"timestamp"`
	Value float64 `json:"value"`
}

// handleWrite accepts a JSON array of samples pushed by clients.
func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var items []writeItem
	body := http.MaxBytesReader(w, r.Body, maxWriteBytes)
	if err := json.NewDecoder(body).Decode(&items); err != nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON array of samples: "+err.Error())
		return
	}

	// Validate the whole batch before storing anything.
	for i, it := range items {
		if !metricRe.MatchString(it.Name) {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("sample %d: invalid metric name %q", i, it.Name))
			return
		}
		for k := range it.Labels {
			if !labelRe.MatchString(k) {
				writeError(w, http.StatusBadRequest, fmt.Sprintf("sample %d: invalid label name %q", i, k))
				return
			}
		}
	}

	now := time.Now()
	for _, it := range items {
		ts := now
		if it.Timestamp != nil {
			ts = time.UnixMilli(*it.Timestamp)
		}
		s.Store.Append(it.Name, it.Labels, ts, it.Value)
	}
	s.ingested.Add(int64(len(items)))
	writeJSON(w, http.StatusAccepted, map[string]int{"accepted": len(items)})
}

// jsonPoint carries a sample with NaN/Inf replaced by null, since JSON cannot represent them.
type jsonPoint struct {
	T int64 `json:"t"`
	V *float64 `json:"v"`
}

type seriesOut struct {
	Name string `json:"name"`
	Labels tsdb.Labels `json:"labels"`
	Samples []jsonPoint `json:"samples"`
}

// handleQuery returns series for ?name=, filtered by any other query parameters treated as exact label matches. Optional from/to (RFC3339 or unix seconds) bound the range, and step (e.g. 15s) averages into buckets.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	name := q.Get("name")
	if !metricRe.MatchString(name) {
		writeError(w, http.StatusBadRequest, "name is required and must be a valid metric name")
		return
	}
	from, err := parseTime(q.Get("from"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "from: "+err.Error())
		return
	}
	to, err := parseTime(q.Get("to"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "to: "+err.Error())
		return
	}
	var step time.Duration
	if raw := q.Get("step"); raw != "" {
		step, err = time.ParseDuration(raw)
		if err != nil || step <= 0 {
			writeError(w, http.StatusBadRequest, "step must be a positive duration such as 15s")
			return
		}
	}

	match := tsdb.Labels{}
	for k, vals := range q {
		if reserved[k] || len(vals) == 0 {
			continue
		}
		match[k] = vals[0]
	}

	results := s.Store.Query(name, match, from, to)
	out := make([]seriesOut, 0, len(results))
	for _, sr := range results {
		samples := sr.Samples
		if step > 0 {
			samples = downsample(samples, step)
		}
		out = append(out, seriesOut{Name: sr.Name, Labels: sr.Labels, Samples: toJSONPoints(samples)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "success", "data": out})
}

func (s *Server) handleSeries(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Store.Names())
}

func (s *Server) handleAlerts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Alerts.Alerts())
}

func (s *Server) handleTargets(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Scraper.Status())
}

// handleSelfMetrics exposes the platform's own health in Prometheus format, so obsd can be scraped by another obsd or by Prometheus.
func (s *Server) handleSelfMetrics(w http.ResponseWriter, _ *http.Request) {
	st := s.Store.Stats()
	scrapes, failures := s.Scraper.Counters()
	firing := 0
	for _, a := range s.Alerts.Alerts() {
		if a.State == alert.StateFiring {
			firing++
		}
	}

	var b strings.Builder
	gauge := func(name string, v any) {
		fmt.Fprintf(&b, "# TYPE %s gauge\n%s %v\n", name, name, v)
	}
	counter := func(name string, v any) {
		fmt.Fprintf(&b, "# TYPE %s counter\n%s %v\n", name, name, v)
	}
	gauge("obsd_series", st.Series)
	gauge("obsd_samples", st.Samples)
	gauge("obsd_alerts_firing", firing)
	counter("obsd_ingested_samples_total", s.ingested.Load())
	counter("obsd_scrapes_total", scrapes)
	counter("obsd_scrape_failures_total", failures)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, b.String())
}

func (s *Server) handleIndex(w http.ResponseWriter, _ *http.Request) {
	data, err := webFS.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "dashboard missing from build", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// parseTime accepts RFC3339 timestamps or Unix seconds. Empty means unbounded.
func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, nil
	}
	secs, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("want RFC3339 or unix seconds, got %q", raw)
	}
	return time.UnixMicro(int64(secs * 1e6)), nil
}

// downsample averages samples into buckets of width step, aligned to the Unix epoch so that repeated queries produce stable timestamps.
func downsample(in []tsdb.Sample, step time.Duration) []tsdb.Sample {
	stepMs := step.Milliseconds()
	if stepMs <= 0 || len(in) == 0 {
		return in
	}
	var out []tsdb.Sample
	cur := in[0].T / stepMs * stepMs
	var sum float64
	count := 0
	for _, sm := range in {
		b := sm.T / stepMs * stepMs
		if b != cur {
			out = append(out, tsdb.Sample{T: cur, V: sum / float64(count)})
			cur, sum, count = b, 0, 0
		}
		sum += sm.V
		count++
	}
	out = append(out, tsdb.Sample{T: cur, V: sum / float64(count)})
	return out
}

func toJSONPoints(in []tsdb.Sample) []jsonPoint {
	out := make([]jsonPoint, len(in))
	for i, s := range in {
		out[i].T = s.T
		if !math.IsNaN(s.V) && !math.IsInf(s.V, 0) {
			v := s.V
			out[i].V = &v
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"status": "error", "error": msg})
}