package scrape

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

const maxBodyBytes = 16 << 20

// Appender receives scraped samples. *tsdb.Store implements it.
type Appender interface {
	Append(name string, labels tsdb.Labels, ts time.Time, value float64)
}

// Target describes one endpoint to scrape.
type Target struct {
	Job string
	URL string
	Interval time.Duration
	Timeout  time.Duration
	Labels tsdb.Labels
}

func (t Target) key() string { return t.Job + "|" + t.URL }

func (t Target) instance() string {
	u, err := url.Parse(t.URL)
	if err != nil {
		return t.URL
	}
	return u.Host
}

// TargetStatus is the outcome of the most recent scrape of a target.
type TargetStatus struct {
	Job        string    `json:"job"`
	URL        string    `json:"url"`
	Up         bool      `json:"up"`
	LastScrape time.Time `json:"last_scrape"`
	Duration   float64   `json:"duration_seconds"`
	Samples    int       `json:"samples"`
	Error      string    `json:"error,omitempty"`
}

// Scraper polls every target on its own interval.
type Scraper struct {
	app     Appender
	client  *http.Client
	targets []Target
	logger  *slog.Logger

	scrapes atomic.Int64
	errors  atomic.Int64

	mu     sync.RWMutex
	status map[string]TargetStatus
}

// NewScraper builds a scraper. Missing intervals default to 15s and missing timeouts to 10s.
func NewScraper(app Appender, targets []Target, logger *slog.Logger) *Scraper {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Scraper{
		app:    app,
		client: &http.Client{},
		logger: logger,
		status: make(map[string]TargetStatus),
	}
	for _, t := range targets {
		if t.Interval <= 0 {
			t.Interval = 15 * time.Second
		}
		if t.Timeout <= 0 {
			t.Timeout = 10 * time.Second
		}
		s.targets = append(s.targets, t)
		s.status[t.key()] = TargetStatus{Job: t.Job, URL: t.URL}
	}
	return s
}

// Run blocks until ctx is cancelled, scraping every target on its interval.
func (s *Scraper) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range s.targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.loop(ctx, t)
		}()
	}
	wg.Wait()
}

func (s *Scraper) loop(ctx context.Context, t Target) {
	ticker := time.NewTicker(t.Interval)
	defer ticker.Stop()
	for {
		s.scrapeOnce(ctx, t)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Scraper) scrapeOnce(ctx context.Context, t Target) {
	start := time.Now()
	n, err := s.fetch(ctx, t, start)
	elapsed := time.Since(start).Seconds()

	st := TargetStatus{Job: t.Job, URL: t.URL, Up: err == nil, LastScrape: start, Duration: elapsed, Samples: n}
	up := 1.0
	if err != nil {
		up = 0
		st.Error = err.Error()
		s.errors.Add(1)
		s.logger.Warn("scrape failed", "job", t.Job, "url", t.URL, "err", err)
	}
	s.scrapes.Add(1)

	base := seriesLabels(t, nil)
	s.app.Append("up", base, start, up)
	s.app.Append("scrape_duration_seconds", base, start, elapsed)
	s.app.Append("scrape_samples_scraped", base, start, float64(n))

	s.mu.Lock()
	s.status[t.key()] = st
	s.mu.Unlock()
}

func (s *Scraper) fetch(ctx context.Context, t Target, now time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return 0, fmt.Errorf("unexpected HTTP status %s", resp.Status)
	}

	points, err := Parse(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return 0, err
	}
	for _, p := range points {
		ts := now
		if p.Timestamp != 0 {
			ts = time.UnixMilli(p.Timestamp)
		}
		s.app.Append(p.Name, seriesLabels(t, p.Labels), ts, p.Value)
	}
	return len(points), nil
}

// seriesLabels merges target labels with scraped labels. Scraped labels win over target labels; job and instance are always set from the target.
func seriesLabels(t Target, scraped tsdb.Labels) tsdb.Labels {
	out := make(tsdb.Labels, len(t.Labels)+len(scraped)+2)
	for k, v := range t.Labels {
		out[k] = v
	}
	for k, v := range scraped {
		out[k] = v
	}
	out["job"] = t.Job
	out["instance"] = t.instance()
	return out
}

// Status returns the latest status of every target, sorted by job then URL.
func (s *Scraper) Status() []TargetStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TargetStatus, 0, len(s.status))
	for _, st := range s.status {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Job != out[j].Job {
			return out[i].Job < out[j].Job
		}
		return out[i].URL < out[j].URL
	})
	return out
}

// Counters returns the total scrapes attempted and the number that failed.
func (s *Scraper) Counters() (scrapes, failures int64) {
	return s.scrapes.Load(), s.errors.Load()
}