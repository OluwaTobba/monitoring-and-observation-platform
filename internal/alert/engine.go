package alert

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

type Querier interface {
	Query(name string, match tsdb.Labels, from, to time.Time) []tsdb.SeriesResult
}

const defaultLookback = 5 * time.Minute

type Engine struct {
	q Querier
	rules []Rule
	notifier Notifier
	interval time.Duration
	logger *slog.Logger

	mu sync.RWMutex
	active map[string]*Alert
}

func NewEngine(q Querier, rules []Rule, n Notifier, interval time.Duration, logger *slog.Logger) (*Engine, error) {
	if interval <= 0 {
		return nil, errors.New("alert evaluation interval must be positive")
	}
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return nil, err
		}
	}
	if n == nil {
		n = MultiNotifier{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		q: q,
		rules: rules,
		notifier: n,
		interval: interval,
		logger: logger,
		active: make(map[string]*Alert),
	}, nil
}

func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_ = e.dispatch(ctx, e.Evaluate(now))
		}
	}
}

func (e *Engine) Evaluate(now time.Time) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []Event
	seen := make(map[string]struct{})

	for _, r := range e.rules {
		lookback := r.Lookback
		if lookback <= 0 {
			lookback = defaultLookback
		}
		for _, sr := range e.q.Query(r.Metric, r.Labels, now.Add(-lookback), now) {
			if len(sr.Samples) == 0 {
				continue
			}
			last := sr.Samples[len(sr.Samples)-1].V
			if math.IsNaN(last) {
				continue
			}
			key := r.Name + "|" + tsdb.Key(sr.Name, sr.Labels)
			seen[key] = struct{}{}
			events = append(events, e.advance(key, r, sr.Labels, last, r.Op.Compare(last, r.Threshold), now)...)
		}
	}

	// Any alert whose series no longer reports data is considered resolved.
	for key, a := range e.active {
		if _, ok := seen[key]; ok {
			continue
		}
		delete(e.active, key)
		if a.State == StateFiring {
			resolved := *a
			resolved.State = StateInactive
			events = append(events, Event{Status: "resolved", Alert: resolved, Time: now})
		}
	}
	return events
}

func (e *Engine) advance(key string, r Rule, series tsdb.Labels, value float64, breached bool, now time.Time) []Event {
	a, ok := e.active[key]

	if !breached {
		if !ok {
			return nil
		}
		delete(e.active, key)
		if a.State != StateFiring {
			return nil
		}
		resolved := *a
		resolved.State = StateInactive
		resolved.Value = value
		return []Event{{Status: "resolved", Alert: resolved, Time: now}}
	}

	if !ok {
		sev := r.Severity
		if sev == "" {
			sev = "warning"
		}
		a = &Alert{Rule: r.Name, Severity: sev, Series: series, State: StatePending, Since: now}
		e.active[key] = a
	}
	a.Value = value

	if a.State == StatePending && now.Sub(a.Since) >= r.For {
		a.State = StateFiring
		fired := now
		a.FiredAt = &fired
		return []Event{{Status: "firing", Alert: *a, Time: now}}
	}
	return nil
}

// Alerts returns all pending and firing alerts, sorted by rule then start time.
func (e *Engine) Alerts() []Alert {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Alert, 0, len(e.active))
	for _, a := range e.active {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rule != out[j].Rule {
			return out[i].Rule < out[j].Rule
		}
		return out[i].Since.Before(out[j].Since)
	})
	return out
}

func (e *Engine) dispatch(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	if err := e.notifier.Notify(ctx, events); err != nil {
		e.logger.Error("alert notification failed", "events", len(events), "err", err)
		return err
	}
	return nil
}