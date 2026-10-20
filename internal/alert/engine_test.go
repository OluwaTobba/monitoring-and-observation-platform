package alert

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

type recorder struct {
	events []Event
}

func (r *recorder) Notify(_ context.Context, ev []Event) error {
	r.events = append(r.events, ev...)
	return nil
}

func TestPendingFiringResolved(t *testing.T) {
	store := tsdb.NewStore(time.Hour, 1000)
	rec := &recorder{}
	rule := Rule{
		Name: "HotRoom",
		Metric: "temp",
		Op: OpGT,
		Threshold: 80,
		For: 30 * time.Second,
	}
	eng, err := NewEngine(store, []Rule{rule}, rec, 15*time.Second, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	labels := tsdb.Labels{"room": "lab"}
	t0 := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	// Breach starts: pending, no notification yet.
	store.Append("temp", labels, t0, 90)
	if ev := eng.Evaluate(t0); len(ev) != 0 {
		t.Fatalf("expected no events while pending, got %+v", ev)
	}
	if a := eng.Alerts(); len(a) != 1 || a[0].State != StatePending {
		t.Fatalf("want one pending alert, got %+v", a)
	}

	// Breach persists past For: firing.
	t1 := t0.Add(31 * time.Second)
	store.Append("temp", labels, t1, 91)
	ev := eng.Evaluate(t1)
	if len(ev) != 1 || ev[0].Status != "firing" {
		t.Fatalf("want one firing event, got %+v", ev)
	}
	if err := eng.dispatch(context.Background(), ev); err != nil {
		t.Fatal(err)
	}

	// Value recovers: resolved.
	t2 := t1.Add(10 * time.Second)
	store.Append("temp", labels, t2, 50)
	ev = eng.Evaluate(t2)
	if len(ev) != 1 || ev[0].Status != "resolved" {
		t.Fatalf("want one resolved event, got %+v", ev)
	}
	if err := eng.dispatch(context.Background(), ev); err != nil {
		t.Fatal(err)
	}

	if len(eng.Alerts()) != 0 {
		t.Fatal("expected no active alerts after resolution")
	}
	if len(rec.events) != 2 {
		t.Fatalf("notifier should have seen 2 events, saw %d", len(rec.events))
	}
}

func TestBlipDoesNotFire(t *testing.T) {
	store := tsdb.NewStore(time.Hour, 1000)
	rule := Rule{Name: "Blip", Metric: "temp", Op: OpGT, Threshold: 80, For: time.Minute}
	eng, err := NewEngine(store, []Rule{rule}, MultiNotifier{}, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

	store.Append("temp", nil, t0, 95)
	eng.Evaluate(t0)
	store.Append("temp", nil, t0.Add(10*time.Second), 20)
	if ev := eng.Evaluate(t0.Add(10 * time.Second)); len(ev) != 0 {
		t.Fatalf("a breach shorter than For must not fire, got %+v", ev)
	}
	if len(eng.Alerts()) != 0 {
		t.Fatal("pending alert should be cleared when the breach ends")
	}
}

func TestInvalidRuleRejected(t *testing.T) {
	store := tsdb.NewStore(time.Hour, 10)
	_, err := NewEngine(store, []Rule{{Name: "x", Metric: "m", Op: "~"}}, nil, time.Second, nil)
	if err == nil {
		t.Fatal("expected error for unknown operator")
	}
}