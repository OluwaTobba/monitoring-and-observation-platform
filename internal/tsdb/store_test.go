package tsdb

import (
	"testing"
	"time"
)

func TestAppendAndQuery(t *testing.T) {
	s := NewStore(time.Hour, 100)
	base := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		s.Append("cpu", Labels{"host": "a"}, base.Add(time.Duration(i)*time.Second), float64(i))
	}
	s.Append("cpu", Labels{"host": "b"}, base, 99)

	all := s.Query("cpu", Labels{"host": "a"}, time.Time{}, time.Time{})
	if len(all) != 1 || len(all[0].Samples) != 5 {
		t.Fatalf("want 1 series with 5 samples, got %+v", all)
	}

	ranged := s.Query("cpu", Labels{"host": "a"}, base.Add(2*time.Second), base.Add(3*time.Second))
	if len(ranged) != 1 || len(ranged[0].Samples) != 2 {
		t.Fatalf("want 2 samples in range, got %+v", ranged)
	}

	if got := s.Query("cpu", nil, time.Time{}, time.Time{}); len(got) != 2 {
		t.Fatalf("empty matcher should match both series, got %d", len(got))
	}
	if got := s.Query("missing", nil, time.Time{}, time.Time{}); len(got) != 0 {
		t.Fatalf("unknown metric should return nothing")
	}
}

func TestOutOfOrderInsert(t *testing.T) {
	s := NewStore(0, 100)
	base := time.Unix(1000, 0)
	s.Append("m", nil, base.Add(2*time.Second), 2)
	s.Append("m", nil, base, 0)
	s.Append("m", nil, base.Add(time.Second), 1)

	res := s.Query("m", nil, time.Time{}, time.Time{})
	for i, sm := range res[0].Samples {
		if sm.V != float64(i) {
			t.Fatalf("sample %d has value %v, want %d", i, sm.V, i)
		}
	}
}

func TestMaxSamplesCap(t *testing.T) {
	s := NewStore(0, 3)
	base := time.Unix(1000, 0)
	for i := 0; i < 10; i++ {
		s.Append("m", nil, base.Add(time.Duration(i)*time.Second), float64(i))
	}
	res := s.Query("m", nil, time.Time{}, time.Time{})
	if len(res[0].Samples) != 3 || res[0].Samples[0].V != 7 {
		t.Fatalf("want last 3 samples (7,8,9), got %+v", res[0].Samples)
	}
}

func TestPruneRetention(t *testing.T) {
	s := NewStore(time.Minute, 100)
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	s.Append("old", nil, now.Add(-10*time.Minute), 1)
	s.Append("new", nil, now.Add(-10*time.Second), 2)

	if dropped := s.Prune(now); dropped != 1 {
		t.Fatalf("want 1 dropped sample, got %d", dropped)
	}
	names := s.Names()
	if len(names) != 1 || names[0] != "new" {
		t.Fatalf("want only 'new' to remain, got %v", names)
	}
	if st := s.Stats(); st.Series != 1 || st.Samples != 1 {
		t.Fatalf("unexpected stats %+v", st)
	}
}
