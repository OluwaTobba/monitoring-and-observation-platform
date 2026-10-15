package tsdb

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Labels map[string]string

type Sample struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

type SeriesResult struct {
	Name    string   `json:"name"`
	Labels  Labels   `json:"labels"`
	Samples []Sample `json:"samples"`
}

type Stats struct {
	Series  int
	Samples int
}

type series struct {
	name    string
	labels  Labels
	samples []Sample
}

type Store struct {
	mu         sync.RWMutex
	series     map[string]*series
	byName     map[string]map[string]struct{}
	retention  time.Duration
	maxSamples int
}

func NewStore(retention time.Duration, maxSamples int) *Store {
	if maxSamples <= 0 {
		maxSamples = 10000
	}
	return &Store{
		series:     make(map[string]*series),
		byName:     make(map[string]map[string]struct{}),
		retention:  retention,
		maxSamples: maxSamples,
	}
}

func Key(name string, labels Labels) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(labels[k]))
	}
	b.WriteByte('}')
	return b.String()
}

func (s *Store) Append(name string, labels Labels, ts time.Time, value float64) {
	key := Key(name, labels)
	t := ts.UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	ser, ok := s.series[key]
	if !ok {
		ser = &series{name: name, labels: cloneLabels(labels)}
		s.series[key] = ser
		idx := s.byName[name]
		if idx == nil {
			idx = make(map[string]struct{})
			s.byName[name] = idx
		}
		idx[key] = struct{}{}
	}

	smp := Sample{T: t, V: value}
	n := len(ser.samples)
	if n == 0 || ser.samples[n-1].T <= t {
		ser.samples = append(ser.samples, smp)
	} else {
		i := sort.Search(n, func(i int) bool { return ser.samples[i].T > t })
		ser.samples = append(ser.samples, Sample{})
		copy(ser.samples[i+1:], ser.samples[i:])
		ser.samples[i] = smp
	}

	if over := len(ser.samples) - s.maxSamples; over > 0 {
		ser.samples = ser.samples[over:]
	}
}

func (s *Store) Query(name string, match Labels, from, to time.Time) []SeriesResult {
	fromMs, toMs := int64(math.MinInt64), int64(math.MaxInt64)
	if !from.IsZero() {
		fromMs = from.UnixMilli()
	}
	if !to.IsZero() {
		toMs = to.UnixMilli()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]SeriesResult, 0, len(s.byName[name]))
	for key := range s.byName[name] {
		ser := s.series[key]
		if !matches(ser.labels, match) {
			continue
		}
		lo := sort.Search(len(ser.samples), func(i int) bool { return ser.samples[i].T >= fromMs })
		hi := sort.Search(len(ser.samples), func(i int) bool { return ser.samples[i].T > toMs })
		if lo >= hi {
			continue
		}
		samples := make([]Sample, hi-lo)
		copy(samples, ser.samples[lo:hi])
		out = append(out, SeriesResult{Name: name, Labels: cloneLabels(ser.labels), Samples: samples})
	}
	sort.Slice(out, func(i, j int) bool {
		return Key(out[i].Name, out[i].Labels) < Key(out[j].Name, out[j].Labels)
	})
	return out
}

func (s *Store) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.byName))
	for n := range s.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := Stats{Series: len(s.series)}
	for _, ser := range s.series {
		st.Samples += len(ser.samples)
	}
	return st
}

func (s *Store) Prune(now time.Time) int {
	if s.retention <= 0 {
		return 0
	}
	cutoff := now.Add(-s.retention).UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	dropped := 0
	for key, ser := range s.series {
		i := sort.Search(len(ser.samples), func(i int) bool { return ser.samples[i].T >= cutoff })
		if i > 0 {
			dropped += i
			ser.samples = ser.samples[i:]
		}
		if len(ser.samples) == 0 {
			delete(s.series, key)
			delete(s.byName[ser.name], key)
			if len(s.byName[ser.name]) == 0 {
				delete(s.byName, ser.name)
			}
		}
	}
	return dropped
}

func matches(have, want Labels) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func cloneLabels(in Labels) Labels {
	out := make(Labels, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}