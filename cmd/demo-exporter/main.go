package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type exporter struct {
	host string

	mu sync.Mutex
	cpu float64
	requests float64
	memBytes float64
}

func newExporter(host string) *exporter {
	return &exporter{host: host, cpu: 0.4, memBytes: 2.0e9}
}

// tick advances the simulated state by one step.
func (e *exporter) tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cpu = clamp(e.cpu+(rand.Float64()-0.5)*0.25, 0, 2)
	e.requests += float64(rand.Intn(50))
	e.memBytes = clamp(e.memBytes+(rand.Float64()-0.5)*1e8, 1e9, 4e9)
}

func (e *exporter) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	e.mu.Lock()
	cpu, reqs, mem := e.cpu, e.requests, e.memBytes
	e.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, `# HELP demo_cpu_load Simulated load average.
# TYPE demo_cpu_load gauge
demo_cpu_load{host=%q} %.3f
# HELP demo_memory_used_bytes Simulated memory usage.
# TYPE demo_memory_used_bytes gauge
demo_memory_used_bytes{host=%q} %.0f
# HELP demo_requests_total Simulated request counter.
# TYPE demo_requests_total counter
demo_requests_total{host=%q,code="200"} %.0f
`, e.host, cpu, e.host, mem, e.host, reqs)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func main() {
	addr := flag.String("addr", ":9100", "listen address")
	host := flag.String("host", "demo-1", "value of the host label")
	flag.Parse()

	e := newExporter(*host)
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			e.tick()
		}
	}()

	http.Handle("/metrics", e)
	log.Printf("demo exporter on %s (host=%s)", *addr, *host)
	log.Fatal(http.ListenAndServe(*addr, nil))
}