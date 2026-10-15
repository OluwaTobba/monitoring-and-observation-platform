# obsd — a small monitoring and observability platform in Go

`obsd` is a single-binary monitoring server written in Go with no third-party
dependencies. It does the core jobs of a metrics platform:

- **Scrapes** Prometheus text-format endpoints on per-target intervals and
  records `up`, `scrape_duration_seconds` and `scrape_samples_scraped` for each.
- **Ingests** pushed samples over a JSON HTTP API.
- **Stores** series in an in-memory, label-indexed time-series store with
  per-series caps and time-based retention.
- **Alerts** with threshold rules that go through `pending → firing → resolved`
  and dispatch to logs and/or a webhook.
- **Queries** over HTTP, with optional bucketed downsampling.
- **Shows** a built-in dashboard at `/` and exposes its own health at `/metrics`.

It is intended as a clear, testable reference implementation and a foundation to
extend, not a drop-in replacement for Prometheus.

## Architecture

```
            ┌────────────┐   scrape    ┌──────────────┐
            │ targets    │◄────────────│   scraper    │──┐
            └────────────┘             └──────────────┘  │ Append
                                                         ▼
  push ──► POST /api/v1/write ──────────────────►┌──────────────┐
                                                 │  tsdb.Store  │◄── prune (retention)
  GET  ◄── /api/v1/query, /series ◄──────────────┤ label index  │
                                                 └──────┬───────┘
                                                        │ Query (sliding window)
                                                 ┌──────▼───────┐   Notify   ┌───────────┐
                                                 │ alert engine │───────────►│ log/webhook│
                                                 └──────────────┘            └───────────┘
```

| Package            | Responsibility                                                    |
| ------------------ | ----------------------------------------------------------------- |
| `internal/tsdb`    | Series storage, label matching, out-of-order inserts, retention  |
| `internal/scrape`  | Prometheus text parser, target scheduling, scrape status          |
| `internal/alert`   | Rule evaluation, alert state machine, notifiers                   |
| `internal/config`  | JSON config loading with defaults and validation                 |
| `internal/api`     | HTTP API, self-metrics, embedded dashboard                        |
| `cmd/obsd`         | The server binary                                                 |
| `cmd/demo-exporter`| A simulated application exposing metrics, for trying things out   |

## Quick start

Requires Go 1.24+.

```bash
# Build everything
go build -o bin/ ./cmd/...

# Terminal 1: simulated application on :9100
./bin/demo-exporter

# Terminal 2: the platform on :8080
./bin/obsd -config config.example.json
```

Open <http://localhost:8080> to see the dashboard. Click a metric such as
`demo_cpu_load` to chart it. Once it stays above 1.2 for 20 seconds, the
`DemoHighCPU` alert fires and appears in the Alerts panel and in the logs.

## HTTP API

| Method | Path                | Description                                                    |
| ------ | ------------------- | -------------------------------------------------------------- |
| GET    | `/`                 | Dashboard                                                      |
| GET    | `/healthz`          | Liveness check                                                 |
| GET    | `/metrics`          | obsd's own metrics in Prometheus format                        |
| POST   | `/api/v1/write`     | Push samples: `[{"name","labels","timestamp"?,"value"}, ...]`  |
| GET    | `/api/v1/query`     | `?name=<metric>&<label>=<value>...&from=&to=&step=`            |
| GET    | `/api/v1/series`    | All metric names                                               |
| GET    | `/api/v1/alerts`    | Pending and firing alerts                                      |
| GET    | `/api/v1/targets`   | Latest scrape status per target                                |

`from` and `to` accept RFC3339 timestamps or Unix seconds. `timestamp` on a
write is Unix milliseconds and defaults to the server's receive time. Any query
parameter other than `name`, `from`, `to` and `step` is an exact label matcher.

Example:

```bash
curl -s -X POST localhost:8080/api/v1/write \
  -d '[{"name":"deploy_total","labels":{"service":"billing"},"value":1}]'

curl -s 'localhost:8080/api/v1/query?name=demo_cpu_load&host=demo-1&step=30s'
```

## Configuration

See `config.example.json`. Durations are strings such as `"15s"` or `"2h"`.
Unknown fields are rejected, so typos fail at startup.

| Key                       | Default  | Meaning                                              |
| ------------------------- | -------- | ---------------------------------------------------- |
| `listen`                  | `:8080`  | HTTP listen address                                  |
| `retention`               | `1h`     | How long samples are kept                            |
| `max_samples_per_series`  | `10000`  | Hard cap per series, oldest dropped first            |
| `alert_eval_interval`     | `15s`    | How often alert rules are evaluated                  |
| `webhook_url`             | empty    | If set, alert events are POSTed here as JSON         |
| `scrape_targets[]`        |          | `job`, `url`, `interval`, `timeout`, `labels`        |
| `alert_rules[]`           |          | `name`, `metric`, `labels`, `op`, `threshold`, `for`, `severity` |

Operators: `>`, `>=`, `<`, `<=`, `==`, `!=`. `for` is how long the breach must
persist before the alert fires, which suppresses flapping.

## Design notes

- **Concurrency.** The store uses a single `RWMutex`. Queries take a read lock
  and copy the samples they return, so slow readers never hold up writers.
  Each scrape target runs in its own goroutine.
- **Out-of-order writes.** Samples are kept sorted by timestamp. Writes in
  order append in O(1); late writes are inserted with a binary search.
- **Lookups.** A name index maps each metric to its series keys, so a query
  only touches series of the requested metric.
- **Alert semantics.** Each (rule, series) pair has its own state. Notifications
  fire only on transitions, not on every evaluation. A series that stops
  reporting is resolved, so alerts never stick.
- **Failure handling.** A failed scrape records `up=0` and does not discard
  previously stored data. A malformed exposition line fails that whole scrape,
  as Prometheus does. Notification failures are logged and do not stop evaluation.
- **Limits.** Write bodies are capped at 5 MiB and scrape bodies at 16 MiB.
  NaN and Inf values are encoded as `null` in JSON responses.

## Limitations (deliberate, for scope)

- Data lives in memory, so it is lost on restart. Persisting to disk (for
  example a write-ahead log plus block files) is the natural next step.
- No query language. Queries are a name plus exact label matches, with bucketed
  averaging. PromQL-style functions such as `rate()` are not implemented.
- Single node, no clustering, no authentication. Put it behind a reverse proxy
  with auth before exposing it.
- Alert rules are simple thresholds on the latest value. There is no
  aggregation across series and no rate-of-change rules.

## Development

```bash
go vet ./...
go test ./...
```

Tests cover the storage engine (ordering, caps, retention), the Prometheus
parser (escapes, timestamps, NaN, malformed input), the alert state machine
(pending, firing, resolved and flap suppression), and config validation.
