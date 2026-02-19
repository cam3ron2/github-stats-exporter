# Load and Resilience Validation

This document defines the operational validation artifacts that back the delivery checklist.

## 1. Compose functional resilience check

Use the script below to validate leader/follower runtime behavior and queue/store dependencies:

```bash
./scripts/compose-functional-check.sh
```

The script validates:

- leader endpoint is healthy and in leader role
- follower endpoint is healthy and in follower role
- key internal metrics exist on both scrape targets
- business-series output is consistent between leader/follower targets
- RabbitMQ backfill queues exist

## 2. Export-path load benchmark

Use the benchmark below to estimate `/metrics` rendering cost under high-cardinality series counts:

```bash
env -u GOROOT GOCACHE=$(pwd)/.gocache GOMODCACHE=$(pwd)/.gomodcache \
  go test ./internal/exporter -run '^$' -bench BenchmarkOpenMetricsHandlerCardinality -benchmem
```

Benchmark file:

- `internal/exporter/openmetrics_benchmark_test.go`
- `docs/internal/copilot_capacity_report.md` (latest measured baseline + sizing
  decision)

## 3. Prometheus recording + alert rules

Prometheus rule file:

- `deploy/prometheus/github-stats-alert-rules.yaml`

Implemented rules:

- recording rules for user-level business rollups
- low GitHub app rate-limit alert
- GitHub dependency degraded alert
- queue backlog age alert
- dropped enqueue alert
- Redis/AMQP dependency critical alert
