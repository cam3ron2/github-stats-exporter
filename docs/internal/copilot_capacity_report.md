# Copilot Capacity Report

## Scope

This report captures the initial sizing signal for Copilot usage metric export
workloads and documents whether Kubernetes requests/limits should change.

## Test date

- 2026-02-19

## Benchmark method

Command:

```bash
env -u GOROOT GOCACHE=$(pwd)/.gocache GOMODCACHE=$(pwd)/.gomodcache \
  go test ./internal/exporter -run '^$' \
  -bench 'BenchmarkOpenMetricsHandler(Cardinality|CopilotUserBreakdownCardinality)$' \
  -benchmem
```

Benchmarks used:

- `BenchmarkOpenMetricsHandlerCardinality`
  - Existing activity metric shape: `5 orgs * 300 repos * 8 users`
  - Approximate business-series count: 12,000
- `BenchmarkOpenMetricsHandlerCopilotUserBreakdownCardinality`
  - Copilot-heavy shape with `scope=users` + optional breakdown labels enabled
  - Approximate business-series count: 60,000

## Results

| Benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `BenchmarkOpenMetricsHandlerCardinality` | 55,760,206 | 93,833,468 | 1,710,085 |
| `BenchmarkOpenMetricsHandlerCopilotUserBreakdownCardinality` | 940,548,125 | 1,367,160,036 | 12,192,987 |

## Decision

- Keep default Kubernetes requests/limits unchanged for the current default
  config (`copilot.enabled=false`, breakdown labels disabled).
- For tenants enabling user-level Copilot with multiple breakdown dimensions,
  treat this as a high-cardinality mode and tune resources before production.

## Recommended sizing workflow for high-cardinality mode

1. Enable Copilot in nonprod with production-like org/repo/user volume.
2. Enable one optional breakdown at a time (`ide`, `feature`, `language`,
   `model`) and observe:
   - pod memory working set and restart count,
   - `/metrics` scrape latency and payload size,
   - `gh_exporter_store_active_series`.
3. Increase exporter resources in nonprod if sustained memory exceeds 70% of
   limit or if scrape latency approaches scrape interval.
4. Promote tuned requests/limits to prod overlay after at least 24h stable run.

## Manifest status

- No `deploy/kustomize` resource request/limit changes were applied in this
  phase because default mode has no measured regression.
