# GitHub Stats Exporter

<p align="center">
  <img src="docs/assets/logo-color-transparent-smooth.png" alt="github-stats-exporter logo" width="180" />
</p>

[![Test](https://github.com/cam3ron2/github-stats-exporter/actions/workflows/test.yml/badge.svg)](https://github.com/cam3ron2/github-stats-exporter/actions/workflows/test.yml)
[![Lint](https://github.com/cam3ron2/github-stats-exporter/actions/workflows/lint.yml/badge.svg)](https://github.com/cam3ron2/github-stats-exporter/actions/workflows/lint.yml)
[![Release](https://github.com/cam3ron2/github-stats-exporter/actions/workflows/release.yml/badge.svg)](https://github.com/cam3ron2/github-stats-exporter/actions/workflows/release.yml)
[![codecov](https://codecov.io/github/cam3ron2/github-stats-exporter/branch/main/graph/badge.svg?token=98QVWNZKBG)](https://codecov.io/github/cam3ron2/github-stats-exporter)
[![Go Report Card](https://goreportcard.com/badge/github.com/cam3ron2/github-stats-exporter)](https://goreportcard.com/report/github.com/cam3ron2/github-stats-exporter)
[![Go Version](https://img.shields.io/github/go-mod/go-version/cam3ron2/github-stats-exporter)](https://github.com/cam3ron2/github-stats-exporter/blob/main/go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`github-stats-exporter` scrapes contributor activity from one or more GitHub organizations and exposes the result as OpenMetrics for Prometheus-style scraping and dashboards, or forwarding to the monitoring/observability platform of your choice.

## Why this exists

GitHub provides rich APIs for organizational and contributor activity, but extracting actionable metrics at scale can be complex. This project offers a turnkey solution to:

- Produce contributor-level metrics with mandatory `org`, `repo`, and `user` labels.
- Support multi-org scraping with separate GitHub App credentials per organization.
- Run in HA mode with leader election and a shared Redis metric store.
- Use RabbitMQ-backed backfill to recover missed scrape windows.
- Expose internal health, rate-limit, queue, and throughput telemetry for operations.

## What you get

- **Business metrics (activity):**
  - `gh_activity_commits_24h{org,repo,user}`
  - `gh_activity_prs_opened_24h{org,repo,user}`
  - `gh_activity_prs_merged_24h{org,repo,user}`
  - `gh_activity_reviews_submitted_24h{org,repo,user}`
  - `gh_activity_issue_comments_24h{org,repo,user}`
  - `gh_activity_loc_added_weekly{org,repo,user}`
  - `gh_activity_loc_removed_weekly{org,repo,user}`
  - `gh_activity_last_event_unixtime{org,repo,user}`
- **Business metrics (optional Copilot usage):**
  - `gh_copilot_usage_user_initiated_interaction_count{org,repo,user,...}`
  - `gh_copilot_usage_code_generation_activity_count{org,repo,user,...}`
  - `gh_copilot_usage_code_acceptance_activity_count{org,repo,user,...}`
  - `gh_copilot_usage_loc_suggested_to_add_sum{org,repo,user,...}`
  - `gh_copilot_usage_loc_suggested_to_delete_sum{org,repo,user,...}`
  - `gh_copilot_usage_loc_added_sum{org,repo,user,...}`
  - `gh_copilot_usage_loc_deleted_sum{org,repo,user,...}`
  - `gh_copilot_usage_pull_requests_total_created{org,repo,user,...}`
  - `gh_copilot_usage_pull_requests_total_reviewed{org,repo,user,...}`
  - `gh_copilot_usage_pull_requests_total_created_by_copilot{org,repo,user,...}`
  - `gh_copilot_usage_pull_requests_total_reviewed_by_copilot{org,repo,user,...}`
- **Operational metrics (exporter/runtime):**
  - scrape outcomes and durations
  - GitHub rate-limit and request counters
  - queue depth/age and backfill outcomes
  - dependency health and store write results

## Documentation index

| Topic | Link |
| --- | --- |
| Full configuration reference | [docs/configuration.md](docs/configuration.md) |
| Required GitHub App permissions | [docs/github_app_permissions.md](docs/github_app_permissions.md) |
| Detailed implementation plan | [docs/internal/implementation_plan.md](docs/internal/implementation_plan.md) |
| Runtime operations runbook | [docs/internal/runtime_operational_runbook.md](docs/internal/runtime_operational_runbook.md) |
| Load/resilience validation | [docs/internal/load_and_resilience_validation.md](docs/internal/load_and_resilience_validation.md) |
| Release pipeline design | [docs/internal/release_pipeline.md](docs/internal/release_pipeline.md) |
| Kustomize secret handling | [deploy/kustomize/overlays/SECRETS.md](deploy/kustomize/overlays/SECRETS.md) |

## Architecture overview

```mermaid
flowchart LR
  subgraph GH["GitHub"]
    G1["Org A (GitHub App A)"]
    G2["Org B (GitHub App B)"]
  end

  subgraph APP["github-stats-exporter replicas"]
    L["Leader role"]
    F["Follower role(s)"]
    H["/metrics, /healthz, /readyz, /livez"]
  end

  R["Redis (shared metric store)"]
  Q["RabbitMQ (backfill queue + DLQ)"]
  P["Prometheus"]

  G1 --> L
  G2 --> L
  L --> R
  L --> Q
  F --> Q
  F --> R
  L --> H
  F --> H
  P -->|"single logical scrape target"| H
```

## Controller and Data Logical Flow

### Leader/follower behavior

```mermaid
stateDiagram-v2
  [*] --> Follower
  Follower --> Leader: "Lease acquired"
  Leader --> Follower: "Lease lost"
  Leader --> Cooldown: "GitHub unhealthy threshold reached"
  Cooldown --> Leader: "Cooldown elapsed and recovered"
```

### Scrape, persist, backfill

```mermaid
sequenceDiagram
  participant Leader
  participant GitHub
  participant Queue as RabbitMQ
  participant Follower
  participant Store as Redis
  participant Prom as Prometheus

  loop "Each org scrape interval"
    Leader->>GitHub: "Fetch repos and activity windows"
    alt "Successful scrape"
      Leader->>Store: "Upsert gh_activity_* and internal metrics"
    else "Partial/failed scrape"
      Leader->>Queue: "Publish missing-window backfill job"
    end
  end

  Follower->>Queue: "Consume backfill jobs"
  Follower->>GitHub: "Retry missing windows"
  Follower->>Store: "Upsert recovered metrics"
  Prom->>Leader: "Scrape /metrics"
  Prom->>Follower: "Scrape /metrics (same data from shared Redis)"
```

## Quick start (local)

### 1) Prerequisites

- Go 1.25+
- Docker + Docker Compose
- GitHub App(s) with required permissions per [docs/github_app_permissions.md](docs/github_app_permissions.md)

### 2) Configure GitHub credentials

1. Copy or create a PEM key at `config/keys/gh-app-key.pem`.
1. Update `config/local.yaml`:
   - `github.orgs[].org`
   - `github.orgs[].app_id`
   - `github.orgs[].installation_id`
   - `github.orgs[].private_key_path`

### 3) Start the exporter and its dependencies

```bash
docker-compose up --build
```

### 4) Validate endpoints

```bash
curl -s http://localhost:8080/livez
curl -s http://localhost:8080/readyz
curl -s http://localhost:8080/healthz
curl -s http://localhost:8080/metrics | head -50
```

## Kubernetes deployment (Kustomize)

The repository includes base manifests plus `nonprod` and `prod` overlays. These manifests are designed for reference and may require adjustments for your specific cluster, namespace, and secret management approach.

> Note: a Helm chart is not currently provided, but may be added in the future based on demand.

```bash
# nonprod
kubectl apply -k deploy/kustomize/overlays/nonprod

# prod
kubectl apply -k deploy/kustomize/overlays/prod
```

Before applying overlays, prepare generated secrets using:

- [deploy/kustomize/overlays/SECRETS.md](deploy/kustomize/overlays/SECRETS.md)

Prometheus scrape and alert examples are in:

- [deploy/prometheus/github-stats-scrape.yaml](deploy/prometheus/github-stats-scrape.yaml)
- [deploy/prometheus/github-stats-alert-rules.yaml](deploy/prometheus/github-stats-alert-rules.yaml)

## Configuration and operations

- Config schema and every field: [docs/configuration.md](docs/configuration.md)
- Runtime operations and failure behavior: [docs/internal/runtime_operational_runbook.md](docs/internal/runtime_operational_runbook.md)
- Load and resilience checks: [docs/internal/load_and_resilience_validation.md](docs/internal/load_and_resilience_validation.md)

### Optional Copilot metrics

Copilot scraping is optional and disabled by default. Enable it only after app
permissions are configured for Copilot usage endpoints.

```yaml
copilot:
  enabled: true
  scrape_interval: "6h"
  include_org_28d: true
  include_org_users_28d: false
  include_pull_request_activity: true
  user_label_mode: "hashed"
  max_records_per_report: 0
  max_users_per_report: 0
  enterprise:
    enabled: false
```

Operational behavior when enabled:

- Leader fetches report-link endpoints and downloads signed NDJSON payloads.
- Failed link/download/parse paths enqueue backfill jobs using 1-day endpoints.
- Optional guardrails cap user/record volume and emit
  `gh_exporter_copilot_records_dropped_total{scope,reason}`.
- Dependency health is split per Copilot path family in
  `gh_exporter_dependency_health{dependency=...}`.

## CI/CD and release model

- PR testing: `.github/workflows/test.yml`
- Linting: `.github/workflows/lint.yml`
- Release on `main` merge: `.github/workflows/release.yml`
- SemVer + changelog automation via Conventional Commits and semantic-release
- Go binaries via GoReleaser
- Multi-arch (`amd64`, `arm64`) container publishing to GHCR

See full details in [docs/internal/release_pipeline.md](docs/internal/release_pipeline.md).

## Development commands

```bash
# tests
make test

# deterministic e2e suite (same path used by CI)
make test-e2e

# live e2e scrape against config/local.yaml + local app keys
make test-e2e-live

# lint
make lint

# compose smoke checks (requires running stack; optional)
./scripts/compose-functional-check.sh
```

## Limitations

- Cardinality is intentionally high due `org`, `repo`, and `user` labels on activity series.
  - This design prioritizes user-level insights and flexibility in aggregation at the cost of higher series counts.
  - Prometheus recording rules can be used to create rollups if needed for performance.
  - Users should monitor series cardinality and adjust scrape intervals or retention as needed based on their scale and Prometheus capacity.
- LOC metrics source data from GitHub's `/stats/contributors` endpoint as their primary source; this is summary-oriented, not real-time. Summaries are updated by GitHub on their own schedule (often daily) and may lag behind real-time activity, but they are more efficient and less rate-limit prone than commit-level data.
  - The fallback LOC mode using `/commits` and `/commits/{sha}` is _considerably_ more API intensive and should be used with caution, especially at scale. It may not be feasible to run regularly for large orgs/repos due to rate limits and performance, as this will cause _every commit_ in the scrape window to require two API calls (one for listing and one for details).
- Backfill and cooldown logic prioritize continuity and API safety over immediate freshness during GitHub outages.
- Copilot metrics are report-based snapshots, not event streams:
  - freshness is bounded by GitHub report generation cadence
  - org and enterprise totals are not strictly additive in all cases
  - signed report download hosts may require explicit egress policy allowances
    in locked-down Kubernetes environments.
