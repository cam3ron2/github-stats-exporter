# Configuration guide

This document covers every YAML configuration field supported by the app.

## How configuration is loaded

- Start command accepts `--config=<path>` (default: `config/local.yaml`).
- YAML parsing is strict (`KnownFields=true`): unknown keys fail startup.
- Durations support Go duration syntax (`30s`, `5m`, `24h`) plus:
  - `d` for days (`7d`)
  - `w` for weeks (`2w`)
- Some defaults are applied at config load time; additional runtime fallbacks
  are applied when values are unset or non-positive.

## Complete example

```yaml
server:
  listen_addr: ":8080"
  log_level: "info"

metrics:
  topology: "single_service_target"
  scrape_service_dns: "github-stats-exporter-metrics.github-stats-exporter.svc.cluster.local:8080"

leader_election:
  enabled: true
  namespace: "github-stats-exporter"
  lease_name: "github-stats-exporter-leader"
  lease_duration: "30s"
  renew_deadline: "20s"
  retry_period: "5s"

github:
  api_base_url: "https://api.github.com"
  request_timeout: "20s"
  unhealthy_failure_threshold: 6
  unhealthy_cooldown: "2m"
  orgs:
    - org: "org-a"
      app_id: 123456
      installation_id: 789012
      private_key_path: "/etc/github-stats-exporter/keys/org-a.pem"
      scrape_interval: "5m"
      repo_allowlist: ["*"]
      per_org_concurrency: 4

rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"

retry:
  max_attempts: 5
  initial_backoff: "2s"
  max_backoff: "30s"
  jitter: true

loc:
  source: "stats_contributors"
  refresh_interval: "24h"
  fallback_enabled: true
  fallback_max_commits_per_repo_per_week: 250
  fallback_max_commit_detail_calls_per_org_per_hour: 1000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"

backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 2
  requeue_delays: ["1m", "5m", "30m", "2h"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 100

amqp:
  url: "amqp://githubstats:githubstats@rabbitmq:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"

store:
  backend: "redis"
  redis_mode: "standalone"
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  redis_password: ""
  redis_db: 0
  retention: "30d"
  metric_refresh_interval: "30s"
  index_shards: 64
  export_cache_mode: "incremental"
  max_series_budget: 500000

health:
  github_probe_interval: "30s"
  github_recover_success_threshold: 3

telemetry:
  otel_enabled: false
  otel_exporter_otlp_endpoint: ""
  otel_trace_mode: "off"
  otel_trace_sample_ratio: 0.05
```

## Field reference

## `server`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `server.listen_addr` | string | Recommended | none | HTTP bind address (example: `:8080`). |
| `server.log_level` | enum | Yes | `info` | Must be one of `debug`, `info`, `warn`, `error`. |

## `metrics`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `metrics.topology` | enum | Yes | `single_service_target` | Currently must be exactly `single_service_target`. |
| `metrics.scrape_service_dns` | string | No | none | Informational for scrape topology/docs; not currently consumed by runtime logic. |

## `leader_election`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `leader_election.enabled` | bool | No | `false` | Enables Kubernetes Lease election mode. |
| `leader_election.namespace` | string | When enabled | none | Kubernetes namespace containing the Lease object. |
| `leader_election.lease_name` | string | When enabled | none | Lease object name. |
| `leader_election.lease_duration` | duration | No | runtime fallback `30s` | Lease duration used by elector if unset/non-positive. |
| `leader_election.renew_deadline` | duration | No | none | Parsed but not currently used in election logic. |
| `leader_election.retry_period` | duration | No | runtime fallback `5s` | Sleep interval between lease renew attempts. |

## `github`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `github.api_base_url` | string (URL) | No | `https://api.github.com/` | Supports GitHub Enterprise API base URLs too. |
| `github.request_timeout` | duration | No | runtime fallback `20s` | Used for GitHub HTTP clients and GitHub `/meta` probe client. |
| `github.unhealthy_failure_threshold` | int | No | runtime fallback `1` | Consecutive failed probes/scrapes before entering cooldown mode. |
| `github.unhealthy_cooldown` | duration | No | runtime fallback `1m` | Cooldown duration when GitHub is marked unhealthy. |
| `github.orgs` | list | Yes | none | Must contain at least one org; org names must be unique. |

## `github.orgs[]`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `github.orgs[].org` | string | Yes | none | GitHub organization name. Must be unique across entries. |
| `github.orgs[].app_id` | int64 | Yes | none | GitHub App ID for this organization. Must be `> 0`. |
| `github.orgs[].installation_id` | int64 | Yes | none | Installation ID for this org-specific app install. Must be `> 0`. |
| `github.orgs[].private_key_path` | string | Yes | none | PEM private key path for app auth. |
| `github.orgs[].scrape_interval` | duration | Yes | none | Per-org scrape interval. Must be `> 0`. |
| `github.orgs[].repo_allowlist` | list[string] | No | all repos | `["*"]` or empty means all repos; otherwise exact repo-name matches. |
| `github.orgs[].per_org_concurrency` | int | No | runtime fallback `1` | Parallel repo workers for that org. |

## `rate_limit`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `rate_limit.min_remaining_threshold` | int | No | none | If remaining calls drop below this, request client pauses until reset. |
| `rate_limit.min_reset_buffer` | duration | No | none | Extra safety wait added after reset timestamp. |
| `rate_limit.secondary_limit_backoff` | duration | No | none | Wait duration used for secondary limits/429 handling. |

## `retry`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `retry.max_attempts` | int | No | GitHub client fallback `1`; queue fallback `7` | Shared by GitHub API retry and queue retry policy wiring. |
| `retry.initial_backoff` | duration | No | none | Initial backoff for GitHub API retries. |
| `retry.max_backoff` | duration | No | none | Maximum capped backoff for GitHub API retries. |
| `retry.jitter` | bool | No | `false` | Parsed but not currently applied in retry calculations. |

## `loc`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `loc.source` | string | No | none | Parsed but current scraper auto-selects source mode based on endpoint behavior/fallback state. |
| `loc.refresh_interval` | duration | No | runtime fallback `24h` | Used by LOC state machine stale-window logic. |
| `loc.fallback_enabled` | bool | No | `false` | Enables commit-based LOC fallback behavior. |
| `loc.fallback_max_commits_per_repo_per_week` | int | Conditional | runtime fallback `500` | Required `> 0` when fallback is enabled; caps list-commits fallback volume per repo window. |
| `loc.fallback_max_commit_detail_calls_per_org_per_hour` | int | Conditional | none | Required `> 0` when fallback is enabled; org-level budget for commit detail calls. |
| `loc.large_repo_zero_detection_windows` | int | No | none | Consecutive zero-add/delete windows before switching to fallback mode. |
| `loc.large_repo_cooldown` | duration | No | none | Cooldown window while in fallback mode. |

## `backfill`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `backfill.enabled` | bool | No | `false` | Parsed but not currently used as a runtime feature gate. |
| `backfill.max_message_age` | duration | No | runtime fallback `24h` | Messages older than this are dropped by consumers. |
| `backfill.consumer_count` | int | No | runtime fallback `1` | Number of follower queue consumer goroutines. |
| `backfill.requeue_delays` | list[duration] | Yes | none | Must contain at least one value; defines retry schedule and retry queue topology. |
| `backfill.coalesce_window` | duration | No | none | Dispatcher coalesces missing windows to this boundary. |
| `backfill.dedup_ttl` | duration | No | runtime fallback `max_message_age` | TTL for dedup/job locks. |
| `backfill.max_enqueues_per_org_per_minute` | int | No | none | Per-org enqueue rate cap in dispatcher. |

## `amqp`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `amqp.url` | string | No | none | AMQP URL. If unset, runtime falls back to in-memory queue. |
| `amqp.exchange` | string | No | none | Exchange name for publish/get APIs. Required for RabbitMQ mode. |
| `amqp.queue` | string | No | runtime fallback `gh.backfill.jobs` | Main queue name. |
| `amqp.dlq` | string | No | none | Dead-letter queue name; if empty, failed messages are not DLQ-published. |

## `store`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `store.backend` | string | No | `redis` | Runtime uses Redis backend only when value is `redis`; otherwise uses in-memory store. |
| `store.redis_mode` | enum | Yes | `standalone` | Must be `standalone` or `sentinel`. |
| `store.redis_addr` | string | For standalone | none | Redis host:port for standalone mode. |
| `store.redis_master_set` | string | For sentinel | none | Sentinel master name. |
| `store.redis_sentinel_addrs` | list[string] | For sentinel | none | Required when `redis_mode=sentinel`. |
| `store.redis_password` | string | No | empty | Redis password for standalone/sentinel clients. |
| `store.redis_db` | int | No | `0` | Redis logical DB index. |
| `store.retention` | duration | No | runtime fallback `24h` | TTL for metrics/checkpoints and GC horizon in store behavior. |
| `store.metric_refresh_interval` | duration | No | runtime fallback `30s` | Cache refresh interval for `/metrics` rendering. |
| `store.index_shards` | int | No | runtime coercion `>=1` | Number of Redis index shards; non-positive values are coerced to `1`. |
| `store.export_cache_mode` | enum | No | `incremental` | `incremental` or `full`; unknown values normalize to `incremental`. |
| `store.max_series_budget` | int | No | runtime fallback `1000000` | Maximum metric series budget enforced by store writes. |

## `health`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `health.github_probe_interval` | duration | No | runtime fallback `30s` | Interval for active GitHub dependency probes. |
| `health.github_recover_success_threshold` | int | No | runtime fallback `1` | Consecutive successful probes required to recover from unhealthy state. |

## `telemetry`

| Field | Type | Required | Default | Notes |
| --- | --- | --- | --- | --- |
| `telemetry.otel_enabled` | bool | No | `false` | Enables OpenTelemetry tracer setup. |
| `telemetry.otel_exporter_otlp_endpoint` | string | No | empty | OTLP HTTP endpoint; `/v1/traces` is auto-appended when path is omitted. |
| `telemetry.otel_trace_mode` | enum | No | normalized to `sampled` when enabled; forced `off` when disabled | Supported: `off`, `sampled`, `errors`, `detailed`. |
| `telemetry.otel_trace_sample_ratio` | float | No | none | Clamped to `[0,1]`; used by sampled/errors modes. |

## Validation rules enforced at startup

- `server.log_level` must be one of `debug|info|warn|error`.
- `metrics.topology` must be `single_service_target`.
- `github.orgs` must have at least one organization.
- For each org:
  - `org` must be non-empty and unique.
  - `app_id > 0`
  - `installation_id > 0`
  - `private_key_path` must be non-empty
  - `scrape_interval > 0`
- `store.redis_mode` must be `standalone` or `sentinel`.
- `store.redis_sentinel_addrs` is required when `store.redis_mode=sentinel`.
- If `loc.fallback_enabled=true`:
  - `loc.fallback_max_commits_per_repo_per_week > 0`
  - `loc.fallback_max_commit_detail_calls_per_org_per_hour > 0`
- `backfill.requeue_delays` must contain at least one duration.

## Runtime-only environment inputs

These are not YAML fields, but affect startup behavior:

- `GITHUB_STATS_INITIAL_ROLE=leader|follower`
  - Used only when `leader_election.enabled=false`.
  - Defaults to leader when unset.
