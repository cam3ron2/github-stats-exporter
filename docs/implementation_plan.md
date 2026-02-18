# GitHub Multi-Org Productivity Exporter (Go) - Implementation Plan

## 1. Review of Requirements

### 1.1 What is feasible as requested

- The full design is feasible in Go with a single binary running in Kubernetes.
- Running one controller loop per organization with per-org GitHub App credentials is the right model.
- Leader-only scheduled scraping plus follower-only queue backfill workers is viable with Kubernetes Lease leader election.
- OpenMetrics output is straightforward with `prometheus/client_golang` and `promhttp` in OpenMetrics mode.

### 1.2 High-risk areas and hard recommendations

- Requirement 3 (always label by `org`, `repo`, `user`) creates very high cardinality. Keep this requirement, but do not add any other high-cardinality labels (no `pr_number`, `sha`, `branch`, `email`, `attempt_id`).
- Define "productivity" as activity signals, not performance scores. Use activity metrics (commits, PRs, reviews, comments) and avoid ranking contributors directly.
- Leader-only scraping can become a bottleneck at high scale. Keep this for MVP, but design for future sharding (`org hash mod N leaders`) if one leader cannot keep up.
- "Exactly once" across GitHub + AMQP is not realistic. Make processing idempotent and at-least-once.
- Requirement 16 says "omitting detailed traces"; this looks like a wording conflict. Plan assumes: tracing can be fully disabled, sampled, or fully detailed by config.
- GitHub API degradation must not crash the app; it should move into degraded mode and enqueue backfill tasks, then recover automatically.
- Avoid duplicate Prometheus ingestion by scraping one logical target (service DNS) with replica failover instead of scraping every pod endpoint.
- Backfill must be coalesced and deduplicated or queue volume will spike during GitHub outages at large org scale.
- Redis metric export must use sharded indexes and incremental cache refresh to avoid full-scan bottlenecks.

### 1.3 Storage choice decision

- Recommended shared backend: **Redis (HA via Sentinel or Redis Cluster)**.
- Why: lightweight, low cost, fast writes/reads, TTL support, mature in Kubernetes.
- Use Redis for:
  - Shared metric state used by all replicas for `/metrics`.
  - Watermarks/checkpoints per org/repo.
  - Idempotency keys for backfill jobs.
  - Retention via TTL, with leader-driven explicit GC for indexes.

## 2. Target Architecture

### 2.1 Runtime roles (single Go binary)

- `Leader` responsibilities:
  - Periodic scheduled scraping for all configured orgs.
  - Enqueue backfill jobs when data windows are missed.
  - Run storage GC.
- `Follower` responsibilities:
  - Consume backfill queue and perform retry/backfill work.
- `All replicas` responsibilities:
  - Serve `/metrics` in OpenMetrics format from shared store-backed cache.
  - Serve `/healthz`, `/readyz`, `/livez`.
  - Emit app telemetry (metrics, logs, optional traces).

### 2.2 Component layout

- `cmd/github-stats/main.go`
- `internal/config` (YAML/env config parsing + validation)
- `internal/leader` (Kubernetes Lease election)
- `internal/controller` (org controller manager, goroutine lifecycle)
- `internal/github` (GitHub App auth, API client, pagination, rate headers)
- `internal/scrape` (scheduled scraping orchestration)
- `internal/backfill` (AMQP producer/consumer, retry, max age handling)
- `internal/store` (Redis backend, retention, checkpoints, idempotency)
- `internal/exporter` (registry builder, OpenMetrics handler)
- `internal/health` (dependency checks + degraded status)
- `internal/telemetry` (OTel + structured logging + app metrics)

### 2.3 Prometheus scrape topology (one logical target with failover)

- Use one ClusterIP service DNS target for scraping: `github-stats-metrics.github-stats.svc.cluster.local:8080`.
- Do not use ServiceMonitor endpoint discovery for this job (that expands to pod targets and can duplicate samples).
- Keep all replicas scrape-capable; kube-proxy load balancing plus shared Redis state provides transparent failover.
- Add recording rules only for business aggregations, not for deduplication.

## 3. Configuration Model

Use one config file with per-org blocks, each allowing a separate GitHub App.

```yaml
server:
  listen_addr: ":8080"
  log_level: "info" # debug|info|warn|error

metrics:
  topology: "single_service_target"
  scrape_service_dns: "github-stats-metrics.github-stats.svc.cluster.local:8080"

leader_election:
  enabled: true
  namespace: "github-stats"
  lease_name: "github-stats-leader"
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
      app_id: 111111
      installation_id: 222222
      private_key_path: "/etc/github-stats/keys/org-a.pem"
      scrape_interval: "5m"
      repo_allowlist: ["*"]
      per_org_concurrency: 8
    - org: "org-b"
      app_id: 333333
      installation_id: 444444
      private_key_path: "/etc/github-stats/keys/org-b.pem"
      scrape_interval: "5m"
      repo_allowlist: ["*"]
      per_org_concurrency: 8

rate_limit:
  min_remaining_threshold: 200
  min_reset_buffer: "10s"
  secondary_limit_backoff: "60s"

retry:
  max_attempts: 7
  initial_backoff: "2s"
  max_backoff: "2m"
  jitter: true

loc:
  source: "stats_contributors" # primary source for LOC
  refresh_interval: "24h"
  fallback_enabled: true
  fallback_max_commits_per_repo_per_week: 500
  fallback_max_commit_detail_calls_per_org_per_hour: 3000
  large_repo_zero_detection_windows: 2
  large_repo_cooldown: "7d"

backfill:
  enabled: true
  max_message_age: "24h"
  consumer_count: 6
  requeue_delays: ["1m", "5m", "30m", "2h"]
  coalesce_window: "15m"
  dedup_ttl: "12h"
  max_enqueues_per_org_per_minute: 200

amqp:
  url: "amqp://githubstats:githubstats@rabbitmq:5672/"
  exchange: "gh.backfill"
  queue: "gh.backfill.jobs"
  dlq: "gh.backfill.dlq"

store:
  backend: "redis"
  redis_mode: "standalone" # standalone|sentinel
  redis_addr: "redis:6379"
  redis_master_set: "mymaster"
  redis_sentinel_addrs: []
  redis_password: ""
  redis_db: 0
  retention: "30d"
  metric_refresh_interval: "30s"
  index_shards: 128
  export_cache_mode: "incremental" # incremental|full
  max_series_budget: 10000000

health:
  github_probe_interval: "30s"
  github_recover_success_threshold: 3

telemetry:
  otel_enabled: true
  otel_exporter_otlp_endpoint: "otel-collector:4317"
  otel_trace_mode: "sampled" # off|errors|sampled|detailed
  otel_trace_sample_ratio: 0.05
```

## 4. Data and Metric Model

### 4.1 Metric cardinality policy

- Mandatory labels on all productivity metrics: `org`, `repo`, `user`.
- Strict denylist for additional labels on productivity metrics.
- Internal app health metrics can use lower cardinality labels (`org`, `dependency`, `status`) without `repo`/`user` when appropriate.

### 4.2 Productivity metrics (OpenMetrics)

- `gh_activity_commits_24h{org,repo,user}` gauge
- `gh_activity_prs_opened_24h{org,repo,user}` gauge
- `gh_activity_prs_merged_24h{org,repo,user}` gauge
- `gh_activity_reviews_submitted_24h{org,repo,user}` gauge
- `gh_activity_issue_comments_24h{org,repo,user}` gauge
- `gh_activity_loc_added_weekly{org,repo,user}` gauge
- `gh_activity_loc_removed_weekly{org,repo,user}` gauge
- `gh_activity_last_event_unixtime{org,repo,user}` gauge

24h activity metrics are rolling-window gauges. LOC metrics are weekly gauges sourced from the most recent complete contributor-stats week. Prometheus will store historical change over time.

Primary LOC source: `GET /repos/{owner}/{repo}/stats/contributors` aggregated by contributor login.

Fallback policy for repos affected by GitHub's large-repo LOC limitation (commonly `>=10k` commits where additions/deletions can be zeroed):

- Detect limitation when contributor stats return non-zero contribution totals but zero additions/deletions for `large_repo_zero_detection_windows` consecutive pulls.
- Switch that repo to fallback mode for `large_repo_cooldown`.
- In fallback mode, compute LOC from capped commit-detail sampling:
  - List commits for the weekly window.
  - Fetch `GET /repos/{owner}/{repo}/commits/{sha}` for up to `fallback_max_commits_per_repo_per_week`.
  - Aggregate `stats.additions` / `stats.deletions` per user and publish as estimated weekly LOC.
- When the cap is hit before completing the window, keep partial estimate and expose fallback quality through internal metrics.

### 4.3 Internal app metrics

- `gh_exporter_scrape_runs_total{org,result}`
- `gh_exporter_scrape_duration_seconds{org}`
- `gh_exporter_github_requests_total{org,endpoint,status_class}`
- `gh_exporter_github_rate_limit_remaining{org,installation_id}`
- `gh_exporter_github_rate_limit_reset_unixtime{org,installation_id}`
- `gh_exporter_github_secondary_limit_hits_total{org}`
- `gh_exporter_backfill_jobs_enqueued_total{org,reason}`
- `gh_exporter_backfill_jobs_deduped_total{org,reason}`
- `gh_exporter_backfill_enqueues_dropped_total{org,reason}` (`reason`: `org_rate_cap` or `global_safety_cap`)
- `gh_exporter_backfill_jobs_processed_total{org,result}`
- `gh_exporter_queue_oldest_message_age_seconds{queue}`
- `gh_exporter_store_write_total{source,result}` (`source`: `leader_scrape` or `worker_backfill`)
- `gh_exporter_loc_source_mode{org,repo,mode}` (`mode`: `stats_contributors` or `sampled_commit_stats`)
- `gh_exporter_loc_fallback_incomplete{org,repo}` (`1` when fallback cap truncated weekly LOC window)
- `gh_exporter_loc_fallback_budget_exhausted_total{org}`
- `gh_exporter_metrics_cache_refresh_duration_seconds`
- `gh_exporter_metrics_series_loaded{metric}`
- `gh_exporter_dependency_health{dependency}` (`1` healthy, `0` unhealthy)
- `gh_app_rate_limit_remaining{org,installation_id}`

## 5. Scraping and Controller Design

### 5.1 Controller pattern

- Use a `ControllerManager` that starts one org controller goroutine per configured org.
- Each org controller has:
  - Dedicated GitHub installation client (separate app support per org).
  - Independent ticker (`scrape_interval`).
  - Bounded worker pool for repo-level concurrency.
  - Independent rate-limit state machine.

### 5.2 Leader scrape flow

1. Tick for org controller.
2. Check GitHub health state and current rate-limit budget.
3. Enumerate repos in org (cached + paginated).
4. For each repo, compute scrape window from checkpoint.
5. Fetch events (commits, PRs, reviews, comments) and fetch LOC using `/stats/contributors` on LOC refresh cadence.
6. If large-repo LOC limitation is detected, switch repo to capped commit-detail fallback mode.
7. Aggregate into required productivity metrics per (`org`,`repo`,`user`).
8. Upsert to Redis.
9. Advance checkpoint.
10. On partial failure, enqueue coalesced backfill message for missed window (dedup + org rate caps).

### 5.3 Follower backfill flow

1. Consume AMQP message.
2. Validate age (`created_at + max_message_age`); if stale, drop/ack.
3. Acquire idempotency lock in Redis (`SETNX` with TTL).
4. Re-run scrape for exact window.
5. Upsert metrics and checkpoint.
6. Ack on success.
7. On failure, requeue with exponential delay until max attempts; then DLQ.

### 5.4 LOC contributor-stats state machine

- `warming`: `/stats/contributors` returns `202 Accepted`; do not enqueue backfill, schedule delayed recheck.
- `ready`: valid weekly stats available; publish LOC gauges from most recent complete week.
- `stale`: repeated `202` or unchanged stale snapshot beyond `2 x refresh_interval`; raise health warning and keep last-known LOC values.
- Transition to fallback only when large-repo zeroing is detected across `large_repo_zero_detection_windows` consecutive successful stats responses.

## 6. Rate-Limit Awareness and Retry Strategy

### 6.1 Rate-limit handling

- Parse GitHub headers on every response:
  - `X-RateLimit-Remaining`
  - `X-RateLimit-Reset`
  - `X-RateLimit-Used`
  - `Retry-After` (if present)
- If remaining < threshold, pause org controller until reset plus jitter.
- If 403 secondary rate limit detected, apply configured cooloff and log at `warn`.
- Expose remaining/reset as metrics and log structured events for alerting.

### 6.2 Retry/backoff policy

- Retry transient errors (`429`, `5xx`, network timeouts, temporary DNS/connectivity issues).
- No retry for permanent client errors (`401`, `403` invalid auth, `404` invalid repo).
- Backoff: exponential with full jitter, capped max.
- If retries exhausted, enqueue backfill message and move on (no crash).

### 6.3 LOC fallback API budget controls

- Apply per-org token bucket to commit-detail fallback calls using `fallback_max_commit_detail_calls_per_org_per_hour`.
- When fallback budget is exhausted, keep partial LOC estimate, mark incomplete quality metric, and defer remainder to later windows.
- Never allow fallback traffic to consume the full installation budget reserved for baseline activity scraping.

## 7. Missing Data and AMQP Design

### 7.1 Backfill message contract

```json
{
  "job_id": "uuid",
  "dedup_key": "org-a:repo-x:2026-02-18T00:00:00Z:2026-02-18T00:15:00Z",
  "org": "org-a",
  "repo": "repo-x",
  "window_start": "2026-02-18T00:00:00Z",
  "window_end": "2026-02-18T00:05:00Z",
  "reason": "scrape_error|rate_limited|github_unhealthy",
  "attempt": 1,
  "max_attempts": 7,
  "created_at": "2026-02-18T00:05:10Z"
}
```

### 7.2 Queue topology

- Exchange: `gh.backfill` (`direct`).
- Main queue: `gh.backfill.jobs`.
- Retry queues: `gh.backfill.retry.1m`, `gh.backfill.retry.5m`, `gh.backfill.retry.30m`, `gh.backfill.retry.2h`.
- DLQ: `gh.backfill.dlq`.
- Per-message or queue-level TTL enforces max age.

### 7.3 Coalescing and enqueue controls

- Coalesce enqueue windows per repo to `coalesce_window` buckets before publishing jobs.
- Use Redis `SETNX` on `dedup_key` with `dedup_ttl` to suppress duplicate backfill jobs.
- Apply `max_enqueues_per_org_per_minute` to prevent queue storms during dependency outages.
- If cap is hit, increment `gh_exporter_backfill_enqueues_dropped_total` and retry coalesced enqueue on next sweep.

## 8. Shared Storage Design (Redis)

### 8.1 Key model

- Metric snapshot key (hash):
  - `ghm:snapshot:<metric>:<org>:<repo>:<user>`
  - Fields: `value`, `window_seconds`, `updated_unix`
- Checkpoint key:
  - `ghm:checkpoint:<org>:<repo>` => last processed timestamp
- Idempotency lock:
  - `ghm:joblock:<job_id>` => TTL lock to avoid duplicate processing
- Sharded metric index sets:
  - `ghm:index:<metric>:<shard>` => set of snapshot keys (shard count from `store.index_shards`)
- Delta cursor:
  - `ghm:delta_cursor:<metric>` => last incremental export position

### 8.2 Retention and GC

- Apply TTL to metric snapshot keys using configurable retention.
- Leader runs periodic GC to remove stale index references, expired checkpoints, and stale dedup locks.
- Followers never run GC.

### 8.3 Write permissions by role

- Leader writes from scheduled scrape path.
- Followers can write only from queue-consumer path.
- Enforce this in code by passing `WriteSource` and validating role before write.

### 8.4 Export performance strategy

- Use incremental export cache updates (`store.export_cache_mode=incremental`) instead of full keyspace scans.
- Set and enforce `max_series_budget`; when exceeded, emit health warnings and refuse to ingest new low-priority series.
- Define SLOs: p95 `/metrics` render latency < 2s and Redis command latency p95 < 25ms under expected cardinality.

## 9. Leader Election and HA

### 9.1 Election mechanism

- Kubernetes Lease API using `client-go/tools/leaderelection`.
- Configure lease timings from config.
- On leadership gain: start scrape schedulers and GC loops.
- On leadership loss: cancel leader loops immediately, continue serving metrics and queue workers.

### 9.2 HA behavior

- Deploy at least 3 replicas.
- All replicas expose identical metric view from shared Redis.
- During failover, scrape gap is compensated by backfill queue processing.

### 9.3 One-target metrics failover behavior

- Prometheus scrapes one DNS target (`github-stats-metrics` service), not individual pods.
- Any healthy replica can answer with equivalent metric content because all read from Redis-backed cache.
- Pod churn does not duplicate time series because Prometheus has one logical target identity.

## 10. Health, Readiness, and Failure Modes

### 10.1 Endpoints

- `/livez`: process alive.
- `/readyz`: app can serve requests and satisfy role-specific readiness checks.
- `/healthz`: detailed JSON for dependencies + mode (`healthy`, `degraded`, `unhealthy`).

Role-aware readiness rules:

- Leader: Redis + AMQP + scheduler loop healthy + GitHub client usable (not necessarily currently healthy).
- Follower: Redis + AMQP + consumer channel healthy.
- All replicas: exporter cache refresh loop healthy.

### 10.2 GitHub degradation policy

- If GitHub error ratio breaches threshold:
  - Mark GitHub status degraded.
  - Stop scheduled scraping temporarily.
  - Emit backfill jobs for missed windows.
  - Continue serving `/metrics` and app endpoints.
- Resume scraping after cooldown and successful probe cycle.

## 11. Observability (Metrics, Logs, Traces)

### 11.1 Logging

- Structured JSON logs with configurable level.
- Default `info`; `debug` only when needed.
- Use log sampling/rate limiting on repetitive warnings.
- Include keys: `org`, `repo`, `user` only where required; avoid secrets/token material.

### 11.2 OpenTelemetry

- Instrument inbound HTTP, outbound GitHub HTTP, AMQP publish/consume, Redis calls.
- Configurable trace mode:
  - `off`: no traces.
  - `errors`: only error spans/events.
  - `sampled`: normal sampled tracing.
  - `detailed`: full dependency tracing for deep debugging.

## 12. Security and Compliance Baseline

- Store GitHub private keys in Kubernetes `Secret` (or external secret manager).
- Use least-privilege GitHub App permissions.
- Use TLS to AMQP/Redis in non-local environments.
- Redact auth headers and token values from logs/traces.
- Add dependency and container image scanning in CI.

## 13. Detailed Implementation Phases

### Phase 0 - Foundations

- Initialize Go module, build pipeline, lint/test setup.
- Add config loader + schema validation.
- Add structured logger and HTTP server skeleton.

### Phase 1 - Core integrations

- Implement GitHub App auth per org (JWT, installation token refresh).
- Implement Redis client with health checks and basic key operations.
- Implement AMQP topology declaration and basic pub/sub.

### Phase 2 - Leader election and runtime roles

- Add Kubernetes Lease election.
- Wire role-based lifecycle manager.
- Ensure non-leader path does not start scheduler loops.

### Phase 3 - Scraper engine

- Implement org controller manager.
- Implement repo discovery + pagination.
- Implement activity data fetchers and aggregation.
- Implement checkpoints and upsert writes.
- Implement LOC state machine (`warming`/`ready`/`stale`) for `/stats/contributors`.

### Phase 4 - Rate limit and retry resilience

- Parse rate-limit headers and enforce adaptive pacing.
- Add retry/backoff wrappers for GitHub API calls.
- Emit rate-limit metrics + warn logs.

### Phase 5 - Backfill queue processing

- Implement missed-window job producer.
- Implement consumer workers with idempotency and retry queues.
- Enforce max message age and DLQ behavior.
- Add coalescing, enqueue dedup (`SETNX`), and per-org enqueue rate caps.

### Phase 6 - Metrics exporter and health endpoints

- Build incremental in-memory refresh cache from sharded Redis indexes for fast `/metrics`.
- Expose OpenMetrics using `promhttp` with OpenMetrics enabled.
- Implement role-aware `/livez`, `/readyz`, `/healthz`.

### Phase 7 - Telemetry and hardening

- Add OTel instrumentation and sampling controls.
- Add load tests for cardinality and scrape volume.
- Add failure-mode tests for GitHub outage, AMQP outage, Redis outage.
- Add duplication guard tests for one-target scraping and failover.
- Add LOC fallback budget-exhaustion tests and quality flag assertions.

### Phase 8 - Deployment artifacts

- Add Dockerfile.
- Add Kubernetes manifests for app + Redis + RabbitMQ.
- Add kustomize `base`, `overlays/nonprod`, `overlays/prod`.
- Add local Docker Compose.
- Add operational runbook and alert suggestions.
- Add Prometheus static scrape config for single logical service target.

## 14. Requirement Traceability Matrix

| Req | Covered By |
| --- | ---------- |
| 1 | Go architecture in all phases |
| 2 | Per-org controller goroutines + per-org app config |
| 3 | Metric schema mandates `org`,`repo`,`user` labels |
| 4 | Rate-limit header parser + pacing logic |
| 5 | Retry/backoff policy for transient failures |
| 6 | AMQP backfill workflow + delayed retries |
| 7 | Kubernetes Lease leader election |
| 8 | Leader: scheduled scrape; followers: queue workers; all serve metrics |
| 9 | `/metrics` OpenMetrics handler |
| 10 | Redis shared backend with retention |
| 11 | Configurable retention in `store.retention` |
| 12 | Leader-only GC loop |
| 13 | Redis HA via Sentinel in prod overlay (or Redis Cluster) |
| 14 | Role-guarded writes (`leader_scrape` vs `worker_backfill`) |
| 15 | Queue/message max age with TTL + consumer check |
| 16 | OTel instrumentation + trace detail modes |
| 17 | App self-observability metrics list |
| 18 | `/healthz` dependency-aware status |
| 19 | Degraded mode enqueues backfill, no crash |
| 20 | Stop scraping on GitHub unhealthy + retry cooldown |
| 21 | Rate-limit metrics + structured warning logs |
| 22 | Kustomize manifests for app + Redis + RabbitMQ with prod/nonprod overlays |
| 23 | Docker Compose below |
| 24 | Configurable log levels + structured logging strategy |

## 15. Kubernetes Manifests (Kustomize Base + Overlays)

Use kustomize with one base and two overlays:

```text
deploy/kustomize/
  base/
    kustomization.yaml
    namespace.yaml
    serviceaccount.yaml
    rbac.yaml
    app-configmap.yaml
    app-secret-template.yaml
    app-deployment.yaml
    app-service-http.yaml
    app-service-metrics.yaml
    app-pdb.yaml
    redis-service.yaml
    redis-statefulset.yaml
    redis-pdb.yaml
    rabbitmq-secret.yaml
    rabbitmq-service.yaml
    rabbitmq-statefulset.yaml
    rabbitmq-pdb.yaml
  overlays/
    nonprod/
      kustomization.yaml
      patch-scale.yaml
      patch-config.yaml
    prod/
      kustomization.yaml
      patch-scale.yaml
      patch-config.yaml
      patch-app-hardening.yaml
      redis-sentinel.yaml
      networkpolicy.yaml
```

Base kustomization:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: github-stats
resources:
  - namespace.yaml
  - serviceaccount.yaml
  - rbac.yaml
  - app-configmap.yaml
  - app-secret-template.yaml
  - app-deployment.yaml
  - app-service-http.yaml
  - app-service-metrics.yaml
  - app-pdb.yaml
  - redis-service.yaml
  - redis-statefulset.yaml
  - redis-pdb.yaml
  - rabbitmq-secret.yaml
  - rabbitmq-service.yaml
  - rabbitmq-statefulset.yaml
  - rabbitmq-pdb.yaml
```

Base foundation resources:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: github-stats
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: github-stats
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: github-stats-leader-election
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: github-stats-leader-election
subjects:
  - kind: ServiceAccount
    name: github-stats
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: github-stats-leader-election
---
apiVersion: v1
kind: Secret
metadata:
  name: github-stats-secrets
type: Opaque
stringData:
  org-a.pem: |
    -----BEGIN RSA PRIVATE KEY-----
    REPLACE_WITH_ORG_A_KEY
    -----END RSA PRIVATE KEY-----
  org-b.pem: |
    -----BEGIN RSA PRIVATE KEY-----
    REPLACE_WITH_ORG_B_KEY
    -----END RSA PRIVATE KEY-----
```

Base app deployment and services:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: github-stats
  labels:
    app: github-stats
spec:
  replicas: 3
  selector:
    matchLabels:
      app: github-stats
  template:
    metadata:
      labels:
        app: github-stats
    spec:
      serviceAccountName: github-stats
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        fsGroup: 65532
      containers:
        - name: github-stats
          image: ghcr.io/your-org/github-stats:latest
          imagePullPolicy: IfNotPresent
          args: ["--config=/etc/github-stats/config.yaml"]
          ports:
            - name: http
              containerPort: 8080
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
          livenessProbe:
            httpGet:
              path: /livez
              port: http
          resources:
            requests:
              cpu: "250m"
              memory: "512Mi"
            limits:
              cpu: "2"
              memory: "2Gi"
          volumeMounts:
            - name: config
              mountPath: /etc/github-stats/config.yaml
              subPath: config.yaml
              readOnly: true
            - name: keys
              mountPath: /etc/github-stats/keys
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: github-stats-config
        - name: keys
          secret:
            secretName: github-stats-secrets
---
apiVersion: v1
kind: Service
metadata:
  name: github-stats-http
  labels:
    app: github-stats
spec:
  selector:
    app: github-stats
  ports:
    - name: http
      port: 8080
      targetPort: http
---
apiVersion: v1
kind: Service
metadata:
  name: github-stats-metrics
  labels:
    app: github-stats
spec:
  selector:
    app: github-stats
  ports:
    - name: http
      port: 8080
      targetPort: http
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: github-stats
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: github-stats
```

Base dependencies (Redis + RabbitMQ):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis
spec:
  clusterIP: None
  selector:
    app: redis
  ports:
    - name: redis
      port: 6379
      targetPort: redis
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7.2-alpine
          ports:
            - name: redis
              containerPort: 6379
          args: ["redis-server", "--appendonly", "yes"]
          readinessProbe:
            exec:
              command: ["redis-cli", "ping"]
          livenessProbe:
            exec:
              command: ["redis-cli", "ping"]
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: redis
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: redis
---
apiVersion: v1
kind: Secret
metadata:
  name: rabbitmq-secret
type: Opaque
stringData:
  username: githubstats
  password: githubstats
  erlang-cookie: CHANGE_ME
---
apiVersion: v1
kind: Service
metadata:
  name: rabbitmq
spec:
  clusterIP: None
  selector:
    app: rabbitmq
  ports:
    - name: amqp
      port: 5672
      targetPort: amqp
    - name: mgmt
      port: 15672
      targetPort: mgmt
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rabbitmq
spec:
  serviceName: rabbitmq
  replicas: 1
  selector:
    matchLabels:
      app: rabbitmq
  template:
    metadata:
      labels:
        app: rabbitmq
    spec:
      containers:
        - name: rabbitmq
          image: rabbitmq:3.13-management-alpine
          ports:
            - name: amqp
              containerPort: 5672
            - name: mgmt
              containerPort: 15672
          env:
            - name: RABBITMQ_DEFAULT_USER
              valueFrom:
                secretKeyRef:
                  name: rabbitmq-secret
                  key: username
            - name: RABBITMQ_DEFAULT_PASS
              valueFrom:
                secretKeyRef:
                  name: rabbitmq-secret
                  key: password
            - name: RABBITMQ_ERLANG_COOKIE
              valueFrom:
                secretKeyRef:
                  name: rabbitmq-secret
                  key: erlang-cookie
          readinessProbe:
            exec:
              command: ["rabbitmq-diagnostics", "check_running"]
          livenessProbe:
            exec:
              command: ["rabbitmq-diagnostics", "check_running"]
          volumeMounts:
            - name: data
              mountPath: /var/lib/rabbitmq
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 20Gi
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: rabbitmq
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: rabbitmq
```

Base app config map:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: github-stats-config
data:
  config.yaml: |
    server:
      listen_addr: ":8080"
      log_level: "info"
    metrics:
      topology: "single_service_target"
      scrape_service_dns: "github-stats-metrics.github-stats.svc.cluster.local:8080"
    leader_election:
      enabled: true
      namespace: "github-stats"
      lease_name: "github-stats-leader"
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
          app_id: 111111
          installation_id: 222222
          private_key_path: "/etc/github-stats/keys/org-a.pem"
          scrape_interval: "5m"
          repo_allowlist: ["*"]
          per_org_concurrency: 8
        - org: "org-b"
          app_id: 333333
          installation_id: 444444
          private_key_path: "/etc/github-stats/keys/org-b.pem"
          scrape_interval: "5m"
          repo_allowlist: ["*"]
          per_org_concurrency: 8
    rate_limit:
      min_remaining_threshold: 200
      min_reset_buffer: "10s"
      secondary_limit_backoff: "60s"
    retry:
      max_attempts: 7
      initial_backoff: "2s"
      max_backoff: "2m"
      jitter: true
    loc:
      source: "stats_contributors"
      refresh_interval: "24h"
      fallback_enabled: true
      fallback_max_commits_per_repo_per_week: 500
      fallback_max_commit_detail_calls_per_org_per_hour: 3000
      large_repo_zero_detection_windows: 2
      large_repo_cooldown: "7d"
    backfill:
      enabled: true
      max_message_age: "24h"
      consumer_count: 6
      requeue_delays: ["1m", "5m", "30m", "2h"]
      coalesce_window: "15m"
      dedup_ttl: "12h"
      max_enqueues_per_org_per_minute: 200
    amqp:
      url: "amqp://githubstats:githubstats@rabbitmq.github-stats.svc.cluster.local:5672/"
      exchange: "gh.backfill"
      queue: "gh.backfill.jobs"
      dlq: "gh.backfill.dlq"
    store:
      backend: "redis"
      redis_mode: "standalone"
      redis_addr: "redis.github-stats.svc.cluster.local:6379"
      redis_master_set: "mymaster"
      redis_sentinel_addrs: []
      redis_password: ""
      redis_db: 0
      retention: "30d"
      metric_refresh_interval: "30s"
      index_shards: 128
      export_cache_mode: "incremental"
      max_series_budget: 10000000
    health:
      github_probe_interval: "30s"
      github_recover_success_threshold: 3
    telemetry:
      otel_enabled: false
      otel_exporter_otlp_endpoint: ""
      otel_trace_mode: "off"
      otel_trace_sample_ratio: 0.05
```

`overlays/nonprod/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
patchesStrategicMerge:
  - patch-scale.yaml
  - patch-config.yaml
```

`overlays/nonprod/patch-scale.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: github-stats
spec:
  replicas: 2
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
spec:
  replicas: 1
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rabbitmq
spec:
  replicas: 1
```

`overlays/nonprod/patch-config.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: github-stats-config
data:
  config.yaml: |
    server:
      listen_addr: ":8080"
      log_level: "debug"
    metrics:
      topology: "single_service_target"
      scrape_service_dns: "github-stats-metrics.github-stats.svc.cluster.local:8080"
    leader_election:
      enabled: true
      namespace: "github-stats"
      lease_name: "github-stats-leader"
      lease_duration: "30s"
      renew_deadline: "20s"
      retry_period: "5s"
    github:
      api_base_url: "https://api.github.com"
      request_timeout: "20s"
      unhealthy_failure_threshold: 6
      unhealthy_cooldown: "1m"
      orgs:
        - org: "org-a"
          app_id: 111111
          installation_id: 222222
          private_key_path: "/etc/github-stats/keys/org-a.pem"
          scrape_interval: "10m"
          repo_allowlist: ["*"]
          per_org_concurrency: 4
    rate_limit:
      min_remaining_threshold: 100
      min_reset_buffer: "10s"
      secondary_limit_backoff: "60s"
    retry:
      max_attempts: 5
      initial_backoff: "2s"
      max_backoff: "90s"
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
      consumer_count: 3
      requeue_delays: ["1m", "5m", "30m", "2h"]
      coalesce_window: "15m"
      dedup_ttl: "12h"
      max_enqueues_per_org_per_minute: 100
    amqp:
      url: "amqp://githubstats:githubstats@rabbitmq.github-stats.svc.cluster.local:5672/"
      exchange: "gh.backfill"
      queue: "gh.backfill.jobs"
      dlq: "gh.backfill.dlq"
    store:
      backend: "redis"
      redis_mode: "standalone"
      redis_addr: "redis.github-stats.svc.cluster.local:6379"
      redis_master_set: "mymaster"
      redis_sentinel_addrs: []
      redis_password: ""
      redis_db: 0
      retention: "14d"
      metric_refresh_interval: "30s"
      index_shards: 64
      export_cache_mode: "incremental"
      max_series_budget: 3000000
    health:
      github_probe_interval: "30s"
      github_recover_success_threshold: 2
    telemetry:
      otel_enabled: false
      otel_exporter_otlp_endpoint: ""
      otel_trace_mode: "off"
      otel_trace_sample_ratio: 0.05
```

`overlays/prod/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
  - redis-sentinel.yaml
  - networkpolicy.yaml
patchesStrategicMerge:
  - patch-scale.yaml
  - patch-config.yaml
  - patch-app-hardening.yaml
```

`overlays/prod/patch-scale.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: github-stats
spec:
  replicas: 5
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
spec:
  replicas: 3
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rabbitmq
spec:
  replicas: 3
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: github-stats
spec:
  minAvailable: 2
```

`overlays/prod/patch-config.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: github-stats-config
data:
  config.yaml: |
    server:
      listen_addr: ":8080"
      log_level: "info"
    metrics:
      topology: "single_service_target"
      scrape_service_dns: "github-stats-metrics.github-stats.svc.cluster.local:8080"
    leader_election:
      enabled: true
      namespace: "github-stats"
      lease_name: "github-stats-leader"
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
          app_id: 111111
          installation_id: 222222
          private_key_path: "/etc/github-stats/keys/org-a.pem"
          scrape_interval: "5m"
          repo_allowlist: ["*"]
          per_org_concurrency: 8
        - org: "org-b"
          app_id: 333333
          installation_id: 444444
          private_key_path: "/etc/github-stats/keys/org-b.pem"
          scrape_interval: "5m"
          repo_allowlist: ["*"]
          per_org_concurrency: 8
    rate_limit:
      min_remaining_threshold: 200
      min_reset_buffer: "10s"
      secondary_limit_backoff: "60s"
    retry:
      max_attempts: 7
      initial_backoff: "2s"
      max_backoff: "2m"
      jitter: true
    loc:
      source: "stats_contributors"
      refresh_interval: "24h"
      fallback_enabled: true
      fallback_max_commits_per_repo_per_week: 500
      fallback_max_commit_detail_calls_per_org_per_hour: 3000
      large_repo_zero_detection_windows: 2
      large_repo_cooldown: "7d"
    backfill:
      enabled: true
      max_message_age: "24h"
      consumer_count: 6
      requeue_delays: ["1m", "5m", "30m", "2h"]
      coalesce_window: "15m"
      dedup_ttl: "12h"
      max_enqueues_per_org_per_minute: 200
    amqp:
      url: "amqp://githubstats:githubstats@rabbitmq.github-stats.svc.cluster.local:5672/"
      exchange: "gh.backfill"
      queue: "gh.backfill.jobs"
      dlq: "gh.backfill.dlq"
    store:
      backend: "redis"
      redis_mode: "sentinel"
      redis_addr: ""
      redis_master_set: "mymaster"
      redis_sentinel_addrs:
        - "redis-sentinel.github-stats.svc.cluster.local:26379"
      redis_password: ""
      redis_db: 0
      retention: "30d"
      metric_refresh_interval: "30s"
      index_shards: 256
      export_cache_mode: "incremental"
      max_series_budget: 20000000
    health:
      github_probe_interval: "30s"
      github_recover_success_threshold: 3
    telemetry:
      otel_enabled: false
      otel_exporter_otlp_endpoint: ""
      otel_trace_mode: "off"
      otel_trace_sample_ratio: 0.05
```

`overlays/prod/redis-sentinel.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis-sentinel
spec:
  selector:
    app: redis-sentinel
  ports:
    - name: sentinel
      port: 26379
      targetPort: sentinel
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis-sentinel
spec:
  replicas: 3
  selector:
    matchLabels:
      app: redis-sentinel
  template:
    metadata:
      labels:
        app: redis-sentinel
    spec:
      containers:
        - name: sentinel
          image: redis:7.2-alpine
          command: ["/bin/sh", "-ec"]
          args:
            - |
              cat <<'EOF' > /tmp/sentinel.conf
              port 26379
              sentinel monitor mymaster redis-0.redis.github-stats.svc.cluster.local 6379 2
              sentinel down-after-milliseconds mymaster 5000
              sentinel failover-timeout mymaster 60000
              sentinel parallel-syncs mymaster 1
              EOF
              exec redis-sentinel /tmp/sentinel.conf
          ports:
            - name: sentinel
              containerPort: 26379
```

`overlays/prod/patch-app-hardening.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: github-stats
spec:
  template:
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values: ["github-stats"]
              topologyKey: kubernetes.io/hostname
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: ScheduleAnyway
          labelSelector:
            matchLabels:
              app: github-stats
```

`overlays/prod/networkpolicy.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: github-stats-default-deny
spec:
  podSelector: {}
  policyTypes: ["Ingress", "Egress"]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: github-stats-allow-required
spec:
  podSelector:
    matchLabels:
      app: github-stats
  policyTypes: ["Ingress", "Egress"]
  ingress:
    - from:
        - namespaceSelector: {}
      ports:
        - protocol: TCP
          port: 8080
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: redis
      ports:
        - protocol: TCP
          port: 6379
    - to:
        - podSelector:
            matchLabels:
              app: rabbitmq
      ports:
        - protocol: TCP
          port: 5672
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
      ports:
        - protocol: TCP
          port: 443
```

Apply with:

```bash
kubectl apply -k deploy/kustomize/overlays/nonprod
kubectl apply -k deploy/kustomize/overlays/prod
```

Prometheus scrape config for one logical target with failover (recommended over ServiceMonitor endpoint discovery):

```yaml
scrape_configs:
  - job_name: github-stats
    metrics_path: /metrics
    static_configs:
      - targets:
          - github-stats-metrics.github-stats.svc.cluster.local:8080
```

## 16. Local Development Compose (App + Redis + RabbitMQ)

```yaml
version: "3.9"

services:
  github-stats:
    image: ghcr.io/your-org/github-stats:dev
    build:
      context: .
      dockerfile: Dockerfile
    command: ["--config=/app/config/local.yaml"]
    volumes:
      - ./config/local.yaml:/app/config/local.yaml:ro
      - ./config/keys:/app/keys:ro
    ports:
      - "8080:8080"
    depends_on:
      redis:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    restart: unless-stopped

  redis:
    image: redis:7.2-alpine
    command: ["redis-server", "--save", "", "--appendonly", "no"]
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 10
    restart: unless-stopped

  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: githubstats
      RABBITMQ_DEFAULT_PASS: githubstats
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_running"]
      interval: 15s
      timeout: 5s
      retries: 10
    restart: unless-stopped
```

## 17. Delivery Checklist (Definition of Done)

- All 24 requirements mapped and tested.
- Leader failover validated without data loss beyond queued backfill windows.
- Load test confirms acceptable scrape duration and memory at expected cardinality.
- Single-target Prometheus scrape validated with no duplicate business series.
- Backfill coalescing and dedup validated during synthetic GitHub outage.
- LOC fallback budget exhaustion behavior validated with quality metrics emitted.
- Alerting rules in place for:
  - rate limit remaining below threshold
  - GitHub API unhealthy state
  - queue backlog growth
  - dropped backfill enqueues due to org safety caps
  - store/queue dependency failures
- Runbook documents incident actions for GitHub outage, Redis outage, AMQP outage.

## 18. Pre-Init GitHub Issue Backlog

Create these issues once the repository is initialized. All are implementation-critical and principal-level risk controls.

1. Title: `Implement single-target Prometheus scrape topology with failover`
   - Priority: `P1`
   - Labels: `architecture`, `observability`, `metrics`
   - Description: Implement and validate single logical scrape target via `github-stats-metrics` service DNS. Remove/avoid pod endpoint ServiceMonitor for this app.
   - Acceptance criteria:
     - Prometheus scrapes one logical target.
     - Business metrics do not duplicate during normal operations or pod churn.
     - Failover works without scrape target reconfiguration.

2. Title: `Add backfill coalescing, dedup keys, and enqueue safety caps`
   - Priority: `P1`
   - Labels: `reliability`, `queueing`
   - Description: Prevent queue storms by coalescing windows, deduplicating jobs via Redis locks, and enforcing per-org enqueue limits.
   - Acceptance criteria:
     - Duplicate enqueue attempts are suppressed and counted.
     - Queue growth remains bounded during GitHub outage simulation.
     - Dropped/capped enqueue events are exposed as metrics.

3. Title: `Implement Redis sharded index model and incremental metrics export cache`
   - Priority: `P1`
   - Labels: `performance`, `storage`, `metrics`
   - Description: Replace single index sets/full scans with shard-aware indexes and incremental export cache updates.
   - Acceptance criteria:
     - No full keyspace scan on each `/metrics` request.
     - `/metrics` p95 < 2s under target load.
     - Series budget enforcement and warning metrics implemented.

4. Title: `Implement LOC contributor-stats state machine (warming/ready/stale)`
   - Priority: `P2`
   - Labels: `metrics`, `github-api`
   - Description: Add explicit handling for `/stats/contributors` async generation (`202`) and stale states.
   - Acceptance criteria:
     - `202` does not immediately enqueue backfill.
     - Stale LOC state is visible in health/metrics.
     - Last-known-good LOC values persist until refreshed.

5. Title: `Add LOC fallback global API budgets and incomplete-quality signaling`
   - Priority: `P2`
   - Labels: `rate-limit`, `metrics`
   - Description: Enforce per-org fallback commit-detail budget and expose incomplete estimates.
   - Acceptance criteria:
     - Budget consumption tracked per org.
     - Exhausted budget stops additional fallback calls.
     - Quality metrics reflect partial weekly LOC coverage.

6. Title: `Implement role-aware readiness and detailed dependency health contracts`
   - Priority: `P2`
   - Labels: `operations`, `kubernetes`, `health`
   - Description: Readiness must validate role-specific loops and dependencies.
   - Acceptance criteria:
     - Leader readiness checks scheduler health.
     - Follower readiness checks queue-consumer channel health.
     - `/healthz` emits structured component status with degraded mode.

7. Title: `Add kustomize base/nonprod/prod deployment with in-repo Redis and RabbitMQ`
   - Priority: `P1`
   - Labels: `deployment`, `kustomize`, `infrastructure`
   - Description: Provide fully versioned manifests for app and dependencies with environment overlays.
   - Acceptance criteria:
     - `kubectl apply -k` works for both overlays.
     - Nonprod defaults are low-cost and test-friendly.
     - Prod overlay includes HA-oriented replica, PDB, and hardening settings.

8. Title: `Harden production Kubernetes posture (anti-affinity, network policy, secret handling)`
   - Priority: `P2`
   - Labels: `security`, `kubernetes`
   - Description: Enforce pod spread, restricted security context, and network policy boundaries. Replace placeholder secrets with secret manager integration.
   - Acceptance criteria:
     - Pods spread across nodes/zone topology constraints.
     - Least-privilege container security context enforced.
     - Network policies restrict traffic to required paths only.

9. Title: `Build resilience test suite for failover, duplicate delivery, and outage recovery`
   - Priority: `P1`
   - Labels: `testing`, `reliability`
   - Description: Add integration tests for leader failover, at-least-once queue semantics, and outage backfill reconciliation.
   - Acceptance criteria:
     - Leader handoff does not lose checkpoints.
     - Duplicate queue delivery is idempotent.
     - Outage recovery backfills missed windows and converges.
