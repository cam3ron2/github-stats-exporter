# Runtime Operational Runbook

## Scope

- Covers runtime behavior introduced for:
  - GitHub unhealthy cooldown gating
  - Leader election + role transitions
  - Queue backend fallback (RabbitMQ -> in-memory)

## Expected Runtime Behavior

## GitHub Cooldown

- Leader tracks consecutive scrape failures.
- When failures reach `github.unhealthy_failure_threshold`, scheduled scraping pauses until `github.unhealthy_cooldown` elapses.
- During cooldown, leader enqueues backfill messages with `reason="github_unhealthy"` instead of scraping.
- Leader resumes scraping after cooldown and marks GitHub healthy after `health.github_recover_success_threshold` successful cycles.

## Leader Election / Roles

- In Kubernetes with `leader_election.enabled=true`, replicas use Lease-based election.
- Only the elected leader runs scheduled scrape + GC loops.
- Followers run queue consumers and can write metrics only from backfill flow.
- All replicas serve `/metrics` from shared store, so scrape failover can happen at the service layer.
- Outside Kubernetes (or if in-cluster API bootstrap fails), runtime falls back to static leader mode.
- If `leader_election.enabled=false`, role is controlled by `GITHUB_STATS_INITIAL_ROLE` (`leader` default, `follower` optional).

## Queue Backend Fallback

- Runtime attempts RabbitMQ-backed queue when AMQP config is present.
- If RabbitMQ queue init fails, runtime logs a warning and falls back to in-memory queue.
- In-memory queue fallback is process-local only (not HA across replicas); treat this as degraded mode.

## Copilot Report Egress Policy

- Nonprod overlay keeps `443/TCP` open for signed URL hostname discovery.
  - Manifest: `deploy/kustomize/overlays/nonprod/networkpolicy.yaml`
- Prod overlay restricts `443/TCP` egress to GitHub-published API/web CIDRs
  plus explicit DNS egress.
  - Manifest: `deploy/kustomize/overlays/prod/networkpolicy.yaml`
- Current GitHub Copilot domain families from `gh api /meta`:
  - `*.github.com`
  - `*.githubusercontent.com`
  - `default.exp-tas.com`
  - `*.githubcopilot.com`
- Refresh CIDRs before a release:

```bash
gh api /meta --jq '([.api[], .web[]] | unique | .[])'
```

- Validate in-cluster egress when Copilot is enabled:
  1. `gh_exporter_dependency_health{dependency="github_copilot_report_download"}`
     remains `1`.
  2. No sustained growth in
     `gh_exporter_backfill_jobs_enqueued_total{reason="copilot_report_download_error"}`.
  3. `/healthz?full=1` does not report Copilot download path degraded.

## Key Signals to Monitor

- `gh_exporter_scrape_runs_total{org,result}`
- `gh_exporter_backfill_jobs_enqueued_total{org,reason}`
- `gh_exporter_backfill_jobs_deduped_total{org,reason}`
- `gh_exporter_backfill_enqueues_dropped_total{org,reason}`
- `gh_exporter_backfill_jobs_processed_total{org,repo,result}`
- `gh_exporter_store_write_total{source,result}`
- `gh_exporter_github_rate_limit_remaining{org,installation_id}`
- `gh_exporter_github_rate_limit_reset_unixtime{org,installation_id}`
- `gh_exporter_github_secondary_limit_hits_total{org}`
- `gh_exporter_dependency_health{dependency}`
- `gh_exporter_copilot_scrape_runs_total{scope,result}`
- `gh_exporter_copilot_report_download_total{scope,result}`
- `gh_exporter_copilot_records_dropped_total{scope,reason}`
- `/healthz` JSON mode + component statuses

Alert and recording rule source:

- `deploy/prometheus/github-stats-alert-rules.yaml`

## Quick Verification Commands (Compose)

```bash
docker compose ps
docker compose logs --tail=100 github-stats-exporter
curl -s http://localhost:8080/metrics | rg 'gh_exporter_dependency_health|gh_exporter_store_write_total'
```

## Failure Triage

- If scrape stops unexpectedly:
  - Check logs for cooldown and `github_unhealthy` enqueue events.
  - Check `gh_exporter_dependency_health{dependency="github"}` and `/healthz`.
- If backfill is not draining:
  - Check RabbitMQ queue depth (`gh.backfill.jobs`) and follower logs.
  - Check `gh_exporter_backfill_jobs_processed_total` and `gh_exporter_store_write_total{source="worker_backfill",result="failure"}`.
- If metrics diverge between replicas:
  - Confirm Redis health and shared-store connectivity.
  - Confirm queue/store fallback warnings are not forcing per-process local mode.
