#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

echo "[check] docker compose services"
docker compose ps

echo "[check] leader health endpoint"
leader_health="$(curl -fsS http://localhost:8080/healthz)"
echo "$leader_health" | grep -q '"role":"leader"'
echo "$leader_health" | grep -q '"ready":true'

echo "[check] follower health endpoint"
follower_health="$(curl -fsS http://localhost:8081/healthz)"
echo "$follower_health" | grep -q '"role":"follower"'
echo "$follower_health" | grep -q '"ready":true'

echo "[check] key metrics on leader target"
leader_metrics="$(curl -fsS http://localhost:8080/metrics)"
echo "$leader_metrics" | grep -q 'gh_exporter_dependency_health'
echo "$leader_metrics" | grep -q 'gh_exporter_queue_oldest_message_age_seconds'
echo "$leader_metrics" | grep -q 'gh_exporter_github_rate_limit_remaining'

echo "[check] key metrics on follower target"
follower_metrics="$(curl -fsS http://localhost:8081/metrics)"
echo "$follower_metrics" | grep -q 'gh_exporter_backfill_jobs_processed_total'

echo "[check] business-series consistency across leader/follower targets"
leader_activity="$(mktemp)"
follower_activity="$(mktemp)"
echo "$leader_metrics" | grep '^gh_activity_' | sort > "$leader_activity"
echo "$follower_metrics" | grep '^gh_activity_' | sort > "$follower_activity"
diff -u "$leader_activity" "$follower_activity" >/dev/null

echo "[check] rabbitmq backfill queues"
queues_json="$(curl -fsS -u githubstats:githubstats http://localhost:15672/api/queues/%2F)"
echo "$queues_json" | grep -q '"name":"gh.backfill.jobs"'
echo "$queues_json" | grep -q '"name":"gh.backfill.dlq"'
echo "$queues_json" | grep -q '"name":"gh.backfill.jobs.retry.1m0s"'

echo "[ok] compose functional checks passed"
