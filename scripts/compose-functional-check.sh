#!/usr/bin/env bash
set -Eeuo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

expected_org_label_count="${GITHUB_STATS_EXPECTED_ORG_LABEL_COUNT:-2}"

tmp_files=()

cleanup() {
  if ((${#tmp_files[@]} == 0)); then
    return
  fi
  rm -f "${tmp_files[@]}"
}
trap cleanup EXIT

fail() {
  echo "[error] $*"
  exit 1
}

contains_or_fail() {
  local haystack="$1"
  local needle="$2"
  local context="$3"

  if ! grep -Fq -- "$needle" <<<"$haystack"; then
    fail "${context}: expected to find '${needle}'"
  fi
}

curl_or_fail() {
  local url="$1"
  local context="$2"
  shift 2

  local output
  if ! output="$(curl -fsS "$@" "$url")"; then
    fail "${context}: request failed (${url})"
  fi
  printf '%s' "$output"
}

echo "[check] docker compose services"
docker compose ps

echo "[check] leader health endpoint"
leader_health="$(curl_or_fail "http://localhost:8080/healthz" "leader health")"
contains_or_fail "$leader_health" '"role":"leader"' "leader health"
contains_or_fail "$leader_health" '"ready":true' "leader health"

echo "[check] follower health endpoint"
follower_health="$(curl_or_fail "http://localhost:8081/healthz" "follower health")"
contains_or_fail "$follower_health" '"role":"follower"' "follower health"
contains_or_fail "$follower_health" '"ready":true' "follower health"

echo "[check] key metrics on leader target"
leader_metrics="$(curl_or_fail "http://localhost:8080/metrics" "leader metrics")"
contains_or_fail "$leader_metrics" 'gh_exporter_dependency_health' "leader metrics"
contains_or_fail "$leader_metrics" 'gh_exporter_queue_oldest_message_age_seconds' "leader metrics"
contains_or_fail "$leader_metrics" 'gh_exporter_github_rate_limit_remaining' "leader metrics"

echo "[check] key metrics on follower target"
follower_metrics="$(curl_or_fail "http://localhost:8081/metrics" "follower metrics")"
contains_or_fail "$follower_metrics" 'gh_exporter_dependency_health' "follower metrics"
contains_or_fail "$follower_metrics" 'gh_exporter_queue_oldest_message_age_seconds' "follower metrics"

echo "[check] unique org labels are present in leader and follower metrics"
leader_org_labels="$(echo "$leader_metrics" | sed -n 's/.*org="\([^"]*\)".*/\1/p' | sort -u)"
follower_org_labels="$(echo "$follower_metrics" | sed -n 's/.*org="\([^"]*\)".*/\1/p' | sort -u)"
leader_org_count="$(echo "$leader_org_labels" | sed '/^$/d' | wc -l | tr -d ' ')"
follower_org_count="$(echo "$follower_org_labels" | sed '/^$/d' | wc -l | tr -d ' ')"
if (( leader_org_count < expected_org_label_count )); then
  echo "[error] expected at least ${expected_org_label_count} unique org labels on leader, got ${leader_org_count}"
  echo "[debug] leader org labels:"
  echo "$leader_org_labels"
  exit 1
fi
if (( follower_org_count < expected_org_label_count )); then
  echo "[error] expected at least ${expected_org_label_count} unique org labels on follower, got ${follower_org_count}"
  echo "[debug] follower org labels:"
  echo "$follower_org_labels"
  exit 1
fi
leader_orgs_file="$(mktemp)"
follower_orgs_file="$(mktemp)"
tmp_files+=("$leader_orgs_file" "$follower_orgs_file")
echo "$leader_org_labels" > "$leader_orgs_file"
echo "$follower_org_labels" > "$follower_orgs_file"
if ! diff -u "$leader_orgs_file" "$follower_orgs_file" >/dev/null; then
  echo "[debug] leader org labels:"
  echo "$leader_org_labels"
  echo "[debug] follower org labels:"
  echo "$follower_org_labels"
  fail "org label sets differ between leader and follower targets"
fi

echo "[check] business-series consistency across leader/follower targets"
leader_activity="$(mktemp)"
follower_activity="$(mktemp)"
tmp_files+=("$leader_activity" "$follower_activity")
if ! grep '^gh_activity_' <<<"$leader_metrics" | sort > "$leader_activity"; then
  fail "leader metrics: no gh_activity_ series found"
fi
if ! grep '^gh_activity_' <<<"$follower_metrics" | sort > "$follower_activity"; then
  fail "follower metrics: no gh_activity_ series found"
fi
if ! diff -u "$leader_activity" "$follower_activity" >/dev/null; then
  fail "gh_activity_ series differ between leader and follower targets"
fi

echo "[check] rabbitmq backfill queues"
queues_json="$(curl_or_fail "http://localhost:15672/api/queues/%2F" "rabbitmq queues" -u githubstats:githubstats)"
contains_or_fail "$queues_json" '"name":"gh.backfill.jobs"' "rabbitmq queues"
contains_or_fail "$queues_json" '"name":"gh.backfill.dlq"' "rabbitmq queues"
contains_or_fail "$queues_json" '"name":"gh.backfill.jobs.retry.1m0s"' "rabbitmq queues"

echo "[check] follower backfill processed metric policy"
backfill_queue_depth="$(
  python3 -c '
import json
import sys

queues = json.load(sys.stdin)
target_names = {"gh.backfill.jobs", "gh.backfill.dlq"}
total = 0
for queue in queues:
    name = str(queue.get("name", ""))
    if name in target_names or name.startswith("gh.backfill.jobs.retry."):
        total += int(queue.get("messages", 0) or 0)
print(total)
' <<<"$queues_json"
)"

if (( backfill_queue_depth > 0 )); then
  contains_or_fail \
    "$follower_metrics" \
    'gh_exporter_backfill_jobs_processed_total' \
    "follower metrics (queue depth ${backfill_queue_depth})"
else
  if grep -Fq 'gh_exporter_backfill_jobs_processed_total' <<<"$follower_metrics"; then
    echo "[info] follower metrics include gh_exporter_backfill_jobs_processed_total with queue depth 0"
  else
    echo "[info] follower metrics omit gh_exporter_backfill_jobs_processed_total because queue depth is 0"
  fi
fi

echo "[ok] compose functional checks passed"
