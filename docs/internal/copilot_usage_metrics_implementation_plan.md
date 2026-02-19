## Copilot Usage Metrics Implementation Plan

### 1. Objective

Add optional GitHub Copilot usage metrics scraping to `github-stats-exporter` and
publish those metrics in OpenMetrics format, while preserving current behavior
when Copilot scraping is disabled.

### 2. Scope and Constraints

- Copilot metrics collection must be config-gated (`copilot.enabled`).
- Existing activity metrics and LOC behavior must be unchanged by default.
- Implementation must follow TDD with table-driven tests.
- New Copilot paths must be resilient to GitHub API/report download failures and
  integrate with existing backfill flow.
- Maintain existing cardinality policy: avoid unbounded labels by default.

### 3. Endpoint and Permission Design

Reference:

- <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28>
- <https://docs.github.com/en/copilot/reference/copilot-usage-metrics/copilot-usage-metrics>

#### 3.1 Endpoints to support

- Enterprise report-link endpoints:
  - `GET /enterprises/{enterprise}/copilot/metrics/reports/enterprise-1-day`
  - `GET /enterprises/{enterprise}/copilot/metrics/reports/enterprise-28-day/latest`
  - `GET /enterprises/{enterprise}/copilot/metrics/reports/users-1-day`
  - `GET /enterprises/{enterprise}/copilot/metrics/reports/users-28-day/latest`
- Organization report-link endpoints:
  - `GET /orgs/{org}/copilot/metrics/reports/organization-1-day`
  - `GET /orgs/{org}/copilot/metrics/reports/organization-28-day/latest`
  - `GET /orgs/{org}/copilot/metrics/reports/users-1-day`
  - `GET /orgs/{org}/copilot/metrics/reports/users-28-day/latest`

Each endpoint returns signed download URLs. The implementation must then fetch
and parse NDJSON payloads from those URLs.

#### 3.2 Permissions to document in `docs/github_app_permissions.md`

- Enterprise endpoints:
  - Fine-grained permission: `View Enterprise Copilot Metrics`
  - Enterprise Copilot policy must enable usage metrics.
- Organization endpoints:
  - Fine-grained permission: `View Organization Copilot Metrics`
- Existing repo permissions remain required for non-Copilot metrics.

Also document that enterprise and org views are not strictly additive due to
organization attribution semantics.

### 4. Config Design

Add a new top-level section to config:

```yaml
copilot:
  enabled: false
  scrape_interval: "6h"
  request_timeout: "30s"
  download_timeout: "90s"
  include_org_28d: true
  include_org_users_28d: false
  include_enterprise_28d: false
  include_enterprise_users_28d: false
  include_breakdown_ide: false
  include_breakdown_feature: false
  include_breakdown_language: false
  include_breakdown_model: false
  include_pull_request_activity: true
  user_label_mode: "login" # login|id|hashed|none
  emit_day_label: false
  max_records_per_report: 0
  max_users_per_report: 0
  refresh_if_report_unchanged: false
  enterprise:
    enabled: false
    slug: ""
    app_id: 0
    installation_id: 0
    private_key_path: ""
```

Validation rules:

- If `copilot.enabled=false`, no additional required fields.
- If `enterprise.enabled=false`, enterprise endpoints are disabled and
  `include_enterprise_28d` / `include_enterprise_users_28d` are coerced to
  `false` at runtime/config load.
- If `enterprise.enabled=true`, `enterprise.slug`, `app_id`,
  `installation_id`, and `private_key_path` are required.
- `user_label_mode` must be one of allowed enum values.
- durations and limits must be positive when set.
- `max_records_per_report=0` and `max_users_per_report=0` mean "unbounded";
  any positive value is enforced as a hard cap with drop counters emitted.

### 5. Metric Contract

#### 5.1 Metric names

- `gh_copilot_usage_user_initiated_interaction_count`
- `gh_copilot_usage_code_generation_activity_count`
- `gh_copilot_usage_code_acceptance_activity_count`
- `gh_copilot_usage_loc_suggested_to_add_sum`
- `gh_copilot_usage_loc_suggested_to_delete_sum`
- `gh_copilot_usage_loc_added_sum`
- `gh_copilot_usage_loc_deleted_sum`
- `gh_copilot_usage_pull_requests_total_created`
- `gh_copilot_usage_pull_requests_total_reviewed`
- `gh_copilot_usage_pull_requests_total_created_by_copilot`
- `gh_copilot_usage_pull_requests_total_reviewed_by_copilot`

#### 5.2 Labels

Required labels for compatibility with current model:

- `org`
- `repo` (set to `"*"` for Copilot metrics)
- `user` (depends on `user_label_mode`)

Optional bounded labels:

- `scope` (`org|enterprise|users`)
- `window` (`1d|28d`)
- `feature`
- `ide`
- `language`
- `model`
- `chat_mode`
- `day` (only if `emit_day_label=true`)

Default behavior should keep optional labels mostly disabled to control
cardinality.

#### 5.3 Metric type and time semantics

- Emit Copilot usage series as `gauge` metrics because report payloads are
  snapshot totals for a report window, not monotonic local counters.
- `window="28d"` series represent the most recent rolling window from `latest`
  endpoints.
- Backfill with `1-day` reports updates the corresponding day window data and
  then rolls forward aggregate views on the next scheduled scrape.
- If `emit_day_label=true`, use an ISO date (`YYYY-MM-DD`) and do not include
  wall-clock timestamps as labels.

### 6. Scraping Strategy

#### 6.1 Leader cycle

- On each Copilot scrape interval:
  - Fetch `28-day/latest` report links for enabled org/enterprise endpoints.
  - Download NDJSON immediately (signed links are short-lived).
  - Stream-parse lines and map to metric points.
  - Upsert metrics to shared store.
  - Record checkpoint per scope/report (`report_end_day`).

#### 6.2 Backfill strategy

- Use existing queue and dispatcher.
- Add backfill reasons:
  - `copilot_report_fetch_error`
  - `copilot_report_download_error`
  - `copilot_report_parse_error`
- Backfill workers use 1-day endpoints for specific missed days to recover data.
- Backfill message payload must include:
  - `scope` (`org|enterprise`)
  - `scope_id` (org name or enterprise slug)
  - `report_type` (`organization|users`)
  - `window_start_day`
  - `window_end_day`
  - `reason`

#### 6.3 Error handling and retry

- Retry endpoint call and report download using current retry policy.
- On repeated failure, enqueue backfill message instead of crashing.
- Preserve degraded operation mode.

### 7. Health and Internal Observability

Add dependency components and metrics:

- `github_api_core`
- `github_api_copilot_org_reports`
- `github_api_copilot_org_user_reports`
- `github_api_copilot_enterprise_reports`
- `github_api_copilot_enterprise_user_reports`
- `github_copilot_report_download`

Add internal metrics:

- `gh_exporter_copilot_scrape_runs_total{scope,result}`
- `gh_exporter_copilot_report_download_total{scope,result}`
- `gh_exporter_copilot_records_parsed_total{scope}`
- `gh_exporter_copilot_last_success_unixtime{scope}`
- `gh_exporter_copilot_report_staleness_seconds{scope}`

Readiness behavior:

- Copilot failures should degrade health but not hard-fail liveness.
- Keep readiness policy configurable if stricter behavior is needed later.

### 8. Package-by-Package Implementation Plan (TDD)

1. `internal/config`
   - Add `CopilotConfig` types and raw parsing.
   - Add defaults + validation tests (table-driven).

2. `internal/githubapi`
   - Add typed client methods for all Copilot report-link endpoints.
   - Add signed URL downloader and NDJSON streaming parser.
   - Add exhaustive tests for status handling, schema tolerance, and malformed
     lines.

3. `internal/scrape`
   - Add Copilot scraper pipeline and mapping to metric points.
   - Add label policy tests and cardinality guard tests.
   - Add checkpoint and backfill reason tests.

4. `internal/app`
   - Integrate Copilot scrape loop into leader cycle with independent interval.
   - Integrate new backfill reasons and health/dependency updates.
   - Add runtime tests for enabled/disabled behavior and degraded mode.

5. `internal/store`
   - Reuse checkpoint storage with distinct Copilot checkpoint keys.
   - Add tests for checkpoint lifecycle and retention.

6. `internal/exporter`
   - Ensure new metric families render correctly in OpenMetrics output.
   - Add contract tests for label presence and type.

### 9. Documentation and Examples

Update:

- `docs/github_app_permissions.md` with Copilot endpoint permissions and
  attribution caveats.
- `docs/configuration.md` to include full `copilot` field reference and examples.
- `README.md` with feature overview, config snippet, and operational caveats.
- `config/local.yaml` with testable `copilot` sample config.

### 10. E2E Test Plan

Deterministic E2E:

- Add fixture handlers for report-link endpoints and signed URL NDJSON payloads.
- Verify:
  - Copilot disabled => no Copilot series.
  - Copilot enabled => expected Copilot series emitted.
  - Leader/follower convergence includes Copilot series.
  - Failed report fetch/download enqueues backfill and recovers.

Live E2E:

- Optional, gated by secrets and explicit test tags.
- Validate at least one org report path end-to-end.

### 11. Additional Steps Needed

- Add schema evolution handling for preview APIs (ignore unknown fields, count
  parse drops).
- Add user identity handling policy (`user_label_mode`) to reduce PII exposure.
- Review NetworkPolicy egress for signed report hostnames, not only
  `api.github.com`.

### 11.1 Explicitly out of scope for this phase

- Dashboard creation and alert rules in external observability providers.
- Long-term historical warehousing beyond current retention model.
- Helm chart refactor (tracked separately).

### 12. Concerns and Recommended Solutions

1. **High cardinality and large report volumes**
   - Concern: user-level + breakdown dimensions can explode series count.
   - Recommendation: default breakdowns off, hard caps on users/records, and
     explicit opt-in for high-cardinality dimensions.

2. **Organization attribution overlap**
   - Concern: the same user can be counted in multiple org-level reports.
   - Recommendation: include `scope` label and document non-additive rollups in
     docs and dashboards.

3. **Preview API changes**
   - Concern: schema/field changes may break strict parsers.
   - Recommendation: tolerant parsing and internal parse error counters.

### 13. Resource and Deployment Impact

- Initial recommendation: no immediate request/limit change for aggregate-only
  mode.
- If user-level and multiple breakdowns are enabled:
  - increase CPU/memory requests in nonprod first,
  - run profile-based sizing,
  - then propagate values to prod overlays.

### 14. Acceptance Criteria

- Copilot scraping fully optional and disabled by default.
- All configured Copilot endpoints scrape successfully when permissions exist.
- Failures do not crash app; backfill/retry path works.
- New metrics appear in OpenMetrics output with expected labels.
- Config, permissions, README, and local examples are fully documented.
- Deterministic E2E covers Copilot enabled/disabled and failure recovery paths.

### 15. Execution Order (issue-aligned)

1. Schema and docs prerequisites: `CP-001`, `CP-002`
2. Data acquisition foundation: `CP-003`, `CP-004`
3. Metric contract and runtime integration: `CP-005`, `CP-006`, `CP-007`
4. Observability and guardrails: `CP-008`, `CP-014`, `CP-016`
5. Documentation and developer workflow: `CP-009`, `CP-010`
6. Validation and scale hardening: `CP-011`, `CP-012`, `CP-013`, `CP-015`
