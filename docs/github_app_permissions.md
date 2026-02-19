# GitHub App Permissions for Scrape Endpoints

Last verified: February 18, 2026.

This document lists the GitHub App permissions required by the endpoints currently used by this project.

## Endpoints and required permissions

### Repository activity and LOC endpoints

| Endpoint | Used by | Required permission (repository) | Source |
| --- | --- | --- | --- |
| `GET /orgs/{org}/repos` | Repo discovery | `Metadata: Read` | <https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#list-organization-repositories> |
| `GET /repos/{owner}/{repo}/stats/contributors` | Primary LOC source | `Metadata: Read` | <https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-all-contributor-commit-activity> |
| `GET /repos/{owner}/{repo}/commits?since=...&until=...` | 24h commit activity | `Contents: Read` | <https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits> |
| `GET /repos/{owner}/{repo}/pulls?state=all` | PR opened/merged activity | `Pull requests: Read` | <https://docs.github.com/en/rest/pulls/pulls?apiVersion=2022-11-28#list-pull-requests> |
| `GET /repos/{owner}/{repo}/pulls/{pull_number}/reviews` | Review submission activity | `Pull requests: Read` | <https://docs.github.com/en/rest/pulls/reviews?apiVersion=2022-11-28#list-reviews-for-a-pull-request> |
| `GET /repos/{owner}/{repo}/issues/comments?since=...` | Issue comment activity | `Issues: Read` | <https://docs.github.com/en/rest/issues/comments?apiVersion=2022-11-28#list-issue-comments-for-a-repository> |
| `GET /repos/{owner}/{repo}/commits` | LOC fallback commit listing | `Contents: Read` | <https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits> |
| `GET /repos/{owner}/{repo}/commits/{sha}` | LOC fallback commit detail (additions/deletions) | `Contents: Read` | <https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#get-a-commit> |

### Copilot usage endpoints

| Endpoint | Used by | Required permission scope | Required permission | Source |
| --- | --- | --- | --- | --- |
| `GET /orgs/{org}/copilot/metrics/reports/organization-1-day` | Copilot org backfill | Organization | `View Organization Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /orgs/{org}/copilot/metrics/reports/organization-28-day/latest` | Copilot org periodic scrape | Organization | `View Organization Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /orgs/{org}/copilot/metrics/reports/users-1-day` | Copilot org-user backfill | Organization | `View Organization Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /orgs/{org}/copilot/metrics/reports/users-28-day/latest` | Copilot org-user periodic scrape | Organization | `View Organization Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /enterprises/{enterprise}/copilot/metrics/reports/enterprise-1-day` | Copilot enterprise backfill | Enterprise | `View Enterprise Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /enterprises/{enterprise}/copilot/metrics/reports/enterprise-28-day/latest` | Copilot enterprise periodic scrape | Enterprise | `View Enterprise Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /enterprises/{enterprise}/copilot/metrics/reports/users-1-day` | Copilot enterprise-user backfill | Enterprise | `View Enterprise Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |
| `GET /enterprises/{enterprise}/copilot/metrics/reports/users-28-day/latest` | Copilot enterprise-user periodic scrape | Enterprise | `View Enterprise Copilot Metrics` | <https://docs.github.com/en/enterprise-cloud@latest/rest/copilot/copilot-usage-metrics?apiVersion=2022-11-28> |

Copilot endpoints return signed report URLs that are downloaded immediately and parsed as NDJSON payloads.

## Minimum GitHub App configuration

- Repository permissions:
  - `Contents`: **Read-only**
  - `Pull requests`: **Read-only**
  - `Issues`: **Read-only**
  - `Metadata`: **Read-only** (GitHub Apps always have metadata read access)
- Organization permissions:
  - None required for the endpoints listed above.
- Additional organization/enterprise permissions when Copilot scraping is enabled:
  - `View Organization Copilot Metrics` for org-scope Copilot endpoints.
  - `View Enterprise Copilot Metrics` for enterprise-scope Copilot endpoints.
- Enterprise Copilot usage metrics policy must be enabled for enterprise endpoints.
- Repository access:
  - The app installation must have access to every repository you want to scrape.
  - If using "Only select repositories", ensure newly created repos are added or discovery will miss them.

Reference: <https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/choosing-permissions-for-a-github-app>

## Project behavior impact

- Primary LOC mode (`/stats/contributors`) works with metadata read.
- 24h activity metrics require:
  - `gh_activity_commits_24h`: `Contents: Read`
  - `gh_activity_prs_opened_24h` and `gh_activity_prs_merged_24h`: `Pull requests: Read`
  - `gh_activity_reviews_submitted_24h`: `Pull requests: Read`
  - `gh_activity_issue_comments_24h`: `Issues: Read`
- Fallback LOC mode (`/commits` + `/commits/{sha}`) requires contents read.
- Copilot usage metrics:
  - always include required labels `org`, `repo="*"`, and `user`
  - may include optional labels `scope`, `window`, `feature`, `ide`,
    `language`, `model`, `chat_mode`, and `day` depending on config toggles.
  - are not guaranteed to be additive between org and enterprise scopes.
- If `Contents: Read` is not granted, commit activity and LOC fallback cannot run.
- If `Pull requests: Read` is not granted, PR and review activity metrics cannot run.
- If `Issues: Read` is not granted, issue comment activity metrics cannot run.
- If Copilot permissions are missing, Copilot endpoints return authorization/availability errors and are pushed to backfill queues.

## Practical validation checklist

1. Confirm app shows `Contents: Read-only` in repository permissions.
2. Confirm installation scope includes all target repositories in each org.
3. Run one scrape cycle and check logs for permission-related status spikes:
   - `repos_stats_forbidden`
   - `repos_stats_not_found`
   - `repos_stats_unavailable`
4. If Copilot is enabled, validate one report-link call per configured endpoint family:
   - org aggregate and org users
   - enterprise aggregate and enterprise users (if `copilot.enterprise.enabled=true`)
5. Confirm `/metrics` includes at least one `gh_copilot_usage_*` series for an allowed scope, or explicit backfill/dependency-health signals when Copilot endpoints are unavailable.
