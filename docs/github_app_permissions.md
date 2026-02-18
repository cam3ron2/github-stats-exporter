# GitHub App Permissions for Scrape Endpoints

Last verified: February 18, 2026.

This document lists the GitHub App permissions required by the endpoints currently used by this project.

## Endpoints and required permissions

| Endpoint | Used by | Required permission (repository) | Source |
|---|---|---|---|
| `GET /orgs/{org}/repos` | Repo discovery | `Metadata: Read` | <https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#list-organization-repositories> |
| `GET /repos/{owner}/{repo}/stats/contributors` | Primary LOC source | `Metadata: Read` | <https://docs.github.com/en/rest/metrics/statistics?apiVersion=2022-11-28#get-all-contributor-commit-activity> |
| `GET /repos/{owner}/{repo}/commits` | LOC fallback commit listing | `Contents: Read` | <https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits> |
| `GET /repos/{owner}/{repo}/commits/{sha}` | LOC fallback commit detail (additions/deletions) | `Contents: Read` | <https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#get-a-commit> |

## Minimum GitHub App configuration

- Repository permissions:
  - `Contents`: **Read-only**
  - `Metadata`: **Read-only** (GitHub Apps always have metadata read access)
- Organization permissions:
  - None required for the endpoints listed above.
- Repository access:
  - The app installation must have access to every repository you want to scrape.
  - If using "Only select repositories", ensure newly created repos are added or discovery will miss them.

Reference: <https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/choosing-permissions-for-a-github-app>

## Project behavior impact

- Primary LOC mode (`/stats/contributors`) works with metadata read.
- Fallback LOC mode (`/commits` + `/commits/{sha}`) requires contents read.
- If `Contents: Read` is not granted, fallback mode cannot compute LOC for large-repo zeroed stats cases.

## Practical validation checklist

1. Confirm app shows `Contents: Read-only` in repository permissions.
2. Confirm installation scope includes all target repositories in each org.
3. Run one scrape cycle and check logs for permission-related status spikes:
   - `repos_stats_forbidden`
   - `repos_stats_not_found`
   - `repos_stats_unavailable`
