# internal/githubapi

## Logic overview

The `githubapi` package contains GitHub API integration primitives:

- GitHub App installation authentication via `ghinstallation`.
- go-github REST client construction with optional Enterprise base URL override.
- Request execution wrapper with retry and rate-limit-aware pause behavior.
- Typed REST endpoint client for org repos, contributor stats, commits, pull requests, pull reviews, and issue comments.
- Rate-limit header parsing and decision policy.
- LOC contributor-stats state machine for ready/warming/stale/fallback transitions.

## API reference

### Types

- `InstallationAuthConfig`: inputs for GitHub App installation HTTP client construction.
- `RESTClient`: wrapper around `*github.Client`.
- `RetryConfig`: retry backoff settings.
- `HTTPDoer`: HTTP client abstraction used by `Client`.
- `CallMetadata`: metadata from `Client.Do` (attempt count, last rate headers, last decision).
- `Client`: retry + rate-limit-aware API caller.
- `EndpointStatus`: normalized endpoint status (`ok`, `accepted`, `forbidden`, `not_found`, `conflict`, `unprocessable`, `unavailable`, `unknown`).
- `Repository`: typed org repository record.
- `OrgReposResult`: typed list-org-repos payload and metadata.
- `ContributorWeek`: typed weekly contributor record.
- `ContributorStats`: typed contributor stats record.
- `ContributorStatsResult`: typed contributor-stats payload and metadata.
- `RepoCommit`: typed commit list row.
- `CommitListResult`: typed list-commits payload and metadata.
- `CommitDetail`: typed commit detail payload and metadata.
- `PullRequest`: typed pull request row.
- `PullRequestListResult`: typed list-pull-requests payload and metadata.
- `PullReview`: typed pull review row.
- `PullReviewsResult`: typed list-pull-reviews payload and metadata.
- `IssueComment`: typed issue comment row.
- `IssueCommentsResult`: typed list-issue-comments payload and metadata.
- `DataClient`: typed endpoint client built on `Client`.
- `RateLimitHeaders`: parsed GitHub limit headers.
- `Decision`: policy decision (`Allow`, `WaitFor`, `Reason`).
- `RateLimitPolicy`: threshold and backoff settings for limit handling.
- `LOCMode`: LOC state enum (`unknown`, `warming`, `ready`, `stale`, `fallback`).
- `LOCState`: persistent state for LOC transition logic.
- `LOCEvent`: one LOC observation input.
- `LOCStateMachine`: transition policy settings.

### Functions

- `NewInstallationHTTPClient(cfg InstallationAuthConfig) (*http.Client, error)`: builds authenticated installation client using private key file.
- `NewGitHubRESTClient(httpClient *http.Client, apiBaseURL string) (*RESTClient, error)`: creates go-github client with optional base URL override.
- `NewClient(doer HTTPDoer, retry RetryConfig, ratePolicy RateLimitPolicy) *Client`: constructs retry/rate-limit aware request client.
- `NewDataClient(baseURL string, requestClient *Client) (*DataClient, error)`: builds the typed endpoint client with optional API base URL override.
- `ParseRateLimitHeaders(header http.Header, statusCode int) RateLimitHeaders`: parses limit and retry headers.

### Methods

- `(*Client) Do(req *http.Request) (*http.Response, CallMetadata, error)`: executes request with retry and rate-limit decision handling.
- `(*DataClient) ListOrgRepos(ctx context.Context, org string) (OrgReposResult, error)`: lists org repos with pagination and typed status handling.
- `(*DataClient) GetContributorStats(ctx context.Context, owner, repo string) (ContributorStatsResult, error)`: reads `/stats/contributors` with explicit `202/403/404/409/422` handling.
- `(*DataClient) ListRepoCommitsWindow(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (CommitListResult, error)`: lists commits for a window with pagination and cap support.
- `(*DataClient) GetCommit(ctx context.Context, owner, repo, sha string) (CommitDetail, error)`: gets commit detail with additions/deletions and typed status handling.
- `(*DataClient) ListRepoPullRequestsWindow(ctx context.Context, owner, repo string, since, until time.Time) (PullRequestListResult, error)`: lists pull requests and filters to the requested window.
- `(*DataClient) ListPullReviews(ctx context.Context, owner, repo string, pullNumber int, since, until time.Time) (PullReviewsResult, error)`: lists pull reviews for one PR and filters to the requested window.
- `(*DataClient) ListIssueCommentsWindow(ctx context.Context, owner, repo string, since, until time.Time) (IssueCommentsResult, error)`: lists repository issue comments and filters to the requested window.
- `(RateLimitPolicy) Evaluate(headers RateLimitHeaders) Decision`: determines allow/pause behavior from parsed headers.
- `(LOCStateMachine) Apply(previous LOCState, event LOCEvent) LOCState`: applies LOC state transition rules.
