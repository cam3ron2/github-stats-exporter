# internal/githubapi

## Logic overview

The `githubapi` package contains GitHub API integration primitives:

- GitHub App installation authentication via `ghinstallation`.
- go-github REST client construction with optional Enterprise base URL override.
- Request execution wrapper with retry and rate-limit-aware pause behavior.
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
- `ParseRateLimitHeaders(header http.Header, statusCode int) RateLimitHeaders`: parses limit and retry headers.

### Methods

- `(*Client) Do(req *http.Request) (*http.Response, CallMetadata, error)`: executes request with retry and rate-limit decision handling.
- `(RateLimitPolicy) Evaluate(headers RateLimitHeaders) Decision`: determines allow/pause behavior from parsed headers.
- `(LOCStateMachine) Apply(previous LOCState, event LOCEvent) LOCState`: applies LOC state transition rules.
