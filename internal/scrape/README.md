# internal/scrape

## Logic overview

The `scrape` package coordinates per-organization scrape execution and defines the productivity metric contract.

- `Manager` runs one scrape per configured org in parallel goroutines.
- `Outcome` captures result+error per org so failures remain isolated.
- `GitHubOrgScraper` performs repo iteration per org with bounded worker concurrency.
- 24h activity metrics are scraped from commits, pull requests, pull reviews, and issue comments endpoints.
- LOC collection uses `/stats/contributors` as primary and commit-detail fallback when large-repo zeroing is detected.
- Optional checkpoint persistence extends scrape windows after outages and advances checkpoints on successful repo windows.
- Partial repo failures are returned as missed windows so runtime can enqueue targeted backfill.
- `NoopOrgScraper` is a safe default implementation used during bootstrap/testing.

## API reference

### Types

- `OrgResult`: scrape output for one organization (`Metrics` plus `MissedWindow` for partial failures).
- `MissedWindow`: failed repo window metadata (`org`, `repo`, `window_start`, `window_end`, `reason`) used for backfill enqueue.
- `Outcome`: per-org wrapper with org name, result, and error.
- `OrgScraper`: org scrape interface.
- `CheckpointStore`: checkpoint persistence contract (`SetCheckpoint`, `GetCheckpoint`).
- `CheckpointAwareScraper`: optional interface for runtime checkpoint-store injection.
- `Manager`: parallel org scrape coordinator.
- `GitHubDataClient`: typed GitHub endpoint interface used by `GitHubOrgScraper`.
- `GitHubOrgScraperConfig`: behavior config for LOC fallback, budgets, and time hooks.
- `GitHubOrgScraper`: production org scraper backed by per-org GitHub App clients.
- `NoopOrgScraper`: no-op implementation of `OrgScraper`.
- `LabelOrg`, `LabelRepo`, `LabelUser`: required productivity label keys.
- `MetricActivity*`: stable productivity metric name constants.

### Functions

- `NewManager(scraper OrgScraper, orgs []config.GitHubOrgConfig) *Manager`: constructs a manager.
- `NewGitHubOrgScraper(clients map[string]GitHubDataClient, cfg GitHubOrgScraperConfig) *GitHubOrgScraper`: constructs the production org scraper.
- `NewOrgScraperFromConfig(cfg *config.Config) (OrgScraper, error)`: builds per-org GitHub App clients from config and returns an org scraper.
- `ProductivityMetricNames() []string`: returns stable supported productivity metric names.
- `IsProductivityMetric(name string) bool`: validates metric names against the contract.
- `RequiredLabels(org, repo, user string) map[string]string`: enforces required labels.
- `NewProductivityMetric(name, org, repo, user string, value float64, updatedAt time.Time) (store.MetricPoint, error)`: creates validated productivity metric points.
- `ValidateProductivityMetric(point store.MetricPoint) error`: validates point contract compliance.

### Methods

- `(*Manager) ScrapeAll(ctx context.Context) []Outcome`: executes one parallel scrape pass for all configured orgs.
- `(*GitHubOrgScraper) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error)`: scrapes one org, emits metrics, and reports missed windows for partial failures.
- `(*GitHubOrgScraper) SetCheckpointStore(checkpoints CheckpointStore)`: injects checkpoint persistence after scraper construction.
- `(*NoopOrgScraper) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error)`: returns empty output with no error.
