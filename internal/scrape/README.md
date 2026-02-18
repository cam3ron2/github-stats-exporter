# internal/scrape

## Logic overview

The `scrape` package coordinates per-organization scrape execution.

- `Manager` runs one scrape per configured org in parallel goroutines.
- `Outcome` captures result+error per org so failures remain isolated.
- `NoopOrgScraper` is a safe default implementation used during bootstrap/testing.

## API reference

### Types

- `OrgResult`: scrape output for one organization (`Metrics []store.MetricPoint`).
- `Outcome`: per-org wrapper with org name, result, and error.
- `OrgScraper`: org scrape interface.
- `Manager`: parallel org scrape coordinator.
- `NoopOrgScraper`: no-op implementation of `OrgScraper`.

### Functions

- `NewManager(scraper OrgScraper, orgs []config.GitHubOrgConfig) *Manager`: constructs a manager.

### Methods

- `(*Manager) ScrapeAll(ctx context.Context) []Outcome`: executes one parallel scrape pass for all configured orgs.
- `(*NoopOrgScraper) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error)`: returns empty output with no error.
