package scrape

import (
	"context"
	"sync"
	"time"

	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/store"
)

// OrgResult is the scrape output for one organization.
type OrgResult struct {
	Metrics      []store.MetricPoint
	MissedWindow []MissedWindow
	Summary      OrgSummary
}

// MissedWindow describes a failed scrape window that can be backfilled later.
type MissedWindow struct {
	Org         string
	Repo        string
	WindowStart time.Time
	WindowEnd   time.Time
	Reason      string
}

// OrgSummary provides per-organization scrape debug counters.
type OrgSummary struct {
	ReposDiscovered         int
	ReposTargeted           int
	ReposProcessed          int
	ReposStatsAccepted      int
	ReposStatsForbidden     int
	ReposStatsNotFound      int
	ReposStatsConflict      int
	ReposStatsUnprocessable int
	ReposStatsUnavailable   int
	ReposNoCompleteWeek     int
	ReposFallbackUsed       int
	ReposFallbackTruncated  int
	MissedWindows           int
	MetricsProduced         int
}

// Outcome contains scrape results and errors for one organization.
type Outcome struct {
	Org    string
	Result OrgResult
	Err    error
}

// OrgScraper scrapes one organization.
type OrgScraper interface {
	ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error)
}

// Manager executes parallel organization scraping.
type Manager struct {
	scraper OrgScraper
	orgs    []config.GitHubOrgConfig
}

// NewManager creates a scrape manager.
func NewManager(scraper OrgScraper, orgs []config.GitHubOrgConfig) *Manager {
	return &Manager{
		scraper: scraper,
		orgs:    orgs,
	}
}

// ScrapeAll performs one parallel scrape pass across configured organizations.
func (m *Manager) ScrapeAll(ctx context.Context) []Outcome {
	if m == nil || m.scraper == nil || len(m.orgs) == 0 {
		return nil
	}

	outcomes := make([]Outcome, len(m.orgs))
	var wg sync.WaitGroup
	for i, org := range m.orgs {
		i := i
		org := org
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := m.scraper.ScrapeOrg(ctx, org)
			outcomes[i] = Outcome{
				Org:    org.Org,
				Result: result,
				Err:    err,
			}
		}()
	}
	wg.Wait()
	return outcomes
}
