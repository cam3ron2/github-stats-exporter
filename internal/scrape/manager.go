package scrape

import (
	"context"
	"fmt"
	"strings"
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
	RateLimitMinRemaining   int
	RateLimitResetUnix      int64
	SecondaryLimitHits      int
	GitHubRequestTotals     map[string]int
	LOCFallbackBudgetHits   int
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

// CheckpointStore persists per-org/repo scrape progress.
type CheckpointStore interface {
	SetCheckpoint(org, repo string, checkpoint time.Time) error
	GetCheckpoint(org, repo string) (time.Time, bool, error)
}

// CheckpointAwareScraper allows runtime to inject checkpoint persistence.
type CheckpointAwareScraper interface {
	SetCheckpointStore(checkpoints CheckpointStore)
}

// BackfillScraper provides backfill-window specific scraping behavior.
type BackfillScraper interface {
	ScrapeBackfill(
		ctx context.Context,
		org config.GitHubOrgConfig,
		repo string,
		windowStart time.Time,
		windowEnd time.Time,
		reason string,
	) (OrgResult, error)
}

// Manager executes organization scraping.
type Manager struct {
	scraper  OrgScraper
	orgs     []config.GitHubOrgConfig
	orgIndex map[string]config.GitHubOrgConfig
}

// NewManager creates a scrape manager.
func NewManager(scraper OrgScraper, orgs []config.GitHubOrgConfig) *Manager {
	orgIndex := make(map[string]config.GitHubOrgConfig, len(orgs))
	copiedOrgs := make([]config.GitHubOrgConfig, 0, len(orgs))
	for _, org := range orgs {
		copiedOrgs = append(copiedOrgs, org)
		orgIndex[strings.TrimSpace(org.Org)] = org
	}

	return &Manager{
		scraper:  scraper,
		orgs:     copiedOrgs,
		orgIndex: orgIndex,
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
			outcomes[i] = m.ScrapeOrg(ctx, org)
		}()
	}
	wg.Wait()
	return outcomes
}

// ScrapeOrg performs one scrape pass for a single configured organization.
func (m *Manager) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) Outcome {
	if m == nil || m.scraper == nil {
		return Outcome{
			Org: org.Org,
			Err: fmt.Errorf("scrape manager is not initialized"),
		}
	}

	result, err := m.scraper.ScrapeOrg(ctx, org)
	return Outcome{
		Org:    org.Org,
		Result: result,
		Err:    err,
	}
}

// ScrapeBackfill re-scrapes one missed org/repo window.
func (m *Manager) ScrapeBackfill(
	ctx context.Context,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
	reason string,
) (OrgResult, error) {
	if m == nil || m.scraper == nil {
		return OrgResult{}, fmt.Errorf("scrape manager is not initialized")
	}

	orgCfg, ok := m.orgIndex[strings.TrimSpace(org)]
	if !ok {
		return OrgResult{}, fmt.Errorf("organization %q is not configured", org)
	}

	if backfillScraper, hasBackfill := m.scraper.(BackfillScraper); hasBackfill {
		return backfillScraper.ScrapeBackfill(ctx, orgCfg, repo, windowStart, windowEnd, reason)
	}

	// Fallback for non-window-aware implementations.
	result, err := m.scraper.ScrapeOrg(ctx, orgCfg)
	if err != nil {
		return OrgResult{}, err
	}
	if strings.TrimSpace(repo) == "" || repo == "*" {
		return result, nil
	}

	filtered := OrgResult{
		MissedWindow: result.MissedWindow,
		Summary:      result.Summary,
	}
	for _, point := range result.Metrics {
		if point.Labels[LabelRepo] == repo {
			filtered.Metrics = append(filtered.Metrics, point)
		}
	}
	filtered.Summary.MetricsProduced = len(filtered.Metrics)
	return filtered, nil
}
