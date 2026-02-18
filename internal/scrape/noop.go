package scrape

import (
	"context"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
)

// NoopOrgScraper is a placeholder scraper implementation for initial bootstrapping.
type NoopOrgScraper struct{}

// ScrapeOrg returns an empty result without error.
func (s *NoopOrgScraper) ScrapeOrg(_ context.Context, _ config.GitHubOrgConfig) (OrgResult, error) {
	return OrgResult{}, nil
}
