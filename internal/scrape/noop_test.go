package scrape

import (
	"context"
	"testing"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
)

func TestNoopOrgScraperScrapeOrg(t *testing.T) {
	t.Parallel()

	scraper := &NoopOrgScraper{}
	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{Org: "org-a"})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(result.Metrics) != 0 {
		t.Fatalf("result metrics len = %d, want 0", len(result.Metrics))
	}
	if len(result.MissedWindow) != 0 {
		t.Fatalf("result missed windows len = %d, want 0", len(result.MissedWindow))
	}
}
