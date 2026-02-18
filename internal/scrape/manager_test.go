package scrape

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

type fakeOrgScraper struct {
	mu      sync.Mutex
	delays  map[string]time.Duration
	results map[string]OrgResult
	errors  map[string]error
	calls   []string
}

type fakeBackfillOrgScraper struct {
	result         OrgResult
	err            error
	backfillResult OrgResult
	backfillErr    error
	backfillCalled bool
}

func (s *fakeOrgScraper) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error) {
	s.mu.Lock()
	s.calls = append(s.calls, org.Org)
	delay := s.delays[org.Org]
	result := s.results[org.Org]
	err := s.errors[org.Org]
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return OrgResult{}, ctx.Err()
	case <-time.After(delay):
	}
	return result, err
}

func (s *fakeOrgScraper) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]string, len(s.calls))
	copy(copied, s.calls)
	return copied
}

func (s *fakeBackfillOrgScraper) ScrapeOrg(_ context.Context, _ config.GitHubOrgConfig) (OrgResult, error) {
	return s.result, s.err
}

func (s *fakeBackfillOrgScraper) ScrapeBackfill(
	_ context.Context,
	_ config.GitHubOrgConfig,
	_ string,
	_ time.Time,
	_ time.Time,
	_ string,
) (OrgResult, error) {
	s.backfillCalled = true
	return s.backfillResult, s.backfillErr
}

func TestManagerScrapeAllParallel(t *testing.T) {
	t.Parallel()

	orgs := []config.GitHubOrgConfig{
		{Org: "org-a"},
		{Org: "org-b"},
	}

	scraper := &fakeOrgScraper{
		delays: map[string]time.Duration{
			"org-a": 120 * time.Millisecond,
			"org-b": 120 * time.Millisecond,
		},
		results: map[string]OrgResult{
			"org-a": {
				Metrics: []store.MetricPoint{
					{Name: "gh_activity_commits_24h", Labels: map[string]string{"org": "org-a", "repo": "repo-1", "user": "alice"}, Value: 5},
				},
			},
			"org-b": {
				Metrics: []store.MetricPoint{
					{Name: "gh_activity_commits_24h", Labels: map[string]string{"org": "org-b", "repo": "repo-2", "user": "bob"}, Value: 3},
				},
			},
		},
		errors: map[string]error{},
	}

	manager := NewManager(scraper, orgs)
	start := time.Now()
	outcomes := manager.ScrapeAll(context.Background())
	elapsed := time.Since(start)

	if len(outcomes) != 2 {
		t.Fatalf("ScrapeAll() outcomes len = %d, want 2", len(outcomes))
	}
	if elapsed > 210*time.Millisecond {
		t.Fatalf("ScrapeAll() took %s, expected concurrent execution", elapsed)
	}
}

func TestManagerScrapeAllCollectsErrorsPerOrg(t *testing.T) {
	t.Parallel()

	orgs := []config.GitHubOrgConfig{
		{Org: "org-a"},
		{Org: "org-b"},
	}

	scraper := &fakeOrgScraper{
		delays: map[string]time.Duration{
			"org-a": 1 * time.Millisecond,
			"org-b": 1 * time.Millisecond,
		},
		results: map[string]OrgResult{
			"org-a": {
				Metrics: []store.MetricPoint{{Name: "gh_activity_commits_24h"}},
			},
		},
		errors: map[string]error{
			"org-b": errors.New("github unavailable"),
		},
	}

	manager := NewManager(scraper, orgs)
	outcomes := manager.ScrapeAll(context.Background())

	testCases := []struct {
		org       string
		wantErr   bool
		errSubstr string
	}{
		{org: "org-a", wantErr: false, errSubstr: ""},
		{org: "org-b", wantErr: true, errSubstr: "github unavailable"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.org, func(t *testing.T) {
			t.Parallel()

			var matched *Outcome
			for i := range outcomes {
				if outcomes[i].Org == tc.org {
					matched = &outcomes[i]
					break
				}
			}
			if matched == nil {
				t.Fatalf("outcome for org %q not found", tc.org)
			}
			if tc.wantErr && matched.Err == nil {
				t.Fatalf("expected error for org %q, got nil", tc.org)
			}
			if !tc.wantErr && matched.Err != nil {
				t.Fatalf("unexpected error for org %q: %v", tc.org, matched.Err)
			}
			if tc.wantErr && !strings.Contains(matched.Err.Error(), tc.errSubstr) {
				t.Fatalf("error = %q, missing %q", matched.Err.Error(), tc.errSubstr)
			}
		})
	}
}

func TestManagerScrapeBackfillUsesBackfillScraper(t *testing.T) {
	t.Parallel()

	orgs := []config.GitHubOrgConfig{{Org: "org-a"}}
	scraper := &fakeBackfillOrgScraper{
		backfillResult: OrgResult{
			Metrics: []store.MetricPoint{
				{
					Name:   "gh_activity_commits_24h",
					Labels: map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
					Value:  5,
				},
			},
		},
	}

	manager := NewManager(scraper, orgs)
	got, err := manager.ScrapeBackfill(
		context.Background(),
		"org-a",
		"repo-a",
		time.Unix(1739836800, 0),
		time.Unix(1739836800, 0).Add(time.Hour),
		"scrape_error",
	)
	if err != nil {
		t.Fatalf("ScrapeBackfill() unexpected error: %v", err)
	}
	if !scraper.backfillCalled {
		t.Fatalf("ScrapeBackfill() did not invoke backfill scraper")
	}
	if len(got.Metrics) != 1 {
		t.Fatalf("ScrapeBackfill() metrics len = %d, want 1", len(got.Metrics))
	}
}

func TestManagerScrapeBackfillFallbackFiltersRepo(t *testing.T) {
	t.Parallel()

	orgs := []config.GitHubOrgConfig{{Org: "org-a"}}
	scraper := &fakeOrgScraper{
		results: map[string]OrgResult{
			"org-a": {
				Metrics: []store.MetricPoint{
					{
						Name:   "gh_activity_commits_24h",
						Labels: map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
						Value:  5,
					},
					{
						Name:   "gh_activity_commits_24h",
						Labels: map[string]string{"org": "org-a", "repo": "repo-b", "user": "bob"},
						Value:  3,
					},
				},
			},
		},
		errors: map[string]error{},
	}

	manager := NewManager(scraper, orgs)
	got, err := manager.ScrapeBackfill(
		context.Background(),
		"org-a",
		"repo-b",
		time.Unix(1739836800, 0),
		time.Unix(1739836800, 0).Add(time.Hour),
		"scrape_error",
	)
	if err != nil {
		t.Fatalf("ScrapeBackfill() unexpected error: %v", err)
	}
	if len(got.Metrics) != 1 {
		t.Fatalf("ScrapeBackfill() metrics len = %d, want 1", len(got.Metrics))
	}
	if got.Metrics[0].Labels["repo"] != "repo-b" {
		t.Fatalf("ScrapeBackfill() repo = %q, want repo-b", got.Metrics[0].Labels["repo"])
	}
}

func TestManagerScrapeBackfillUnknownOrg(t *testing.T) {
	t.Parallel()

	manager := NewManager(&fakeOrgScraper{}, []config.GitHubOrgConfig{{Org: "org-a"}})
	_, err := manager.ScrapeBackfill(
		context.Background(),
		"org-b",
		"repo-a",
		time.Unix(1739836800, 0),
		time.Unix(1739836800, 0).Add(time.Hour),
		"scrape_error",
	)
	if err == nil {
		t.Fatalf("ScrapeBackfill() expected error for unknown org, got nil")
	}
	if !strings.Contains(err.Error(), "organization \"org-b\" is not configured") {
		t.Fatalf("ScrapeBackfill() error = %q, want unknown org error", err.Error())
	}
}
