package scrape

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/store"
)

type fakeOrgScraper struct {
	mu      sync.Mutex
	delays  map[string]time.Duration
	results map[string]OrgResult
	errors  map[string]error
	calls   []string
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
