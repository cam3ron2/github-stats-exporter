package scrape

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/githubapi"
	"github.com/cam3ron2/github-stats/internal/store"
)

type fakeGitHubDataClient struct {
	listOrgReposFn        func(ctx context.Context, org string) (githubapi.OrgReposResult, error)
	getContributorStatsFn func(ctx context.Context, owner, repo string) (githubapi.ContributorStatsResult, error)
	listRepoCommitsFn     func(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (githubapi.CommitListResult, error)
	getCommitFn           func(ctx context.Context, owner, repo, sha string) (githubapi.CommitDetail, error)
}

func (f *fakeGitHubDataClient) ListOrgRepos(ctx context.Context, org string) (githubapi.OrgReposResult, error) {
	if f.listOrgReposFn != nil {
		return f.listOrgReposFn(ctx, org)
	}
	return githubapi.OrgReposResult{}, nil
}

func (f *fakeGitHubDataClient) GetContributorStats(ctx context.Context, owner, repo string) (githubapi.ContributorStatsResult, error) {
	if f.getContributorStatsFn != nil {
		return f.getContributorStatsFn(ctx, owner, repo)
	}
	return githubapi.ContributorStatsResult{}, nil
}

func (f *fakeGitHubDataClient) ListRepoCommitsWindow(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (githubapi.CommitListResult, error) {
	if f.listRepoCommitsFn != nil {
		return f.listRepoCommitsFn(ctx, owner, repo, since, until, maxCommits)
	}
	return githubapi.CommitListResult{}, nil
}

func (f *fakeGitHubDataClient) GetCommit(ctx context.Context, owner, repo, sha string) (githubapi.CommitDetail, error) {
	if f.getCommitFn != nil {
		return f.getCommitFn(ctx, owner, repo, sha)
	}
	return githubapi.CommitDetail{}, nil
}

func TestGitHubOrgScraperPrimaryStats(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos: []githubapi.Repository{
					{Name: "repo-a"},
				},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			return githubapi.ContributorStatsResult{
				Status: githubapi.EndpointStatusOK,
				Contributors: []githubapi.ContributorStats{
					{
						User:         "alice",
						TotalCommits: 7,
						Weeks: []githubapi.ContributorWeek{
							{
								WeekStart: now.Add(-14 * 24 * time.Hour),
								Additions: 22,
								Deletions: 5,
								Commits:   7,
							},
						},
					},
				},
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 1,
		RepoAllowlist:     []string{"*"},
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(result.MissedWindow) != 0 {
		t.Fatalf("MissedWindow len = %d, want 0", len(result.MissedWindow))
	}
	if result.Summary.ReposDiscovered != 1 || result.Summary.ReposTargeted != 1 || result.Summary.ReposProcessed != 1 {
		t.Fatalf("summary repo counts = %#v, want discovered=1 targeted=1 processed=1", result.Summary)
	}
	if result.Summary.MetricsProduced != len(result.Metrics) {
		t.Fatalf("summary metrics produced = %d, want %d", result.Summary.MetricsProduced, len(result.Metrics))
	}

	addedMetric := findMetric(result.Metrics, MetricActivityLOCAddedWeekly, "org-a", "repo-a", "alice")
	if addedMetric == nil {
		t.Fatalf("missing %s metric", MetricActivityLOCAddedWeekly)
	}
	if addedMetric.Value != 22 {
		t.Fatalf("loc added value = %v, want 22", addedMetric.Value)
	}

	removedMetric := findMetric(result.Metrics, MetricActivityLOCRemovedWeekly, "org-a", "repo-a", "alice")
	if removedMetric == nil {
		t.Fatalf("missing %s metric", MetricActivityLOCRemovedWeekly)
	}
	if removedMetric.Value != 5 {
		t.Fatalf("loc removed value = %v, want 5", removedMetric.Value)
	}

	for _, point := range result.Metrics {
		if IsProductivityMetric(point.Name) {
			if err := ValidateProductivityMetric(point); err != nil {
				t.Fatalf("ValidateProductivityMetric(%s) unexpected error: %v", point.Name, err)
			}
		}
	}
}

func TestGitHubOrgScraperContributorStatsAccepted(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  []githubapi.Repository{{Name: "repo-a"}},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			return githubapi.ContributorStatsResult{
				Status: githubapi.EndpointStatusAccepted,
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(result.Metrics) != 0 {
		t.Fatalf("metrics len = %d, want 0", len(result.Metrics))
	}
	if len(result.MissedWindow) != 0 {
		t.Fatalf("missed len = %d, want 0", len(result.MissedWindow))
	}
	if result.Summary.ReposStatsAccepted != 1 {
		t.Fatalf("ReposStatsAccepted = %d, want 1", result.Summary.ReposStatsAccepted)
	}
	if result.Summary.MetricsProduced != 0 {
		t.Fatalf("MetricsProduced = %d, want 0", result.Summary.MetricsProduced)
	}
}

func TestGitHubOrgScraperPartialFailureReturnsMetricsAndMissedWindow(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos: []githubapi.Repository{
					{Name: "repo-a"},
					{Name: "repo-b"},
				},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, repo string) (githubapi.ContributorStatsResult, error) {
			if repo == "repo-a" {
				return githubapi.ContributorStatsResult{Status: githubapi.EndpointStatusForbidden}, nil
			}
			return githubapi.ContributorStatsResult{
				Status: githubapi.EndpointStatusOK,
				Contributors: []githubapi.ContributorStats{
					{
						User: "alice",
						Weeks: []githubapi.ContributorWeek{
							{
								WeekStart: now.Add(-14 * 24 * time.Hour),
								Additions: 11,
								Deletions: 1,
								Commits:   3,
							},
						},
					},
				},
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 2,
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if findMetric(result.Metrics, MetricActivityLOCAddedWeekly, "org-a", "repo-b", "alice") == nil {
		t.Fatalf("missing repo-b productivity metric")
	}
	if len(result.MissedWindow) != 1 {
		t.Fatalf("missed len = %d, want 1", len(result.MissedWindow))
	}
	if result.MissedWindow[0].Repo != "repo-a" {
		t.Fatalf("missed repo = %q, want repo-a", result.MissedWindow[0].Repo)
	}
	if result.MissedWindow[0].Reason != repoMissReasonContributorStats {
		t.Fatalf("missed reason = %q, want %q", result.MissedWindow[0].Reason, repoMissReasonContributorStats)
	}
	if result.Summary.ReposStatsForbidden != 1 {
		t.Fatalf("ReposStatsForbidden = %d, want 1", result.Summary.ReposStatsForbidden)
	}
	if result.Summary.MissedWindows != 1 {
		t.Fatalf("MissedWindows = %d, want 1", result.Summary.MissedWindows)
	}
}

func TestGitHubOrgScraperFallbackLOC(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  []githubapi.Repository{{Name: "repo-a"}},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			return githubapi.ContributorStatsResult{
				Status: githubapi.EndpointStatusOK,
				Contributors: []githubapi.ContributorStats{
					{
						User:         "alice",
						TotalCommits: 4,
						Weeks: []githubapi.ContributorWeek{
							{
								WeekStart: now.Add(-14 * 24 * time.Hour),
								Additions: 0,
								Deletions: 0,
								Commits:   4,
							},
						},
					},
				},
			}, nil
		},
		listRepoCommitsFn: func(_ context.Context, _, _ string, _, _ time.Time, _ int) (githubapi.CommitListResult, error) {
			return githubapi.CommitListResult{
				Status: githubapi.EndpointStatusOK,
				Commits: []githubapi.RepoCommit{
					{
						SHA:         "sha-1",
						Author:      "alice",
						CommittedAt: now.Add(-2 * time.Hour),
					},
				},
			}, nil
		},
		getCommitFn: func(_ context.Context, _, _, _ string) (githubapi.CommitDetail, error) {
			return githubapi.CommitDetail{
				Status:    githubapi.EndpointStatusOK,
				SHA:       "sha-1",
				Author:    "alice",
				Additions: 7,
				Deletions: 2,
				Total:     9,
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		FallbackEnabled:                           true,
		LargeRepoZeroDetectionWindows:             1,
		FallbackMaxCommitsPerRepoPerWeek:          50,
		FallbackMaxCommitDetailCallsPerOrgPerHour: 50,
		Now: func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(result.MissedWindow) != 0 {
		t.Fatalf("missed len = %d, want 0", len(result.MissedWindow))
	}
	addedMetric := findMetric(result.Metrics, MetricActivityLOCAddedWeekly, "org-a", "repo-a", "alice")
	if addedMetric == nil || addedMetric.Value != 7 {
		t.Fatalf("fallback loc added metric = %#v, want 7", addedMetric)
	}
	removedMetric := findMetric(result.Metrics, MetricActivityLOCRemovedWeekly, "org-a", "repo-a", "alice")
	if removedMetric == nil || removedMetric.Value != 2 {
		t.Fatalf("fallback loc removed metric = %#v, want 2", removedMetric)
	}
	if result.Summary.ReposFallbackUsed != 1 {
		t.Fatalf("ReposFallbackUsed = %d, want 1", result.Summary.ReposFallbackUsed)
	}
	if result.Summary.ReposFallbackTruncated != 0 {
		t.Fatalf("ReposFallbackTruncated = %d, want 0", result.Summary.ReposFallbackTruncated)
	}
}

func TestGitHubOrgScraperFallbackBudgetCapsCommitDetails(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  []githubapi.Repository{{Name: "repo-a"}},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			return githubapi.ContributorStatsResult{
				Status: githubapi.EndpointStatusOK,
				Contributors: []githubapi.ContributorStats{
					{
						User:         "alice",
						TotalCommits: 4,
						Weeks: []githubapi.ContributorWeek{
							{
								WeekStart: now.Add(-14 * 24 * time.Hour),
								Additions: 0,
								Deletions: 0,
								Commits:   4,
							},
						},
					},
				},
			}, nil
		},
		listRepoCommitsFn: func(_ context.Context, _, _ string, _, _ time.Time, _ int) (githubapi.CommitListResult, error) {
			return githubapi.CommitListResult{
				Status: githubapi.EndpointStatusOK,
				Commits: []githubapi.RepoCommit{
					{SHA: "sha-1", Author: "alice", CommittedAt: now.Add(-2 * time.Hour)},
					{SHA: "sha-2", Author: "alice", CommittedAt: now.Add(-3 * time.Hour)},
				},
			}, nil
		},
		getCommitFn: func(_ context.Context, _, _, sha string) (githubapi.CommitDetail, error) {
			if sha == "sha-1" {
				return githubapi.CommitDetail{
					Status:    githubapi.EndpointStatusOK,
					Author:    "alice",
					Additions: 6,
					Deletions: 1,
				}, nil
			}
			return githubapi.CommitDetail{
				Status:    githubapi.EndpointStatusOK,
				Author:    "alice",
				Additions: 9,
				Deletions: 3,
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		FallbackEnabled:                           true,
		LargeRepoZeroDetectionWindows:             1,
		FallbackMaxCommitDetailCallsPerOrgPerHour: 1,
		FallbackMaxCommitsPerRepoPerWeek:          50,
		Now:                                       func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	addedMetric := findMetric(result.Metrics, MetricActivityLOCAddedWeekly, "org-a", "repo-a", "alice")
	if addedMetric == nil || addedMetric.Value != 6 {
		t.Fatalf("fallback capped loc added metric = %#v, want 6", addedMetric)
	}
	if len(result.MissedWindow) != 1 {
		t.Fatalf("missed len = %d, want 1", len(result.MissedWindow))
	}
	if result.MissedWindow[0].Reason != "fallback_truncated" {
		t.Fatalf("missed reason = %q, want fallback_truncated", result.MissedWindow[0].Reason)
	}
	if result.Summary.ReposFallbackUsed != 1 {
		t.Fatalf("ReposFallbackUsed = %d, want 1", result.Summary.ReposFallbackUsed)
	}
	if result.Summary.ReposFallbackTruncated != 1 {
		t.Fatalf("ReposFallbackTruncated = %d, want 1", result.Summary.ReposFallbackTruncated)
	}
}

func TestGitHubOrgScraperPerOrgConcurrency(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32

	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos: []githubapi.Repository{
					{Name: "repo-a"},
					{Name: "repo-b"},
				},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			current := inFlight.Add(1)
			for {
				existing := maxInFlight.Load()
				if current <= existing || maxInFlight.CompareAndSwap(existing, current) {
					break
				}
			}
			time.Sleep(120 * time.Millisecond)
			inFlight.Add(-1)
			return githubapi.ContributorStatsResult{
				Status: githubapi.EndpointStatusAccepted,
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
	})

	start := time.Now()
	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 2,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if maxInFlight.Load() < 2 {
		t.Fatalf("max concurrent stats calls = %d, want at least 2", maxInFlight.Load())
	}
	if elapsed > 220*time.Millisecond {
		t.Fatalf("elapsed = %s, expected parallel execution", elapsed)
	}
	if result.Summary.ReposStatsAccepted != 2 {
		t.Fatalf("ReposStatsAccepted = %d, want 2", result.Summary.ReposStatsAccepted)
	}
}

func TestGitHubOrgScraperRateLimitAwarePacing(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	sleepCalls := make([]time.Duration, 0, 1)
	var mu sync.Mutex

	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
				Metadata: githubapi.CallMetadata{
					LastDecision: githubapi.Decision{
						Allow:   false,
						WaitFor: 2 * time.Second,
						Reason:  "remaining_below_threshold",
					},
				},
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
		Sleep: func(delay time.Duration) {
			mu.Lock()
			sleepCalls = append(sleepCalls, delay)
			mu.Unlock()
		},
	})

	_, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{Org: "org-a"})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sleepCalls) != 1 {
		t.Fatalf("sleep call count = %d, want 1", len(sleepCalls))
	}
	if sleepCalls[0] != 2*time.Second {
		t.Fatalf("sleep delay = %s, want %s", sleepCalls[0], 2*time.Second)
	}
}

func findMetric(points []store.MetricPoint, name, org, repo, user string) *store.MetricPoint {
	for i := range points {
		point := &points[i]
		if point.Name != name {
			continue
		}
		if point.Labels[LabelOrg] != org {
			continue
		}
		if point.Labels[LabelRepo] != repo {
			continue
		}
		if point.Labels[LabelUser] != user {
			continue
		}
		return point
	}
	return nil
}
