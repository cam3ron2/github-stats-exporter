package scrape

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

type fakeCheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]time.Time
}

func (s *fakeCheckpointStore) SetCheckpoint(org, repo string, checkpoint time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkpoints == nil {
		s.checkpoints = make(map[string]time.Time)
	}
	s.checkpoints[org+"/"+repo] = checkpoint.UTC()
	return nil
}

func (s *fakeCheckpointStore) GetCheckpoint(org, repo string) (time.Time, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	checkpoint, found := s.checkpoints[org+"/"+repo]
	return checkpoint, found, nil
}

type fakeGitHubDataClient struct {
	listOrgReposFn        func(ctx context.Context, org string) (githubapi.OrgReposResult, error)
	getContributorStatsFn func(ctx context.Context, owner, repo string) (githubapi.ContributorStatsResult, error)
	listRepoCommitsFn     func(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (githubapi.CommitListResult, error)
	listRepoPullsFn       func(ctx context.Context, owner, repo string, since, until time.Time) (githubapi.PullRequestListResult, error)
	listPullReviewsFn     func(ctx context.Context, owner, repo string, pullNumber int, since, until time.Time) (githubapi.PullReviewsResult, error)
	listIssueCommentsFn   func(ctx context.Context, owner, repo string, since, until time.Time) (githubapi.IssueCommentsResult, error)
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
	return githubapi.CommitListResult{Status: githubapi.EndpointStatusOK}, nil
}

func (f *fakeGitHubDataClient) ListRepoPullRequestsWindow(ctx context.Context, owner, repo string, since, until time.Time) (githubapi.PullRequestListResult, error) {
	if f.listRepoPullsFn != nil {
		return f.listRepoPullsFn(ctx, owner, repo, since, until)
	}
	return githubapi.PullRequestListResult{Status: githubapi.EndpointStatusOK}, nil
}

func (f *fakeGitHubDataClient) ListPullReviews(ctx context.Context, owner, repo string, pullNumber int, since, until time.Time) (githubapi.PullReviewsResult, error) {
	if f.listPullReviewsFn != nil {
		return f.listPullReviewsFn(ctx, owner, repo, pullNumber, since, until)
	}
	return githubapi.PullReviewsResult{Status: githubapi.EndpointStatusOK}, nil
}

func (f *fakeGitHubDataClient) ListIssueCommentsWindow(ctx context.Context, owner, repo string, since, until time.Time) (githubapi.IssueCommentsResult, error) {
	if f.listIssueCommentsFn != nil {
		return f.listIssueCommentsFn(ctx, owner, repo, since, until)
	}
	return githubapi.IssueCommentsResult{Status: githubapi.EndpointStatusOK}, nil
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

func TestGitHubOrgScraperPrimaryLOCSourceMetrics(t *testing.T) {
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
						TotalCommits: 2,
						Weeks: []githubapi.ContributorWeek{
							{
								WeekStart: now.Add(-14 * 24 * time.Hour),
								Additions: 11,
								Deletions: 3,
								Commits:   2,
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
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}

	testCases := []struct {
		name      string
		metric    string
		labels    map[string]string
		wantValue float64
	}{
		{
			name:   "stats_mode_active",
			metric: internalMetricLOCSourceMode,
			labels: map[string]string{
				LabelOrg:  "org-a",
				LabelRepo: "repo-a",
				"mode":    locModeStatsContributors,
			},
			wantValue: 1,
		},
		{
			name:   "fallback_mode_inactive",
			metric: internalMetricLOCSourceMode,
			labels: map[string]string{
				LabelOrg:  "org-a",
				LabelRepo: "repo-a",
				"mode":    locModeSampledCommitStats,
			},
			wantValue: 0,
		},
		{
			name:      "fallback_incomplete_is_zero",
			metric:    internalMetricLOCFallbackIncom,
			labels:    map[string]string{LabelOrg: "org-a", LabelRepo: "repo-a"},
			wantValue: 0,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metric := findMetricWithLabels(result.Metrics, tc.metric, tc.labels)
			if metric == nil {
				t.Fatalf("missing metric %q labels=%v", tc.metric, tc.labels)
			}
			if metric.Value != tc.wantValue {
				t.Fatalf("%s value = %v, want %v", tc.metric, metric.Value, tc.wantValue)
			}
		})
	}
}

func TestGitHubOrgScraperUsesCheckpointForExtendedWindowAndAdvances(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	checkpoint := now.Add(-72 * time.Hour)
	checkpoints := &fakeCheckpointStore{
		checkpoints: map[string]time.Time{
			"org-a/repo-a": checkpoint,
		},
	}

	seenCommitWindowStart := time.Time{}
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  []githubapi.Repository{{Name: "repo-a"}},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			return githubapi.ContributorStatsResult{Status: githubapi.EndpointStatusAccepted}, nil
		},
		listRepoCommitsFn: func(
			_ context.Context,
			_, _ string,
			since, _ time.Time,
			_ int,
		) (githubapi.CommitListResult, error) {
			seenCommitWindowStart = since
			return githubapi.CommitListResult{Status: githubapi.EndpointStatusOK}, nil
		},
		listRepoPullsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.PullRequestListResult, error) {
			return githubapi.PullRequestListResult{Status: githubapi.EndpointStatusOK}, nil
		},
		listIssueCommentsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.IssueCommentsResult, error) {
			return githubapi.IssueCommentsResult{Status: githubapi.EndpointStatusOK}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Checkpoints: checkpoints,
		Now:         func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(result.MissedWindow) != 0 {
		t.Fatalf("MissedWindow len = %d, want 0", len(result.MissedWindow))
	}
	if !seenCommitWindowStart.Equal(checkpoint) {
		t.Fatalf("commits since = %s, want %s", seenCommitWindowStart, checkpoint)
	}

	advanced, found, err := checkpoints.GetCheckpoint("org-a", "repo-a")
	if err != nil {
		t.Fatalf("GetCheckpoint() unexpected error: %v", err)
	}
	if !found {
		t.Fatalf("GetCheckpoint() found = false, want true")
	}
	if !advanced.Equal(now) {
		t.Fatalf("checkpoint after scrape = %s, want %s", advanced, now)
	}
}

func TestGitHubOrgScraperCheckpointNotAdvancedOnMissedWindow(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	checkpoint := now.Add(-72 * time.Hour)
	checkpoints := &fakeCheckpointStore{
		checkpoints: map[string]time.Time{
			"org-a/repo-a": checkpoint,
		},
	}

	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  []githubapi.Repository{{Name: "repo-a"}},
			}, nil
		},
		getContributorStatsFn: func(_ context.Context, _, _ string) (githubapi.ContributorStatsResult, error) {
			return githubapi.ContributorStatsResult{Status: githubapi.EndpointStatusAccepted}, nil
		},
		listRepoCommitsFn: func(_ context.Context, _, _ string, _, _ time.Time, _ int) (githubapi.CommitListResult, error) {
			return githubapi.CommitListResult{}, context.DeadlineExceeded
		},
		listRepoPullsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.PullRequestListResult, error) {
			return githubapi.PullRequestListResult{Status: githubapi.EndpointStatusOK}, nil
		},
		listIssueCommentsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.IssueCommentsResult, error) {
			return githubapi.IssueCommentsResult{Status: githubapi.EndpointStatusOK}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Checkpoints: checkpoints,
		Now:         func() time.Time { return now },
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org:               "org-a",
		PerOrgConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(result.MissedWindow) == 0 {
		t.Fatalf("MissedWindow len = 0, want > 0")
	}

	foundCommitMiss := false
	for _, missed := range result.MissedWindow {
		if missed.Reason != repoMissReasonActivityCommits {
			continue
		}
		foundCommitMiss = true
		if !missed.WindowStart.Equal(checkpoint) {
			t.Fatalf("missed.WindowStart = %s, want %s", missed.WindowStart, checkpoint)
		}
	}
	if !foundCommitMiss {
		t.Fatalf("missing %q missed window", repoMissReasonActivityCommits)
	}

	preserved, found, err := checkpoints.GetCheckpoint("org-a", "repo-a")
	if err != nil {
		t.Fatalf("GetCheckpoint() unexpected error: %v", err)
	}
	if !found {
		t.Fatalf("GetCheckpoint() found = false, want true")
	}
	if !preserved.Equal(checkpoint) {
		t.Fatalf("checkpoint after missed window = %s, want %s", preserved, checkpoint)
	}
}

func TestGitHubOrgScraperSummarizesGitHubRequests(t *testing.T) {
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
						User: "alice",
						Weeks: []githubapi.ContributorWeek{
							{
								WeekStart: now.Add(-14 * 24 * time.Hour),
								Additions: 1,
								Deletions: 1,
								Commits:   1,
							},
						},
					},
				},
			}, nil
		},
		listRepoCommitsFn: func(_ context.Context, _, _ string, _, _ time.Time, _ int) (githubapi.CommitListResult, error) {
			return githubapi.CommitListResult{Status: githubapi.EndpointStatusOK}, nil
		},
		listRepoPullsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.PullRequestListResult, error) {
			return githubapi.PullRequestListResult{Status: githubapi.EndpointStatusOK}, nil
		},
		listIssueCommentsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.IssueCommentsResult, error) {
			return githubapi.IssueCommentsResult{Status: githubapi.EndpointStatusOK}, nil
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

	testCases := []struct {
		name string
		key  string
		want int
	}{
		{name: "list_org_repos", key: "list_org_repos|2xx", want: 1},
		{name: "get_contributor_stats", key: "get_contributor_stats|2xx", want: 1},
		{name: "list_repo_commits_window", key: "list_repo_commits_window|2xx", want: 1},
		{name: "list_repo_pull_requests_window", key: "list_repo_pull_requests_window|2xx", want: 1},
		{name: "list_issue_comments", key: "list_issue_comments|2xx", want: 1},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := result.Summary.GitHubRequestTotals[tc.key]
			if got != tc.want {
				t.Fatalf("GitHubRequestTotals[%q] = %d, want %d", tc.key, got, tc.want)
			}
		})
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
	if result.Summary.LOCFallbackBudgetHits != 1 {
		t.Fatalf("LOCFallbackBudgetHits = %d, want 1", result.Summary.LOCFallbackBudgetHits)
	}

	testCases := []struct {
		name      string
		metric    string
		labels    map[string]string
		wantValue float64
	}{
		{
			name:   "stats_mode_disabled",
			metric: internalMetricLOCSourceMode,
			labels: map[string]string{
				LabelOrg:  "org-a",
				LabelRepo: "repo-a",
				"mode":    locModeStatsContributors,
			},
			wantValue: 0,
		},
		{
			name:   "fallback_mode_enabled",
			metric: internalMetricLOCSourceMode,
			labels: map[string]string{
				LabelOrg:  "org-a",
				LabelRepo: "repo-a",
				"mode":    locModeSampledCommitStats,
			},
			wantValue: 1,
		},
		{
			name:      "fallback_incomplete_is_one",
			metric:    internalMetricLOCFallbackIncom,
			labels:    map[string]string{LabelOrg: "org-a", LabelRepo: "repo-a"},
			wantValue: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metric := findMetricWithLabels(result.Metrics, tc.metric, tc.labels)
			if metric == nil {
				t.Fatalf("missing metric %q labels=%v", tc.metric, tc.labels)
			}
			if metric.Value != tc.wantValue {
				t.Fatalf("%s value = %v, want %v", tc.metric, metric.Value, tc.wantValue)
			}
		})
	}
}

func TestGitHubOrgScraperActivityMetrics(t *testing.T) {
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
		listRepoCommitsFn: func(_ context.Context, _, _ string, _, _ time.Time, _ int) (githubapi.CommitListResult, error) {
			return githubapi.CommitListResult{
				Status: githubapi.EndpointStatusOK,
				Commits: []githubapi.RepoCommit{
					{SHA: "sha-1", Author: "alice", CommittedAt: now.Add(-2 * time.Hour)},
					{SHA: "sha-2", Author: "alice", CommittedAt: now.Add(-3 * time.Hour)},
					{SHA: "sha-3", Author: "bob", CommittedAt: now.Add(-4 * time.Hour)},
					{SHA: "sha-4", Committer: "github-actions[bot]", CommittedAt: now.Add(-5 * time.Hour)},
					{
						SHA:         "sha-5",
						AuthorEmail: "12345+janedoe@users.noreply.github.com",
						CommittedAt: now.Add(-6 * time.Hour),
					},
					{
						SHA:         "sha-6",
						AuthorName:  "Some Git User",
						CommittedAt: now.Add(-7 * time.Hour),
					},
					{
						SHA:         "sha-7",
						AuthorEmail: "platform-engineering@nielseniq.com",
						CommittedAt: now.Add(-8 * time.Hour),
					},
				},
			}, nil
		},
		listRepoPullsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.PullRequestListResult, error) {
			return githubapi.PullRequestListResult{
				Status: githubapi.EndpointStatusOK,
				PullRequests: []githubapi.PullRequest{
					{Number: 101, User: "alice", CreatedAt: now.Add(-2 * time.Hour)},
					{Number: 102, User: "bob", MergedAt: now.Add(-90 * time.Minute)},
				},
			}, nil
		},
		listPullReviewsFn: func(_ context.Context, _, _ string, pullNumber int, _, _ time.Time) (githubapi.PullReviewsResult, error) {
			if pullNumber == 101 {
				return githubapi.PullReviewsResult{
					Status: githubapi.EndpointStatusOK,
					Reviews: []githubapi.PullReview{
						{ID: 1, User: "carol", SubmittedAt: now.Add(-30 * time.Minute), State: "APPROVED"},
					},
				}, nil
			}
			return githubapi.PullReviewsResult{
				Status: githubapi.EndpointStatusOK,
				Reviews: []githubapi.PullReview{
					{ID: 2, User: "carol", SubmittedAt: now.Add(-20 * time.Minute), State: "COMMENTED"},
				},
			}, nil
		},
		listIssueCommentsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.IssueCommentsResult, error) {
			return githubapi.IssueCommentsResult{
				Status: githubapi.EndpointStatusOK,
				Comments: []githubapi.IssueComment{
					{ID: 11, User: "dave", CreatedAt: now.Add(-10 * time.Minute)},
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
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}

	commitsAlice := findMetric(result.Metrics, MetricActivityCommits24h, "org-a", "repo-a", "alice")
	if commitsAlice == nil || commitsAlice.Value != 2 {
		t.Fatalf("alice commits metric = %#v, want 2", commitsAlice)
	}
	commitsBob := findMetric(result.Metrics, MetricActivityCommits24h, "org-a", "repo-a", "bob")
	if commitsBob == nil || commitsBob.Value != 1 {
		t.Fatalf("bob commits metric = %#v, want 1", commitsBob)
	}
	commitsBot := findMetric(result.Metrics, MetricActivityCommits24h, "org-a", "repo-a", "github-actions[bot]")
	if commitsBot == nil || commitsBot.Value != 1 {
		t.Fatalf("bot commits metric = %#v, want 1", commitsBot)
	}
	commitsInferred := findMetric(result.Metrics, MetricActivityCommits24h, "org-a", "repo-a", "janedoe")
	if commitsInferred == nil || commitsInferred.Value != 1 {
		t.Fatalf("inferred commits metric = %#v, want 1", commitsInferred)
	}
	commitsUnlinked := findMetric(result.Metrics, MetricActivityCommits24h, "org-a", "repo-a", "unlinked_git_author")
	if commitsUnlinked == nil || commitsUnlinked.Value != 1 {
		t.Fatalf("unlinked commits metric = %#v, want 1", commitsUnlinked)
	}
	commitsEmailResolved := findMetric(result.Metrics, MetricActivityCommits24h, "org-a", "repo-a", "platform-engineering")
	if commitsEmailResolved == nil || commitsEmailResolved.Value != 1 {
		t.Fatalf("email-resolved commits metric = %#v, want 1", commitsEmailResolved)
	}
	prsOpened := findMetric(result.Metrics, MetricActivityPROpened24h, "org-a", "repo-a", "alice")
	if prsOpened == nil || prsOpened.Value != 1 {
		t.Fatalf("prs opened metric = %#v, want 1", prsOpened)
	}
	prsMerged := findMetric(result.Metrics, MetricActivityPRMerged24h, "org-a", "repo-a", "bob")
	if prsMerged == nil || prsMerged.Value != 1 {
		t.Fatalf("prs merged metric = %#v, want 1", prsMerged)
	}
	reviews := findMetric(result.Metrics, MetricActivityReviewsSubmitted24h, "org-a", "repo-a", "carol")
	if reviews == nil || reviews.Value != 2 {
		t.Fatalf("reviews metric = %#v, want 2", reviews)
	}
	comments := findMetric(result.Metrics, MetricActivityIssueComments24h, "org-a", "repo-a", "dave")
	if comments == nil || comments.Value != 1 {
		t.Fatalf("issue comments metric = %#v, want 1", comments)
	}
}

func TestGitHubOrgScraperActivityEndpointFailureQueuesMissedWindow(t *testing.T) {
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
			return githubapi.ContributorStatsResult{Status: githubapi.EndpointStatusAccepted}, nil
		},
		listRepoCommitsFn: func(_ context.Context, _, _ string, _, _ time.Time, _ int) (githubapi.CommitListResult, error) {
			return githubapi.CommitListResult{
				Status: githubapi.EndpointStatusForbidden,
			}, nil
		},
		listRepoPullsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.PullRequestListResult, error) {
			return githubapi.PullRequestListResult{
				Status: githubapi.EndpointStatusOK,
				PullRequests: []githubapi.PullRequest{
					{Number: 101, User: "alice", CreatedAt: now.Add(-time.Hour)},
				},
			}, nil
		},
		listPullReviewsFn: func(_ context.Context, _, _ string, _ int, _, _ time.Time) (githubapi.PullReviewsResult, error) {
			return githubapi.PullReviewsResult{
				Status: githubapi.EndpointStatusOK,
			}, nil
		},
		listIssueCommentsFn: func(_ context.Context, _, _ string, _, _ time.Time) (githubapi.IssueCommentsResult, error) {
			return githubapi.IssueCommentsResult{
				Status: githubapi.EndpointStatusOK,
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
	if len(result.MissedWindow) == 0 {
		t.Fatalf("expected missed windows for activity failure, got 0")
	}
	if findMetric(result.Metrics, MetricActivityPROpened24h, "org-a", "repo-a", "alice") == nil {
		t.Fatalf("expected PR opened metric despite commit activity failure")
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

func findMetricWithLabels(points []store.MetricPoint, name string, labels map[string]string) *store.MetricPoint {
	for i := range points {
		point := &points[i]
		if point.Name != name {
			continue
		}
		match := true
		for key, value := range labels {
			if point.Labels[key] != value {
				match = false
				break
			}
		}
		if match {
			return point
		}
	}
	return nil
}
