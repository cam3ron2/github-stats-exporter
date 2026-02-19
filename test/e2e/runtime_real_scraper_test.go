//go:build e2e

package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cam3ron2/github-stats-exporter/internal/app"
	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
	"github.com/cam3ron2/github-stats-exporter/internal/scrape"
	"go.uber.org/zap"
)

func TestRuntimeEndpointsWithRealGitHubScraperConverge(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := seedFixtureWithTwoOrganizations(t, now)
	orgs := []config.GitHubOrgConfig{
		{
			Org:               "cam3ron2",
			AppID:             1,
			InstallationID:    1001,
			ScrapeInterval:    200 * time.Millisecond,
			PerOrgConcurrency: 2,
		},
		{
			Org:               "shigops",
			AppID:             2,
			InstallationID:    2002,
			ScrapeInterval:    200 * time.Millisecond,
			PerOrgConcurrency: 2,
		},
	}

	harness := newRealScraperDualRuntimeHarness(t, fixture, orgs, nil)
	waitForLeaderFollowerReady(t, harness.httpClient, harness.leaderURL, harness.followerURL)

	err := waitForCondition(90*time.Second, 500*time.Millisecond, func() (bool, error) {
		leaderMetrics, followerMetrics, fetchErr := fetchMetricsPair(
			harness.httpClient,
			harness.leaderURL,
			harness.followerURL,
		)
		if fetchErr != nil {
			return false, fetchErr
		}

		requiredNames := []string{
			scrape.MetricActivityCommits24h,
			scrape.MetricActivityPROpened24h,
			scrape.MetricActivityPRMerged24h,
			scrape.MetricActivityReviewsSubmitted24h,
			scrape.MetricActivityIssueComments24h,
			scrape.MetricActivityLOCAddedWeekly,
			scrape.MetricActivityLOCRemovedWeekly,
		}
		for _, metricName := range requiredNames {
			if !hasMetricName(leaderMetrics, metricName) || !hasMetricName(followerMetrics, metricName) {
				return false, nil
			}
		}

		leaderOrgs := extractOrgLabels(leaderMetrics)
		followerOrgs := extractOrgLabels(followerMetrics)
		if len(leaderOrgs) < 2 || len(followerOrgs) < 2 {
			return false, nil
		}
		if !equalStringSets(leaderOrgs, followerOrgs) {
			return false, nil
		}

		leaderActivity := extractMetricLineSet(leaderMetrics, "gh_activity_")
		followerActivity := extractMetricLineSet(followerMetrics, "gh_activity_")
		if len(leaderActivity) == 0 || len(followerActivity) == 0 {
			return false, nil
		}
		if !equalStringSets(leaderActivity, followerActivity) {
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		t.Fatalf("real-scraper metrics did not converge: %v", err)
	}
}

func TestBackfillFlowProcessesMissedWindowOnFollower(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newFakeGitHubAPI(t)
	fixture.SetOrgRepos("cam3ron2", []string{"repo-a"})
	fixture.SetRepository("cam3ron2", "repo-a", buildRepositoryFixture(now, "alice"))
	fixture.FailPath("/repos/cam3ron2/repo-a/issues/comments", http.StatusServiceUnavailable, 1)

	orgs := []config.GitHubOrgConfig{
		{
			Org:               "cam3ron2",
			AppID:             1,
			InstallationID:    1001,
			ScrapeInterval:    10 * time.Minute,
			PerOrgConcurrency: 1,
		},
	}
	harness := newRealScraperSingleRuntimeHarness(t, fixture, orgs, nil)
	harness.runtime.StartLeader(harness.ctx)
	t.Cleanup(harness.runtime.StopLeader)

	err := waitForCondition(30*time.Second, 200*time.Millisecond, func() (bool, error) {
		metrics, fetchErr := fetchEndpoint(harness.httpClient, harness.baseURL+"/metrics")
		if fetchErr != nil {
			return false, fetchErr
		}
		value, found := metricValue(metrics, "gh_exporter_backfill_jobs_enqueued_total", map[string]string{
			"org":    "cam3ron2",
			"reason": "activity_issue_comments_failed",
		})
		if !found || value < 1 {
			return false, nil
		}
		return harness.runtime.QueueDepth() > 0, nil
	})
	if err != nil {
		t.Fatalf("leader did not enqueue backfill message: %v", err)
	}

	harness.runtime.StopLeader()
	harness.runtime.StartFollower(harness.ctx)
	t.Cleanup(harness.runtime.StopFollower)

	err = waitForCondition(45*time.Second, 200*time.Millisecond, func() (bool, error) {
		metrics, fetchErr := fetchEndpoint(harness.httpClient, harness.baseURL+"/metrics")
		if fetchErr != nil {
			return false, fetchErr
		}
		processed, foundProcessed := metricValue(
			metrics,
			"gh_exporter_backfill_jobs_processed_total",
			map[string]string{
				"org":    "cam3ron2",
				"repo":   "repo-a",
				"result": "processed",
			},
		)
		if !foundProcessed || processed < 1 {
			return false, nil
		}

		comments, foundComments := metricValue(
			metrics,
			scrape.MetricActivityIssueComments24h,
			map[string]string{
				"org":  "cam3ron2",
				"repo": "repo-a",
				"user": "alice",
			},
		)
		if !foundComments || comments < 1 {
			return false, nil
		}
		return harness.runtime.QueueDepth() == 0, nil
	})
	if err != nil {
		t.Fatalf("follower did not process backfill message: %v", err)
	}
}

func TestLeaderCooldownAndRecoveryWhenGitHubIsUnhealthy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newFakeGitHubAPI(t)
	fixture.SetOrgRepos("cam3ron2", []string{"repo-a"})
	fixture.SetRepository("cam3ron2", "repo-a", buildRepositoryFixture(now, "alice"))
	fixture.FailPath("/orgs/cam3ron2/repos", http.StatusServiceUnavailable, 1)

	orgs := []config.GitHubOrgConfig{
		{
			Org:               "cam3ron2",
			AppID:             1,
			InstallationID:    1001,
			ScrapeInterval:    120 * time.Millisecond,
			PerOrgConcurrency: 1,
		},
	}
	cfgOverride := func(cfg *config.Config) {
		cfg.GitHub.UnhealthyFailureThreshold = 1
		cfg.GitHub.UnhealthyCooldown = 700 * time.Millisecond
		cfg.Health.GitHubProbeInterval = time.Hour
		cfg.Health.GitHubRecoverSuccessThreshold = 1
	}

	harness := newRealScraperSingleRuntimeHarness(t, fixture, orgs, cfgOverride)
	harness.runtime.StartLeader(harness.ctx)
	t.Cleanup(harness.runtime.StopLeader)

	err := waitForCondition(20*time.Second, 100*time.Millisecond, func() (bool, error) {
		metrics, fetchErr := fetchEndpoint(harness.httpClient, harness.baseURL+"/metrics")
		if fetchErr != nil {
			return false, fetchErr
		}
		value, found := metricValue(
			metrics,
			"gh_exporter_scrape_runs_total",
			map[string]string{"org": "cam3ron2", "result": "failure"},
		)
		return found && value >= 1, nil
	})
	if err != nil {
		t.Fatalf("did not observe unhealthy scrape failure: %v", err)
	}

	err = waitForCondition(30*time.Second, 150*time.Millisecond, func() (bool, error) {
		metrics, fetchErr := fetchEndpoint(harness.httpClient, harness.baseURL+"/metrics")
		if fetchErr != nil {
			return false, fetchErr
		}
		skipped, foundSkipped := metricValue(
			metrics,
			"gh_exporter_scrape_runs_total",
			map[string]string{"org": "cam3ron2", "result": "skipped_unhealthy"},
		)
		if !foundSkipped || skipped < 1 {
			return false, nil
		}
		enqueued, foundEnqueued := metricValue(
			metrics,
			"gh_exporter_backfill_jobs_enqueued_total",
			map[string]string{"org": "cam3ron2", "reason": "github_unhealthy"},
		)
		if !foundEnqueued || enqueued < 1 {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("did not observe cooldown skip and queue fallback: %v", err)
	}

	err = waitForCondition(30*time.Second, 150*time.Millisecond, func() (bool, error) {
		metrics, fetchErr := fetchEndpoint(harness.httpClient, harness.baseURL+"/metrics")
		if fetchErr != nil {
			return false, fetchErr
		}
		success, foundSuccess := metricValue(
			metrics,
			"gh_exporter_scrape_runs_total",
			map[string]string{"org": "cam3ron2", "result": "success"},
		)
		if !foundSuccess || success < 1 {
			return false, nil
		}
		githubHealth, foundHealth := metricValue(
			metrics,
			"gh_exporter_dependency_health",
			map[string]string{"dependency": "github"},
		)
		if !foundHealth || githubHealth < 1 {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("runtime did not recover after cooldown: %v", err)
	}
}

type singleRuntimeHarness struct {
	ctx        context.Context
	baseURL    string
	httpClient *http.Client
	runtime    *app.Runtime
}

func newRealScraperDualRuntimeHarness(
	t *testing.T,
	fixture *fakeGitHubAPI,
	orgs []config.GitHubOrgConfig,
	override func(*config.Config),
) runtimeHarness {
	t.Helper()

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	cfg := buildRealScraperConfig(redisServer.Addr(), fixture.URL(), orgs)
	if override != nil {
		override(cfg)
	}
	orgScraper := newFixtureOrgScraper(t, fixture.URL(), orgs, time.Now)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	leaderRuntime := app.NewRuntime(cfg, orgScraper, zap.NewNop())
	followerRuntime := app.NewRuntime(cfg, orgScraper, zap.NewNop())
	leaderRuntime.StartLeader(ctx)
	followerRuntime.StartFollower(ctx)

	t.Cleanup(func() {
		leaderRuntime.StopLeader()
		followerRuntime.StopFollower()
	})

	leaderServer := httptest.NewServer(leaderRuntime.Handler())
	followerServer := httptest.NewServer(followerRuntime.Handler())
	t.Cleanup(leaderServer.Close)
	t.Cleanup(followerServer.Close)

	return runtimeHarness{
		leaderURL:   leaderServer.URL,
		followerURL: followerServer.URL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func newRealScraperSingleRuntimeHarness(
	t *testing.T,
	fixture *fakeGitHubAPI,
	orgs []config.GitHubOrgConfig,
	override func(*config.Config),
) singleRuntimeHarness {
	t.Helper()

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	cfg := buildRealScraperConfig(redisServer.Addr(), fixture.URL(), orgs)
	if override != nil {
		override(cfg)
	}
	orgScraper := newFixtureOrgScraper(t, fixture.URL(), orgs, time.Now)
	runtime := app.NewRuntime(cfg, orgScraper, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := httptest.NewServer(runtime.Handler())
	t.Cleanup(server.Close)

	return singleRuntimeHarness{
		ctx:     ctx,
		baseURL: server.URL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		runtime: runtime,
	}
}

func buildRealScraperConfig(
	redisAddr string,
	apiBaseURL string,
	orgs []config.GitHubOrgConfig,
) *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			ListenAddr: ":0",
			LogLevel:   "debug",
		},
		Metrics: config.MetricsConfig{
			Topology:         "single_service_target",
			ScrapeServiceDNS: "localhost",
		},
		GitHub: config.GitHubConfig{
			APIBaseURL:                apiBaseURL,
			RequestTimeout:            2 * time.Second,
			UnhealthyFailureThreshold: 3,
			UnhealthyCooldown:         2 * time.Second,
			Orgs:                      append([]config.GitHubOrgConfig(nil), orgs...),
		},
		Retry: config.RetryConfig{
			MaxAttempts:    1,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		},
		Backfill: config.BackfillConfig{
			MaxMessageAge:              time.Hour,
			ConsumerCount:              1,
			RequeueDelays:              []time.Duration{100 * time.Millisecond},
			DedupTTL:                   5 * time.Minute,
			MaxEnqueuesPerOrgPerMinute: 1000,
		},
		Store: config.StoreConfig{
			Backend:               "redis",
			RedisMode:             "standalone",
			RedisAddr:             redisAddr,
			Retention:             24 * time.Hour,
			MetricRefreshInterval: 50 * time.Millisecond,
			ExportCacheMode:       "full",
			MaxSeriesBudget:       10000,
			IndexShards:           4,
		},
		Health: config.HealthConfig{
			GitHubProbeInterval:           250 * time.Millisecond,
			GitHubRecoverSuccessThreshold: 1,
		},
	}
}

func newFixtureOrgScraper(
	t *testing.T,
	apiBaseURL string,
	orgs []config.GitHubOrgConfig,
	nowFn func() time.Time,
) scrape.OrgScraper {
	t.Helper()

	clients := make(map[string]scrape.GitHubDataClient, len(orgs))
	for _, org := range orgs {
		httpClient := &http.Client{Timeout: 2 * time.Second}
		requestClient := githubapi.NewClient(httpClient, githubapi.RetryConfig{
			MaxAttempts:    1,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		}, githubapi.RateLimitPolicy{})
		dataClient, err := githubapi.NewDataClient(apiBaseURL, requestClient)
		if err != nil {
			t.Fatalf("create data client for %q: %v", org.Org, err)
		}
		clients[org.Org] = dataClient
	}

	return scrape.NewGitHubOrgScraper(clients, scrape.GitHubOrgScraperConfig{
		LOCRefreshInterval:                        24 * time.Hour,
		FallbackEnabled:                           true,
		FallbackMaxCommitsPerRepoPerWeek:          500,
		FallbackMaxCommitDetailCallsPerOrgPerHour: 5000,
		Now:   nowFn,
		Sleep: func(time.Duration) {},
	})
}

func waitForLeaderFollowerReady(t *testing.T, client *http.Client, leaderURL string, followerURL string) {
	t.Helper()

	err := waitForCondition(30*time.Second, 100*time.Millisecond, func() (bool, error) {
		leaderStatus, leaderErr := fetchHealthStatus(client, leaderURL)
		if leaderErr != nil {
			return false, leaderErr
		}
		followerStatus, followerErr := fetchHealthStatus(client, followerURL)
		if followerErr != nil {
			return false, followerErr
		}
		leaderReady := leaderStatus.Role == "leader" && leaderStatus.Ready
		followerReady := followerStatus.Role == "follower" && followerStatus.Ready
		return leaderReady && followerReady, nil
	})
	if err != nil {
		t.Fatalf("leader/follower health did not converge: %v", err)
	}
}

func buildRepositoryFixture(now time.Time, user string) repositoryFixture {
	weekStart := now.Add(-14 * 24 * time.Hour).Truncate(24 * time.Hour)
	commitAt := now.Add(-2 * time.Hour).UTC()
	prUpdated := now.Add(-90 * time.Minute).UTC()
	prMerged := now.Add(-80 * time.Minute).UTC()
	reviewAt := now.Add(-70 * time.Minute).UTC()
	commentAt := now.Add(-60 * time.Minute).UTC()

	return repositoryFixture{
		Contributors: []fixtureContributor{
			{
				User:  user,
				Total: 5,
				Weeks: []fixtureContributorWeek{
					{
						WeekStart: weekStart,
						Additions: 40,
						Deletions: 12,
						Commits:   3,
					},
				},
			},
		},
		Commits: []fixtureCommit{
			{
				SHA:            "abc123",
				Author:         user,
				Committer:      user,
				AuthorName:     user,
				AuthorEmail:    user + "@example.com",
				CommitterName:  user,
				CommitterEmail: user + "@example.com",
				CommittedAt:    commitAt,
				Additions:      40,
				Deletions:      12,
			},
		},
		Pulls: []fixturePull{
			{
				Number:    101,
				User:      user,
				CreatedAt: commitAt,
				UpdatedAt: prUpdated,
				MergedAt:  prMerged,
			},
		},
		ReviewsByPR: map[int][]fixtureReview{
			101: {
				{
					ID:          9001,
					User:        user,
					State:       "approved",
					SubmittedAt: reviewAt,
				},
			},
		},
		Comments: []fixtureComment{
			{
				ID:        8001,
				User:      user,
				CreatedAt: commentAt,
			},
		},
	}
}

func seedFixtureWithTwoOrganizations(t *testing.T, now time.Time) *fakeGitHubAPI {
	t.Helper()

	fixture := newFakeGitHubAPI(t)
	fixture.SetOrgRepos("cam3ron2", []string{"repo-a"})
	fixture.SetRepository("cam3ron2", "repo-a", buildRepositoryFixture(now, "alice"))
	fixture.SetOrgRepos("shigops", []string{"repo-b"})
	fixture.SetRepository("shigops", "repo-b", buildRepositoryFixture(now, "carol"))
	return fixture
}

func hasMetricName(metrics string, metricName string) bool {
	prefix := metricName + "{"
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func metricValue(metrics string, metricName string, wantLabels map[string]string) (float64, bool) {
	for _, line := range strings.Split(metrics, "\n") {
		name, labels, value, ok := parseMetricLine(line)
		if !ok || name != metricName {
			continue
		}
		if !containsLabels(labels, wantLabels) {
			continue
		}
		parsedValue, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			continue
		}
		return parsedValue, true
	}
	return 0, false
}

func parseMetricLine(line string) (string, map[string]string, string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") {
		return "", nil, "", false
	}

	fields := strings.Fields(trimmed)
	if len(fields) < 2 {
		return "", nil, "", false
	}
	metricToken := fields[0]
	value := fields[len(fields)-1]

	if !strings.Contains(metricToken, "{") {
		return metricToken, nil, value, true
	}

	openIndex := strings.Index(metricToken, "{")
	closeIndex := strings.LastIndex(metricToken, "}")
	if openIndex < 0 || closeIndex <= openIndex {
		return "", nil, "", false
	}
	name := metricToken[:openIndex]
	labels, ok := parseLabelSet(metricToken[openIndex+1 : closeIndex])
	if !ok {
		return "", nil, "", false
	}
	return name, labels, value, true
}

func parseLabelSet(raw string) (map[string]string, bool) {
	if strings.TrimSpace(raw) == "" {
		return map[string]string{}, true
	}
	result := make(map[string]string)
	parts := strings.Split(raw, ",")
	for _, part := range parts {
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			return nil, false
		}
		key := strings.TrimSpace(pieces[0])
		value := strings.TrimSpace(pieces[1])
		if len(value) < 2 || !strings.HasPrefix(value, "\"") || !strings.HasSuffix(value, "\"") {
			return nil, false
		}
		result[key] = strings.Trim(value, "\"")
	}
	return result, true
}

func containsLabels(actual map[string]string, wanted map[string]string) bool {
	for key, wantedValue := range wanted {
		actualValue, ok := actual[key]
		if !ok {
			return false
		}
		if actualValue != wantedValue {
			return false
		}
	}
	return true
}
