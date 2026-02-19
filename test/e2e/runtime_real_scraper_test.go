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
	"github.com/cam3ron2/github-stats-exporter/internal/store"
	"github.com/redis/go-redis/v9"
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

func TestRuntimeEndpointsWithCopilotMetricsConverge(t *testing.T) {
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

	harness := newRealScraperDualRuntimeHarness(t, fixture, orgs, func(cfg *config.Config) {
		cfg.Copilot.Enabled = true
		cfg.Copilot.IncludeOrg28d = true
		cfg.Copilot.IncludeOrgUsers28d = true
		cfg.Copilot.ScrapeInterval = 200 * time.Millisecond
		cfg.Copilot.RefreshIfReportUnchanged = true
	})
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
			scrape.MetricCopilotCodeAcceptanceActivity,
			scrape.MetricCopilotCodeGenerationActivity,
			scrape.MetricCopilotLOCAdded,
		}
		for _, metricName := range requiredNames {
			if !hasMetricName(leaderMetrics, metricName) || !hasMetricName(followerMetrics, metricName) {
				return false, nil
			}
		}

		leaderCopilot := extractMetricLineSet(leaderMetrics, "gh_copilot_usage_")
		followerCopilot := extractMetricLineSet(followerMetrics, "gh_copilot_usage_")
		if len(leaderCopilot) == 0 || len(followerCopilot) == 0 {
			return false, nil
		}
		return equalStringSets(leaderCopilot, followerCopilot), nil
	})
	if err != nil {
		t.Fatalf("copilot metrics did not converge: %v", err)
	}
}

func TestRuntimeCopilotDisabledEmitsNoCopilotSeries(t *testing.T) {
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
	}

	harness := newRealScraperDualRuntimeHarness(t, fixture, orgs, func(cfg *config.Config) {
		cfg.Copilot.Enabled = false
	})
	waitForLeaderFollowerReady(t, harness.httpClient, harness.leaderURL, harness.followerURL)

	err := waitForCondition(60*time.Second, 500*time.Millisecond, func() (bool, error) {
		leaderMetrics, followerMetrics, fetchErr := fetchMetricsPair(
			harness.httpClient,
			harness.leaderURL,
			harness.followerURL,
		)
		if fetchErr != nil {
			return false, fetchErr
		}
		if !hasMetricName(leaderMetrics, scrape.MetricActivityCommits24h) {
			return false, nil
		}
		if hasMetricPrefix(leaderMetrics, "gh_copilot_usage_") {
			return false, nil
		}
		if hasMetricPrefix(followerMetrics, "gh_copilot_usage_") {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("copilot disabled assertions did not converge: %v", err)
	}
}

func TestCopilotBackfillRecoveryOnFollower(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newFakeGitHubAPI(t)
	fixture.SetOrgRepos("cam3ron2", []string{"repo-a"})
	fixture.SetRepository("cam3ron2", "repo-a", buildRepositoryFixture(now, "alice"))
	fixture.FailPath("/orgs/cam3ron2/copilot/metrics/reports/organization-28-day/latest", http.StatusServiceUnavailable, 1)

	orgs := []config.GitHubOrgConfig{
		{
			Org:               "cam3ron2",
			AppID:             1,
			InstallationID:    1001,
			ScrapeInterval:    10 * time.Minute,
			PerOrgConcurrency: 1,
		},
	}
	harness := newRealScraperSingleRuntimeHarness(t, fixture, orgs, func(cfg *config.Config) {
		cfg.Copilot.Enabled = true
		cfg.Copilot.IncludeOrg28d = true
		cfg.Copilot.IncludeOrgUsers28d = false
		cfg.Copilot.ScrapeInterval = time.Minute
	})

	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, orgs[0]); err != nil {
		t.Fatalf("leader run should enqueue copilot backfill without failing cycle: %v", err)
	}
	points := harness.runtime.Store().Snapshot()
	if value, found := snapshotMetricValue(
		points,
		"gh_exporter_backfill_jobs_enqueued_total",
		map[string]string{"org": "cam3ron2", "reason": "copilot_report_fetch_error"},
	); !found || value < 1 {
		t.Fatalf("expected copilot backfill enqueue metric after leader cycle")
	}
	if harness.runtime.QueueDepth() == 0 {
		t.Fatalf("leader cycle should enqueue at least one copilot backfill message")
	}

	harness.runtime.StartFollower(harness.ctx)
	t.Cleanup(harness.runtime.StopFollower)

	var lastPoints []store.MetricPoint
	err := waitForCondition(45*time.Second, 200*time.Millisecond, func() (bool, error) {
		points := harness.runtime.Store().Snapshot()
		lastPoints = points
		processed, foundProcessed := snapshotMetricValue(
			points,
			"gh_exporter_backfill_jobs_processed_total",
			map[string]string{
				"org":    "cam3ron2",
				"repo":   "__copilot__org__28d",
				"result": "processed",
			},
		)
		if !foundProcessed || processed < 1 {
			return false, nil
		}

		accepted, foundAccepted := snapshotMetricValue(
			points,
			scrape.MetricCopilotCodeAcceptanceActivity,
			map[string]string{"org": "cam3ron2", "repo": "*", "user": "all"},
		)
		if !foundAccepted || accepted <= 0 {
			return false, nil
		}
		return harness.runtime.QueueDepth() == 0, nil
	})
	if err != nil {
		failed, _ := snapshotMetricValue(
			lastPoints,
			"gh_exporter_backfill_jobs_processed_total",
			map[string]string{
				"org":    "cam3ron2",
				"repo":   "__copilot__org__28d",
				"result": "failed",
			},
		)
		t.Fatalf(
			"follower did not recover copilot backfill message: %v (queue_depth=%d failed_count=%v metrics=%d)",
			err,
			harness.runtime.QueueDepth(),
			failed,
			len(lastPoints),
		)
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
	assertCheckpointMissing(t, harness.redisAddr, "cam3ron2", "repo-a")

	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, orgs[0]); err != nil {
		t.Fatalf("leader run should enqueue backfill without failing cycle: %v", err)
	}
	points := harness.runtime.Store().Snapshot()
	if value, found := snapshotMetricValue(
		points,
		"gh_exporter_backfill_jobs_enqueued_total",
		map[string]string{"org": "cam3ron2", "reason": "activity_issue_comments_failed"},
	); !found || value < 1 {
		t.Fatalf("expected backfill enqueue metric after leader cycle")
	}
	if harness.runtime.QueueDepth() == 0 {
		t.Fatalf("leader cycle should enqueue at least one backfill message")
	}
	assertCheckpointMissing(t, harness.redisAddr, "cam3ron2", "repo-a")

	harness.runtime.StartFollower(harness.ctx)
	t.Cleanup(harness.runtime.StopFollower)

	err := waitForCondition(45*time.Second, 200*time.Millisecond, func() (bool, error) {
		points := harness.runtime.Store().Snapshot()
		processed, foundProcessed := snapshotMetricValue(
			points,
			"gh_exporter_backfill_jobs_processed_total",
			map[string]string{"org": "cam3ron2", "repo": "repo-a", "result": "processed"},
		)
		if !foundProcessed || processed < 1 {
			return false, nil
		}

		comments, foundComments := snapshotMetricValue(
			points,
			scrape.MetricActivityIssueComments24h,
			map[string]string{"org": "cam3ron2", "repo": "repo-a", "user": "alice"},
		)
		if !foundComments || comments < 1 {
			return false, nil
		}
		return harness.runtime.QueueDepth() == 0, nil
	})
	if err != nil {
		t.Fatalf("follower did not process backfill message: %v", err)
	}
	waitForCheckpointAdvance(t, harness.redisAddr, "cam3ron2", "repo-a", 0)
}

func TestCheckpointAdvancesAcrossLeaderCycles(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	fixture := newFakeGitHubAPI(t)
	fixture.SetOrgRepos("cam3ron2", []string{"repo-a"})
	fixture.SetRepository("cam3ron2", "repo-a", buildRepositoryFixture(now, "alice"))

	orgs := []config.GitHubOrgConfig{
		{
			Org:               "cam3ron2",
			AppID:             1,
			InstallationID:    1001,
			ScrapeInterval:    120 * time.Millisecond,
			PerOrgConcurrency: 1,
		},
	}
	harness := newRealScraperSingleRuntimeHarness(t, fixture, orgs, nil)
	assertCheckpointMissing(t, harness.redisAddr, "cam3ron2", "repo-a")

	current := now
	harness.runtime.Now = func() time.Time { return current }
	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, orgs[0]); err != nil {
		t.Fatalf("first leader cycle failed: %v", err)
	}
	firstCheckpoint := mustReadCheckpointUnixNano(t, harness.redisAddr, "cam3ron2", "repo-a")

	current = current.Add(2 * time.Minute)
	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, orgs[0]); err != nil {
		t.Fatalf("second leader cycle failed: %v", err)
	}
	secondCheckpoint := mustReadCheckpointUnixNano(t, harness.redisAddr, "cam3ron2", "repo-a")
	if secondCheckpoint <= firstCheckpoint {
		t.Fatalf(
			"expected checkpoint to advance; first=%d second=%d",
			firstCheckpoint,
			secondCheckpoint,
		)
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

	org := orgs[0]
	current := now
	harness.runtime.Now = func() time.Time { return current }

	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, org); err == nil {
		t.Fatalf("first leader cycle should fail while github API is unhealthy")
	}
	points := harness.runtime.Store().Snapshot()
	if value, found := snapshotMetricValue(
		points,
		"gh_exporter_scrape_runs_total",
		map[string]string{"org": "cam3ron2", "result": "failure"},
	); !found || value < 1 {
		t.Fatalf("expected failure scrape metric after unhealthy cycle")
	}

	current = current.Add(100 * time.Millisecond)
	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, org); err != nil {
		t.Fatalf("cooldown cycle should not return error: %v", err)
	}
	points = harness.runtime.Store().Snapshot()
	if value, found := snapshotMetricValue(
		points,
		"gh_exporter_scrape_runs_total",
		map[string]string{"org": "cam3ron2", "result": "skipped_unhealthy"},
	); !found || value < 1 {
		t.Fatalf("expected skipped_unhealthy scrape metric during cooldown")
	}
	enqueuedValue, enqueuedFound := snapshotMetricValue(
		points,
		"gh_exporter_backfill_jobs_enqueued_total",
		map[string]string{"org": "cam3ron2", "reason": "github_unhealthy"},
	)
	dedupedValue, dedupedFound := snapshotMetricValue(
		points,
		"gh_exporter_backfill_jobs_deduped_total",
		map[string]string{"org": "cam3ron2", "reason": "github_unhealthy"},
	)
	if (!enqueuedFound || enqueuedValue < 1) && (!dedupedFound || dedupedValue < 1) {
		t.Fatalf("expected github_unhealthy enqueue or dedupe metric during cooldown")
	}
	if harness.runtime.QueueDepth() == 0 {
		t.Fatalf("expected backfill queue depth to increase during cooldown")
	}

	current = current.Add(700*time.Millisecond + time.Millisecond)
	if err := harness.runtime.RunLeaderOrgCycle(harness.ctx, org); err != nil {
		t.Fatalf("post-cooldown cycle should recover: %v", err)
	}
	points = harness.runtime.Store().Snapshot()
	if value, found := snapshotMetricValue(
		points,
		"gh_exporter_scrape_runs_total",
		map[string]string{"org": "cam3ron2", "result": "success"},
	); !found || value < 1 {
		t.Fatalf("expected success scrape metric after cooldown")
	}
	if value, found := snapshotMetricValue(
		points,
		"gh_exporter_dependency_health",
		map[string]string{"dependency": "github"},
	); !found || value < 1 {
		t.Fatalf("expected github dependency health metric to recover")
	}
}

type singleRuntimeHarness struct {
	ctx        context.Context
	baseURL    string
	httpClient *http.Client
	runtime    *app.Runtime
	redisAddr  string
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
	orgScraper := newFixtureOrgScraper(t, cfg, time.Now)

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
	orgScraper := newFixtureOrgScraper(t, cfg, time.Now)
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
		runtime:   runtime,
		redisAddr: redisServer.Addr(),
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
	cfg *config.Config,
	nowFn func() time.Time,
) scrape.OrgScraper {
	t.Helper()

	clients := make(map[string]scrape.GitHubDataClient, len(cfg.GitHub.Orgs))
	primaryOrg := ""
	for _, org := range cfg.GitHub.Orgs {
		if primaryOrg == "" {
			primaryOrg = org.Org
		}
		httpClient := &http.Client{Timeout: 2 * time.Second}
		requestClient := githubapi.NewClient(httpClient, githubapi.RetryConfig{
			MaxAttempts:    1,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		}, githubapi.RateLimitPolicy{})
		dataClient, err := githubapi.NewDataClient(cfg.GitHub.APIBaseURL, requestClient)
		if err != nil {
			t.Fatalf("create data client for %q: %v", org.Org, err)
		}
		clients[org.Org] = dataClient
	}

	var enterpriseClient scrape.GitHubDataClient
	if cfg.Copilot.Enabled && cfg.Copilot.Enterprise.Enabled {
		httpClient := &http.Client{Timeout: 2 * time.Second}
		requestClient := githubapi.NewClient(httpClient, githubapi.RetryConfig{
			MaxAttempts:    1,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
		}, githubapi.RateLimitPolicy{})
		dataClient, err := githubapi.NewDataClient(cfg.GitHub.APIBaseURL, requestClient)
		if err != nil {
			t.Fatalf("create enterprise data client: %v", err)
		}
		enterpriseClient = dataClient
	}

	return scrape.NewGitHubOrgScraper(clients, scrape.GitHubOrgScraperConfig{
		LOCRefreshInterval:                        24 * time.Hour,
		FallbackEnabled:                           true,
		FallbackMaxCommitsPerRepoPerWeek:          500,
		FallbackMaxCommitDetailCallsPerOrgPerHour: 5000,
		Copilot:                     cfg.Copilot,
		CopilotEnterpriseClient:     enterpriseClient,
		CopilotEnterprisePrimaryOrg: primaryOrg,
		Now:                         nowFn,
		Sleep:                       func(time.Duration) {},
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

func hasMetricPrefix(metrics string, metricPrefix string) bool {
	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), metricPrefix) {
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

func snapshotMetricValue(points []store.MetricPoint, name string, wantLabels map[string]string) (float64, bool) {
	for _, point := range points {
		if point.Name != name {
			continue
		}
		if !containsLabels(point.Labels, wantLabels) {
			continue
		}
		return point.Value, true
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

func assertCheckpointMissing(t *testing.T, redisAddr string, org string, repo string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	unixNanos, found, err := readCheckpointUnixNano(ctx, redisAddr, org, repo)
	if err != nil {
		t.Fatalf("read checkpoint for %s/%s: %v", org, repo, err)
	}
	if found {
		t.Fatalf("expected checkpoint to be missing for %s/%s, got %d", org, repo, unixNanos)
	}
}

func mustReadCheckpointUnixNano(t *testing.T, redisAddr string, org string, repo string) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	unixNanos, found, err := readCheckpointUnixNano(ctx, redisAddr, org, repo)
	if err != nil {
		t.Fatalf("read checkpoint for %s/%s: %v", org, repo, err)
	}
	if !found {
		t.Fatalf("expected checkpoint for %s/%s to exist", org, repo)
	}
	return unixNanos
}

func waitForCheckpointAdvance(
	t *testing.T,
	redisAddr string,
	org string,
	repo string,
	afterUnixNano int64,
) int64 {
	t.Helper()

	var observed int64
	err := waitForCondition(30*time.Second, 120*time.Millisecond, func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		unixNanos, found, err := readCheckpointUnixNano(ctx, redisAddr, org, repo)
		if err != nil {
			return false, err
		}
		if !found {
			return false, nil
		}
		if unixNanos <= afterUnixNano {
			return false, nil
		}
		observed = unixNanos
		return true, nil
	})
	if err != nil {
		t.Fatalf("checkpoint for %s/%s did not advance beyond %d: %v", org, repo, afterUnixNano, err)
	}
	return observed
}

func readCheckpointUnixNano(
	ctx context.Context,
	redisAddr string,
	org string,
	repo string,
) (int64, bool, error) {
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()

	fields, err := client.HGetAll(ctx, checkpointRedisKey(org, repo)).Result()
	if err != nil {
		return 0, false, err
	}
	rawCheckpoint := strings.TrimSpace(fields["checkpoint_unix_nano"])
	if rawCheckpoint == "" {
		return 0, false, nil
	}
	unixNanos, err := strconv.ParseInt(rawCheckpoint, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return unixNanos, true, nil
}

func checkpointRedisKey(org string, repo string) string {
	return "ghm:checkpoint:" + strings.TrimSpace(org) + "/" + strings.TrimSpace(repo)
}
