package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/backfill"
	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/health"
	"github.com/cam3ron2/github-stats-exporter/internal/scrape"
	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

type fakeOrgScraper struct {
	result  scrape.OrgResult
	err     error
	results []scrape.OrgResult
	errs    []error
	calls   int
}

type checkpointAwareFakeScraper struct {
	fakeOrgScraper
	checkpoints scrape.CheckpointStore
}

func (s *checkpointAwareFakeScraper) SetCheckpointStore(checkpoints scrape.CheckpointStore) {
	s.checkpoints = checkpoints
}

func (s *fakeOrgScraper) ScrapeOrg(_ context.Context, _ config.GitHubOrgConfig) (scrape.OrgResult, error) {
	s.calls++
	index := s.calls - 1
	if index < len(s.results) || index < len(s.errs) {
		var result scrape.OrgResult
		if index < len(s.results) {
			result = s.results[index]
		}
		var err error
		if index < len(s.errs) {
			err = s.errs[index]
		}
		return result, err
	}
	return s.result, s.err
}

func testConfig() *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			ListenAddr: ":8080",
			LogLevel:   "info",
		},
		Metrics: config.MetricsConfig{
			Topology: "single_service_target",
		},
		GitHub: config.GitHubConfig{
			Orgs: []config.GitHubOrgConfig{
				{
					Org:            "org-a",
					AppID:          1,
					InstallationID: 2,
					PrivateKeyPath: "/tmp/key",
					ScrapeInterval: 5 * time.Minute,
				},
			},
		},
		Backfill: config.BackfillConfig{
			MaxMessageAge:              24 * time.Hour,
			ConsumerCount:              1,
			RequeueDelays:              []time.Duration{time.Minute},
			CoalesceWindow:             15 * time.Minute,
			DedupTTL:                   12 * time.Hour,
			MaxEnqueuesPerOrgPerMinute: 10,
		},
		Store: config.StoreConfig{
			Retention:       24 * time.Hour,
			MaxSeriesBudget: 10000,
		},
		Health: config.HealthConfig{
			GitHubProbeInterval:           30 * time.Second,
			GitHubRecoverSuccessThreshold: 3,
		},
	}
}

func TestRuntimeRunLeaderCycleWritesMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			Metrics: []store.MetricPoint{
				{
					Name: "gh_activity_commits_24h",
					Labels: map[string]string{
						"org":  "org-a",
						"repo": "repo-a",
						"user": "alice",
					},
					Value:     7,
					UpdatedAt: now,
				},
			},
		},
	})
	runtime.Now = func() time.Time { return now }

	err := runtime.RunLeaderCycle(context.Background())
	if err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	if len(snapshot) < 2 {
		t.Fatalf("snapshot len = %d, want at least 2", len(snapshot))
	}
	activityMetric := findMetric(snapshot, "gh_activity_commits_24h")
	if activityMetric == nil {
		t.Fatalf("missing gh_activity_commits_24h")
	}
	if activityMetric.Labels["user"] != "alice" {
		t.Fatalf("metric user = %q, want alice", activityMetric.Labels["user"])
	}

	internalMetric := findMetric(snapshot, "gh_exporter_leader_cycle_last_run_unixtime")
	if internalMetric == nil {
		t.Fatalf("missing gh_exporter_leader_cycle_last_run_unixtime")
	}
	if internalMetric.Value != float64(now.Unix()) {
		t.Fatalf("leader cycle timestamp = %v, want %v", internalMetric.Value, float64(now.Unix()))
	}
}

func TestNewRuntimeInjectsCheckpointStore(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	scraper := &checkpointAwareFakeScraper{}

	runtime := NewRuntime(cfg, scraper)
	if runtime == nil {
		t.Fatalf("NewRuntime() returned nil")
	}
	if scraper.checkpoints == nil {
		t.Fatalf("checkpoint store was not injected into scraper")
	}
}

func TestRuntimeProbeDependenciesUpdatesHealth(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	storeProbe := &healthToggleStore{
		runtimeStore: runtime.store,
		healthy:      false,
	}
	queueProbe := &healthToggleQueue{
		runtimeQueue: runtime.queue,
		healthy:      false,
	}
	runtime.store = storeProbe
	runtime.queue = queueProbe

	runtime.probeDependencies(context.Background())
	status := runtime.CurrentStatus(context.Background())
	if status.Components["redis"] {
		t.Fatalf("redis component health = true, want false")
	}
	if status.Components["amqp"] {
		t.Fatalf("amqp component health = true, want false")
	}

	storeProbe.healthy = true
	queueProbe.healthy = true
	runtime.probeDependencies(context.Background())
	status = runtime.CurrentStatus(context.Background())
	if !status.Components["redis"] {
		t.Fatalf("redis component health = false, want true")
	}
	if !status.Components["amqp"] {
		t.Fatalf("amqp component health = false, want true")
	}
}

func TestRuntimeProbeDependenciesChecksGitHubAPIHealth(t *testing.T) {
	t.Parallel()

	statusCode := http.StatusServiceUnavailable

	cfg := testConfig()
	cfg.GitHub.APIBaseURL = "https://api.github.local"
	cfg.GitHub.UnhealthyFailureThreshold = 1
	cfg.GitHub.UnhealthyCooldown = time.Minute
	cfg.Health.GitHubRecoverSuccessThreshold = 1

	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	runtime.githubProbeClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/meta" {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("not found")),
					Header:     http.Header{},
				}, nil
			}
			return &http.Response{
				StatusCode: statusCode,
				Body:       io.NopCloser(strings.NewReader(http.StatusText(statusCode))),
				Header:     http.Header{},
			}, nil
		}),
	}
	runtime.probeDependencies(context.Background())
	status := runtime.CurrentStatus(context.Background())
	if status.Components["github_healthy"] {
		t.Fatalf("github_healthy component = true, want false")
	}

	statusCode = http.StatusOK
	runtime.probeDependencies(context.Background())
	status = runtime.CurrentStatus(context.Background())
	if !status.Components["github_healthy"] {
		t.Fatalf("github_healthy component = false, want true")
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestRuntimeRunLeaderCycleWritesInternalMetricsWithoutActivity(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{},
	})
	runtime.Now = func() time.Time { return now }

	err := runtime.RunLeaderCycle(context.Background())
	if err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	internalMetric := findMetric(snapshot, "gh_exporter_leader_cycle_last_run_unixtime")
	if internalMetric == nil {
		t.Fatalf("missing gh_exporter_leader_cycle_last_run_unixtime")
	}
	if internalMetric.Value != float64(now.Unix()) {
		t.Fatalf("leader cycle timestamp = %v, want %v", internalMetric.Value, float64(now.Unix()))
	}
}

func TestRuntimeRunLeaderCycleWritesInternalMetricsWithoutConfiguredOrgs(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.GitHub.Orgs = nil
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	runtime.Now = func() time.Time { return now }

	err := runtime.RunLeaderCycle(context.Background())
	if err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	internalMetric := findMetric(snapshot, "gh_exporter_leader_cycle_last_run_unixtime")
	if internalMetric == nil {
		t.Fatalf("missing gh_exporter_leader_cycle_last_run_unixtime")
	}
	if internalMetric.Value != float64(now.Unix()) {
		t.Fatalf("leader cycle timestamp = %v, want %v", internalMetric.Value, float64(now.Unix()))
	}
}

func TestRuntimeLeaderFailureEnqueuesBackfill(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		err: errors.New("github unavailable"),
	})
	runtime.Now = func() time.Time { return now }

	err := runtime.RunLeaderCycle(context.Background())
	if err == nil {
		t.Fatalf("RunLeaderCycle() expected aggregated error, got nil")
	}
	if runtime.QueueDepth() != 1 {
		t.Fatalf("QueueDepth() = %d, want 1", runtime.QueueDepth())
	}
}

func TestRuntimeLeaderPartialFailureEnqueuesBackfillAndKeepsMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			Metrics: []store.MetricPoint{
				{
					Name: "gh_activity_commits_24h",
					Labels: map[string]string{
						"org":  "org-a",
						"repo": "repo-a",
						"user": "alice",
					},
					Value:     4,
					UpdatedAt: now,
				},
			},
			MissedWindow: []scrape.MissedWindow{
				{
					Org:         "org-a",
					Repo:        "repo-b",
					WindowStart: now.Add(-time.Hour),
					WindowEnd:   now,
					Reason:      "repo_scrape_failed",
				},
			},
		},
	})
	runtime.Now = func() time.Time { return now }

	err := runtime.RunLeaderCycle(context.Background())
	if err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}
	if runtime.QueueDepth() != 1 {
		t.Fatalf("QueueDepth() = %d, want 1", runtime.QueueDepth())
	}

	snapshot := runtime.Store().Snapshot()
	activityMetric := findMetric(snapshot, "gh_activity_commits_24h")
	if activityMetric == nil {
		t.Fatalf("missing gh_activity_commits_24h")
	}
}

func TestRuntimeCurrentStatusRoleAware(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	runtime := NewRuntime(cfg, &fakeOrgScraper{})

	runtime.StartFollower(context.Background())
	followerStatus := runtime.CurrentStatus(context.Background())
	if followerStatus.Role != health.RoleFollower {
		t.Fatalf("follower role = %q, want %q", followerStatus.Role, health.RoleFollower)
	}

	runtime.StopFollower()
	runtime.StartLeader(context.Background())
	leaderStatus := runtime.CurrentStatus(context.Background())
	if leaderStatus.Role != health.RoleLeader {
		t.Fatalf("leader role = %q, want %q", leaderStatus.Role, health.RoleLeader)
	}
}

func TestRuntimeRunLeaderCycleAppliesGitHubUnhealthyCooldown(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.GitHub.UnhealthyFailureThreshold = 2
	cfg.GitHub.UnhealthyCooldown = 10 * time.Minute
	cfg.Health.GitHubRecoverSuccessThreshold = 2

	scraper := &fakeOrgScraper{
		results: []scrape.OrgResult{{}, {}, {}, {}},
		errs: []error{
			errors.New("github unavailable"),
			errors.New("github unavailable"),
			nil,
			nil,
		},
	}
	runtime := NewRuntime(cfg, scraper)

	now := time.Unix(1739836800, 0)
	runtime.Now = func() time.Time { return now }

	if err := runtime.RunLeaderCycle(context.Background()); err == nil {
		t.Fatalf("RunLeaderCycle(first) expected error, got nil")
	}

	now = now.Add(time.Minute)
	if err := runtime.RunLeaderCycle(context.Background()); err == nil {
		t.Fatalf("RunLeaderCycle(second) expected error, got nil")
	}

	now = now.Add(time.Minute)
	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle(third/cooldown) unexpected error: %v", err)
	}
	if scraper.calls != 2 {
		t.Fatalf("scrape calls during cooldown = %d, want 2", scraper.calls)
	}

	now = now.Add(cfg.GitHub.UnhealthyCooldown + time.Second)
	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle(fourth/post-cooldown) unexpected error: %v", err)
	}
	if scraper.calls != 3 {
		t.Fatalf("scrape calls after cooldown = %d, want 3", scraper.calls)
	}

	now = now.Add(time.Minute)
	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle(fifth/recovery) unexpected error: %v", err)
	}
	if scraper.calls != 4 {
		t.Fatalf("scrape calls after recovery = %d, want 4", scraper.calls)
	}

	status := runtime.CurrentStatus(context.Background())
	if got := status.Components["github_healthy"]; !got {
		t.Fatalf("github_healthy = %t, want true after recovery", got)
	}
}

func TestRuntimeRunLeaderCycleWritesOperationalMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			MissedWindow: []scrape.MissedWindow{
				{
					Org:         "org-a",
					Repo:        "repo-a",
					WindowStart: now.Add(-time.Hour),
					WindowEnd:   now,
					Reason:      "scrape_error",
				},
			},
		},
	})
	runtime.Now = func() time.Time { return now }

	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	testCases := []struct {
		name   string
		metric string
		labels map[string]string
	}{
		{
			name:   "scrape run success",
			metric: "gh_exporter_scrape_runs_total",
			labels: map[string]string{"org": "org-a", "result": "success"},
		},
		{
			name:   "store write success",
			metric: "gh_exporter_store_write_total",
			labels: map[string]string{"source": "leader_scrape", "result": "success"},
		},
		{
			name:   "backfill enqueue count",
			metric: "gh_exporter_backfill_jobs_enqueued_total",
			labels: map[string]string{"org": "org-a", "reason": "scrape_error"},
		},
		{
			name:   "dependency health github",
			metric: "gh_exporter_dependency_health",
			labels: map[string]string{"dependency": "github"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metric := findMetricWithLabels(snapshot, tc.metric, tc.labels)
			if metric == nil {
				t.Fatalf("missing metric %s labels=%v", tc.metric, tc.labels)
			}
		})
	}
}

func TestRuntimeRunLeaderCycleCopilotDependencyHealthSplit(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name        string
		repo        string
		reason      string
		wantHealthy map[string]bool
	}{
		{
			name:   "org_report_fetch_error_marks_org_and_download_unhealthy",
			repo:   "__copilot__org__28d",
			reason: "copilot_report_fetch_error",
			wantHealthy: map[string]bool{
				"github_api_core":                            true,
				"github_api_copilot_org_reports":             false,
				"github_api_copilot_org_user_reports":        true,
				"github_api_copilot_enterprise_reports":      true,
				"github_api_copilot_enterprise_user_reports": true,
				"github_copilot_report_download":             false,
			},
		},
		{
			name:   "users_report_parse_error_marks_only_users_unhealthy",
			repo:   "__copilot__users__28d",
			reason: "copilot_report_parse_error",
			wantHealthy: map[string]bool{
				"github_api_core":                            true,
				"github_api_copilot_org_reports":             true,
				"github_api_copilot_org_user_reports":        false,
				"github_api_copilot_enterprise_reports":      true,
				"github_api_copilot_enterprise_user_reports": true,
				"github_copilot_report_download":             true,
			},
		},
		{
			name:   "enterprise_report_download_error_marks_enterprise_and_download_unhealthy",
			repo:   "__copilot__enterprise__28d",
			reason: "copilot_report_download_error",
			wantHealthy: map[string]bool{
				"github_api_core":                            true,
				"github_api_copilot_org_reports":             true,
				"github_api_copilot_org_user_reports":        true,
				"github_api_copilot_enterprise_reports":      false,
				"github_api_copilot_enterprise_user_reports": true,
				"github_copilot_report_download":             false,
			},
		},
		{
			name:   "enterprise_users_report_fetch_error_marks_enterprise_users_and_download_unhealthy",
			repo:   "__copilot__enterprise_users__28d",
			reason: "copilot_report_fetch_error",
			wantHealthy: map[string]bool{
				"github_api_core":                            true,
				"github_api_copilot_org_reports":             true,
				"github_api_copilot_org_user_reports":        true,
				"github_api_copilot_enterprise_reports":      true,
				"github_api_copilot_enterprise_user_reports": false,
				"github_copilot_report_download":             false,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			runtime := NewRuntime(cfg, &fakeOrgScraper{
				result: scrape.OrgResult{
					MissedWindow: []scrape.MissedWindow{
						{
							Org:         "org-a",
							Repo:        tc.repo,
							WindowStart: now,
							WindowEnd:   now,
							Reason:      tc.reason,
						},
					},
				},
			})
			runtime.Now = func() time.Time { return now }

			if err := runtime.RunLeaderCycle(context.Background()); err != nil {
				t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
			}

			snapshot := runtime.Store().Snapshot()
			for dependency, want := range tc.wantHealthy {
				point := findMetricWithLabels(snapshot, "gh_exporter_dependency_health", map[string]string{
					"dependency": dependency,
				})
				if point == nil {
					t.Fatalf("missing dependency metric for %s", dependency)
				}
				got := point.Value == 1
				if got != want {
					t.Fatalf(
						"dependency metric %s healthy=%t, want %t",
						dependency,
						got,
						want,
					)
				}
			}

			status := runtime.CurrentStatus(context.Background())
			for dependency, want := range tc.wantHealthy {
				got, ok := status.Components[dependency]
				if !ok {
					t.Fatalf("status missing component %s", dependency)
				}
				if got != want {
					t.Fatalf("status component %s=%t, want %t", dependency, got, want)
				}
			}
		})
	}
}

func TestRuntimeRunLeaderCycleWritesRateLimitMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	resetUnix := now.Add(7 * time.Minute).Unix()
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			Summary: scrape.OrgSummary{
				RateLimitMinRemaining: 1234,
				RateLimitResetUnix:    resetUnix,
				SecondaryLimitHits:    2,
			},
		},
	})
	runtime.Now = func() time.Time { return now }

	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	testCases := []struct {
		name      string
		metric    string
		labels    map[string]string
		wantValue float64
	}{
		{
			name:      "exporter_rate_limit_remaining",
			metric:    "gh_exporter_github_rate_limit_remaining",
			labels:    map[string]string{"org": "org-a", "installation_id": "2"},
			wantValue: 1234,
		},
		{
			name:      "app_rate_limit_remaining",
			metric:    "gh_app_rate_limit_remaining",
			labels:    map[string]string{"org": "org-a", "installation_id": "2"},
			wantValue: 1234,
		},
		{
			name:      "rate_limit_reset_unix",
			metric:    "gh_exporter_github_rate_limit_reset_unixtime",
			labels:    map[string]string{"org": "org-a", "installation_id": "2"},
			wantValue: float64(resetUnix),
		},
		{
			name:      "secondary_limit_hits",
			metric:    "gh_exporter_github_secondary_limit_hits_total",
			labels:    map[string]string{"org": "org-a"},
			wantValue: 2,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			point := findMetricWithLabels(snapshot, tc.metric, tc.labels)
			if point == nil {
				t.Fatalf("missing metric %s labels=%v", tc.metric, tc.labels)
			}
			if point.Value != tc.wantValue {
				t.Fatalf("%s value = %v, want %v", tc.metric, point.Value, tc.wantValue)
			}
		})
	}
}

func TestRuntimeRunLeaderCycleWritesLOCFallbackBudgetMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			Summary: scrape.OrgSummary{
				LOCFallbackBudgetHits: 3,
			},
		},
	})
	runtime.Now = func() time.Time { return now }

	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	point := findMetricWithLabels(snapshot, "gh_exporter_loc_fallback_budget_exhausted_total", map[string]string{
		"org": "org-a",
	})
	if point == nil {
		t.Fatalf("missing gh_exporter_loc_fallback_budget_exhausted_total")
	}
	if point.Value != 3 {
		t.Fatalf("gh_exporter_loc_fallback_budget_exhausted_total = %v, want 3", point.Value)
	}
}

func TestRuntimeRunLeaderCycleWritesGitHubRequestMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			Summary: scrape.OrgSummary{
				GitHubRequestTotals: map[string]int{
					"list_org_repos|2xx":          1,
					"get_contributor_stats|4xx":   2,
					"list_issue_comments|unknown": 3,
				},
			},
		},
	})
	runtime.Now = func() time.Time { return now }

	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	testCases := []struct {
		name      string
		labels    map[string]string
		wantValue float64
	}{
		{
			name:      "list_org_repos_2xx",
			labels:    map[string]string{"org": "org-a", "endpoint": "list_org_repos", "status_class": "2xx"},
			wantValue: 1,
		},
		{
			name:      "get_contributor_stats_4xx",
			labels:    map[string]string{"org": "org-a", "endpoint": "get_contributor_stats", "status_class": "4xx"},
			wantValue: 2,
		},
		{
			name:      "list_issue_comments_unknown",
			labels:    map[string]string{"org": "org-a", "endpoint": "list_issue_comments", "status_class": "unknown"},
			wantValue: 3,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			point := findMetricWithLabels(snapshot, "gh_exporter_github_requests_total", tc.labels)
			if point == nil {
				t.Fatalf("missing gh_exporter_github_requests_total labels=%v", tc.labels)
			}
			if point.Value != tc.wantValue {
				t.Fatalf("value = %v, want %v", point.Value, tc.wantValue)
			}
		})
	}
}

func TestRuntimeRunLeaderCycleWritesScrapeDurationAndQueueAgeMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0)
	runtime := NewRuntime(cfg, &fakeOrgScraper{result: scrape.OrgResult{}})
	runtime.Now = func() time.Time { return now }
	runtime.queue = &queueWithOldestAge{age: 90 * time.Second}

	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	durationMetric := findMetricWithLabels(snapshot, "gh_exporter_scrape_duration_seconds", map[string]string{"org": "org-a"})
	if durationMetric == nil {
		t.Fatalf("missing gh_exporter_scrape_duration_seconds")
	}
	if durationMetric.Value < 0 {
		t.Fatalf("gh_exporter_scrape_duration_seconds = %v, want >= 0", durationMetric.Value)
	}

	queueMetric := findMetricWithLabels(snapshot, "gh_exporter_queue_oldest_message_age_seconds", map[string]string{"queue": "gh.backfill.jobs"})
	if queueMetric == nil {
		t.Fatalf("missing gh_exporter_queue_oldest_message_age_seconds")
	}
	if queueMetric.Value != 90 {
		t.Fatalf("gh_exporter_queue_oldest_message_age_seconds = %v, want 90", queueMetric.Value)
	}
}

func TestRuntimeRunLeaderCycleRunsStoreGC(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Store.Retention = time.Nanosecond
	now := time.Unix(1739836800, 0)

	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	runtime.Now = func() time.Time { return now }

	err := runtime.Store().UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, store.MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     1,
		UpdatedAt: now.Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("preload metric unexpected error: %v", err)
	}

	if err := runtime.RunLeaderCycle(context.Background()); err != nil {
		t.Fatalf("RunLeaderCycle() unexpected error: %v", err)
	}

	points := runtime.Store().Snapshot()
	if metric := findMetric(points, "gh_activity_commits_24h"); metric != nil {
		t.Fatalf("stale activity metric was not garbage-collected")
	}
}

type singleMessageQueue struct {
	message backfill.Message
}

type healthToggleStore struct {
	runtimeStore
	healthy bool
}

func (s *healthToggleStore) Healthy(_ context.Context) bool {
	return s.healthy
}

type healthToggleQueue struct {
	runtimeQueue
	healthy bool
}

func (q *healthToggleQueue) Healthy(_ context.Context) bool {
	return q.healthy
}

func (q *singleMessageQueue) Publish(_ backfill.Message) error {
	return nil
}

func (q *singleMessageQueue) Consume(
	_ context.Context,
	handler func(backfill.Message) error,
	_ time.Duration,
	_ func() time.Time,
) {
	_ = handler(q.message)
}

func (q *singleMessageQueue) Depth() int {
	return 0
}

func (q *singleMessageQueue) OldestMessageAge(_ time.Time) time.Duration {
	return 0
}

func (q *singleMessageQueue) Healthy(_ context.Context) bool {
	return true
}

type countingConsumeQueue struct {
	mu           sync.Mutex
	consumeCalls int
}

func (q *countingConsumeQueue) Publish(_ backfill.Message) error {
	return nil
}

func (q *countingConsumeQueue) Consume(
	ctx context.Context,
	_ func(backfill.Message) error,
	_ time.Duration,
	_ func() time.Time,
) {
	q.mu.Lock()
	q.consumeCalls++
	q.mu.Unlock()
	<-ctx.Done()
}

func (q *countingConsumeQueue) Depth() int {
	return 0
}

func (q *countingConsumeQueue) OldestMessageAge(_ time.Time) time.Duration {
	return 0
}

func (q *countingConsumeQueue) Healthy(_ context.Context) bool {
	return true
}

func (q *countingConsumeQueue) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumeCalls
}

type queueWithOldestAge struct {
	age time.Duration
}

func (q *queueWithOldestAge) Publish(_ backfill.Message) error {
	return nil
}

func (q *queueWithOldestAge) Consume(
	_ context.Context,
	_ func(backfill.Message) error,
	_ time.Duration,
	_ func() time.Time,
) {
}

func (q *queueWithOldestAge) Depth() int {
	return 0
}

func (q *queueWithOldestAge) OldestMessageAge(_ time.Time) time.Duration {
	return q.age
}

func (q *queueWithOldestAge) Healthy(_ context.Context) bool {
	return true
}

type orgCountingScraper struct {
	mu     sync.Mutex
	counts map[string]int
}

func (s *orgCountingScraper) ScrapeOrg(_ context.Context, org config.GitHubOrgConfig) (scrape.OrgResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = make(map[string]int)
	}
	s.counts[org.Org]++
	return scrape.OrgResult{}, nil
}

func (s *orgCountingScraper) count(org string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[org]
}

func TestRuntimeFollowerConsumesBackfillAndWritesScrapedMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0).UTC()
	runtime := NewRuntime(cfg, &fakeOrgScraper{
		result: scrape.OrgResult{
			Metrics: []store.MetricPoint{
				{
					Name: "gh_activity_commits_24h",
					Labels: map[string]string{
						"org":  "org-a",
						"repo": "repo-a",
						"user": "alice",
					},
					Value:     3,
					UpdatedAt: now,
				},
			},
		},
	})
	runtime.Now = func() time.Time { return now }
	runtime.queue = &singleMessageQueue{
		message: backfill.Message{
			JobID:       "job-1",
			Org:         "org-a",
			Repo:        "repo-a",
			WindowStart: now.Add(-time.Hour),
			WindowEnd:   now,
			Reason:      "activity_commits_failed",
			Attempt:     1,
			MaxAttempts: 7,
			CreatedAt:   now,
		},
	}

	runtime.runFollowerLoop(context.Background())

	snapshot := runtime.Store().Snapshot()
	if findMetric(snapshot, "gh_activity_commits_24h") == nil {
		t.Fatalf("missing gh_activity_commits_24h after follower backfill consume")
	}
}

func TestRuntimeFollowerUsesConfiguredConsumerCount(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Backfill.ConsumerCount = 3
	queue := &countingConsumeQueue{}
	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	runtime.queue = queue

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runtime.runFollowerLoop(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if queue.callCount() >= cfg.Backfill.ConsumerCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if queue.callCount() != cfg.Backfill.ConsumerCount {
		t.Fatalf("consume worker count = %d, want %d", queue.callCount(), cfg.Backfill.ConsumerCount)
	}
}

func TestRuntimeLeaderLoopHonorsPerOrgIntervals(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.GitHub.Orgs = []config.GitHubOrgConfig{
		{
			Org:            "org-fast",
			AppID:          1,
			InstallationID: 2,
			PrivateKeyPath: "/tmp/key-fast",
			ScrapeInterval: 40 * time.Millisecond,
		},
		{
			Org:            "org-slow",
			AppID:          3,
			InstallationID: 4,
			PrivateKeyPath: "/tmp/key-slow",
			ScrapeInterval: 120 * time.Millisecond,
		},
	}

	scraper := &orgCountingScraper{}
	runtime := NewRuntime(cfg, scraper)
	runtime.Now = time.Now

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime.StartLeader(ctx)
	time.Sleep(420 * time.Millisecond)
	runtime.StopLeader()
	time.Sleep(60 * time.Millisecond)

	fastCount := scraper.count("org-fast")
	slowCount := scraper.count("org-slow")

	if slowCount == 0 {
		t.Fatalf("org-slow scrape count = 0, want > 0")
	}
	if fastCount <= slowCount {
		t.Fatalf("org-fast scrape count = %d, org-slow scrape count = %d; want org-fast > org-slow", fastCount, slowCount)
	}
}

func findMetric(points []store.MetricPoint, name string) *store.MetricPoint {
	for i := range points {
		if points[i].Name == name {
			return &points[i]
		}
	}
	return nil
}

func findMetricWithLabels(points []store.MetricPoint, name string, labels map[string]string) *store.MetricPoint {
	for i := range points {
		point := points[i]
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
			return &points[i]
		}
	}
	return nil
}
