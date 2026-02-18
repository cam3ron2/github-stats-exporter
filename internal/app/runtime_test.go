package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/health"
	"github.com/cam3ron2/github-stats/internal/scrape"
	"github.com/cam3ron2/github-stats/internal/store"
)

type fakeOrgScraper struct {
	result  scrape.OrgResult
	err     error
	results []scrape.OrgResult
	errs    []error
	calls   int
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
