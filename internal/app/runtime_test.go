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
	result scrape.OrgResult
	err    error
}

func (s *fakeOrgScraper) ScrapeOrg(_ context.Context, _ config.GitHubOrgConfig) (scrape.OrgResult, error) {
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

func findMetric(points []store.MetricPoint, name string) *store.MetricPoint {
	for i := range points {
		if points[i].Name == name {
			return &points[i]
		}
	}
	return nil
}
