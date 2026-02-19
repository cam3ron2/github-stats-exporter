package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/backfill"
	"github.com/cam3ron2/github-stats-exporter/internal/scrape"
	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

func TestRuntimeRunCooldownCycleForOrgMetrics(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Backfill.CoalesceWindow = 0
	cfg.Backfill.MaxEnqueuesPerOrgPerMinute = 1

	now := time.Unix(1739836800, 0).UTC()
	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	runtime.Now = func() time.Time { return now }

	org := cfg.GitHub.Orgs[0]
	if err := runtime.runCooldownCycleForOrg(now, org); err != nil {
		t.Fatalf("runCooldownCycleForOrg(first) unexpected error: %v", err)
	}
	if runtime.QueueDepth() != 1 {
		t.Fatalf("QueueDepth() after first cooldown run = %d, want 1", runtime.QueueDepth())
	}

	second := now.Add(1 * time.Second)
	if err := runtime.runCooldownCycleForOrg(second, org); err != nil {
		t.Fatalf("runCooldownCycleForOrg(second) unexpected error: %v", err)
	}

	// Same timestamp as second run forces dedup suppression.
	if err := runtime.runCooldownCycleForOrg(second, org); err != nil {
		t.Fatalf("runCooldownCycleForOrg(third) unexpected error: %v", err)
	}

	snapshot := runtime.Store().Snapshot()
	if point := findMetricWithLabels(snapshot, "gh_exporter_backfill_jobs_enqueued_total", map[string]string{
		"org":    "org-a",
		"reason": "github_unhealthy",
	}); point == nil {
		t.Fatalf("missing enqueue metric for github_unhealthy")
	}
	if point := findMetricWithLabels(snapshot, "gh_exporter_backfill_enqueues_dropped_total", map[string]string{
		"org":    "org-a",
		"reason": "org_rate_cap",
	}); point == nil {
		t.Fatalf("missing org_rate_cap drop metric")
	}
	if point := findMetricWithLabels(snapshot, "gh_exporter_backfill_jobs_deduped_total", map[string]string{
		"org":    "org-a",
		"reason": "github_unhealthy",
	}); point == nil {
		t.Fatalf("missing deduped metric for github_unhealthy")
	}
}

func TestRuntimeProcessBackfillMessageBranches(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0).UTC()
	message := backfill.Message{
		JobID:       "job-1",
		Org:         "org-a",
		Repo:        "repo-a",
		WindowStart: now.Add(-time.Hour),
		WindowEnd:   now,
		Reason:      "activity_commits_failed",
		Attempt:     1,
		MaxAttempts: 7,
		CreatedAt:   now,
	}

	testCases := []struct {
		name             string
		storeWrapper     func(runtimeStore) runtimeStore
		scraper          *fakeOrgScraper
		wantErrSubstr    string
		wantProcessLabel string
	}{
		{
			name: "duplicate_lock_skips_message",
			storeWrapper: func(base runtimeStore) runtimeStore {
				return &lockControlledStore{
					runtimeStore: base,
					acquireLock:  false,
				}
			},
			scraper:          &fakeOrgScraper{},
			wantErrSubstr:    "",
			wantProcessLabel: "",
		},
		{
			name: "missed_windows_returns_error",
			storeWrapper: func(base runtimeStore) runtimeStore {
				return base
			},
			scraper: &fakeOrgScraper{
				result: scrape.OrgResult{
					MissedWindow: []scrape.MissedWindow{{
						Org:         "org-a",
						Repo:        "repo-a",
						WindowStart: now.Add(-time.Hour),
						WindowEnd:   now,
						Reason:      "scrape_error",
					}},
				},
			},
			wantErrSubstr:    "missed windows",
			wantProcessLabel: "failed",
		},
		{
			name: "upsert_failure_returns_error",
			storeWrapper: func(base runtimeStore) runtimeStore {
				return &failingMetricUpsertStore{runtimeStore: base}
			},
			scraper: &fakeOrgScraper{
				result: scrape.OrgResult{
					Metrics: []store.MetricPoint{{
						Name: "gh_activity_commits_24h",
						Labels: map[string]string{
							"org":  "org-a",
							"repo": "repo-a",
							"user": "alice",
						},
						Value:     1,
						UpdatedAt: now,
					}},
				},
			},
			wantErrSubstr:    "forced upsert failure",
			wantProcessLabel: "",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			runtime := NewRuntime(cfg, tc.scraper)
			runtime.Now = func() time.Time { return now }
			runtime.queue = &queueWithOldestAge{age: 0}
			runtime.store = tc.storeWrapper(runtime.store)

			err := runtime.processBackfillMessage(context.Background(), message, time.Hour)
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Fatalf("processBackfillMessage() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("processBackfillMessage() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("error = %q, missing %q", err.Error(), tc.wantErrSubstr)
			}

			if tc.wantProcessLabel != "" {
				snapshot := runtime.store.Snapshot()
				if point := findMetricWithLabels(snapshot, "gh_exporter_backfill_jobs_processed_total", map[string]string{
					"org":    "org-a",
					"repo":   "repo-a",
					"result": tc.wantProcessLabel,
				}); point == nil {
					t.Fatalf("missing backfill processed metric result=%q", tc.wantProcessLabel)
				}
			}
		})
	}
}

func TestRuntimeHelperFunctions(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	runtime := NewRuntime(cfg, &fakeOrgScraper{})

	if got := runtime.backfillQueueName(); got != defaultBackfillQueueName {
		t.Fatalf("backfillQueueName() = %q, want %s", got, defaultBackfillQueueName)
	}
	cfg.AMQP.Queue = "custom.queue"
	if got := runtime.backfillQueueName(); got != "custom.queue" {
		t.Fatalf("backfillQueueName(custom) = %q, want custom.queue", got)
	}

	if got := runtime.installationIDLabel("org-a"); got != "2" {
		t.Fatalf("installationIDLabel(org-a) = %q, want 2", got)
	}
	if got := runtime.installationIDLabel("org-missing"); got != "unknown" {
		t.Fatalf("installationIDLabel(org-missing) = %q, want unknown", got)
	}

	scopeTests := []struct {
		repo      string
		wantScope string
		wantOK    bool
	}{
		{repo: "__copilot__org__28d", wantScope: "org", wantOK: true},
		{repo: "__copilot__enterprise_users__28d", wantScope: "enterprise_users", wantOK: true},
		{repo: "__copilot____28d", wantScope: "", wantOK: false},
		{repo: "repo-a", wantScope: "", wantOK: false},
	}
	for _, tc := range scopeTests {
		tc := tc
		t.Run(tc.repo, func(t *testing.T) {
			t.Parallel()
			scope, ok := copilotScopeFromRepoKey(tc.repo)
			if ok != tc.wantOK {
				t.Fatalf("copilotScopeFromRepoKey(%q) ok=%t, want %t", tc.repo, ok, tc.wantOK)
			}
			if scope != tc.wantScope {
				t.Fatalf("copilotScopeFromRepoKey(%q) scope=%q, want %q", tc.repo, scope, tc.wantScope)
			}
		})
	}
}

func TestRuntimeMetricBestEffortHelpersDoNotPanicOnStoreErrors(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	now := time.Unix(1739836800, 0).UTC()
	runtime := NewRuntime(cfg, &fakeOrgScraper{})
	runtime.store = &failingMetricUpsertStore{runtimeStore: runtime.store}

	runtime.recordLeaderCycleMetricBestEffort(now, "gh_exporter_dependency_health", 1, map[string]string{"dependency": "redis"})
	runtime.recordFollowerMetricBestEffort(now, "gh_exporter_backfill_jobs_processed_total", 1, map[string]string{
		"org":    "org-a",
		"repo":   "repo-a",
		"result": "processed",
	})
}

type lockControlledStore struct {
	runtimeStore
	acquireLock bool
}

func (s *lockControlledStore) AcquireJobLock(_ string, _ time.Duration, _ time.Time) bool {
	return s.acquireLock
}

type failingMetricUpsertStore struct {
	runtimeStore
}

func (s *failingMetricUpsertStore) UpsertMetric(
	_ store.RuntimeRole,
	_ store.WriteSource,
	point store.MetricPoint,
) error {
	if strings.HasPrefix(point.Name, "gh_activity_") || strings.HasPrefix(point.Name, "gh_exporter_") {
		return errors.New("forced upsert failure")
	}
	return nil
}
