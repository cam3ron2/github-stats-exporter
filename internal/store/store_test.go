package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMemoryStoreUpsertMetricWriteGuards(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(24*time.Hour, 1000)
	now := time.Unix(1739836800, 0)

	testCases := []struct {
		name    string
		role    RuntimeRole
		source  WriteSource
		wantErr bool
	}{
		{
			name:    "leader_can_write_leader_scrape_metrics",
			role:    RoleLeader,
			source:  SourceLeaderScrape,
			wantErr: false,
		},
		{
			name:    "follower_can_write_worker_backfill_metrics",
			role:    RoleFollower,
			source:  SourceWorkerBackfill,
			wantErr: false,
		},
		{
			name:    "follower_cannot_write_leader_scrape_metrics",
			role:    RoleFollower,
			source:  SourceLeaderScrape,
			wantErr: true,
		},
		{
			name:    "leader_cannot_write_worker_backfill_metrics",
			role:    RoleLeader,
			source:  SourceWorkerBackfill,
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := store.UpsertMetric(tc.role, tc.source, MetricPoint{
				Name: "gh_activity_commits_24h",
				Labels: map[string]string{
					"org":  "org-a",
					"repo": "repo-a",
					"user": "alice",
				},
				Value:     10,
				UpdatedAt: now,
			})

			if tc.wantErr && err == nil {
				t.Fatalf("UpsertMetric() expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("UpsertMetric() unexpected error: %v", err)
			}
		})
	}
}

func TestMemoryStoreAcquireJobLock(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(24*time.Hour, 1000)
	now := time.Unix(1739836800, 0)

	acquired := store.AcquireJobLock("job-1", 10*time.Minute, now)
	if !acquired {
		t.Fatalf("AcquireJobLock() first acquisition = false, want true")
	}

	acquired = store.AcquireJobLock("job-1", 10*time.Minute, now.Add(5*time.Minute))
	if acquired {
		t.Fatalf("AcquireJobLock() second acquisition before expiry = true, want false")
	}

	acquired = store.AcquireJobLock("job-1", 10*time.Minute, now.Add(11*time.Minute))
	if !acquired {
		t.Fatalf("AcquireJobLock() acquisition after expiry = false, want true")
	}
}

func TestMemoryStoreGC(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(1*time.Hour, 1000)
	now := time.Unix(1739836800, 0)

	err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name: "gh_activity_commits_24h",
		Labels: map[string]string{
			"org":  "org-a",
			"repo": "repo-a",
			"user": "alice",
		},
		Value:     10,
		UpdatedAt: now.Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	err = store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name: "gh_activity_commits_24h",
		Labels: map[string]string{
			"org":  "org-a",
			"repo": "repo-a",
			"user": "bob",
		},
		Value:     5,
		UpdatedAt: now.Add(-30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	store.GC(now)

	series := store.Snapshot()
	if len(series) != 1 {
		t.Fatalf("Snapshot() len = %d, want 1", len(series))
	}
	if series[0].Labels["user"] != "bob" {
		t.Fatalf("remaining user = %q, want %q", series[0].Labels["user"], "bob")
	}
}

func TestMemoryStoreCheckpointLifecycle(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(time.Hour, 1000)
	now := time.Unix(1739836800, 0)

	if err := store.SetCheckpoint("org-a", "repo-a", now); err != nil {
		t.Fatalf("SetCheckpoint() unexpected error: %v", err)
	}

	checkpoint, found, err := store.GetCheckpoint("org-a", "repo-a")
	if err != nil {
		t.Fatalf("GetCheckpoint() unexpected error: %v", err)
	}
	if !found {
		t.Fatalf("GetCheckpoint() found = false, want true")
	}
	if !checkpoint.Equal(now) {
		t.Fatalf("GetCheckpoint() = %s, want %s", checkpoint, now)
	}

	store.GC(now.Add(2 * time.Hour))
	_, found, err = store.GetCheckpoint("org-a", "repo-a")
	if err != nil {
		t.Fatalf("GetCheckpoint(after gc) unexpected error: %v", err)
	}
	if found {
		t.Fatalf("GetCheckpoint(after gc) found = true, want false")
	}
}

func TestMemoryStoreUpsertMetricValidationAndLimits(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name      string
		store     *MemoryStore
		role      RuntimeRole
		source    WriteSource
		point     MetricPoint
		primeWith []MetricPoint
		wantErr   string
	}{
		{
			name:   "unknown_role_rejected",
			store:  NewMemoryStore(24*time.Hour, 1000),
			role:   RuntimeRole("other"),
			source: SourceLeaderScrape,
			point: MetricPoint{
				Name:      "gh_activity_commits_24h",
				Labels:    map[string]string{"org": "org-a"},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr: "unknown role",
		},
		{
			name:   "name_required",
			store:  NewMemoryStore(24*time.Hour, 1000),
			role:   RoleLeader,
			source: SourceLeaderScrape,
			point: MetricPoint{
				Labels:    map[string]string{"org": "org-a"},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr: "metric name is required",
		},
		{
			name:   "updated_time_required",
			store:  NewMemoryStore(24*time.Hour, 1000),
			role:   RoleLeader,
			source: SourceLeaderScrape,
			point: MetricPoint{
				Name:   "gh_activity_commits_24h",
				Labels: map[string]string{"org": "org-a"},
				Value:  1,
			},
			wantErr: "metric updated time is required",
		},
		{
			name:   "max_series_budget_exceeded",
			store:  NewMemoryStore(24*time.Hour, 1),
			role:   RoleLeader,
			source: SourceLeaderScrape,
			primeWith: []MetricPoint{
				{
					Name:      "metric_one",
					Labels:    map[string]string{"org": "org-a"},
					Value:     1,
					UpdatedAt: now,
				},
			},
			point: MetricPoint{
				Name:      "metric_two",
				Labels:    map[string]string{"org": "org-a"},
				Value:     2,
				UpdatedAt: now,
			},
			wantErr: "max series budget exceeded",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, point := range tc.primeWith {
				if err := tc.store.UpsertMetric(RoleLeader, SourceLeaderScrape, point); err != nil {
					t.Fatalf("prime UpsertMetric() unexpected error: %v", err)
				}
			}

			err := tc.store.UpsertMetric(tc.role, tc.source, tc.point)
			if err == nil {
				t.Fatalf("UpsertMetric() expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("UpsertMetric() error = %q, missing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestMemoryStoreAcquireDedupLockAndAcquireAlias(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(24*time.Hour, 1000)
	now := time.Unix(1739836800, 0)

	if ok := store.AcquireDedupLock("key-1", 10*time.Minute, now); !ok {
		t.Fatalf("AcquireDedupLock(first) = false, want true")
	}
	if ok := store.Acquire("key-1", 10*time.Minute, now.Add(5*time.Minute)); ok {
		t.Fatalf("Acquire(alias before expiry) = true, want false")
	}
	if ok := store.Acquire("key-1", 10*time.Minute, now.Add(11*time.Minute)); !ok {
		t.Fatalf("Acquire(alias after expiry) = false, want true")
	}
}

func TestMemoryStoreHealthyAndCheckpointValidation(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store := NewMemoryStore(24*time.Hour, 1000)
	if !store.Healthy(context.Background()) {
		t.Fatalf("Healthy() = false, want true")
	}

	var nilStore *MemoryStore
	if nilStore.Healthy(context.Background()) {
		t.Fatalf("nil Healthy() = true, want false")
	}

	checkpointErrCases := []struct {
		name     string
		store    *MemoryStore
		org      string
		repo     string
		at       time.Time
		wantErr  string
		isGetter bool
	}{
		{
			name:    "set_nil_store",
			store:   nilStore,
			org:     "org-a",
			repo:    "repo-a",
			at:      now,
			wantErr: "memory store is not initialized",
		},
		{
			name:    "set_org_required",
			store:   store,
			org:     " ",
			repo:    "repo-a",
			at:      now,
			wantErr: "org is required",
		},
		{
			name:    "set_repo_required",
			store:   store,
			org:     "org-a",
			repo:    " ",
			at:      now,
			wantErr: "repo is required",
		},
		{
			name:    "set_time_required",
			store:   store,
			org:     "org-a",
			repo:    "repo-a",
			at:      time.Time{},
			wantErr: "checkpoint time is required",
		},
		{
			name:     "get_nil_store",
			store:    nilStore,
			org:      "org-a",
			repo:     "repo-a",
			wantErr:  "memory store is not initialized",
			isGetter: true,
		},
		{
			name:     "get_org_required",
			store:    store,
			org:      " ",
			repo:     "repo-a",
			wantErr:  "org is required",
			isGetter: true,
		},
		{
			name:     "get_repo_required",
			store:    store,
			org:      "org-a",
			repo:     " ",
			wantErr:  "repo is required",
			isGetter: true,
		},
	}

	for _, tc := range checkpointErrCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.isGetter {
				_, _, err := tc.store.GetCheckpoint(tc.org, tc.repo)
				if err == nil {
					t.Fatalf("GetCheckpoint() expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("GetCheckpoint() error = %q, missing %q", err.Error(), tc.wantErr)
				}
				return
			}

			err := tc.store.SetCheckpoint(tc.org, tc.repo, tc.at)
			if err == nil {
				t.Fatalf("SetCheckpoint() expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("SetCheckpoint() error = %q, missing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestMemoryStoreSnapshotReturnsLabelCopies(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(0, 1000)
	now := time.Unix(1739836800, 0)
	if err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"repo": "repo-a", "org": "org-a"},
		Value:     10,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	snapshot := store.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("Snapshot() len = %d, want 1", len(snapshot))
	}
	snapshot[0].Labels["org"] = "mutated"

	snapshotAgain := store.Snapshot()
	if got := snapshotAgain[0].Labels["org"]; got != "org-a" {
		t.Fatalf("Snapshot() labels were mutated, org = %q, want %q", got, "org-a")
	}
}
