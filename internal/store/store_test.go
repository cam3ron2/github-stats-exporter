package store

import (
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
