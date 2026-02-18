package exporter

import (
	"errors"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/store"
)

type fakeCacheSource struct {
	mu sync.Mutex

	snapshots     [][]store.MetricPoint
	snapshotCalls int

	cursor     uint64
	deltas     map[uint64]store.SnapshotDelta
	deltaErr   error
	deltaCalls int
}

func (f *fakeCacheSource) Snapshot() []store.MetricPoint {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.snapshotCalls++
	if len(f.snapshots) == 0 {
		return nil
	}
	index := f.snapshotCalls - 1
	if index >= len(f.snapshots) {
		index = len(f.snapshots) - 1
	}
	return clonePoints(f.snapshots[index])
}

func (f *fakeCacheSource) SnapshotCursor() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cursor, nil
}

func (f *fakeCacheSource) SnapshotDelta(cursor uint64) (store.SnapshotDelta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deltaCalls++
	if f.deltaErr != nil {
		return store.SnapshotDelta{}, f.deltaErr
	}
	delta, ok := f.deltas[cursor]
	if !ok {
		return store.SnapshotDelta{NextCursor: cursor}, nil
	}
	return delta, nil
}

func (f *fakeCacheSource) stats() (snapshotCalls int, deltaCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshotCalls, f.deltaCalls
}

func TestCachedSnapshotReaderFullMode(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	source := &fakeCacheSource{
		snapshots: [][]store.MetricPoint{
			{
				{
					Name:      "gh_activity_commits_24h",
					Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
					Value:     1,
					UpdatedAt: now,
				},
			},
			{
				{
					Name:      "gh_activity_commits_24h",
					Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
					Value:     2,
					UpdatedAt: now.Add(time.Minute),
				},
			},
		},
	}

	readerTime := now
	reader := NewCachedSnapshotReader(source, CacheConfig{
		Mode:            "full",
		RefreshInterval: time.Minute,
		Now:             func() time.Time { return readerTime },
	})

	first := reader.Snapshot()
	if len(first) != 1 || first[0].Value != 1 {
		t.Fatalf("first snapshot = %#v, want value=1", first)
	}

	second := reader.Snapshot()
	if len(second) != 1 || second[0].Value != 1 {
		t.Fatalf("second snapshot = %#v, want cached value=1", second)
	}

	readerTime = readerTime.Add(2 * time.Minute)
	third := reader.Snapshot()
	if len(third) != 1 || third[0].Value != 2 {
		t.Fatalf("third snapshot = %#v, want refreshed value=2", third)
	}

	snapshotCalls, deltaCalls := source.stats()
	if snapshotCalls != 2 {
		t.Fatalf("snapshot calls = %d, want 2", snapshotCalls)
	}
	if deltaCalls != 0 {
		t.Fatalf("delta calls = %d, want 0", deltaCalls)
	}
}

func TestCachedSnapshotReaderIncrementalMode(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	source := &fakeCacheSource{
		snapshots: [][]store.MetricPoint{
			{
				{
					Name:      "gh_activity_commits_24h",
					Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
					Value:     1,
					UpdatedAt: now,
				},
			},
		},
		cursor: 10,
		deltas: map[uint64]store.SnapshotDelta{
			10: {
				NextCursor: 11,
				Events: []store.SnapshotDeltaEvent{
					{
						Point: store.MetricPoint{
							Name:      "gh_activity_commits_24h",
							Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
							Value:     2,
							UpdatedAt: now.Add(time.Minute),
						},
					},
					{
						Point: store.MetricPoint{
							Name:      "gh_activity_commits_24h",
							Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "bob"},
							Value:     3,
							UpdatedAt: now.Add(time.Minute),
						},
					},
				},
			},
		},
	}

	readerTime := now
	reader := NewCachedSnapshotReader(source, CacheConfig{
		Mode:            "incremental",
		RefreshInterval: time.Minute,
		Now:             func() time.Time { return readerTime },
	})

	first := reader.Snapshot()
	if len(first) != 1 || first[0].Value != 1 {
		t.Fatalf("first snapshot = %#v, want value=1", first)
	}

	readerTime = readerTime.Add(2 * time.Minute)
	second := reader.Snapshot()
	if len(second) != 2 {
		t.Fatalf("second snapshot len = %d, want 2", len(second))
	}

	found := map[string]float64{}
	for _, point := range second {
		found[point.Labels["user"]] = point.Value
	}
	if found["alice"] != 2 {
		t.Fatalf("alice value = %v, want 2", found["alice"])
	}
	if found["bob"] != 3 {
		t.Fatalf("bob value = %v, want 3", found["bob"])
	}

	snapshotCalls, deltaCalls := source.stats()
	if snapshotCalls != 1 {
		t.Fatalf("snapshot calls = %d, want 1", snapshotCalls)
	}
	if deltaCalls != 1 {
		t.Fatalf("delta calls = %d, want 1", deltaCalls)
	}
}

func TestCachedSnapshotReaderIncrementalFallbackToFull(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	source := &fakeCacheSource{
		snapshots: [][]store.MetricPoint{
			{
				{
					Name:      "gh_activity_commits_24h",
					Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
					Value:     1,
					UpdatedAt: now,
				},
			},
			{
				{
					Name:      "gh_activity_commits_24h",
					Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
					Value:     5,
					UpdatedAt: now.Add(time.Minute),
				},
			},
		},
		cursor:   1,
		deltaErr: errors.New("delta unavailable"),
	}

	readerTime := now
	reader := NewCachedSnapshotReader(source, CacheConfig{
		Mode:            "incremental",
		RefreshInterval: time.Minute,
		Now:             func() time.Time { return readerTime },
	})

	first := reader.Snapshot()
	if len(first) != 1 || first[0].Value != 1 {
		t.Fatalf("first snapshot = %#v, want value=1", first)
	}

	readerTime = readerTime.Add(2 * time.Minute)
	second := reader.Snapshot()
	if len(second) != 1 || second[0].Value != 5 {
		t.Fatalf("second snapshot = %#v, want fallback full value=5", second)
	}

	snapshotCalls, deltaCalls := source.stats()
	if snapshotCalls != 2 {
		t.Fatalf("snapshot calls = %d, want 2", snapshotCalls)
	}
	if deltaCalls != 1 {
		t.Fatalf("delta calls = %d, want 1", deltaCalls)
	}
}

func TestClonePointCopiesLabels(t *testing.T) {
	t.Parallel()

	point := store.MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a"},
		Value:     1,
		UpdatedAt: time.Unix(1739836800, 0),
	}

	cloned := clonePoint(point)
	cloned.Labels["org"] = "org-b"
	if point.Labels["org"] != "org-a" {
		t.Fatalf("clonePoint mutated source labels: %#v", point.Labels)
	}
	if !maps.Equal(point.Labels, map[string]string{"org": "org-a"}) {
		t.Fatalf("source labels changed after clone mutation")
	}
}
