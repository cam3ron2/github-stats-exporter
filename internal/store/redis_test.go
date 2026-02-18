package store

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeRedisClient struct {
	now         time.Time
	hashes      map[string]map[string]string
	sets        map[string]map[string]struct{}
	stringLocks map[string]string
	expiresAt   map[string]time.Time
}

func newFakeRedisClient(now time.Time) *fakeRedisClient {
	return &fakeRedisClient{
		now:         now,
		hashes:      make(map[string]map[string]string),
		sets:        make(map[string]map[string]struct{}),
		stringLocks: make(map[string]string),
		expiresAt:   make(map[string]time.Time),
	}
}

func (c *fakeRedisClient) Advance(duration time.Duration) {
	c.now = c.now.Add(duration)
	c.purgeAllExpired()
}

func (c *fakeRedisClient) SIsMember(_ context.Context, key string, member any) *redis.BoolCmd {
	c.purgeIfExpired(key)
	set := c.sets[key]
	_, ok := set[fmt.Sprint(member)]
	return redis.NewBoolResult(ok, nil)
}

func (c *fakeRedisClient) SCard(_ context.Context, key string) *redis.IntCmd {
	c.purgeIfExpired(key)
	return redis.NewIntResult(int64(len(c.sets[key])), nil)
}

func (c *fakeRedisClient) HSet(_ context.Context, key string, values ...any) *redis.IntCmd {
	c.purgeIfExpired(key)
	if len(values) != 1 {
		return redis.NewIntResult(0, fmt.Errorf("unsupported HSet argument format"))
	}

	fieldMap, ok := values[0].(map[string]any)
	if !ok {
		return redis.NewIntResult(0, fmt.Errorf("unsupported HSet value type"))
	}
	if _, exists := c.hashes[key]; !exists {
		c.hashes[key] = make(map[string]string)
	}

	changed := int64(0)
	for field, value := range fieldMap {
		if _, exists := c.hashes[key][field]; !exists {
			changed++
		}
		c.hashes[key][field] = fmt.Sprint(value)
	}
	return redis.NewIntResult(changed, nil)
}

func (c *fakeRedisClient) SAdd(_ context.Context, key string, members ...any) *redis.IntCmd {
	c.purgeIfExpired(key)
	if _, exists := c.sets[key]; !exists {
		c.sets[key] = make(map[string]struct{})
	}

	added := int64(0)
	for _, member := range members {
		memberKey := fmt.Sprint(member)
		if _, exists := c.sets[key][memberKey]; exists {
			continue
		}
		c.sets[key][memberKey] = struct{}{}
		added++
	}
	return redis.NewIntResult(added, nil)
}

func (c *fakeRedisClient) ExpireAt(_ context.Context, key string, tm time.Time) *redis.BoolCmd {
	if !c.keyExists(key) {
		return redis.NewBoolResult(false, nil)
	}
	c.expiresAt[key] = tm
	return redis.NewBoolResult(true, nil)
}

func (c *fakeRedisClient) SetNX(_ context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	c.purgeIfExpired(key)
	if _, exists := c.stringLocks[key]; exists {
		return redis.NewBoolResult(false, nil)
	}

	c.stringLocks[key] = fmt.Sprint(value)
	if expiration > 0 {
		c.expiresAt[key] = c.now.Add(expiration)
	}
	return redis.NewBoolResult(true, nil)
}

func (c *fakeRedisClient) SMembers(_ context.Context, key string) *redis.StringSliceCmd {
	c.purgeIfExpired(key)
	members := make([]string, 0, len(c.sets[key]))
	for member := range c.sets[key] {
		members = append(members, member)
	}
	sort.Strings(members)
	return redis.NewStringSliceResult(members, nil)
}

func (c *fakeRedisClient) HGetAll(_ context.Context, key string) *redis.MapStringStringCmd {
	c.purgeIfExpired(key)
	if !c.keyExists(key) {
		return redis.NewMapStringStringResult(map[string]string{}, nil)
	}
	return redis.NewMapStringStringResult(maps.Clone(c.hashes[key]), nil)
}

func (c *fakeRedisClient) Exists(_ context.Context, keys ...string) *redis.IntCmd {
	exists := int64(0)
	for _, key := range keys {
		c.purgeIfExpired(key)
		if c.keyExists(key) {
			exists++
		}
	}
	return redis.NewIntResult(exists, nil)
}

func (c *fakeRedisClient) SRem(_ context.Context, key string, members ...any) *redis.IntCmd {
	c.purgeIfExpired(key)
	set := c.sets[key]
	removed := int64(0)
	for _, member := range members {
		memberKey := fmt.Sprint(member)
		if _, exists := set[memberKey]; !exists {
			continue
		}
		delete(set, memberKey)
		removed++
	}
	if len(set) == 0 {
		delete(c.sets, key)
	}
	return redis.NewIntResult(removed, nil)
}

func (c *fakeRedisClient) keyExists(key string) bool {
	if _, exists := c.hashes[key]; exists {
		return true
	}
	if _, exists := c.sets[key]; exists {
		return true
	}
	if _, exists := c.stringLocks[key]; exists {
		return true
	}
	return false
}

func (c *fakeRedisClient) purgeIfExpired(key string) {
	expiry, ok := c.expiresAt[key]
	if !ok || c.now.Before(expiry) {
		return
	}

	delete(c.expiresAt, key)
	delete(c.hashes, key)
	delete(c.sets, key)
	delete(c.stringLocks, key)
}

func (c *fakeRedisClient) purgeAllExpired() {
	for key := range c.expiresAt {
		c.purgeIfExpired(key)
	}
}

func newRedisStoreForTest(t *testing.T, now time.Time, retention time.Duration, maxSeries int) (*RedisStore, *fakeRedisClient) {
	t.Helper()

	client := newFakeRedisClient(now)
	store := newRedisStoreFromCommander(client, nil, RedisStoreConfig{
		Namespace: "github-stats-test",
		Retention: retention,
		MaxSeries: maxSeries,
	})
	return store, client
}

func TestRedisStoreUpsertMetricWriteGuards(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, _ := newRedisStoreForTest(t, now, 24*time.Hour, 100)

	testCases := []struct {
		name    string
		role    RuntimeRole
		source  WriteSource
		point   MetricPoint
		wantErr bool
	}{
		{
			name:   "leader_can_write_leader_scrape_metrics",
			role:   RoleLeader,
			source: SourceLeaderScrape,
			point: MetricPoint{
				Name:      "gh_activity_commits_24h",
				Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
				Value:     10,
				UpdatedAt: now,
			},
			wantErr: false,
		},
		{
			name:   "follower_can_write_worker_backfill_metrics",
			role:   RoleFollower,
			source: SourceWorkerBackfill,
			point: MetricPoint{
				Name:      "gh_activity_commits_24h",
				Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
				Value:     10,
				UpdatedAt: now,
			},
			wantErr: false,
		},
		{
			name:   "follower_cannot_write_leader_scrape_metrics",
			role:   RoleFollower,
			source: SourceLeaderScrape,
			point: MetricPoint{
				Name:      "gh_activity_commits_24h",
				Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
				Value:     10,
				UpdatedAt: now,
			},
			wantErr: true,
		},
		{
			name:   "leader_cannot_write_worker_backfill_metrics",
			role:   RoleLeader,
			source: SourceWorkerBackfill,
			point: MetricPoint{
				Name:      "gh_activity_commits_24h",
				Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
				Value:     10,
				UpdatedAt: now,
			},
			wantErr: true,
		},
		{
			name:   "metric_name_required",
			role:   RoleLeader,
			source: SourceLeaderScrape,
			point: MetricPoint{
				Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
				Value:     10,
				UpdatedAt: now,
			},
			wantErr: true,
		},
		{
			name:   "updated_time_required",
			role:   RoleLeader,
			source: SourceLeaderScrape,
			point: MetricPoint{
				Name:   "gh_activity_commits_24h",
				Labels: map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
				Value:  10,
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := store.UpsertMetric(tc.role, tc.source, tc.point)
			if tc.wantErr && err == nil {
				t.Fatalf("UpsertMetric() expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("UpsertMetric() unexpected error: %v", err)
			}
		})
	}
}

func TestRedisStoreMaxSeriesBudget(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, _ := newRedisStoreForTest(t, now, 24*time.Hour, 1)

	err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     10,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertMetric(first) unexpected error: %v", err)
	}

	err = store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "bob"},
		Value:     10,
		UpdatedAt: now,
	})
	if err == nil {
		t.Fatalf("UpsertMetric(second) expected max series error, got nil")
	}
}

func TestRedisStoreAcquireLocks(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, client := newRedisStoreForTest(t, now, 24*time.Hour, 100)

	if !store.AcquireJobLock("job-1", 10*time.Minute, now) {
		t.Fatalf("AcquireJobLock(first) = false, want true")
	}
	if store.AcquireJobLock("job-1", 10*time.Minute, now.Add(5*time.Minute)) {
		t.Fatalf("AcquireJobLock(second) = true, want false")
	}

	client.Advance(11 * time.Minute)
	if !store.AcquireJobLock("job-1", 10*time.Minute, now.Add(11*time.Minute)) {
		t.Fatalf("AcquireJobLock(after expiry) = false, want true")
	}

	if !store.AcquireDedupLock("dedup-1", 10*time.Minute, now) {
		t.Fatalf("AcquireDedupLock(first) = false, want true")
	}
	if store.Acquire("dedup-1", 10*time.Minute, now.Add(2*time.Minute)) {
		t.Fatalf("Acquire(alias) = true, want false")
	}
}

func TestRedisStoreSnapshotAndGC(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, client := newRedisStoreForTest(t, now, 1*time.Hour, 100)

	err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     10,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertMetric(alice) unexpected error: %v", err)
	}
	err = store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "bob"},
		Value:     5,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertMetric(bob) unexpected error: %v", err)
	}

	snapshot := store.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("Snapshot() len = %d, want 2", len(snapshot))
	}
	if snapshot[0].Labels["user"] != "alice" {
		t.Fatalf("Snapshot()[0].user = %q, want alice", snapshot[0].Labels["user"])
	}
	if snapshot[1].Labels["user"] != "bob" {
		t.Fatalf("Snapshot()[1].user = %q, want bob", snapshot[1].Labels["user"])
	}

	client.Advance(2 * time.Hour)
	store.GC(now.Add(2 * time.Hour))

	snapshot = store.Snapshot()
	if len(snapshot) != 0 {
		t.Fatalf("Snapshot() after expiration len = %d, want 0", len(snapshot))
	}

	indexCount, err := client.SCard(context.Background(), store.metricsIndexKey()).Result()
	if err != nil {
		t.Fatalf("SCard(index) unexpected error: %v", err)
	}
	if indexCount != 0 {
		t.Fatalf("index member count = %d, want 0", indexCount)
	}
}
