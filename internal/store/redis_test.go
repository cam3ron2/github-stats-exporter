package store

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type fakeRedisClient struct {
	mu          sync.Mutex
	now         time.Time
	hashes      map[string]map[string]string
	sets        map[string]map[string]struct{}
	stringLocks map[string]string
	zsets       map[string]map[string]float64
	expiresAt   map[string]time.Time
}

func newFakeRedisClient(now time.Time) *fakeRedisClient {
	return &fakeRedisClient{
		now:         now,
		hashes:      make(map[string]map[string]string),
		sets:        make(map[string]map[string]struct{}),
		stringLocks: make(map[string]string),
		zsets:       make(map[string]map[string]float64),
		expiresAt:   make(map[string]time.Time),
	}
}

func (c *fakeRedisClient) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(duration)
	c.purgeAllExpiredLocked()
}

func (c *fakeRedisClient) SIsMember(_ context.Context, key string, member any) *redis.BoolCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	set := c.sets[key]
	_, ok := set[fmt.Sprint(member)]
	return redis.NewBoolResult(ok, nil)
}

func (c *fakeRedisClient) SCard(_ context.Context, key string) *redis.IntCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	return redis.NewIntResult(int64(len(c.sets[key])), nil)
}

func (c *fakeRedisClient) HSet(_ context.Context, key string, values ...any) *redis.IntCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
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
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.keyExistsLocked(key) {
		return redis.NewBoolResult(false, nil)
	}
	c.expiresAt[key] = tm
	return redis.NewBoolResult(true, nil)
}

func (c *fakeRedisClient) SetNX(_ context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	if _, exists := c.stringLocks[key]; exists {
		return redis.NewBoolResult(false, nil)
	}

	c.stringLocks[key] = fmt.Sprint(value)
	if expiration > 0 {
		c.expiresAt[key] = c.now.Add(expiration)
	}
	return redis.NewBoolResult(true, nil)
}

func (c *fakeRedisClient) Incr(_ context.Context, key string) *redis.IntCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	current := int64(0)
	if raw, exists := c.stringLocks[key]; exists {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return redis.NewIntResult(0, err)
		}
		current = parsed
	}
	current++
	c.stringLocks[key] = strconv.FormatInt(current, 10)
	return redis.NewIntResult(current, nil)
}

func (c *fakeRedisClient) Get(_ context.Context, key string) *redis.StringCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	value, exists := c.stringLocks[key]
	if !exists {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(value, nil)
}

func (c *fakeRedisClient) ZAdd(_ context.Context, key string, members ...redis.Z) *redis.IntCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	if _, exists := c.zsets[key]; !exists {
		c.zsets[key] = make(map[string]float64)
	}

	added := int64(0)
	for _, member := range members {
		memberKey := fmt.Sprint(member.Member)
		if _, exists := c.zsets[key][memberKey]; !exists {
			added++
		}
		c.zsets[key][memberKey] = member.Score
	}

	return redis.NewIntResult(added, nil)
}

func (c *fakeRedisClient) ZRangeByScoreWithScores(
	_ context.Context,
	key string,
	opt *redis.ZRangeBy,
) *redis.ZSliceCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	set := c.zsets[key]
	if len(set) == 0 {
		return redis.NewZSliceCmdResult(nil, nil)
	}

	min, minExclusive, err := parseRangeBoundary(opt.Min)
	if err != nil {
		return redis.NewZSliceCmdResult(nil, err)
	}
	max, maxExclusive, err := parseRangeBoundary(opt.Max)
	if err != nil {
		return redis.NewZSliceCmdResult(nil, err)
	}

	items := make([]redis.Z, 0, len(set))
	for member, score := range set {
		if !inScoreRange(score, min, minExclusive, max, maxExclusive) {
			continue
		}
		items = append(items, redis.Z{Member: member, Score: score})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Score == items[j].Score {
			return fmt.Sprint(items[i].Member) < fmt.Sprint(items[j].Member)
		}
		return items[i].Score < items[j].Score
	})
	return redis.NewZSliceCmdResult(items, nil)
}

func (c *fakeRedisClient) SMembers(_ context.Context, key string) *redis.StringSliceCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	members := make([]string, 0, len(c.sets[key]))
	for member := range c.sets[key] {
		members = append(members, member)
	}
	sort.Strings(members)
	return redis.NewStringSliceResult(members, nil)
}

func (c *fakeRedisClient) HGetAll(_ context.Context, key string) *redis.MapStringStringCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
	if !c.keyExistsLocked(key) {
		return redis.NewMapStringStringResult(map[string]string{}, nil)
	}
	return redis.NewMapStringStringResult(maps.Clone(c.hashes[key]), nil)
}

func (c *fakeRedisClient) Exists(_ context.Context, keys ...string) *redis.IntCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	exists := int64(0)
	for _, key := range keys {
		c.purgeIfExpiredLocked(key)
		if c.keyExistsLocked(key) {
			exists++
		}
	}
	return redis.NewIntResult(exists, nil)
}

func (c *fakeRedisClient) SRem(_ context.Context, key string, members ...any) *redis.IntCmd {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
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
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.keyExistsLocked(key)
}

func (c *fakeRedisClient) keyExistsLocked(key string) bool {
	if _, exists := c.hashes[key]; exists {
		return true
	}
	if _, exists := c.sets[key]; exists {
		return true
	}
	if _, exists := c.stringLocks[key]; exists {
		return true
	}
	if _, exists := c.zsets[key]; exists {
		return true
	}
	return false
}

func (c *fakeRedisClient) purgeIfExpired(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeIfExpiredLocked(key)
}

func (c *fakeRedisClient) purgeIfExpiredLocked(key string) {
	expiry, ok := c.expiresAt[key]
	if !ok || c.now.Before(expiry) {
		return
	}

	delete(c.expiresAt, key)
	delete(c.hashes, key)
	delete(c.sets, key)
	delete(c.stringLocks, key)
	delete(c.zsets, key)
}

func (c *fakeRedisClient) purgeAllExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.purgeAllExpiredLocked()
}

func (c *fakeRedisClient) purgeAllExpiredLocked() {
	for key := range c.expiresAt {
		c.purgeIfExpiredLocked(key)
	}
}

func parseRangeBoundary(raw string) (float64, bool, error) {
	if raw == "" || raw == "-inf" {
		return -1 << 62, false, nil
	}
	if raw == "+inf" {
		return 1 << 62, false, nil
	}

	exclusive := strings.HasPrefix(raw, "(")
	cleaned := strings.TrimPrefix(raw, "(")
	value, err := strconv.ParseFloat(cleaned, 64)
	if err != nil {
		return 0, false, err
	}
	return value, exclusive, nil
}

func inScoreRange(score float64, min float64, minExclusive bool, max float64, maxExclusive bool) bool {
	if minExclusive {
		if score <= min {
			return false
		}
	} else if score < min {
		return false
	}

	if maxExclusive {
		if score >= max {
			return false
		}
	} else if score > max {
		return false
	}

	return true
}

func newRedisStoreForTest(t *testing.T, now time.Time, retention time.Duration, maxSeries int) (*RedisStore, *fakeRedisClient) {
	return newRedisStoreForTestWithShards(t, now, retention, maxSeries, 1)
}

func newRedisStoreForTestWithShards(
	t *testing.T,
	now time.Time,
	retention time.Duration,
	maxSeries int,
	indexShards int,
) (*RedisStore, *fakeRedisClient) {
	t.Helper()

	client := newFakeRedisClient(now)
	store := newRedisStoreFromCommander(client, nil, RedisStoreConfig{
		Namespace:   "github-stats-test",
		Retention:   retention,
		MaxSeries:   maxSeries,
		IndexShards: indexShards,
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

func TestRedisStoreSnapshotDelta(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, _ := newRedisStoreForTest(t, now, 24*time.Hour, 100)

	err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     1,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertMetric(alice) unexpected error: %v", err)
	}

	delta, err := store.SnapshotDelta(0)
	if err != nil {
		t.Fatalf("SnapshotDelta(first) unexpected error: %v", err)
	}
	if len(delta.Events) != 1 {
		t.Fatalf("first delta events len = %d, want 1", len(delta.Events))
	}
	if delta.Events[0].Deleted {
		t.Fatalf("first delta event is delete, want upsert")
	}
	if delta.Events[0].Point.Value != 1 {
		t.Fatalf("first delta value = %v, want 1", delta.Events[0].Point.Value)
	}
	if delta.NextCursor == 0 {
		t.Fatalf("first delta cursor = 0, want > 0")
	}

	cursor := delta.NextCursor
	err = store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     2,
		UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("UpsertMetric(alice-update) unexpected error: %v", err)
	}
	err = store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "bob"},
		Value:     3,
		UpdatedAt: now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("UpsertMetric(bob) unexpected error: %v", err)
	}

	delta, err = store.SnapshotDelta(cursor)
	if err != nil {
		t.Fatalf("SnapshotDelta(second) unexpected error: %v", err)
	}
	if len(delta.Events) != 2 {
		t.Fatalf("second delta events len = %d, want 2", len(delta.Events))
	}

	found := map[string]float64{}
	for _, event := range delta.Events {
		if event.Deleted {
			t.Fatalf("unexpected delete event: %+v", event)
		}
		found[event.Point.Labels["user"]] = event.Point.Value
	}
	if found["alice"] != 2 {
		t.Fatalf("alice updated value = %v, want 2", found["alice"])
	}
	if found["bob"] != 3 {
		t.Fatalf("bob value = %v, want 3", found["bob"])
	}
}

func TestRedisStoreSnapshotDeltaDeleteEvent(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, client := newRedisStoreForTest(t, now, time.Hour, 100)

	point := MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     1,
		UpdatedAt: now,
	}
	err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, point)
	if err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	cursor, err := store.SnapshotCursor()
	if err != nil {
		t.Fatalf("SnapshotCursor() unexpected error: %v", err)
	}
	if cursor == 0 {
		t.Fatalf("SnapshotCursor() = 0, want > 0")
	}

	client.Advance(2 * time.Hour)
	store.GC(now.Add(2 * time.Hour))

	delta, err := store.SnapshotDelta(cursor)
	if err != nil {
		t.Fatalf("SnapshotDelta(delete) unexpected error: %v", err)
	}
	if len(delta.Events) != 1 {
		t.Fatalf("delete delta events len = %d, want 1", len(delta.Events))
	}
	if !delta.Events[0].Deleted {
		t.Fatalf("delete delta event Deleted = false, want true")
	}
	if delta.Events[0].SeriesID != hashSeriesID(metricKey(point.Name, point.Labels)) {
		t.Fatalf("delete series id = %q, want %q", delta.Events[0].SeriesID, hashSeriesID(metricKey(point.Name, point.Labels)))
	}
}

func TestRedisStoreCheckpointLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, client := newRedisStoreForTest(t, now, time.Hour, 100)

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

	client.Advance(2 * time.Hour)
	_, found, err = store.GetCheckpoint("org-a", "repo-a")
	if err != nil {
		t.Fatalf("GetCheckpoint(after expiry) unexpected error: %v", err)
	}
	if found {
		t.Fatalf("GetCheckpoint(after expiry) found = true, want false")
	}
}

func TestRedisStoreEmitsTracingSpans(t *testing.T) {
	now := time.Unix(1739836800, 0)

	previousProvider := otel.GetTracerProvider()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		_ = provider.Shutdown(context.Background())
	})

	store, _ := newRedisStoreForTest(t, now, time.Hour, 100)
	if err := store.SetCheckpoint("org-a", "repo-a", now); err != nil {
		t.Fatalf("SetCheckpoint() unexpected error: %v", err)
	}
	if err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, MetricPoint{
		Name:      "gh_activity_commits_24h",
		Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
		Value:     1,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	seen := map[string]bool{}
	for _, span := range recorder.Ended() {
		seen[span.Name()] = true
	}
	if !seen["redis.set_checkpoint"] {
		t.Fatalf("missing redis.set_checkpoint span")
	}
	if !seen["redis.upsert_metric"] {
		t.Fatalf("missing redis.upsert_metric span")
	}
}

func TestRedisStoreUsesShardedIndexes(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	store, client := newRedisStoreForTestWithShards(t, now, 24*time.Hour, 100, 4)

	points := []MetricPoint{
		{
			Name:      "gh_activity_commits_24h",
			Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "alice"},
			Value:     1,
			UpdatedAt: now,
		},
		{
			Name:      "gh_activity_commits_24h",
			Labels:    map[string]string{"org": "org-a", "repo": "repo-a", "user": "bob"},
			Value:     2,
			UpdatedAt: now,
		},
	}
	for _, point := range points {
		if err := store.UpsertMetric(RoleLeader, SourceLeaderScrape, point); err != nil {
			t.Fatalf("UpsertMetric(%v) unexpected error: %v", point.Labels["user"], err)
		}
	}

	shardedKeys := 0
	for key := range client.sets {
		if strings.HasPrefix(key, "github-stats-test:metrics:index:") {
			shardedKeys++
		}
	}
	if shardedKeys == 0 {
		t.Fatalf("expected sharded metric index keys to be used")
	}

	snapshot := store.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("Snapshot() len = %d, want 2", len(snapshot))
	}
}
