package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisCommander interface {
	SIsMember(ctx context.Context, key string, member any) *redis.BoolCmd
	SCard(ctx context.Context, key string) *redis.IntCmd
	HSet(ctx context.Context, key string, values ...any) *redis.IntCmd
	SAdd(ctx context.Context, key string, members ...any) *redis.IntCmd
	ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	SRem(ctx context.Context, key string, members ...any) *redis.IntCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd
}

// RedisStoreConfig configures the Redis-backed shared metric store.
type RedisStoreConfig struct {
	Namespace string
	Retention time.Duration
	MaxSeries int
}

// RedisStore stores shared metrics and lock state in Redis.
type RedisStore struct {
	client    redisCommander
	closeFn   func() error
	namespace string
	retention time.Duration
	maxSeries int
}

// NewRedisStore creates a Redis-backed metric store.
func NewRedisStore(client redis.UniversalClient, cfg RedisStoreConfig) *RedisStore {
	closeFn := func() error { return nil }
	if client != nil {
		closeFn = client.Close
	}
	return newRedisStoreFromCommander(client, closeFn, cfg)
}

func newRedisStoreFromCommander(client redisCommander, closeFn func() error, cfg RedisStoreConfig) *RedisStore {
	namespace := cfg.Namespace
	if namespace == "" {
		namespace = "github-stats"
	}
	if closeFn == nil {
		closeFn = func() error { return nil }
	}

	return &RedisStore{
		client:    client,
		closeFn:   closeFn,
		namespace: namespace,
		retention: cfg.Retention,
		maxSeries: cfg.MaxSeries,
	}
}

// Close closes the underlying Redis client.
func (s *RedisStore) Close() error {
	if s == nil || s.closeFn == nil {
		return nil
	}
	return s.closeFn()
}

// UpsertMetric inserts or updates a metric point with role/source write guards.
func (s *RedisStore) UpsertMetric(role RuntimeRole, source WriteSource, point MetricPoint) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis store is not initialized")
	}
	if err := validateWriteSource(role, source); err != nil {
		return err
	}
	if point.Name == "" {
		return fmt.Errorf("metric name is required")
	}
	if point.UpdatedAt.IsZero() {
		return fmt.Errorf("metric updated time is required")
	}

	ctx := context.Background()
	seriesID := hashSeriesID(metricKey(point.Name, point.Labels))
	isMember, err := s.client.SIsMember(ctx, s.metricsIndexKey(), seriesID).Result()
	if err != nil {
		return fmt.Errorf("check metric membership: %w", err)
	}
	if !isMember && s.maxSeries > 0 {
		seriesCount, countErr := s.client.SCard(ctx, s.metricsIndexKey()).Result()
		if countErr != nil {
			return fmt.Errorf("count metric series: %w", countErr)
		}
		if seriesCount >= int64(s.maxSeries) {
			return fmt.Errorf("max series budget exceeded")
		}
	}

	labelsJSON, err := json.Marshal(point.Labels)
	if err != nil {
		return fmt.Errorf("marshal metric labels: %w", err)
	}

	fields := map[string]any{
		"name":       point.Name,
		"labels":     string(labelsJSON),
		"value":      strconv.FormatFloat(point.Value, 'f', -1, 64),
		"updated_at": strconv.FormatInt(point.UpdatedAt.UnixNano(), 10),
	}

	metricKey := s.metricDataKey(seriesID)
	if err := s.client.HSet(ctx, metricKey, fields).Err(); err != nil {
		return fmt.Errorf("write metric hash: %w", err)
	}
	if err := s.client.SAdd(ctx, s.metricsIndexKey(), seriesID).Err(); err != nil {
		return fmt.Errorf("index metric series: %w", err)
	}

	if s.retention > 0 {
		expiresAt := point.UpdatedAt.Add(s.retention)
		if err := s.client.ExpireAt(ctx, metricKey, expiresAt).Err(); err != nil {
			return fmt.Errorf("set metric ttl: %w", err)
		}
	}
	if _, err := s.recordSeriesChange(ctx, seriesID); err != nil {
		return fmt.Errorf("record metric change: %w", err)
	}

	return nil
}

// AcquireJobLock acquires an idempotency lock for a job id.
func (s *RedisStore) AcquireJobLock(jobID string, ttl time.Duration, now time.Time) bool {
	return s.acquireLock("lock:job:"+jobID, ttl, now)
}

// AcquireDedupLock acquires a dedup lock for a key.
func (s *RedisStore) AcquireDedupLock(key string, ttl time.Duration, now time.Time) bool {
	return s.acquireLock("lock:dedup:"+key, ttl, now)
}

// Acquire acquires a dedup lock for a key. It is an adapter for queue deduper interfaces.
func (s *RedisStore) Acquire(key string, ttl time.Duration, now time.Time) bool {
	return s.AcquireDedupLock(key, ttl, now)
}

// GC removes stale metric index references where series keys have already expired.
func (s *RedisStore) GC(_ time.Time) {
	if s == nil || s.client == nil {
		return
	}

	ctx := context.Background()
	seriesIDs, err := s.client.SMembers(ctx, s.metricsIndexKey()).Result()
	if err != nil {
		return
	}

	for _, seriesID := range seriesIDs {
		exists, err := s.client.Exists(ctx, s.metricDataKey(seriesID)).Result()
		if err != nil {
			continue
		}
		if exists == 0 {
			removedCount, remErr := s.client.SRem(ctx, s.metricsIndexKey(), seriesID).Result()
			if remErr != nil {
				continue
			}
			if removedCount == 0 {
				continue
			}
			if _, changeErr := s.recordSeriesChange(ctx, seriesID); changeErr != nil {
				continue
			}
		}
	}
}

// Snapshot returns all currently available metric series from Redis.
func (s *RedisStore) Snapshot() []MetricPoint {
	if s == nil || s.client == nil {
		return nil
	}

	ctx := context.Background()
	seriesIDs, err := s.client.SMembers(ctx, s.metricsIndexKey()).Result()
	if err != nil {
		return nil
	}

	result := make([]MetricPoint, 0, len(seriesIDs))
	for _, seriesID := range seriesIDs {
		fields, err := s.client.HGetAll(ctx, s.metricDataKey(seriesID)).Result()
		if err != nil || len(fields) == 0 {
			continue
		}

		point, ok := decodeMetricPoint(fields)
		if !ok {
			continue
		}
		result = append(result, point)
	}

	sort.Slice(result, func(i, j int) bool {
		leftKey := metricKey(result[i].Name, result[i].Labels)
		rightKey := metricKey(result[j].Name, result[j].Labels)
		return leftKey < rightKey
	})
	return result
}

// SnapshotCursor returns the current incremental snapshot cursor.
func (s *RedisStore) SnapshotCursor() (uint64, error) {
	if s == nil || s.client == nil {
		return 0, fmt.Errorf("redis store is not initialized")
	}

	raw, err := s.client.Get(context.Background(), s.metricsSequenceKey()).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("read snapshot cursor: %w", err)
	}
	if raw == "" {
		return 0, nil
	}

	cursor, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse snapshot cursor %q: %w", raw, err)
	}
	return cursor, nil
}

// SnapshotDelta returns incremental series changes after the provided cursor.
func (s *RedisStore) SnapshotDelta(cursor uint64) (SnapshotDelta, error) {
	if s == nil || s.client == nil {
		return SnapshotDelta{}, fmt.Errorf("redis store is not initialized")
	}

	ctx := context.Background()
	changes, err := s.client.ZRangeByScoreWithScores(ctx, s.metricsChangesKey(), &redis.ZRangeBy{
		Min: fmt.Sprintf("(%d", cursor),
		Max: "+inf",
	}).Result()
	if err != nil {
		return SnapshotDelta{}, fmt.Errorf("read snapshot delta: %w", err)
	}

	delta := SnapshotDelta{
		NextCursor: cursor,
		Events:     make([]SnapshotDeltaEvent, 0, len(changes)),
	}
	for _, change := range changes {
		seriesID := fmt.Sprint(change.Member)
		if seriesID == "" {
			continue
		}
		if change.Score > float64(delta.NextCursor) {
			delta.NextCursor = uint64(change.Score)
		}

		fields, readErr := s.client.HGetAll(ctx, s.metricDataKey(seriesID)).Result()
		if readErr != nil || len(fields) == 0 {
			delta.Events = append(delta.Events, SnapshotDeltaEvent{
				SeriesID: seriesID,
				Deleted:  true,
			})
			continue
		}

		point, ok := decodeMetricPoint(fields)
		if !ok {
			delta.Events = append(delta.Events, SnapshotDeltaEvent{
				SeriesID: seriesID,
				Deleted:  true,
			})
			continue
		}

		delta.Events = append(delta.Events, SnapshotDeltaEvent{
			SeriesID: seriesID,
			Point:    point,
		})
	}

	return delta, nil
}

func decodeMetricPoint(fields map[string]string) (MetricPoint, bool) {
	name := fields["name"]
	if name == "" {
		return MetricPoint{}, false
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(fields["labels"]), &labels); err != nil {
		return MetricPoint{}, false
	}

	value, err := strconv.ParseFloat(fields["value"], 64)
	if err != nil {
		return MetricPoint{}, false
	}
	updatedAtNanos, err := strconv.ParseInt(fields["updated_at"], 10, 64)
	if err != nil {
		return MetricPoint{}, false
	}

	return MetricPoint{
		Name:      name,
		Labels:    maps.Clone(labels),
		Value:     value,
		UpdatedAt: time.Unix(0, updatedAtNanos),
	}, true
}

func (s *RedisStore) acquireLock(key string, ttl time.Duration, now time.Time) bool {
	if s == nil || s.client == nil {
		return false
	}
	if ttl <= 0 {
		return true
	}

	acquired, err := s.client.SetNX(context.Background(), s.prefixed(key), now.UTC().Format(time.RFC3339Nano), ttl).Result()
	if err != nil {
		return false
	}
	return acquired
}

func (s *RedisStore) prefixed(suffix string) string {
	return s.namespace + ":" + suffix
}

func (s *RedisStore) recordSeriesChange(ctx context.Context, seriesID string) (uint64, error) {
	sequence, err := s.client.Incr(ctx, s.metricsSequenceKey()).Result()
	if err != nil {
		return 0, err
	}
	if sequence < 0 {
		return 0, fmt.Errorf("snapshot sequence is negative")
	}
	if err := s.client.ZAdd(ctx, s.metricsChangesKey(), redis.Z{
		Score:  float64(sequence),
		Member: seriesID,
	}).Err(); err != nil {
		return 0, err
	}
	return uint64(sequence), nil
}

func (s *RedisStore) metricsIndexKey() string {
	return s.prefixed("metrics:index")
}

func (s *RedisStore) metricsSequenceKey() string {
	return s.prefixed("metrics:seq")
}

func (s *RedisStore) metricsChangesKey() string {
	return s.prefixed("metrics:changes")
}

func (s *RedisStore) metricDataKey(seriesID string) string {
	return s.prefixed("metric:" + seriesID)
}

func hashSeriesID(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
