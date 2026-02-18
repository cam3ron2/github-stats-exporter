package store

import (
	"context"
	"crypto/sha1"
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
		seriesCount, err := s.client.SCard(ctx, s.metricsIndexKey()).Result()
		if err != nil {
			return fmt.Errorf("count metric series: %w", err)
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
			_ = s.client.SRem(ctx, s.metricsIndexKey(), seriesID).Err()
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

func (s *RedisStore) metricsIndexKey() string {
	return s.prefixed("metrics:index")
}

func (s *RedisStore) metricDataKey(seriesID string) string {
	return s.prefixed("metric:" + seriesID)
}

func hashSeriesID(raw string) string {
	sum := sha1.Sum([]byte(raw))
	return hex.EncodeToString(sum[:])
}
