# internal/store

## Logic overview

The `store` package manages shared metric state and distributed locks.

- `MemoryStore` provides an in-process implementation for tests/local runs.
- `RedisStore` provides shared HA-compatible storage over Redis.
- Both implementations enforce role/source write guards and support dedup/job locks.
- Metric retention is handled by GC (memory) and key TTL + index cleanup (Redis).

## API reference

### Types

- `RuntimeRole`: writer role enum (`leader`, `follower`).
- `WriteSource`: write-path enum (`leader_scrape`, `worker_backfill`).
- `MetricPoint`: one metric sample (`Name`, `Labels`, `Value`, `UpdatedAt`).
- `SnapshotDeltaEvent`: one incremental series change (`SeriesID`, `Point`, `Deleted`).
- `SnapshotDelta`: incremental change batch with `NextCursor`.
- `MemoryStore`: in-memory shared store.
- `RedisStoreConfig`: Redis store settings (`Namespace`, `Retention`, `MaxSeries`).
- `RedisStore`: Redis-backed shared store.

### Functions

- `NewMemoryStore(retention time.Duration, maxSeries int) *MemoryStore`: creates memory store.
- `NewRedisStore(client redis.UniversalClient, cfg RedisStoreConfig) *RedisStore`: creates Redis-backed store.

### Methods

#### `MemoryStore`

- `UpsertMetric(role RuntimeRole, source WriteSource, point MetricPoint) error`: validates and upserts one series sample.
- `AcquireJobLock(jobID string, ttl time.Duration, now time.Time) bool`: acquires idempotency lock.
- `AcquireDedupLock(key string, ttl time.Duration, now time.Time) bool`: acquires dedup lock.
- `Acquire(key string, ttl time.Duration, now time.Time) bool`: deduper adapter alias.
- `GC(now time.Time)`: removes expired metrics and expired lock entries.
- `Snapshot() []MetricPoint`: returns sorted metric snapshot.

#### `RedisStore`

- `Close() error`: closes underlying Redis client.
- `UpsertMetric(role RuntimeRole, source WriteSource, point MetricPoint) error`: validates/upserts series to Redis hash + index.
- `AcquireJobLock(jobID string, ttl time.Duration, now time.Time) bool`: acquires Redis lock key with TTL.
- `AcquireDedupLock(key string, ttl time.Duration, now time.Time) bool`: acquires dedup lock key with TTL.
- `Acquire(key string, ttl time.Duration, now time.Time) bool`: deduper adapter alias.
- `GC(now time.Time)`: cleans stale index members whose metric keys have expired.
- `Snapshot() []MetricPoint`: reads indexed series from Redis and returns sorted snapshot.
- `SnapshotCursor() (uint64, error)`: returns current incremental cursor sequence.
- `SnapshotDelta(cursor uint64) (SnapshotDelta, error)`: returns changed/deleted series after a cursor for incremental exporter refresh.
