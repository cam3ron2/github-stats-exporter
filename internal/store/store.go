package store

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"
)

// RuntimeRole represents the current runtime role.
type RuntimeRole string

const (
	// RoleLeader is the leader role.
	RoleLeader RuntimeRole = "leader"
	// RoleFollower is the follower role.
	RoleFollower RuntimeRole = "follower"
)

// WriteSource represents the source path for a metric write.
type WriteSource string

const (
	// SourceLeaderScrape writes originate from scheduled leader scrape.
	SourceLeaderScrape WriteSource = "leader_scrape"
	// SourceWorkerBackfill writes originate from backfill workers.
	SourceWorkerBackfill WriteSource = "worker_backfill"
)

// MetricPoint is a single metric sample.
type MetricPoint struct {
	Name      string
	Labels    map[string]string
	Value     float64
	UpdatedAt time.Time
}

// SnapshotDeltaEvent is one incremental snapshot change for a series.
type SnapshotDeltaEvent struct {
	SeriesID string
	Point    MetricPoint
	Deleted  bool
}

// SnapshotDelta contains a set of incremental changes after a cursor.
type SnapshotDelta struct {
	NextCursor uint64
	Events     []SnapshotDeltaEvent
}

type storedMetric struct {
	point MetricPoint
}

// MemoryStore is an in-memory shared metric store.
type MemoryStore struct {
	mu         sync.RWMutex
	retention  time.Duration
	maxSeries  int
	metrics    map[string]storedMetric
	jobLocks   map[string]time.Time
	dedupLocks map[string]time.Time
}

// NewMemoryStore creates a memory store.
func NewMemoryStore(retention time.Duration, maxSeries int) *MemoryStore {
	return &MemoryStore{
		retention:  retention,
		maxSeries:  maxSeries,
		metrics:    make(map[string]storedMetric),
		jobLocks:   make(map[string]time.Time),
		dedupLocks: make(map[string]time.Time),
	}
}

// UpsertMetric inserts or updates a metric point with role/source write guards.
func (s *MemoryStore) UpsertMetric(role RuntimeRole, source WriteSource, point MetricPoint) error {
	if err := validateWriteSource(role, source); err != nil {
		return err
	}
	if point.Name == "" {
		return fmt.Errorf("metric name is required")
	}
	if point.UpdatedAt.IsZero() {
		return fmt.Errorf("metric updated time is required")
	}

	key := metricKey(point.Name, point.Labels)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metrics[key]; !exists && s.maxSeries > 0 && len(s.metrics) >= s.maxSeries {
		return fmt.Errorf("max series budget exceeded")
	}
	s.metrics[key] = storedMetric{
		point: MetricPoint{
			Name:      point.Name,
			Labels:    maps.Clone(point.Labels),
			Value:     point.Value,
			UpdatedAt: point.UpdatedAt,
		},
	}
	return nil
}

// AcquireJobLock acquires an idempotency lock for a job id.
func (s *MemoryStore) AcquireJobLock(jobID string, ttl time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return acquireLock(s.jobLocks, jobID, ttl, now)
}

// AcquireDedupLock acquires a dedup lock for a key.
func (s *MemoryStore) AcquireDedupLock(key string, ttl time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return acquireLock(s.dedupLocks, key, ttl, now)
}

// Acquire acquires a dedup lock for a key. It is an adapter for queue deduper interfaces.
func (s *MemoryStore) Acquire(key string, ttl time.Duration, now time.Time) bool {
	return s.AcquireDedupLock(key, ttl, now)
}

// GC deletes expired metrics and locks.
func (s *MemoryStore) GC(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.retention > 0 {
		for key, metric := range s.metrics {
			if now.Sub(metric.point.UpdatedAt) > s.retention {
				delete(s.metrics, key)
			}
		}
	}

	trimExpiredLocks(s.jobLocks, now)
	trimExpiredLocks(s.dedupLocks, now)
}

// Snapshot returns all non-expired metrics.
func (s *MemoryStore) Snapshot() []MetricPoint {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]MetricPoint, 0, len(s.metrics))
	for _, metric := range s.metrics {
		result = append(result, MetricPoint{
			Name:      metric.point.Name,
			Labels:    maps.Clone(metric.point.Labels),
			Value:     metric.point.Value,
			UpdatedAt: metric.point.UpdatedAt,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		leftKey := metricKey(result[i].Name, result[i].Labels)
		rightKey := metricKey(result[j].Name, result[j].Labels)
		return leftKey < rightKey
	})
	return result
}

func validateWriteSource(role RuntimeRole, source WriteSource) error {
	switch role {
	case RoleLeader:
		if source != SourceLeaderScrape {
			return fmt.Errorf("leader may only write source=%s", SourceLeaderScrape)
		}
	case RoleFollower:
		if source != SourceWorkerBackfill {
			return fmt.Errorf("follower may only write source=%s", SourceWorkerBackfill)
		}
	default:
		return fmt.Errorf("unknown role: %s", role)
	}
	return nil
}

func metricKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	builder := strings.Builder{}
	builder.WriteString(name)
	builder.WriteString("|")
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteString("=")
		builder.WriteString(labels[key])
		builder.WriteString(";")
	}
	return builder.String()
}

func acquireLock(lockMap map[string]time.Time, key string, ttl time.Duration, now time.Time) bool {
	expiry, exists := lockMap[key]
	if exists && now.Before(expiry) {
		return false
	}
	lockMap[key] = now.Add(ttl)
	return true
}

func trimExpiredLocks(lockMap map[string]time.Time, now time.Time) {
	for key, expiry := range lockMap {
		if !now.Before(expiry) {
			delete(lockMap, key)
		}
	}
}
