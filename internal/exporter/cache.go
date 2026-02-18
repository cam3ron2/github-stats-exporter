package exporter

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cam3ron2/github-stats/internal/store"
)

const (
	cacheModeFull        = "full"
	cacheModeIncremental = "incremental"
)

type incrementalSnapshotReader interface {
	SnapshotCursor() (uint64, error)
	SnapshotDelta(cursor uint64) (store.SnapshotDelta, error)
}

// CacheConfig configures the snapshot cache used by /metrics rendering.
type CacheConfig struct {
	Mode            string
	RefreshInterval time.Duration
	Now             func() time.Time
}

type cachedSnapshotReader struct {
	source      SnapshotReader
	incremental incrementalSnapshotReader

	mode            string
	refreshInterval time.Duration
	now             func() time.Time

	mu          sync.RWMutex
	initialized bool
	lastRefresh time.Time
	cursor      uint64
	series      map[string]store.MetricPoint
}

// NewCachedSnapshotReader wraps a snapshot reader with periodic cache refresh.
func NewCachedSnapshotReader(source SnapshotReader, cfg CacheConfig) SnapshotReader {
	if source == nil {
		return &cachedSnapshotReader{}
	}
	if _, alreadyCached := source.(*cachedSnapshotReader); alreadyCached {
		return source
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	refreshInterval := cfg.RefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = 30 * time.Second
	}

	cached := &cachedSnapshotReader{
		source:          source,
		mode:            normalizeCacheMode(cfg.Mode),
		refreshInterval: refreshInterval,
		now:             nowFn,
		series:          make(map[string]store.MetricPoint),
	}
	if incremental, ok := source.(incrementalSnapshotReader); ok {
		cached.incremental = incremental
	}

	return cached
}

func (c *cachedSnapshotReader) Snapshot() []store.MetricPoint {
	if c == nil || c.source == nil {
		return nil
	}
	c.refreshIfNeeded()

	c.mu.RLock()
	defer c.mu.RUnlock()
	return clonePoints(c.sortedSnapshotLocked())
}

func (c *cachedSnapshotReader) refreshIfNeeded() {
	now := c.now()

	c.mu.RLock()
	if c.initialized && now.Sub(c.lastRefresh) < c.refreshInterval {
		c.mu.RUnlock()
		return
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialized && now.Sub(c.lastRefresh) < c.refreshInterval {
		return
	}

	if c.mode == cacheModeIncremental && c.incremental != nil && c.initialized {
		delta, err := c.incremental.SnapshotDelta(c.cursor)
		if err == nil {
			c.applyDeltaLocked(delta)
			c.lastRefresh = now
			return
		}
	}

	c.refreshFullLocked()
	if c.mode == cacheModeIncremental && c.incremental != nil {
		if cursor, err := c.incremental.SnapshotCursor(); err == nil {
			c.cursor = cursor
		}
	}
	c.lastRefresh = now
	c.initialized = true
}

func (c *cachedSnapshotReader) refreshFullLocked() {
	points := c.source.Snapshot()

	next := make(map[string]store.MetricPoint, len(points))
	for _, point := range points {
		key := seriesIdentity(point)
		next[key] = clonePoint(point)
	}

	c.series = next
}

func (c *cachedSnapshotReader) applyDeltaLocked(delta store.SnapshotDelta) {
	for _, event := range delta.Events {
		seriesID := strings.TrimSpace(event.SeriesID)
		if seriesID == "" {
			seriesID = seriesIdentity(event.Point)
		}

		if event.Deleted {
			delete(c.series, seriesID)
			continue
		}
		c.series[seriesID] = clonePoint(event.Point)
	}

	if delta.NextCursor > c.cursor {
		c.cursor = delta.NextCursor
	}
	c.initialized = true
}

func (c *cachedSnapshotReader) sortedSnapshotLocked() []store.MetricPoint {
	if len(c.series) == 0 {
		return nil
	}

	keys := make([]string, 0, len(c.series))
	for key := range c.series {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]store.MetricPoint, 0, len(keys))
	for _, key := range keys {
		result = append(result, clonePoint(c.series[key]))
	}
	return result
}

func clonePoints(points []store.MetricPoint) []store.MetricPoint {
	if len(points) == 0 {
		return nil
	}
	copied := make([]store.MetricPoint, 0, len(points))
	for _, point := range points {
		copied = append(copied, clonePoint(point))
	}
	return copied
}

func clonePoint(point store.MetricPoint) store.MetricPoint {
	return store.MetricPoint{
		Name:      point.Name,
		Labels:    maps.Clone(point.Labels),
		Value:     point.Value,
		UpdatedAt: point.UpdatedAt,
	}
}

func seriesKey(point store.MetricPoint) string {
	labelKeys := make([]string, 0, len(point.Labels))
	for key := range point.Labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)

	builder := strings.Builder{}
	builder.WriteString(point.Name)
	builder.WriteString("|")
	for _, key := range labelKeys {
		builder.WriteString(key)
		builder.WriteString("=")
		builder.WriteString(point.Labels[key])
		builder.WriteString(";")
	}
	return builder.String()
}

func seriesIdentity(point store.MetricPoint) string {
	sum := sha256.Sum256([]byte(seriesKey(point)))
	return hex.EncodeToString(sum[:])
}

func normalizeCacheMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case cacheModeFull:
		return cacheModeFull
	default:
		return cacheModeIncremental
	}
}
