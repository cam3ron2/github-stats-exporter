# internal/exporter

## Logic overview

The `exporter` package bridges store snapshots into Prometheus/OpenMetrics output.

- Snapshot data is read from a `SnapshotReader`.
- Optional snapshot caching supports `full` or `incremental` refresh strategy.
- A custom collector emits each snapshot point as a gauge sample.
- A dedicated registry is served with `promhttp` and OpenMetrics enabled.

## API reference

### Types

- `SnapshotReader`: interface with `Snapshot() []store.MetricPoint` used as the exporter input contract.
- `CacheConfig`: cache settings (`Mode`, `RefreshInterval`, `Now`) for snapshot refresh behavior.

### Functions

- `NewOpenMetricsHandler(reader SnapshotReader) http.Handler`: builds a Prometheus registry, registers the snapshot collector, and returns an OpenMetrics-capable HTTP handler.
- `NewCachedSnapshotReader(source SnapshotReader, cfg CacheConfig) SnapshotReader`: wraps a reader with refresh-based caching. In `incremental` mode, it uses `SnapshotCursor`/`SnapshotDelta` when supported by the source.
