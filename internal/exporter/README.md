# internal/exporter

## Logic overview

The `exporter` package bridges store snapshots into Prometheus/OpenMetrics output.

- Snapshot data is read from a `SnapshotReader`.
- A custom collector emits each snapshot point as a gauge sample.
- A dedicated registry is served with `promhttp` and OpenMetrics enabled.

## API reference

### Types

- `SnapshotReader`: interface with `Snapshot() []store.MetricPoint` used as the exporter input contract.

### Functions

- `NewOpenMetricsHandler(reader SnapshotReader) http.Handler`: builds a Prometheus registry, registers the snapshot collector, and returns an OpenMetrics-capable HTTP handler.
