# internal/telemetry

## Logic overview

The `telemetry` package configures OpenTelemetry tracing behavior.

- Supports operational trace modes: `off`, `sampled`, `errors`, `detailed`.
- Applies sampler strategy based on configured mode and sample ratio.
- Registers a global tracer provider.
- Exposes shutdown hook for graceful provider flush/cleanup.

## API reference

### Types

- `Config`: telemetry config input (`Enabled`, `ServiceName`, `TraceMode`, `TraceSampleRatio`).
- `Runtime`: initialized telemetry runtime (`TracerProvider`, `Shutdown`).

### Functions

- `Setup(cfg Config) (Runtime, error)`: initializes global tracing provider and returns runtime handles.
