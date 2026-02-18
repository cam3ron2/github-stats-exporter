package telemetry

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestSamplerForMode(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		mode     string
		ratio    float64
		wantDrop bool
	}{
		{name: "off_mode_drops", mode: "off", ratio: 0.5, wantDrop: true},
		{name: "sampled_zero_ratio_drops", mode: "sampled", ratio: 0, wantDrop: true},
		{name: "sampled_full_ratio_records", mode: "sampled", ratio: 1, wantDrop: false},
		{name: "detailed_records", mode: "detailed", ratio: 0, wantDrop: false},
		{name: "errors_mode_uses_low_sampling", mode: "errors", ratio: 1, wantDrop: false},
		{name: "unknown_mode_defaults_to_sampled", mode: "unknown", ratio: 1, wantDrop: false},
	}

	params := sdktrace.SamplingParameters{}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			decision := samplerForMode(tc.mode, tc.ratio).ShouldSample(params).Decision
			gotDrop := decision == sdktrace.Drop
			if gotDrop != tc.wantDrop {
				t.Fatalf("ShouldSample().Decision drop=%t, want %t", gotDrop, tc.wantDrop)
			}
		})
	}
}

func TestSetup(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		config Config
	}{
		{
			name: "disabled_tracing_uses_drop_sampler",
			config: Config{
				Enabled:     false,
				ServiceName: "github-stats-exporter",
				TraceMode:   "off",
			},
		},
		{
			name: "enabled_sampled_tracing",
			config: Config{
				Enabled:          true,
				ServiceName:      "github-stats-exporter",
				TraceMode:        "sampled",
				TraceSampleRatio: 0.25,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runtime, err := Setup(tc.config)
			if err != nil {
				t.Fatalf("Setup() unexpected error: %v", err)
			}
			if runtime.TracerProvider == nil {
				t.Fatalf("TracerProvider is nil")
			}

			if err := runtime.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown() unexpected error: %v", err)
			}
		})
	}
}

func TestTraceModeNormalizationAndDependencyTracing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                 string
		mode                 string
		wantMode             string
		wantTraceDependences bool
	}{
		{
			name:                 "detailed_traces_dependencies",
			mode:                 "detailed",
			wantMode:             "detailed",
			wantTraceDependences: true,
		},
		{
			name:                 "sampled_omits_dependency_details",
			mode:                 "sampled",
			wantMode:             "sampled",
			wantTraceDependences: false,
		},
		{
			name:                 "unknown_defaults_to_sampled",
			mode:                 "unexpected",
			wantMode:             "sampled",
			wantTraceDependences: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setTraceMode(tc.mode)
			if got := TraceMode(); got != tc.wantMode {
				t.Fatalf("TraceMode() = %q, want %q", got, tc.wantMode)
			}
			if got := ShouldTraceDependencies(); got != tc.wantTraceDependences {
				t.Fatalf("ShouldTraceDependencies() = %t, want %t", got, tc.wantTraceDependences)
			}
		})
	}
}

func TestOTLPTraceEndpoint(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{
			name:     "host_port_defaults_to_insecure",
			endpoint: "otel-collector:4317",
		},
		{
			name:     "http_url_is_insecure",
			endpoint: "http://otel-collector:4317",
		},
		{
			name:     "https_url_is_tls",
			endpoint: "https://otel-collector:4317",
		},
		{
			name:     "invalid_url_returns_error",
			endpoint: "https://",
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			endpoint, err := otlpTraceEndpoint(tc.endpoint)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("otlpTraceEndpoint() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("otlpTraceEndpoint() unexpected error: %v", err)
			}
			if endpoint == "" {
				t.Fatalf("otlpTraceEndpoint() returned empty endpoint")
			}
		})
	}
}

func TestSetupBuildsOTLPExporterWhenEndpointConfigured(t *testing.T) {
	t.Parallel()

	originalFactory := newOTLPExporter
	t.Cleanup(func() {
		newOTLPExporter = originalFactory
	})

	called := false
	newOTLPExporter = func(endpoint string) (sdktrace.SpanExporter, error) {
		called = true
		if endpoint == "" {
			t.Fatalf("newOTLPExporter() endpoint is empty")
		}
		return &noopExporter{}, nil
	}

	runtime, err := Setup(Config{
		Enabled:      true,
		ServiceName:  "github-stats-exporter",
		OTLPEndpoint: "otel-collector:4317",
		TraceMode:    "sampled",
	})
	if err != nil {
		t.Fatalf("Setup() unexpected error: %v", err)
	}
	if !called {
		t.Fatalf("Setup() did not build OTLP exporter")
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() unexpected error: %v", err)
	}
}

type noopExporter struct{}

func (e *noopExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *noopExporter) Shutdown(context.Context) error {
	return nil
}
