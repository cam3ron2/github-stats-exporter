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
				ServiceName: "github-stats",
				TraceMode:   "off",
			},
		},
		{
			name: "enabled_sampled_tracing",
			config: Config{
				Enabled:          true,
				ServiceName:      "github-stats",
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
