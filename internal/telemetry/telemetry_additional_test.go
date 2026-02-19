package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestClampRatio(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input float64
		want  float64
	}{
		{name: "below_zero", input: -0.25, want: 0},
		{name: "within_bounds", input: 0.42, want: 0.42},
		{name: "above_one", input: 1.25, want: 1},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := clampRatio(tc.input); got != tc.want {
				t.Fatalf("clampRatio(%v) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestOTLPHTTPExporterExportSpans(t *testing.T) {
	spans := buildTestReadOnlySpans(t)
	if len(spans) == 0 {
		t.Fatalf("expected non-empty span list")
	}

	testCases := []struct {
		name        string
		endpoint    string
		client      *http.Client
		spans       []sdktrace.ReadOnlySpan
		wantErr     bool
		errContains string
	}{
		{
			name:     "no_spans_noop",
			endpoint: "http://collector.local/v1/traces",
			client: &http.Client{
				Timeout: 1 * time.Second,
				Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
					t.Fatalf("unexpected HTTP call for empty span slice")
					return nil, nil
				}),
			},
			spans:   nil,
			wantErr: false,
		},
		{
			name:        "invalid_endpoint",
			endpoint:    "://invalid-endpoint",
			client:      &http.Client{Timeout: 1 * time.Second},
			spans:       spans,
			wantErr:     true,
			errContains: "build otlp request",
		},
		{
			name:     "http_transport_error",
			endpoint: "http://collector.local/v1/traces",
			client: &http.Client{
				Timeout: 1 * time.Second,
				Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
					return nil, errors.New("network down")
				}),
			},
			spans:       spans,
			wantErr:     true,
			errContains: "send otlp request",
		},
		{
			name:     "non_2xx_status_with_body",
			endpoint: "http://collector.local/v1/traces",
			client: &http.Client{
				Timeout: 1 * time.Second,
				Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusBadGateway,
						Body:       io.NopCloser(strings.NewReader("upstream failed")),
						Header:     make(http.Header),
					}, nil
				}),
			},
			spans:       spans,
			wantErr:     true,
			errContains: "otlp export failed status=502 body=upstream failed",
		},
		{
			name:     "non_2xx_status_with_body_read_error",
			endpoint: "http://collector.local/v1/traces",
			client: &http.Client{
				Timeout: 1 * time.Second,
				Transport: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Body:       &failingReadCloser{},
						Header:     make(http.Header),
					}, nil
				}),
			},
			spans:       spans,
			wantErr:     true,
			errContains: "body-read-error",
		},
		{
			name:     "success",
			endpoint: "http://collector.local/v1/traces",
			client: &http.Client{
				Timeout: 1 * time.Second,
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method != http.MethodPost {
						return nil, fmt.Errorf("method = %s, want POST", req.Method)
					}
					if req.Header.Get("Content-Type") != "application/json" {
						return nil, fmt.Errorf("content type = %q", req.Header.Get("Content-Type"))
					}
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader("{}")),
						Header:     make(http.Header),
					}, nil
				}),
			},
			spans:   spans,
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			exporter := &otlpHTTPExporter{
				endpoint: tc.endpoint,
				client:   tc.client,
			}

			err := exporter.ExportSpans(context.Background(), tc.spans)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ExportSpans() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("ExportSpans() error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("ExportSpans() unexpected error: %v", err)
			}
		})
	}
}

func TestBuildOTLPTraceRequestAndAttributes(t *testing.T) {
	t.Parallel()

	spans := buildTestReadOnlySpans(t)
	request := buildOTLPTraceRequest(spans)
	if len(request.ResourceSpans) == 0 {
		t.Fatalf("ResourceSpans len = 0, want > 0")
	}

	var foundChild bool
	for _, resourceSpans := range request.ResourceSpans {
		if len(resourceSpans.ScopeSpans) == 0 {
			continue
		}
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				if span.Name != "child" {
					continue
				}
				foundChild = true
				if span.ParentSpanID == "" {
					t.Fatalf("ParentSpanID empty for child span")
				}
				if span.Status.Code != int(codes.Error) {
					t.Fatalf("child status code = %d, want %d", span.Status.Code, int(codes.Error))
				}
				if span.Status.Message != "boom" {
					t.Fatalf("child status message = %q, want boom", span.Status.Message)
				}
				if len(span.Attributes) == 0 {
					t.Fatalf("child attributes len = 0, want > 0")
				}
			}
		}
	}
	if !foundChild {
		t.Fatalf("missing converted child span")
	}

	attributes := toOTLPAttributes([]attribute.KeyValue{
		attribute.Bool("enabled", true),
		attribute.Int64("count", 7),
		attribute.Float64("ratio", 0.5),
		attribute.String("name", "copilot"),
	})
	if len(attributes) != 4 {
		t.Fatalf("toOTLPAttributes len = %d, want 4", len(attributes))
	}
}

func TestOTLPHTTPExporterShutdown(t *testing.T) {
	t.Parallel()

	exporter := &otlpHTTPExporter{
		endpoint: "http://collector.local/v1/traces",
		client:   &http.Client{Timeout: 1 * time.Second},
	}
	if err := exporter.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() unexpected error: %v", err)
	}
}

func buildTestReadOnlySpans(t *testing.T) []sdktrace.ReadOnlySpan {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
		sdktrace.WithResource(sdkresource.Empty()),
	)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	tracer := provider.Tracer("telemetry-test")
	ctx, parent := tracer.Start(context.Background(), "parent")
	_, child := tracer.Start(
		ctx,
		"child",
		trace.WithAttributes(
			attribute.Bool("enabled", true),
			attribute.Int64("count", 3),
			attribute.Float64("ratio", 0.75),
			attribute.String("mode", "detailed"),
		),
	)
	child.SetStatus(codes.Error, "boom")
	child.End()
	parent.End()

	return recorder.Ended()
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type failingReadCloser struct{}

func (f *failingReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (f *failingReadCloser) Close() error {
	return nil
}
