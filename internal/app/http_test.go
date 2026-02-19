package app

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewHTTPHandler(t *testing.T) {
	t.Parallel()

	metricsHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metrics"))
	})
	healthHandler := http.NewServeMux()
	healthHandler.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("live"))
	})
	healthHandler.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})
	healthHandler.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"mode":"healthy"}`))
	})

	handler := NewHTTPHandler(metricsHandler, healthHandler)

	testCases := []struct {
		path     string
		wantCode int
		wantBody string
	}{
		{path: "/metrics", wantCode: http.StatusOK, wantBody: "metrics"},
		{path: "/livez", wantCode: http.StatusOK, wantBody: "live"},
		{path: "/readyz", wantCode: http.StatusServiceUnavailable, wantBody: "not ready"},
		{path: "/healthz", wantCode: http.StatusOK, wantBody: `{"mode":"healthy"}`},
		{path: "/unknown", wantCode: http.StatusNotFound, wantBody: "404 page not found\n"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d", rec.Code, tc.wantCode)
			}
			if rec.Body.String() != tc.wantBody {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tc.wantBody)
			}
		})
	}
}

func TestWrapHTTPHandlerByTraceMode(t *testing.T) {
	t.Parallel()

	base := &staticHandler{}

	testCases := []struct {
		name        string
		traceMode   string
		wantWrapped bool
	}{
		{
			name:        "trace_off",
			traceMode:   "off",
			wantWrapped: false,
		},
		{
			name:        "trace_sampled",
			traceMode:   "sampled",
			wantWrapped: true,
		},
		{
			name:        "trace_detailed",
			traceMode:   "detailed",
			wantWrapped: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wrapped := wrapHTTPHandler(tc.traceMode, "metrics", base)
			gotWrapped := wrapped != base
			if gotWrapped != tc.wantWrapped {
				t.Fatalf("wrapped = %t, want %t", gotWrapped, tc.wantWrapped)
			}
		})
	}
}

func TestWrapHTTPHandlerNilHandlerAndStatusCapture(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		traceMode string
		route     string
		handler   http.Handler
		wantCode  int
	}{
		{
			name:      "nil_handler_uses_not_found",
			traceMode: "sampled",
			route:     "metrics",
			handler:   nil,
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "empty_route_defaults_operation_name",
			traceMode: "detailed",
			route:     "",
			handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			}),
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wrapped := wrapHTTPHandler(tc.traceMode, tc.route, tc.handler)
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			rec := httptest.NewRecorder()
			wrapped.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantCode)
			}
		})
	}
}

type staticHandler struct{}

func (h *staticHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
