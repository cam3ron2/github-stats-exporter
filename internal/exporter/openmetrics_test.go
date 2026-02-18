package exporter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/store"
)

func TestOpenMetricsHandler(t *testing.T) {
	t.Parallel()

	memStore := store.NewMemoryStore(24*time.Hour, 1000)
	now := time.Unix(1739836800, 0)
	err := memStore.UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, store.MetricPoint{
		Name: "gh_activity_commits_24h",
		Labels: map[string]string{
			"org":  "org-a",
			"repo": "repo-a",
			"user": "alice",
		},
		Value:     42,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	handler := NewOpenMetricsHandler(memStore)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", rec.Code)
	}

	body := rec.Body.String()
	wantSubstrs := []string{
		`# TYPE gh_activity_commits_24h gauge`,
		`gh_activity_commits_24h{org="org-a",repo="repo-a",user="alice"} 42`,
		"# EOF",
	}
	for _, substr := range wantSubstrs {
		if !strings.Contains(body, substr) {
			t.Fatalf("metrics output missing %q:\n%s", substr, body)
		}
	}
}

func TestOpenMetricsHandlerIncludesCacheMetrics(t *testing.T) {
	t.Parallel()

	memStore := store.NewMemoryStore(24*time.Hour, 1000)
	now := time.Unix(1739836800, 0)
	err := memStore.UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, store.MetricPoint{
		Name: "gh_activity_commits_24h",
		Labels: map[string]string{
			"org":  "org-a",
			"repo": "repo-a",
			"user": "alice",
		},
		Value:     7,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("UpsertMetric() unexpected error: %v", err)
	}

	cached := NewCachedSnapshotReader(memStore, CacheConfig{
		Mode:            "incremental",
		RefreshInterval: time.Minute,
		Now:             func() time.Time { return now },
	})

	handler := NewOpenMetricsHandler(cached)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200", rec.Code)
	}

	body := rec.Body.String()
	wantSubstrs := []string{
		`gh_exporter_metrics_series_loaded{metric="gh_activity_commits_24h"} 1`,
		`# TYPE gh_exporter_metrics_cache_refresh_duration_seconds gauge`,
	}
	for _, substr := range wantSubstrs {
		if !strings.Contains(body, substr) {
			t.Fatalf("metrics output missing %q:\n%s", substr, body)
		}
	}
}
