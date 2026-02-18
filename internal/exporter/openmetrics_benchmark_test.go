package exporter

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

func BenchmarkOpenMetricsHandlerCardinality(b *testing.B) {
	now := time.Unix(1739836800, 0)
	memStore := store.NewMemoryStore(24*time.Hour, 200000)

	const (
		orgs         = 5
		reposPerOrg  = 300
		usersPerRepo = 8
	)

	for orgIndex := range orgs {
		org := fmt.Sprintf("org-%d", orgIndex)
		for repoIndex := range reposPerOrg {
			repo := fmt.Sprintf("repo-%d", repoIndex)
			for userIndex := range usersPerRepo {
				user := fmt.Sprintf("user-%d", userIndex)
				err := memStore.UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, store.MetricPoint{
					Name: "gh_activity_commits_24h",
					Labels: map[string]string{
						"org":  org,
						"repo": repo,
						"user": user,
					},
					Value:     float64(userIndex + 1),
					UpdatedAt: now,
				})
				if err != nil {
					b.Fatalf("UpsertMetric() unexpected error: %v", err)
				}
			}
		}
	}

	handler := NewOpenMetricsHandler(memStore)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "application/openmetrics-text")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
		}
	}
}
