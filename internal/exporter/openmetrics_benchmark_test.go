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
				err := memStore.UpsertMetric(
					store.RoleLeader,
					store.SourceLeaderScrape,
					store.MetricPoint{
						Name: "gh_activity_commits_24h",
						Labels: map[string]string{
							"org":  org,
							"repo": repo,
							"user": user,
						},
						Value:     float64(userIndex + 1),
						UpdatedAt: now,
					},
				)
				if err != nil {
					b.Fatalf("UpsertMetric() unexpected error: %v", err)
				}
			}
		}
	}

	runOpenMetricsBenchmark(b, memStore)
}

func BenchmarkOpenMetricsHandlerCopilotUserBreakdownCardinality(b *testing.B) {
	now := time.Unix(1739836800, 0)
	memStore := store.NewMemoryStore(24*time.Hour, 200000)

	const (
		orgs          = 5
		usersPerOrg   = 250
		ides          = 2
		languages     = 2
		features      = 2
		modelFamilies = 2
	)

	for orgIndex := range orgs {
		org := fmt.Sprintf("org-%d", orgIndex)
		for userIndex := range usersPerOrg {
			user := fmt.Sprintf("user-%d", userIndex)
			for ideIndex := range ides {
				for languageIndex := range languages {
					for featureIndex := range features {
						for modelIndex := range modelFamilies {
							baseLabels := map[string]string{
								"org":      org,
								"repo":     "*",
								"user":     user,
								"scope":    "users",
								"window":   "28d",
								"ide":      fmt.Sprintf("ide-%d", ideIndex),
								"language": fmt.Sprintf("lang-%d", languageIndex),
								"feature":  fmt.Sprintf("feature-%d", featureIndex),
								"model":    fmt.Sprintf("model-%d", modelIndex),
							}
							addBenchmarkMetric(b, memStore, now, "gh_copilot_usage_user_initiated_interaction_count", baseLabels, 5)
							addBenchmarkMetric(b, memStore, now, "gh_copilot_usage_code_generation_activity_count", baseLabels, 8)
							addBenchmarkMetric(b, memStore, now, "gh_copilot_usage_loc_added_sum", baseLabels, 21)
						}
					}
				}
			}
		}
	}

	runOpenMetricsBenchmark(b, memStore)
}

func addBenchmarkMetric(
	b *testing.B,
	memStore *store.MemoryStore,
	now time.Time,
	name string,
	labels map[string]string,
	value float64,
) {
	err := memStore.UpsertMetric(store.RoleLeader, store.SourceLeaderScrape, store.MetricPoint{
		Name:      name,
		Labels:    labels,
		Value:     value,
		UpdatedAt: now,
	})
	if err != nil {
		b.Fatalf("UpsertMetric() unexpected error: %v", err)
	}
}

func runOpenMetricsBenchmark(b *testing.B, memStore *store.MemoryStore) {
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
