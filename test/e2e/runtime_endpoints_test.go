//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cam3ron2/github-stats-exporter/internal/app"
	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/scrape"
	"github.com/cam3ron2/github-stats-exporter/internal/store"
	"go.uber.org/zap"
)

type runtimeHarness struct {
	leaderURL   string
	followerURL string
	httpClient  *http.Client
}

func TestRuntimeEndpointsConverge(t *testing.T) {
	t.Parallel()

	harness := newRuntimeHarness(t)

	t.Run("health_endpoints_report_ready_roles", func(t *testing.T) {
		err := waitForCondition(20*time.Second, 100*time.Millisecond, func() (bool, error) {
			leaderStatus, err := fetchHealthStatus(harness.httpClient, harness.leaderURL)
			if err != nil {
				return false, err
			}
			followerStatus, err := fetchHealthStatus(harness.httpClient, harness.followerURL)
			if err != nil {
				return false, err
			}

			leaderReady := leaderStatus.Role == "leader" && leaderStatus.Ready
			followerReady := followerStatus.Role == "follower" && followerStatus.Ready
			return leaderReady && followerReady, nil
		})
		if err != nil {
			t.Fatalf("health endpoints did not converge: %v", err)
		}
	})

	t.Run("metrics_endpoints_converge_with_parallel_fetch", func(t *testing.T) {
		var lastLeaderMetrics string
		var lastFollowerMetrics string

		err := waitForCondition(2*time.Minute, 500*time.Millisecond, func() (bool, error) {
			leaderMetrics, followerMetrics, fetchErr := fetchMetricsPair(
				harness.httpClient,
				harness.leaderURL,
				harness.followerURL,
			)
			if fetchErr != nil {
				return false, fetchErr
			}
			lastLeaderMetrics = leaderMetrics
			lastFollowerMetrics = followerMetrics

			if !strings.Contains(leaderMetrics, "gh_exporter_dependency_health") {
				return false, nil
			}
			if !strings.Contains(followerMetrics, "gh_exporter_dependency_health") {
				return false, nil
			}

			leaderOrgs := extractOrgLabels(leaderMetrics)
			followerOrgs := extractOrgLabels(followerMetrics)
			if len(leaderOrgs) < 2 || len(followerOrgs) < 2 {
				return false, nil
			}
			if !equalStringSets(leaderOrgs, followerOrgs) {
				return false, nil
			}

			leaderActivity := extractMetricLineSet(leaderMetrics, "gh_activity_")
			followerActivity := extractMetricLineSet(followerMetrics, "gh_activity_")
			if len(leaderActivity) == 0 || len(followerActivity) == 0 {
				return false, nil
			}
			if !equalStringSets(leaderActivity, followerActivity) {
				return false, nil
			}

			return true, nil
		})
		if err != nil {
			leaderOrgs := extractOrgLabels(lastLeaderMetrics)
			followerOrgs := extractOrgLabels(lastFollowerMetrics)
			leaderActivity := extractMetricLineSet(lastLeaderMetrics, "gh_activity_")
			followerActivity := extractMetricLineSet(lastFollowerMetrics, "gh_activity_")

			t.Fatalf(
				"metrics did not converge: %v; leader_orgs=%v follower_orgs=%v leader_only=%v follower_only=%v",
				err,
				sortedKeys(leaderOrgs),
				sortedKeys(followerOrgs),
				missingKeys(leaderActivity, followerActivity, 8),
				missingKeys(followerActivity, leaderActivity, 8),
			)
		}
	})
}

func newRuntimeHarness(t *testing.T) runtimeHarness {
	t.Helper()

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(redisServer.Close)

	now := time.Now().UTC()
	scraper := &fakeOrgScraper{
		resultsByOrg: map[string]scrape.OrgResult{
			"cam3ron2": buildOrgResult(t, now, "cam3ron2", "repo-a", map[string]float64{
				"alice": 3,
				"bob":   2,
			}),
			"shigops": buildOrgResult(t, now, "shigops", "repo-b", map[string]float64{
				"carol": 4,
			}),
		},
	}

	cfg := buildRuntimeConfig(redisServer.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	leaderRuntime := app.NewRuntime(cfg, scraper, zap.NewNop())
	followerRuntime := app.NewRuntime(cfg, scraper, zap.NewNop())
	leaderRuntime.StartLeader(ctx)
	followerRuntime.StartFollower(ctx)

	t.Cleanup(func() {
		leaderRuntime.StopLeader()
		followerRuntime.StopFollower()
	})

	leaderServer := httptest.NewServer(leaderRuntime.Handler())
	followerServer := httptest.NewServer(followerRuntime.Handler())
	t.Cleanup(leaderServer.Close)
	t.Cleanup(followerServer.Close)

	return runtimeHarness{
		leaderURL:   leaderServer.URL,
		followerURL: followerServer.URL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func buildRuntimeConfig(redisAddr string) *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			ListenAddr: ":0",
			LogLevel:   "debug",
		},
		Metrics: config.MetricsConfig{
			Topology:         "single_service_target",
			ScrapeServiceDNS: "localhost",
		},
		GitHub: config.GitHubConfig{
			APIBaseURL:                "",
			RequestTimeout:            2 * time.Second,
			UnhealthyFailureThreshold: 5,
			UnhealthyCooldown:         time.Minute,
			Orgs: []config.GitHubOrgConfig{
				{
					Org:            "cam3ron2",
					InstallationID: 101,
					ScrapeInterval: time.Hour,
				},
				{
					Org:            "shigops",
					InstallationID: 202,
					ScrapeInterval: time.Hour,
				},
			},
		},
		Retry: config.RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     500 * time.Millisecond,
		},
		Backfill: config.BackfillConfig{
			MaxMessageAge: time.Hour,
			ConsumerCount: 1,
			RequeueDelays: []time.Duration{time.Minute},
			DedupTTL:      time.Hour,
		},
		Store: config.StoreConfig{
			Backend:               "redis",
			RedisMode:             "standalone",
			RedisAddr:             redisAddr,
			Retention:             24 * time.Hour,
			MetricRefreshInterval: 100 * time.Millisecond,
			ExportCacheMode:       "full",
			MaxSeriesBudget:       10000,
			IndexShards:           4,
		},
		Health: config.HealthConfig{
			GitHubProbeInterval:           250 * time.Millisecond,
			GitHubRecoverSuccessThreshold: 1,
		},
	}
}

func buildOrgResult(
	t *testing.T,
	now time.Time,
	org string,
	repo string,
	commitsByUser map[string]float64,
) scrape.OrgResult {
	t.Helper()

	users := make([]string, 0, len(commitsByUser))
	for user := range commitsByUser {
		users = append(users, user)
	}
	sort.Strings(users)

	points := make([]store.MetricPoint, 0, len(users)*2)
	for _, user := range users {
		commits := commitsByUser[user]
		commitMetric, err := scrape.NewProductivityMetric(
			scrape.MetricActivityCommits24h,
			org,
			repo,
			user,
			commits,
			now,
		)
		if err != nil {
			t.Fatalf("build commit metric for %s/%s/%s: %v", org, repo, user, err)
		}
		locMetric, err := scrape.NewProductivityMetric(
			scrape.MetricActivityLOCAddedWeekly,
			org,
			repo,
			user,
			commits*10,
			now,
		)
		if err != nil {
			t.Fatalf("build loc metric for %s/%s/%s: %v", org, repo, user, err)
		}
		points = append(points, commitMetric, locMetric)
	}

	return scrape.OrgResult{
		Metrics: points,
		Summary: scrape.OrgSummary{
			ReposDiscovered:       1,
			ReposTargeted:         1,
			ReposProcessed:        1,
			MetricsProduced:       len(points),
			RateLimitMinRemaining: 4500,
			RateLimitResetUnix:    now.Unix() + 3600,
		},
	}
}

type fakeOrgScraper struct {
	resultsByOrg map[string]scrape.OrgResult
}

func (s *fakeOrgScraper) ScrapeOrg(_ context.Context, org config.GitHubOrgConfig) (scrape.OrgResult, error) {
	result, ok := s.resultsByOrg[org.Org]
	if !ok {
		return scrape.OrgResult{}, fmt.Errorf("org %q not configured in fake scraper", org.Org)
	}
	return cloneOrgResult(result), nil
}

func cloneOrgResult(in scrape.OrgResult) scrape.OrgResult {
	metrics := make([]store.MetricPoint, 0, len(in.Metrics))
	for _, point := range in.Metrics {
		metrics = append(metrics, store.MetricPoint{
			Name:      point.Name,
			Labels:    cloneStringMap(point.Labels),
			Value:     point.Value,
			UpdatedAt: point.UpdatedAt,
		})
	}
	missed := make([]scrape.MissedWindow, 0, len(in.MissedWindow))
	missed = append(missed, in.MissedWindow...)

	return scrape.OrgResult{
		Metrics:      metrics,
		MissedWindow: missed,
		Summary:      in.Summary,
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(in))
	for key, value := range in {
		cloned[key] = value
	}
	return cloned
}

type healthStatus struct {
	Role  string `json:"role"`
	Ready bool   `json:"ready"`
}

func fetchHealthStatus(client *http.Client, baseURL string) (healthStatus, error) {
	body, err := fetchEndpoint(client, baseURL+"/healthz")
	if err != nil {
		return healthStatus{}, err
	}
	var status healthStatus
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		return healthStatus{}, fmt.Errorf("decode health payload: %w", err)
	}
	return status, nil
}

func fetchMetricsPair(client *http.Client, leaderURL, followerURL string) (string, string, error) {
	type response struct {
		body string
		err  error
	}

	var wg sync.WaitGroup
	leaderCh := make(chan response, 1)
	followerCh := make(chan response, 1)

	wg.Go(func() {
		body, err := fetchEndpoint(client, leaderURL+"/metrics")
		leaderCh <- response{body: body, err: err}
	})
	wg.Go(func() {
		body, err := fetchEndpoint(client, followerURL+"/metrics")
		followerCh <- response{body: body, err: err}
	})
	wg.Wait()

	leaderResponse := <-leaderCh
	followerResponse := <-followerCh
	if leaderResponse.err != nil {
		return "", "", leaderResponse.err
	}
	if followerResponse.err != nil {
		return "", "", followerResponse.err
	}
	return leaderResponse.body, followerResponse.body, nil
}

func fetchEndpoint(client *http.Client, endpoint string) (string, error) {
	resp, err := client.Get(endpoint)
	if err != nil {
		return "", fmt.Errorf("request %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read %s body: %w", endpoint, err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("request %s returned status %d", endpoint, resp.StatusCode)
	}
	return string(body), nil
}

func waitForCondition(timeout, interval time.Duration, fn func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := fn()
		if ok && err == nil {
			return nil
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(interval)
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("condition did not converge before timeout")
}

func extractOrgLabels(metrics string) map[string]struct{} {
	labels := make(map[string]struct{})
	for _, line := range strings.Split(metrics, "\n") {
		start := strings.Index(line, `org="`)
		if start < 0 {
			continue
		}
		label := extractLabelValue(line[start+len(`org="`):])
		if label == "" {
			continue
		}
		labels[label] = struct{}{}
	}
	return labels
}

func extractLabelValue(raw string) string {
	end := strings.Index(raw, `"`)
	if end < 0 {
		return ""
	}
	return raw[:end]
}

func extractMetricLineSet(metrics string, prefix string) map[string]struct{} {
	lines := make(map[string]struct{})
	for _, line := range strings.Split(metrics, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		lines[line] = struct{}{}
	}
	return lines
}

func equalStringSets(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func sortedKeys(input map[string]struct{}) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func missingKeys(left, right map[string]struct{}, limit int) []string {
	missing := make([]string, 0)
	for key := range left {
		if _, ok := right[key]; ok {
			continue
		}
		missing = append(missing, key)
	}
	sort.Strings(missing)
	if limit > 0 && len(missing) > limit {
		return missing[:limit]
	}
	return missing
}
