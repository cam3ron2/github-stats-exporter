package scrape

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
)

func TestGitHubOrgScraperScrapeOrgCopilot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
			}, nil
		},
		getOrgCopilotOrganization28DayLatestReportLinkFn: func(_ context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
			return githubapi.CopilotReportLinkResult{
				Status:         githubapi.EndpointStatusOK,
				URL:            "https://reports.example.com/copilot.ndjson",
				DownloadLinks:  []string{"https://reports.example.com/copilot.ndjson"},
				ReportStartDay: time.Date(2026, time.January, 22, 0, 0, 0, 0, time.UTC),
				ReportEndDay:   time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC),
			}, nil
		},
		streamCopilotReportNDJSONFn: func(
			_ context.Context,
			_ string,
			handler func(record map[string]any) error,
		) (githubapi.CopilotReportStreamResult, error) {
			if err := handler(map[string]any{
				"total_code_acceptances":                  float64(3),
				"total_code_suggestions":                  float64(10),
				"total_code_lines_suggested":              float64(120),
				"total_code_lines_accepted":               float64(90),
				"total_active_users":                      float64(5),
				"total_pr_summaries_created":              float64(2),
				"total_pr_summaries_created_users":        float64(2),
				"total_pull_requests_created":             float64(4),
				"total_pull_requests_reviewed":            float64(7),
				"total_pull_requests_created_by_copilot":  float64(1),
				"total_pull_requests_reviewed_by_copilot": float64(3),
				"day": "2026-02-18",
			}); err != nil {
				return githubapi.CopilotReportStreamResult{}, err
			}
			return githubapi.CopilotReportStreamResult{
				Status:        githubapi.EndpointStatusOK,
				RecordsParsed: 1,
				ParseErrors:   0,
			}, nil
		},
	}

	checkpoints := &fakeCheckpointStore{}
	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now:         func() time.Time { return now },
		Checkpoints: checkpoints,
		Copilot: config.CopilotConfig{
			Enabled:                   true,
			IncludeOrg28d:             true,
			IncludeOrgUsers28d:        false,
			IncludeEnterprise28d:      false,
			IncludeEnterpriseUsers28d: false,
			IncludeBreakdownFeature:   false,
			EmitDayLabel:              true,
		},
	})

	got, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(got.MissedWindow) != 0 {
		t.Fatalf("MissedWindow len = %d, want 0", len(got.MissedWindow))
	}
	if got.Summary.MetricsProduced == 0 {
		t.Fatalf("summary metrics produced = 0, want > 0")
	}

	metric := findMetric(got.Metrics, MetricCopilotCodeAcceptanceActivity, "org-a", "*", "all")
	if metric == nil {
		t.Fatalf("missing %s metric", MetricCopilotCodeAcceptanceActivity)
	}
	if metric.Value != 3 {
		t.Fatalf("metric value = %v, want 3", metric.Value)
	}
	if metric.Labels[LabelScope] != "org" {
		t.Fatalf("scope label = %q, want org", metric.Labels[LabelScope])
	}
	if metric.Labels[LabelWindow] != "28d" {
		t.Fatalf("window label = %q, want 28d", metric.Labels[LabelWindow])
	}
	if metric.Labels[LabelDay] != "2026-02-18" {
		t.Fatalf("day label = %q, want 2026-02-18", metric.Labels[LabelDay])
	}

	checkpointKey := "org-a/" + copilotCheckpointRepoKey("org", "28d")
	checkpoint := checkpoints.checkpoints[checkpointKey]
	wantCheckpoint := time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC)
	if !checkpoint.Equal(wantCheckpoint) {
		t.Fatalf("checkpoint = %s, want %s", checkpoint, wantCheckpoint)
	}
}

func TestGitHubOrgScraperScrapeOrgCopilotFailuresEnqueueMissedWindows(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
			}, nil
		},
		getOrgCopilotOrganization28DayLatestReportLinkFn: func(_ context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
			return githubapi.CopilotReportLinkResult{}, context.DeadlineExceeded
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
		Copilot: config.CopilotConfig{
			Enabled:            true,
			IncludeOrg28d:      true,
			IncludeOrgUsers28d: false,
		},
	})

	got, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if len(got.MissedWindow) != 1 {
		t.Fatalf("MissedWindow len = %d, want 1", len(got.MissedWindow))
	}
	if got.MissedWindow[0].Reason != "copilot_report_fetch_error" {
		t.Fatalf(
			"missed reason = %q, want copilot_report_fetch_error",
			got.MissedWindow[0].Reason,
		)
	}
}

func TestGitHubOrgScraperScrapeBackfillCopilot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	windowDay := time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC)
	client := &fakeGitHubDataClient{
		getOrgCopilotOrganization1DayReportLinkFn: func(
			_ context.Context,
			_ string,
			day time.Time,
		) (githubapi.CopilotReportLinkResult, error) {
			if !day.Equal(windowDay) {
				t.Fatalf("day = %s, want %s", day, windowDay)
			}
			return githubapi.CopilotReportLinkResult{
				Status:        githubapi.EndpointStatusOK,
				URL:           "https://reports.example.com/copilot.ndjson",
				DownloadLinks: []string{"https://reports.example.com/copilot.ndjson"},
				ReportDay:     windowDay,
				ReportEndDay:  windowDay,
			}, nil
		},
		streamCopilotReportNDJSONFn: func(
			_ context.Context,
			_ string,
			handler func(record map[string]any) error,
		) (githubapi.CopilotReportStreamResult, error) {
			if err := handler(map[string]any{
				"total_code_acceptances": float64(2),
				"day":                    "2026-02-18",
			}); err != nil {
				return githubapi.CopilotReportStreamResult{}, err
			}
			return githubapi.CopilotReportStreamResult{
				Status:        githubapi.EndpointStatusOK,
				RecordsParsed: 1,
			}, nil
		},
	}

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
		Copilot: config.CopilotConfig{
			Enabled:            true,
			IncludeOrg28d:      true,
			UserLabelMode:      "login",
			EmitDayLabel:       true,
			RequestTimeout:     30 * time.Second,
			DownloadTimeout:    90 * time.Second,
			ScrapeInterval:     6 * time.Hour,
			IncludeOrgUsers28d: false,
		},
	})

	got, err := scraper.ScrapeBackfill(
		context.Background(),
		config.GitHubOrgConfig{Org: "org-a"},
		copilotCheckpointRepoKey("org", "28d"),
		windowDay,
		windowDay,
		repoMissReasonCopilotFetch,
	)
	if err != nil {
		t.Fatalf("ScrapeBackfill() unexpected error: %v", err)
	}
	if len(got.MissedWindow) != 0 {
		t.Fatalf("MissedWindow len = %d, want 0", len(got.MissedWindow))
	}
	metric := findMetric(got.Metrics, MetricCopilotCodeAcceptanceActivity, "org-a", "*", "all")
	if metric == nil {
		t.Fatalf("missing %s metric", MetricCopilotCodeAcceptanceActivity)
	}
	if metric.Value != 2 {
		t.Fatalf("metric value = %v, want 2", metric.Value)
	}
}

func TestGitHubOrgScraperScrapeOrgCopilotHonorsScrapeInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	current := now
	windowEnd := time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC)

	var fetchCalls atomic.Int32
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
			}, nil
		},
		getOrgCopilotOrganization28DayLatestReportLinkFn: func(_ context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
			fetchCalls.Add(1)
			return githubapi.CopilotReportLinkResult{
				Status: githubapi.EndpointStatusOK,
				URL:    "https://reports.example.com/copilot.ndjson",
				DownloadLinks: []string{
					"https://reports.example.com/copilot.ndjson",
				},
				ReportEndDay: windowEnd,
			}, nil
		},
		streamCopilotReportNDJSONFn: func(
			_ context.Context,
			_ string,
			handler func(record map[string]any) error,
		) (githubapi.CopilotReportStreamResult, error) {
			if err := handler(map[string]any{
				"total_code_acceptances": float64(3),
			}); err != nil {
				return githubapi.CopilotReportStreamResult{}, err
			}
			return githubapi.CopilotReportStreamResult{
				Status:        githubapi.EndpointStatusOK,
				RecordsParsed: 1,
			}, nil
		},
	}

	checkpoints := &fakeCheckpointStore{
		checkpoints: map[string]time.Time{
			"org-a/" + copilotLastRunRepoKey("org", "28d"): now.Add(-30 * time.Minute),
		},
	}
	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now:         func() time.Time { return current },
		Checkpoints: checkpoints,
		Copilot: config.CopilotConfig{
			Enabled:        true,
			IncludeOrg28d:  true,
			ScrapeInterval: 6 * time.Hour,
		},
	})

	first, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg(first) unexpected error: %v", err)
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("copilot fetch calls(first) = %d, want 0", got)
	}
	if metric := findMetricWithLabels(first.Metrics, "gh_exporter_copilot_scrape_runs_total", map[string]string{
		"scope":  "org",
		"result": "skipped_interval",
	}); metric == nil {
		t.Fatalf("missing copilot skipped_interval metric")
	}

	current = current.Add(7 * time.Hour)
	second, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg(second) unexpected error: %v", err)
	}
	if got := fetchCalls.Load(); got != 1 {
		t.Fatalf("copilot fetch calls(second) = %d, want 1", got)
	}
	if metric := findMetric(second.Metrics, MetricCopilotCodeAcceptanceActivity, "org-a", "*", "all"); metric == nil {
		t.Fatalf("missing %s metric after interval elapsed", MetricCopilotCodeAcceptanceActivity)
	}
}

func TestGitHubOrgScraperScrapeOrgCopilotSkipsUnchangedReport(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	reportEnd := time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC)
	streamCalls := 0
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
			}, nil
		},
		getOrgCopilotOrganization28DayLatestReportLinkFn: func(_ context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
			return githubapi.CopilotReportLinkResult{
				Status: githubapi.EndpointStatusOK,
				URL:    "https://reports.example.com/copilot.ndjson",
				DownloadLinks: []string{
					"https://reports.example.com/copilot.ndjson",
				},
				ReportEndDay: reportEnd,
			}, nil
		},
		streamCopilotReportNDJSONFn: func(
			_ context.Context,
			_ string,
			_ func(record map[string]any) error,
		) (githubapi.CopilotReportStreamResult, error) {
			streamCalls++
			return githubapi.CopilotReportStreamResult{
				Status: githubapi.EndpointStatusOK,
			}, nil
		},
	}
	checkpoints := &fakeCheckpointStore{
		checkpoints: map[string]time.Time{
			"org-a/" + copilotCheckpointRepoKey("org", "28d"): reportEnd,
		},
	}
	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now:         func() time.Time { return now },
		Checkpoints: checkpoints,
		Copilot: config.CopilotConfig{
			Enabled:                  true,
			IncludeOrg28d:            true,
			RefreshIfReportUnchanged: false,
		},
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if streamCalls != 0 {
		t.Fatalf("stream calls = %d, want 0", streamCalls)
	}
	if metric := findMetricWithLabels(result.Metrics, "gh_exporter_copilot_scrape_runs_total", map[string]string{
		"scope":  "org",
		"result": "skipped_unchanged",
	}); metric == nil {
		t.Fatalf("missing skipped_unchanged metric")
	}
}

func TestGitHubOrgScraperScrapeOrgCopilotRequestAndDownloadTimeouts(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	var requestRemaining time.Duration
	var downloadRemaining time.Duration

	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
			}, nil
		},
		getOrgCopilotOrganization28DayLatestReportLinkFn: func(ctx context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatalf("expected request context deadline to be set")
			}
			requestRemaining = time.Until(deadline)
			return githubapi.CopilotReportLinkResult{
				Status: githubapi.EndpointStatusOK,
				URL:    "https://reports.example.com/copilot.ndjson",
				DownloadLinks: []string{
					"https://reports.example.com/copilot.ndjson",
				},
				ReportEndDay: now,
			}, nil
		},
		streamCopilotReportNDJSONFn: func(
			ctx context.Context,
			_ string,
			handler func(record map[string]any) error,
		) (githubapi.CopilotReportStreamResult, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatalf("expected download context deadline to be set")
			}
			downloadRemaining = time.Until(deadline)
			if err := handler(map[string]any{
				"total_code_acceptances": float64(3),
			}); err != nil {
				return githubapi.CopilotReportStreamResult{}, err
			}
			return githubapi.CopilotReportStreamResult{
				Status:        githubapi.EndpointStatusOK,
				RecordsParsed: 1,
			}, nil
		},
	}
	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
		Copilot: config.CopilotConfig{
			Enabled:         true,
			IncludeOrg28d:   true,
			RequestTimeout:  3 * time.Second,
			DownloadTimeout: 5 * time.Second,
		},
	})

	_, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if requestRemaining <= 0 || requestRemaining > 4*time.Second {
		t.Fatalf("request timeout remaining = %s, want 0 < d <= 4s", requestRemaining)
	}
	if downloadRemaining <= 0 || downloadRemaining > 6*time.Second {
		t.Fatalf("download timeout remaining = %s, want 0 < d <= 6s", downloadRemaining)
	}
}

func TestGitHubOrgScraperScrapeOrgCopilotPullRequestActivityToggle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	client := &fakeGitHubDataClient{
		listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
			return githubapi.OrgReposResult{
				Status: githubapi.EndpointStatusOK,
				Repos:  nil,
			}, nil
		},
		getOrgCopilotOrganization28DayLatestReportLinkFn: func(_ context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
			return githubapi.CopilotReportLinkResult{
				Status: githubapi.EndpointStatusOK,
				URL:    "https://reports.example.com/copilot.ndjson",
				DownloadLinks: []string{
					"https://reports.example.com/copilot.ndjson",
				},
				ReportEndDay: now,
			}, nil
		},
		streamCopilotReportNDJSONFn: func(
			_ context.Context,
			_ string,
			handler func(record map[string]any) error,
		) (githubapi.CopilotReportStreamResult, error) {
			if err := handler(map[string]any{
				"total_code_acceptances":       float64(3),
				"total_pull_requests_created":  float64(4),
				"total_pull_requests_reviewed": float64(7),
			}); err != nil {
				return githubapi.CopilotReportStreamResult{}, err
			}
			return githubapi.CopilotReportStreamResult{
				Status:        githubapi.EndpointStatusOK,
				RecordsParsed: 1,
			}, nil
		},
	}
	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": client,
	}, GitHubOrgScraperConfig{
		Now: func() time.Time { return now },
		Copilot: config.CopilotConfig{
			Enabled:                    true,
			IncludeOrg28d:              true,
			IncludePullRequestActivity: false,
		},
	})

	result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
		Org: "org-a",
	})
	if err != nil {
		t.Fatalf("ScrapeOrg() unexpected error: %v", err)
	}
	if metric := findMetric(result.Metrics, MetricCopilotCodeAcceptanceActivity, "org-a", "*", "all"); metric == nil {
		t.Fatalf("missing %s metric", MetricCopilotCodeAcceptanceActivity)
	}
	for _, metricName := range []string{
		MetricCopilotPRTotalCreated,
		MetricCopilotPRTotalReviewed,
		MetricCopilotPRTotalCreatedByCopilot,
		MetricCopilotPRTotalReviewedByCopilot,
	} {
		if metric := findMetric(result.Metrics, metricName, "org-a", "*", "all"); metric != nil {
			t.Fatalf("expected %s metric to be omitted when include_pull_request_activity=false", metricName)
		}
	}
}

func TestGitHubOrgScraperScrapeOrgCopilotGuardrailMetrics(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name          string
		maxRecords    int
		maxUsers      int
		records       []map[string]any
		wantReason    string
		wantDropCount float64
	}{
		{
			name:       "record_limit_drops_extra_records",
			maxRecords: 1,
			records: []map[string]any{
				{
					"user_login":             "alice",
					"total_code_acceptances": float64(3),
				},
				{
					"user_login":             "alice",
					"total_code_acceptances": float64(4),
				},
			},
			wantReason:    "max_records_per_report",
			wantDropCount: 1,
		},
		{
			name:     "user_limit_drops_new_users_after_cap",
			maxUsers: 1,
			records: []map[string]any{
				{
					"user_login":             "alice",
					"total_code_acceptances": float64(3),
				},
				{
					"user_login":             "bob",
					"total_code_acceptances": float64(4),
				},
			},
			wantReason:    "max_users_per_report",
			wantDropCount: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := &fakeGitHubDataClient{
				listOrgReposFn: func(_ context.Context, _ string) (githubapi.OrgReposResult, error) {
					return githubapi.OrgReposResult{
						Status: githubapi.EndpointStatusOK,
						Repos:  nil,
					}, nil
				},
				getOrgCopilotUsers28DayLatestReportLinkFn: func(_ context.Context, _ string) (githubapi.CopilotReportLinkResult, error) {
					return githubapi.CopilotReportLinkResult{
						Status: githubapi.EndpointStatusOK,
						URL:    "https://reports.example.com/copilot.ndjson",
						DownloadLinks: []string{
							"https://reports.example.com/copilot.ndjson",
						},
						ReportEndDay: now,
					}, nil
				},
				streamCopilotReportNDJSONFn: func(
					_ context.Context,
					_ string,
					handler func(record map[string]any) error,
				) (githubapi.CopilotReportStreamResult, error) {
					for _, record := range tc.records {
						if err := handler(record); err != nil {
							return githubapi.CopilotReportStreamResult{}, err
						}
					}
					return githubapi.CopilotReportStreamResult{
						Status:        githubapi.EndpointStatusOK,
						RecordsParsed: len(tc.records),
					}, nil
				},
			}

			scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
				"org-a": client,
			}, GitHubOrgScraperConfig{
				Now: func() time.Time { return now },
				Copilot: config.CopilotConfig{
					Enabled:             true,
					IncludeOrg28d:       false,
					IncludeOrgUsers28d:  true,
					MaxRecordsPerReport: tc.maxRecords,
					MaxUsersPerReport:   tc.maxUsers,
				},
			})

			result, err := scraper.ScrapeOrg(context.Background(), config.GitHubOrgConfig{
				Org: "org-a",
			})
			if err != nil {
				t.Fatalf("ScrapeOrg() unexpected error: %v", err)
			}
			metric := findMetricWithLabels(result.Metrics, "gh_exporter_copilot_records_dropped_total", map[string]string{
				"scope":  "users",
				"reason": tc.wantReason,
			})
			if metric == nil {
				t.Fatalf("missing guardrail metric reason=%s", tc.wantReason)
			}
			if metric.Value != tc.wantDropCount {
				t.Fatalf("guardrail metric value = %v, want %v", metric.Value, tc.wantDropCount)
			}
		})
	}
}
