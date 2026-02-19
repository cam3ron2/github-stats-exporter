package scrape

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

func TestProductivityMetricNames(t *testing.T) {
	t.Parallel()

	got := ProductivityMetricNames()
	want := []string{
		MetricActivityCommits24h,
		MetricActivityPROpened24h,
		MetricActivityPRMerged24h,
		MetricActivityReviewsSubmitted24h,
		MetricActivityIssueComments24h,
		MetricActivityLOCAddedWeekly,
		MetricActivityLOCRemovedWeekly,
		MetricActivityLastEventUnixTime,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ProductivityMetricNames() = %v, want %v", got, want)
	}
}

func TestIsProductivityMetric(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		input  string
		wantOK bool
	}{
		{name: "known_metric", input: MetricActivityCommits24h, wantOK: true},
		{name: "known_metric_with_spaces", input: "  " + MetricActivityLOCAddedWeekly + "  ", wantOK: true},
		{name: "unknown_metric", input: "gh_activity_unknown", wantOK: false},
		{name: "empty_metric", input: "", wantOK: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsProductivityMetric(tc.input)
			if got != tc.wantOK {
				t.Fatalf("IsProductivityMetric(%q) = %t, want %t", tc.input, got, tc.wantOK)
			}
		})
	}
}

func TestRequiredLabels(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		org  string
		repo string
		user string
		want map[string]string
	}{
		{
			name: "uses_trimmed_values",
			org:  " org-a ",
			repo: " repo-a ",
			user: " alice ",
			want: map[string]string{
				LabelOrg:  "org-a",
				LabelRepo: "repo-a",
				LabelUser: "alice",
			},
		},
		{
			name: "normalizes_blank_values_to_unknown",
			org:  "",
			repo: " ",
			user: "",
			want: map[string]string{
				LabelOrg:  UnknownLabelValue,
				LabelRepo: UnknownLabelValue,
				LabelUser: UnknownLabelValue,
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := RequiredLabels(tc.org, tc.repo, tc.user)
			if got[LabelOrg] != tc.want[LabelOrg] {
				t.Fatalf("org label = %q, want %q", got[LabelOrg], tc.want[LabelOrg])
			}
			if got[LabelRepo] != tc.want[LabelRepo] {
				t.Fatalf("repo label = %q, want %q", got[LabelRepo], tc.want[LabelRepo])
			}
			if got[LabelUser] != tc.want[LabelUser] {
				t.Fatalf("user label = %q, want %q", got[LabelUser], tc.want[LabelUser])
			}
		})
	}
}

func TestNewProductivityMetric(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name        string
		metricName  string
		org         string
		repo        string
		user        string
		updatedAt   time.Time
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid_metric",
			metricName: MetricActivityLOCAddedWeekly,
			org:        "org-a",
			repo:       "repo-a",
			user:       "alice",
			updatedAt:  now,
		},
		{
			name:        "invalid_metric_name",
			metricName:  "gh_activity_unknown",
			org:         "org-a",
			repo:        "repo-a",
			user:        "alice",
			updatedAt:   now,
			wantErr:     true,
			errContains: "unsupported productivity metric",
		},
		{
			name:        "missing_updated_at",
			metricName:  MetricActivityLOCAddedWeekly,
			org:         "org-a",
			repo:        "repo-a",
			user:        "alice",
			updatedAt:   time.Time{},
			wantErr:     true,
			errContains: "updated time is required",
		},
		{
			name:       "normalizes_blank_labels",
			metricName: MetricActivityLOCAddedWeekly,
			org:        "",
			repo:       " ",
			user:       "",
			updatedAt:  now,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			point, err := NewProductivityMetric(tc.metricName, tc.org, tc.repo, tc.user, 1, tc.updatedAt)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewProductivityMetric() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewProductivityMetric() unexpected error: %v", err)
			}
			if err := ValidateProductivityMetric(point); err != nil {
				t.Fatalf("ValidateProductivityMetric() unexpected error: %v", err)
			}
		})
	}
}

func TestValidateProductivityMetric(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name        string
		point       store.MetricPoint
		wantErr     bool
		errContains string
	}{
		{
			name: "valid",
			point: store.MetricPoint{
				Name: MetricActivityLOCRemovedWeekly,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "repo-a",
					LabelUser: "alice",
				},
				Value:     1,
				UpdatedAt: now,
			},
		},
		{
			name: "missing_label",
			point: store.MetricPoint{
				Name: MetricActivityLOCRemovedWeekly,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "repo-a",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "exactly org, repo, and user",
		},
		{
			name: "unexpected_extra_label",
			point: store.MetricPoint{
				Name: MetricActivityLOCRemovedWeekly,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "repo-a",
					LabelUser: "alice",
					"team":    "platform",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "exactly org, repo, and user",
		},
		{
			name: "blank_user",
			point: store.MetricPoint{
				Name: MetricActivityLOCRemovedWeekly,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "repo-a",
					LabelUser: " ",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "user",
		},
		{
			name: "invalid_metric",
			point: store.MetricPoint{
				Name: "gh_activity_unknown",
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "repo-a",
					LabelUser: "alice",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "unsupported productivity metric",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateProductivityMetric(tc.point)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ValidateProductivityMetric() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateProductivityMetric() unexpected error: %v", err)
			}
		})
	}
}

func TestCopilotMetricNames(t *testing.T) {
	t.Parallel()

	got := CopilotMetricNames()
	want := []string{
		MetricCopilotUserInitiatedInteractions,
		MetricCopilotCodeGenerationActivity,
		MetricCopilotCodeAcceptanceActivity,
		MetricCopilotLOCSuggestedToAdd,
		MetricCopilotLOCSuggestedToDelete,
		MetricCopilotLOCAdded,
		MetricCopilotLOCDeleted,
		MetricCopilotPRTotalCreated,
		MetricCopilotPRTotalReviewed,
		MetricCopilotPRTotalCreatedByCopilot,
		MetricCopilotPRTotalReviewedByCopilot,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("CopilotMetricNames() = %v, want %v", got, want)
	}
}

func TestIsCopilotMetric(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		input  string
		wantOK bool
	}{
		{name: "known_metric", input: MetricCopilotLOCAdded, wantOK: true},
		{
			name:   "known_metric_with_spaces",
			input:  "  " + MetricCopilotPRTotalReviewed + "  ",
			wantOK: true,
		},
		{name: "unknown_metric", input: "gh_copilot_usage_unknown", wantOK: false},
		{name: "empty_metric", input: "", wantOK: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsCopilotMetric(tc.input)
			if got != tc.wantOK {
				t.Fatalf("IsCopilotMetric(%q) = %t, want %t", tc.input, got, tc.wantOK)
			}
		})
	}
}

func TestNewCopilotMetric(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name          string
		metricName    string
		org           string
		user          string
		optional      map[string]string
		updatedAt     time.Time
		wantErr       bool
		errContains   string
		wantUserLabel string
	}{
		{
			name:       "valid_metric",
			metricName: MetricCopilotLOCAdded,
			org:        "org-a",
			user:       "alice",
			optional: map[string]string{
				LabelScope:  "org",
				LabelWindow: "28d",
			},
			updatedAt:     now,
			wantUserLabel: "alice",
		},
		{
			name:        "invalid_metric_name",
			metricName:  "gh_copilot_usage_unknown",
			org:         "org-a",
			user:        "alice",
			updatedAt:   now,
			wantErr:     true,
			errContains: "unsupported copilot metric",
		},
		{
			name:        "missing_updated_at",
			metricName:  MetricCopilotLOCAdded,
			org:         "org-a",
			user:        "alice",
			updatedAt:   time.Time{},
			wantErr:     true,
			errContains: "updated time is required",
		},
		{
			name:       "normalizes_blank_labels",
			metricName: MetricCopilotLOCAdded,
			org:        "",
			user:       "",
			optional: map[string]string{
				LabelScope: " ",
			},
			updatedAt:     now,
			wantUserLabel: UnknownLabelValue,
		},
		{
			name:       "rejects_unsupported_optional_label",
			metricName: MetricCopilotLOCAdded,
			org:        "org-a",
			user:       "alice",
			optional: map[string]string{
				"team": "platform",
			},
			updatedAt:   now,
			wantErr:     true,
			errContains: "unsupported label",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			point, err := NewCopilotMetric(
				tc.metricName,
				tc.org,
				tc.user,
				1,
				tc.updatedAt,
				tc.optional,
			)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewCopilotMetric() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewCopilotMetric() unexpected error: %v", err)
			}
			if point.Labels[LabelRepo] != "*" {
				t.Fatalf("repo label = %q, want *", point.Labels[LabelRepo])
			}
			if point.Labels[LabelUser] != tc.wantUserLabel {
				t.Fatalf("user label = %q, want %q", point.Labels[LabelUser], tc.wantUserLabel)
			}
			if err := ValidateCopilotMetric(point); err != nil {
				t.Fatalf("ValidateCopilotMetric() unexpected error: %v", err)
			}
		})
	}
}

func TestValidateCopilotMetric(t *testing.T) {
	t.Parallel()

	now := time.Unix(1739836800, 0)
	testCases := []struct {
		name        string
		point       store.MetricPoint
		wantErr     bool
		errContains string
	}{
		{
			name: "valid",
			point: store.MetricPoint{
				Name: MetricCopilotLOCDeleted,
				Labels: map[string]string{
					LabelOrg:    "org-a",
					LabelRepo:   "*",
					LabelUser:   "alice",
					LabelScope:  "org",
					LabelWindow: "28d",
				},
				Value:     1,
				UpdatedAt: now,
			},
		},
		{
			name: "missing_required_label",
			point: store.MetricPoint{
				Name: MetricCopilotLOCDeleted,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "*",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "missing \"user\"",
		},
		{
			name: "repo_must_be_wildcard",
			point: store.MetricPoint{
				Name: MetricCopilotLOCDeleted,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "repo-a",
					LabelUser: "alice",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "label must be \"*\"",
		},
		{
			name: "unsupported_optional_label",
			point: store.MetricPoint{
				Name: MetricCopilotLOCDeleted,
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "*",
					LabelUser: "alice",
					"team":    "platform",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "unsupported label",
		},
		{
			name: "unsupported_metric",
			point: store.MetricPoint{
				Name: "gh_copilot_usage_unknown",
				Labels: map[string]string{
					LabelOrg:  "org-a",
					LabelRepo: "*",
					LabelUser: "alice",
				},
				Value:     1,
				UpdatedAt: now,
			},
			wantErr:     true,
			errContains: "unsupported copilot metric",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCopilotMetric(tc.point)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ValidateCopilotMetric() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateCopilotMetric() unexpected error: %v", err)
			}
		})
	}
}
