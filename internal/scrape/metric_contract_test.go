package scrape

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats/internal/store"
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
