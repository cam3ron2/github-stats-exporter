package scrape

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/store"
)

const (
	// LabelOrg is the required organization label key.
	LabelOrg = "org"
	// LabelRepo is the required repository label key.
	LabelRepo = "repo"
	// LabelUser is the required contributor label key.
	LabelUser = "user"

	// UnknownLabelValue is used when a required label is blank.
	UnknownLabelValue = "unknown"

	// MetricActivityCommits24h is the rolling 24h commit activity gauge.
	MetricActivityCommits24h = "gh_activity_commits_24h"
	// MetricActivityPROpened24h is the rolling 24h opened PR activity gauge.
	MetricActivityPROpened24h = "gh_activity_prs_opened_24h"
	// MetricActivityPRMerged24h is the rolling 24h merged PR activity gauge.
	MetricActivityPRMerged24h = "gh_activity_prs_merged_24h"
	// MetricActivityReviewsSubmitted24h is the rolling 24h submitted review activity gauge.
	MetricActivityReviewsSubmitted24h = "gh_activity_reviews_submitted_24h"
	// MetricActivityIssueComments24h is the rolling 24h issue comment activity gauge.
	MetricActivityIssueComments24h = "gh_activity_issue_comments_24h"
	// MetricActivityLOCAddedWeekly is the weekly LOC additions gauge.
	MetricActivityLOCAddedWeekly = "gh_activity_loc_added_weekly"
	// MetricActivityLOCRemovedWeekly is the weekly LOC removals gauge.
	MetricActivityLOCRemovedWeekly = "gh_activity_loc_removed_weekly"
	// MetricActivityLastEventUnixTime is the timestamp of the latest activity event.
	MetricActivityLastEventUnixTime = "gh_activity_last_event_unixtime"
)

var productivityMetricNames = []string{
	MetricActivityCommits24h,
	MetricActivityPROpened24h,
	MetricActivityPRMerged24h,
	MetricActivityReviewsSubmitted24h,
	MetricActivityIssueComments24h,
	MetricActivityLOCAddedWeekly,
	MetricActivityLOCRemovedWeekly,
	MetricActivityLastEventUnixTime,
}

// ProductivityMetricNames returns the stable set of supported productivity metric names.
func ProductivityMetricNames() []string {
	return slices.Clone(productivityMetricNames)
}

// IsProductivityMetric reports whether a metric name is supported by the scrape contract.
func IsProductivityMetric(name string) bool {
	return slices.Contains(productivityMetricNames, strings.TrimSpace(name))
}

// RequiredLabels builds the enforced org/repo/user label map.
func RequiredLabels(org, repo, user string) map[string]string {
	return map[string]string{
		LabelOrg:  normalizeRequiredLabel(org),
		LabelRepo: normalizeRequiredLabel(repo),
		LabelUser: normalizeRequiredLabel(user),
	}
}

// NewProductivityMetric builds a productivity metric point with required labels.
func NewProductivityMetric(name, org, repo, user string, value float64, updatedAt time.Time) (store.MetricPoint, error) {
	if !IsProductivityMetric(name) {
		return store.MetricPoint{}, fmt.Errorf("unsupported productivity metric: %s", name)
	}
	if updatedAt.IsZero() {
		return store.MetricPoint{}, fmt.Errorf("updated time is required")
	}

	point := store.MetricPoint{
		Name:      strings.TrimSpace(name),
		Labels:    RequiredLabels(org, repo, user),
		Value:     value,
		UpdatedAt: updatedAt,
	}
	if err := ValidateProductivityMetric(point); err != nil {
		return store.MetricPoint{}, err
	}
	return point, nil
}

// ValidateProductivityMetric validates that a metric point conforms to the scrape output contract.
func ValidateProductivityMetric(point store.MetricPoint) error {
	if !IsProductivityMetric(point.Name) {
		return fmt.Errorf("unsupported productivity metric: %s", point.Name)
	}
	if len(point.Labels) != 3 {
		return fmt.Errorf("productivity metric labels must contain exactly org, repo, and user")
	}

	for _, key := range []string{LabelOrg, LabelRepo, LabelUser} {
		value, ok := point.Labels[key]
		if !ok {
			return fmt.Errorf("productivity metric is missing %q label", key)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("productivity metric %q label must be non-empty", key)
		}
	}
	return nil
}

func normalizeRequiredLabel(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return UnknownLabelValue
	}
	return trimmed
}
