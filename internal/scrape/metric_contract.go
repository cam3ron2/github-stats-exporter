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

	// MetricCopilotUserInitiatedInteractions is the Copilot user-initiated interaction count.
	MetricCopilotUserInitiatedInteractions = "gh_copilot_usage_user_initiated_interaction_count"
	// MetricCopilotCodeGenerationActivity is the Copilot code generation activity count.
	MetricCopilotCodeGenerationActivity = "gh_copilot_usage_code_generation_activity_count"
	// MetricCopilotCodeAcceptanceActivity is the Copilot code acceptance activity count.
	MetricCopilotCodeAcceptanceActivity = "gh_copilot_usage_code_acceptance_activity_count"
	// MetricCopilotLOCSuggestedToAdd is Copilot-suggested lines to add.
	MetricCopilotLOCSuggestedToAdd = "gh_copilot_usage_loc_suggested_to_add_sum"
	// MetricCopilotLOCSuggestedToDelete is Copilot-suggested lines to delete.
	MetricCopilotLOCSuggestedToDelete = "gh_copilot_usage_loc_suggested_to_delete_sum"
	// MetricCopilotLOCAdded is accepted Copilot lines added.
	MetricCopilotLOCAdded = "gh_copilot_usage_loc_added_sum"
	// MetricCopilotLOCDeleted is accepted Copilot lines deleted.
	MetricCopilotLOCDeleted = "gh_copilot_usage_loc_deleted_sum"
	// MetricCopilotPRTotalCreated is the total pull requests created.
	MetricCopilotPRTotalCreated = "gh_copilot_usage_pull_requests_total_created"
	// MetricCopilotPRTotalReviewed is the total pull requests reviewed.
	MetricCopilotPRTotalReviewed = "gh_copilot_usage_pull_requests_total_reviewed"
	// MetricCopilotPRTotalCreatedByCopilot is pull requests created by Copilot.
	MetricCopilotPRTotalCreatedByCopilot = "gh_copilot_usage_pull_requests_total_created_by_copilot"
	// MetricCopilotPRTotalReviewedByCopilot is pull requests reviewed by Copilot.
	MetricCopilotPRTotalReviewedByCopilot = "gh_copilot_usage_pull_requests_total_reviewed_by_copilot"

	// LabelScope identifies Copilot metric scope.
	LabelScope = "scope"
	// LabelWindow identifies Copilot report window.
	LabelWindow = "window"
	// LabelFeature identifies Copilot feature breakdown.
	LabelFeature = "feature"
	// LabelIDE identifies IDE/editor breakdown.
	LabelIDE = "ide"
	// LabelLanguage identifies language breakdown.
	LabelLanguage = "language"
	// LabelModel identifies model breakdown.
	LabelModel = "model"
	// LabelChatMode identifies chat mode breakdown.
	LabelChatMode = "chat_mode"
	// LabelDay identifies report day when enabled.
	LabelDay = "day"
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

var copilotMetricNames = []string{
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

var copilotOptionalLabelKeys = []string{
	LabelScope,
	LabelWindow,
	LabelFeature,
	LabelIDE,
	LabelLanguage,
	LabelModel,
	LabelChatMode,
	LabelDay,
}

// ProductivityMetricNames returns the stable set of supported productivity metric names.
func ProductivityMetricNames() []string {
	return slices.Clone(productivityMetricNames)
}

// IsProductivityMetric reports whether a metric name is supported by the scrape contract.
func IsProductivityMetric(name string) bool {
	return slices.Contains(productivityMetricNames, strings.TrimSpace(name))
}

// CopilotMetricNames returns the stable set of supported Copilot metric names.
func CopilotMetricNames() []string {
	return slices.Clone(copilotMetricNames)
}

// IsCopilotMetric reports whether a metric name is supported by the Copilot contract.
func IsCopilotMetric(name string) bool {
	return slices.Contains(copilotMetricNames, strings.TrimSpace(name))
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

// NewCopilotMetric builds a Copilot metric point with required labels.
func NewCopilotMetric(
	name string,
	org string,
	user string,
	value float64,
	updatedAt time.Time,
	optionalLabels map[string]string,
) (store.MetricPoint, error) {
	if !IsCopilotMetric(name) {
		return store.MetricPoint{}, fmt.Errorf("unsupported copilot metric: %s", name)
	}
	if updatedAt.IsZero() {
		return store.MetricPoint{}, fmt.Errorf("updated time is required")
	}

	labels := RequiredLabels(org, "*", user)
	for key, raw := range optionalLabels {
		trimmedKey := strings.TrimSpace(key)
		if trimmedKey == "" {
			continue
		}
		labels[trimmedKey] = normalizeRequiredLabel(raw)
	}

	point := store.MetricPoint{
		Name:      strings.TrimSpace(name),
		Labels:    labels,
		Value:     value,
		UpdatedAt: updatedAt,
	}
	if err := ValidateCopilotMetric(point); err != nil {
		return store.MetricPoint{}, err
	}
	return point, nil
}

// ValidateCopilotMetric validates that a metric point conforms to the Copilot output contract.
func ValidateCopilotMetric(point store.MetricPoint) error {
	if !IsCopilotMetric(point.Name) {
		return fmt.Errorf("unsupported copilot metric: %s", point.Name)
	}

	for _, key := range []string{LabelOrg, LabelRepo, LabelUser} {
		value, ok := point.Labels[key]
		if !ok {
			return fmt.Errorf("copilot metric is missing %q label", key)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("copilot metric %q label must be non-empty", key)
		}
	}
	if point.Labels[LabelRepo] != "*" {
		return fmt.Errorf("copilot metric %q label must be \"*\"", LabelRepo)
	}

	for key := range point.Labels {
		if key == LabelOrg || key == LabelRepo || key == LabelUser {
			continue
		}
		if !slices.Contains(copilotOptionalLabelKeys, key) {
			return fmt.Errorf("copilot metric contains unsupported label %q", key)
		}
	}

	return nil
}
