package scrape

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/githubapi"
	"github.com/cam3ron2/github-stats/internal/store"
)

const (
	repoMissReasonContributorStats = "contributor_stats_failed"
	repoMissReasonFallbackList     = "fallback_list_commits_failed"
	repoMissReasonFallbackDetail   = "fallback_commit_detail_failed"
)

// GitHubDataClient is the typed GitHub API interface consumed by the org scraper.
type GitHubDataClient interface {
	ListOrgRepos(ctx context.Context, org string) (githubapi.OrgReposResult, error)
	GetContributorStats(ctx context.Context, owner, repo string) (githubapi.ContributorStatsResult, error)
	ListRepoCommitsWindow(ctx context.Context, owner, repo string, since, until time.Time, maxCommits int) (githubapi.CommitListResult, error)
	GetCommit(ctx context.Context, owner, repo, sha string) (githubapi.CommitDetail, error)
}

// GitHubOrgScraperConfig configures GitHub-backed org scraping behavior.
type GitHubOrgScraperConfig struct {
	LOCRefreshInterval                        time.Duration
	FallbackEnabled                           bool
	FallbackMaxCommitsPerRepoPerWeek          int
	FallbackMaxCommitDetailCallsPerOrgPerHour int
	LargeRepoZeroDetectionWindows             int
	LargeRepoCooldown                         time.Duration
	Now                                       func() time.Time
	Sleep                                     func(time.Duration)
}

// GitHubOrgScraper implements OrgScraper using typed GitHub API clients.
type GitHubOrgScraper struct {
	clients map[string]GitHubDataClient
	cfg     GitHubOrgScraperConfig

	stateMachine githubapi.LOCStateMachine

	mu                    sync.Mutex
	locStateByRepo        map[string]githubapi.LOCState
	fallbackOrgBudgetByHr map[string]orgFallbackBudget
}

type orgFallbackBudget struct {
	windowStart time.Time
	used        int
}

type repositoryOutcome struct {
	metrics      []store.MetricPoint
	missed       []MissedWindow
	summaryDelta repoSummaryDelta
}

type repoSummaryDelta struct {
	processed          int
	statsAccepted      int
	statsForbidden     int
	statsNotFound      int
	statsConflict      int
	statsUnprocessable int
	statsUnavailable   int
	noCompleteWeek     int
	fallbackUsed       int
	fallbackTruncated  int
}

// NewGitHubOrgScraper creates a production org scraper over per-org GitHub clients.
func NewGitHubOrgScraper(clients map[string]GitHubDataClient, cfg GitHubOrgScraperConfig) *GitHubOrgScraper {
	normalizedClients := make(map[string]GitHubDataClient, len(clients))
	for org, client := range clients {
		normalizedClients[strings.TrimSpace(org)] = client
	}

	if cfg.LOCRefreshInterval <= 0 {
		cfg.LOCRefreshInterval = 24 * time.Hour
	}
	if cfg.FallbackMaxCommitsPerRepoPerWeek <= 0 {
		cfg.FallbackMaxCommitsPerRepoPerWeek = 500
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = time.Sleep
	}

	return &GitHubOrgScraper{
		clients: normalizedClients,
		cfg:     cfg,
		stateMachine: githubapi.LOCStateMachine{
			RefreshInterval:       cfg.LOCRefreshInterval,
			StaleWindowMultiplier: 2,
			ZeroDetectionWindows:  cfg.LargeRepoZeroDetectionWindows,
			FallbackCooldown:      cfg.LargeRepoCooldown,
		},
		locStateByRepo:        make(map[string]githubapi.LOCState),
		fallbackOrgBudgetByHr: make(map[string]orgFallbackBudget),
	}
}

// ScrapeOrg scrapes one organization and returns metrics plus missed windows for partial failures.
func (s *GitHubOrgScraper) ScrapeOrg(ctx context.Context, org config.GitHubOrgConfig) (OrgResult, error) {
	if s == nil {
		return OrgResult{}, fmt.Errorf("github org scraper is nil")
	}

	orgName := strings.TrimSpace(org.Org)
	client, ok := s.clients[orgName]
	if !ok || client == nil {
		return OrgResult{}, fmt.Errorf("no github client configured for org %q", orgName)
	}

	reposResult, err := client.ListOrgRepos(ctx, orgName)
	if err != nil {
		return OrgResult{}, fmt.Errorf("list org repos for %q: %w", orgName, err)
	}
	s.applyRateLimitPacing(reposResult.Metadata)

	if reposResult.Status != githubapi.EndpointStatusOK {
		return OrgResult{}, fmt.Errorf("list org repos for %q returned status %q", orgName, reposResult.Status)
	}

	result := OrgResult{
		Summary: OrgSummary{
			ReposDiscovered: len(reposResult.Repos),
		},
	}

	repos := filterRepositories(reposResult.Repos, org.RepoAllowlist)
	result.Summary.ReposTargeted = len(repos)
	if len(repos) == 0 {
		return result, nil
	}

	workerCount := org.PerOrgConcurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	jobs := make(chan githubapi.Repository, len(repos))
	outcomes := make(chan repositoryOutcome, len(repos))

	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			for repo := range jobs {
				outcome := s.scrapeRepository(ctx, client, orgName, repo.Name)
				outcomes <- outcome
			}
		})
	}

	for _, repo := range repos {
		jobs <- repo
	}
	close(jobs)

	wg.Wait()
	close(outcomes)

	for outcome := range outcomes {
		result.Metrics = append(result.Metrics, outcome.metrics...)
		result.MissedWindow = append(result.MissedWindow, outcome.missed...)
		result.Summary.ReposProcessed += outcome.summaryDelta.processed
		result.Summary.ReposStatsAccepted += outcome.summaryDelta.statsAccepted
		result.Summary.ReposStatsForbidden += outcome.summaryDelta.statsForbidden
		result.Summary.ReposStatsNotFound += outcome.summaryDelta.statsNotFound
		result.Summary.ReposStatsConflict += outcome.summaryDelta.statsConflict
		result.Summary.ReposStatsUnprocessable += outcome.summaryDelta.statsUnprocessable
		result.Summary.ReposStatsUnavailable += outcome.summaryDelta.statsUnavailable
		result.Summary.ReposNoCompleteWeek += outcome.summaryDelta.noCompleteWeek
		result.Summary.ReposFallbackUsed += outcome.summaryDelta.fallbackUsed
		result.Summary.ReposFallbackTruncated += outcome.summaryDelta.fallbackTruncated
	}
	result.Summary.MissedWindows = len(result.MissedWindow)
	result.Summary.MetricsProduced = len(result.Metrics)

	return result, nil
}

func (s *GitHubOrgScraper) scrapeRepository(ctx context.Context, client GitHubDataClient, org, repo string) repositoryOutcome {
	now := s.cfg.Now().UTC()
	windowStart := now.Add(-7 * 24 * time.Hour)
	windowEnd := now
	summary := repoSummaryDelta{processed: 1}

	statsResult, err := client.GetContributorStats(ctx, org, repo)
	if err != nil {
		summary.statsUnavailable++
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonContributorStats,
			}},
			summaryDelta: summary,
		}
	}
	s.applyRateLimitPacing(statsResult.Metadata)

	switch statsResult.Status {
	case githubapi.EndpointStatusAccepted:
		summary.statsAccepted++
		s.recordLOCState(org, repo, githubapi.LOCEvent{
			ObservedAt: now,
			HTTPStatus: 202,
		})
		return repositoryOutcome{summaryDelta: summary}
	case githubapi.EndpointStatusForbidden, githubapi.EndpointStatusUnavailable, githubapi.EndpointStatusUnknown:
		if statsResult.Status == githubapi.EndpointStatusForbidden {
			summary.statsForbidden++
		} else {
			summary.statsUnavailable++
		}
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonContributorStats,
			}},
			summaryDelta: summary,
		}
	case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
		switch statsResult.Status {
		case githubapi.EndpointStatusNotFound:
			summary.statsNotFound++
		case githubapi.EndpointStatusConflict:
			summary.statsConflict++
		case githubapi.EndpointStatusUnprocessable:
			summary.statsUnprocessable++
		}
		return repositoryOutcome{summaryDelta: summary}
	case githubapi.EndpointStatusOK:
		// Continue below.
	default:
		summary.statsUnavailable++
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonContributorStats,
			}},
			summaryDelta: summary,
		}
	}

	weeklyByUser, weekEnd, contributionsNonZero, additionsDeletionsZero := extractLatestCompleteWeek(statsResult.Contributors, now)
	state := s.recordLOCState(org, repo, githubapi.LOCEvent{
		ObservedAt:             now,
		HTTPStatus:             200,
		StatsPresent:           true,
		ContributionsNonZero:   contributionsNonZero,
		AdditionsDeletionsZero: additionsDeletionsZero,
	})

	if len(weeklyByUser) == 0 {
		summary.noCompleteWeek++
	}

	if s.cfg.FallbackEnabled && state.Mode == githubapi.LOCModeFallback {
		fallbackOutcome := s.scrapeRepositoryFallback(ctx, client, org, repo, windowStart, windowEnd)
		fallbackOutcome.summaryDelta.processed += summary.processed
		fallbackOutcome.summaryDelta.fallbackUsed++
		return fallbackOutcome
	}

	metrics := make([]store.MetricPoint, 0, len(weeklyByUser)*3)
	for user, weekly := range weeklyByUser {
		addMetric, err := NewProductivityMetric(MetricActivityLOCAddedWeekly, org, repo, user, float64(weekly.added), now)
		if err == nil {
			metrics = append(metrics, addMetric)
		}
		removedMetric, err := NewProductivityMetric(MetricActivityLOCRemovedWeekly, org, repo, user, float64(weekly.removed), now)
		if err == nil {
			metrics = append(metrics, removedMetric)
		}
		lastEventMetric, err := NewProductivityMetric(MetricActivityLastEventUnixTime, org, repo, user, float64(weekEnd.Unix()), now)
		if err == nil {
			metrics = append(metrics, lastEventMetric)
		}
	}

	return repositoryOutcome{
		metrics:      metrics,
		summaryDelta: summary,
	}
}

func (s *GitHubOrgScraper) scrapeRepositoryFallback(
	ctx context.Context,
	client GitHubDataClient,
	org string,
	repo string,
	windowStart time.Time,
	windowEnd time.Time,
) repositoryOutcome {
	summary := repoSummaryDelta{}

	commitListResult, err := client.ListRepoCommitsWindow(ctx, org, repo, windowStart, windowEnd, s.cfg.FallbackMaxCommitsPerRepoPerWeek)
	if err != nil {
		summary.statsUnavailable++
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackList,
			}},
			summaryDelta: summary,
		}
	}
	s.applyRateLimitPacing(commitListResult.Metadata)

	switch commitListResult.Status {
	case githubapi.EndpointStatusOK:
		// continue
	case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
		return repositoryOutcome{summaryDelta: summary}
	default:
		summary.statsUnavailable++
		return repositoryOutcome{
			missed: []MissedWindow{{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackList,
			}},
			summaryDelta: summary,
		}
	}

	allowedDetails := s.consumeFallbackBudget(org, len(commitListResult.Commits), windowEnd)
	if allowedDetails < len(commitListResult.Commits) {
		commitListResult.Commits = commitListResult.Commits[:allowedDetails]
		commitListResult.Truncated = true
	}

	addedByUser := make(map[string]int)
	removedByUser := make(map[string]int)
	commits24hByUser := make(map[string]int)
	lastEventByUser := make(map[string]time.Time)
	missed := make([]MissedWindow, 0, 1)

	for _, commit := range commitListResult.Commits {
		detailResult, err := client.GetCommit(ctx, org, repo, commit.SHA)
		if err != nil {
			summary.statsUnavailable++
			missed = append(missed, MissedWindow{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackDetail,
			})
			continue
		}
		s.applyRateLimitPacing(detailResult.Metadata)

		switch detailResult.Status {
		case githubapi.EndpointStatusOK:
			// continue
		case githubapi.EndpointStatusNotFound, githubapi.EndpointStatusConflict, githubapi.EndpointStatusUnprocessable:
			continue
		default:
			summary.statsUnavailable++
			missed = append(missed, MissedWindow{
				Org:         org,
				Repo:        repo,
				WindowStart: windowStart,
				WindowEnd:   windowEnd,
				Reason:      repoMissReasonFallbackDetail,
			})
			continue
		}

		user := detailResult.Author
		if strings.TrimSpace(user) == "" {
			user = commit.Author
		}
		if strings.TrimSpace(user) == "" {
			user = UnknownLabelValue
		}

		addedByUser[user] += detailResult.Additions
		removedByUser[user] += detailResult.Deletions

		if commit.CommittedAt.After(windowEnd.Add(-24 * time.Hour)) {
			commits24hByUser[user]++
		}
		if commit.CommittedAt.After(lastEventByUser[user]) {
			lastEventByUser[user] = commit.CommittedAt
		}
	}

	metrics := make([]store.MetricPoint, 0, len(addedByUser)*4)
	for user, added := range addedByUser {
		addMetric, err := NewProductivityMetric(MetricActivityLOCAddedWeekly, org, repo, user, float64(added), windowEnd)
		if err == nil {
			metrics = append(metrics, addMetric)
		}
		removedMetric, err := NewProductivityMetric(MetricActivityLOCRemovedWeekly, org, repo, user, float64(removedByUser[user]), windowEnd)
		if err == nil {
			metrics = append(metrics, removedMetric)
		}
		commitsMetric, err := NewProductivityMetric(MetricActivityCommits24h, org, repo, user, float64(commits24hByUser[user]), windowEnd)
		if err == nil {
			metrics = append(metrics, commitsMetric)
		}
		lastEvent := lastEventByUser[user]
		if lastEvent.IsZero() {
			lastEvent = windowEnd
		}
		lastEventMetric, err := NewProductivityMetric(MetricActivityLastEventUnixTime, org, repo, user, float64(lastEvent.Unix()), windowEnd)
		if err == nil {
			metrics = append(metrics, lastEventMetric)
		}
	}

	if commitListResult.Truncated {
		summary.fallbackTruncated++
		missed = append(missed, MissedWindow{
			Org:         org,
			Repo:        repo,
			WindowStart: windowStart,
			WindowEnd:   windowEnd,
			Reason:      "fallback_truncated",
		})
	}

	return repositoryOutcome{
		metrics:      metrics,
		missed:       missed,
		summaryDelta: summary,
	}
}

func (s *GitHubOrgScraper) recordLOCState(org, repo string, event githubapi.LOCEvent) githubapi.LOCState {
	key := org + "/" + repo

	s.mu.Lock()
	defer s.mu.Unlock()

	previous := s.locStateByRepo[key]
	next := s.stateMachine.Apply(previous, event)
	s.locStateByRepo[key] = next
	return next
}

func (s *GitHubOrgScraper) consumeFallbackBudget(org string, requested int, now time.Time) int {
	if requested <= 0 {
		return 0
	}
	if s.cfg.FallbackMaxCommitDetailCallsPerOrgPerHour <= 0 {
		return requested
	}

	hourStart := now.UTC().Truncate(time.Hour)

	s.mu.Lock()
	defer s.mu.Unlock()

	budget := s.fallbackOrgBudgetByHr[org]
	if budget.windowStart.IsZero() || !budget.windowStart.Equal(hourStart) {
		budget = orgFallbackBudget{
			windowStart: hourStart,
			used:        0,
		}
	}

	remaining := s.cfg.FallbackMaxCommitDetailCallsPerOrgPerHour - budget.used
	if remaining <= 0 {
		s.fallbackOrgBudgetByHr[org] = budget
		return 0
	}
	if requested > remaining {
		requested = remaining
	}
	budget.used += requested
	s.fallbackOrgBudgetByHr[org] = budget
	return requested
}

func (s *GitHubOrgScraper) applyRateLimitPacing(metadata githubapi.CallMetadata) {
	if metadata.LastDecision.Allow {
		return
	}
	if metadata.LastDecision.WaitFor <= 0 {
		return
	}
	s.cfg.Sleep(metadata.LastDecision.WaitFor)
}

func filterRepositories(repos []githubapi.Repository, allowlist []string) []githubapi.Repository {
	if len(repos) == 0 {
		return nil
	}
	normalizedAllowlist := make(map[string]struct{}, len(allowlist))
	allowAll := false
	for _, item := range allowlist {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if trimmed == "*" {
			allowAll = true
			break
		}
		normalizedAllowlist[trimmed] = struct{}{}
	}

	if allowAll || len(normalizedAllowlist) == 0 {
		filtered := make([]githubapi.Repository, 0, len(repos))
		filtered = append(filtered, repos...)
		return filtered
	}

	filtered := make([]githubapi.Repository, 0, len(repos))
	for _, repo := range repos {
		if _, ok := normalizedAllowlist[repo.Name]; ok {
			filtered = append(filtered, repo)
		}
	}
	return filtered
}

type weeklyLOC struct {
	added   int
	removed int
	commits int
}

func extractLatestCompleteWeek(contributors []githubapi.ContributorStats, now time.Time) (map[string]weeklyLOC, time.Time, bool, bool) {
	selectedWeekStart := time.Time{}
	weeklyByUser := make(map[string]weeklyLOC)
	contributionsNonZero := false
	additionsDeletionsZero := false

	type weekEntry struct {
		user string
		week githubapi.ContributorWeek
	}
	latestByUser := make(map[string]weekEntry)

	for _, contributor := range contributors {
		user := strings.TrimSpace(contributor.User)
		if user == "" {
			user = UnknownLabelValue
		}
		for _, week := range contributor.Weeks {
			if week.WeekStart.IsZero() {
				continue
			}
			weekStart := week.WeekStart.UTC()
			weekEnd := weekStart.Add(7 * 24 * time.Hour)
			if weekEnd.After(now) {
				continue
			}

			entry, exists := latestByUser[user]
			if !exists || weekStart.After(entry.week.WeekStart) {
				latestByUser[user] = weekEntry{
					user: user,
					week: githubapi.ContributorWeek{
						WeekStart: weekStart,
						Additions: week.Additions,
						Deletions: week.Deletions,
						Commits:   week.Commits,
					},
				}
			}
		}
	}

	for user, entry := range latestByUser {
		if entry.week.WeekStart.After(selectedWeekStart) {
			selectedWeekStart = entry.week.WeekStart
		}
		weeklyByUser[user] = weeklyLOC{
			added:   entry.week.Additions,
			removed: entry.week.Deletions,
			commits: entry.week.Commits,
		}
		if entry.week.Commits > 0 {
			contributionsNonZero = true
		}
	}

	if len(weeklyByUser) == 0 {
		return weeklyByUser, time.Time{}, false, false
	}

	totalAdded := 0
	totalRemoved := 0
	for _, values := range weeklyByUser {
		totalAdded += values.added
		totalRemoved += values.removed
	}
	additionsDeletionsZero = (totalAdded == 0 && totalRemoved == 0 && contributionsNonZero)

	return weeklyByUser, selectedWeekStart.Add(7 * 24 * time.Hour), contributionsNonZero, additionsDeletionsZero
}
