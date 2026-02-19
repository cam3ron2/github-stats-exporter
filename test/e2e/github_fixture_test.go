//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeGitHubAPI struct {
	mu sync.Mutex

	server *httptest.Server

	orgRepos  map[string][]string
	userRepos map[string][]string
	repoData  map[string]repositoryFixture
	failures  map[string]*failureRule
	callCount map[string]int
}

type failureRule struct {
	status    int
	remaining int
	body      map[string]string
}

type repositoryFixture struct {
	Contributors []fixtureContributor
	Commits      []fixtureCommit
	Pulls        []fixturePull
	ReviewsByPR  map[int][]fixtureReview
	Comments     []fixtureComment
}

type fixtureContributor struct {
	User  string
	Total int
	Weeks []fixtureContributorWeek
}

type fixtureContributorWeek struct {
	WeekStart time.Time
	Additions int
	Deletions int
	Commits   int
}

type fixtureCommit struct {
	SHA            string
	Author         string
	Committer      string
	AuthorName     string
	AuthorEmail    string
	CommitterName  string
	CommitterEmail string
	CommittedAt    time.Time
	Additions      int
	Deletions      int
}

type fixturePull struct {
	Number    int
	User      string
	CreatedAt time.Time
	UpdatedAt time.Time
	MergedAt  time.Time
}

type fixtureReview struct {
	ID          int64
	User        string
	State       string
	SubmittedAt time.Time
}

type fixtureComment struct {
	ID        int64
	User      string
	CreatedAt time.Time
}

func newFakeGitHubAPI(t *testing.T) *fakeGitHubAPI {
	t.Helper()

	fixture := &fakeGitHubAPI{
		orgRepos:  make(map[string][]string),
		userRepos: make(map[string][]string),
		repoData:  make(map[string]repositoryFixture),
		failures:  make(map[string]*failureRule),
		callCount: make(map[string]int),
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(fixture.Close)
	return fixture
}

func (f *fakeGitHubAPI) URL() string {
	if f == nil || f.server == nil {
		return ""
	}
	return f.server.URL
}

func (f *fakeGitHubAPI) Close() {
	if f == nil || f.server == nil {
		return
	}
	f.server.Close()
}

func (f *fakeGitHubAPI) SetOrgRepos(org string, repos []string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.orgRepos[strings.TrimSpace(org)] = append([]string(nil), repos...)
}

func (f *fakeGitHubAPI) SetUserRepos(user string, repos []string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.userRepos[strings.TrimSpace(user)] = append([]string(nil), repos...)
}

func (f *fakeGitHubAPI) SetRepository(owner string, repo string, data repositoryFixture) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repoData[repoKey(owner, repo)] = data
}

func (f *fakeGitHubAPI) FailPath(path string, statusCode int, times int) {
	if f == nil || statusCode <= 0 || times <= 0 {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[path] = &failureRule{
		status:    statusCode,
		remaining: times,
		body: map[string]string{
			"message": fmt.Sprintf("forced failure for %s", path),
		},
	}
}

func (f *fakeGitHubAPI) PathCallCount(path string) int {
	if f == nil {
		return 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount[path]
}

func (f *fakeGitHubAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	f.incrementCall(path)

	if f.tryFailPath(path, w) {
		return
	}
	if path == "/meta" {
		f.writeJSON(w, http.StatusOK, map[string]any{
			"verifiable_password_authentication": true,
		})
		return
	}

	segments := splitPath(path)
	if f.handleCopilotRoutes(w, r, segments) {
		return
	}
	if len(segments) == 3 && segments[0] == "orgs" && segments[2] == "repos" {
		f.handleOwnerRepos(w, segments[1], true)
		return
	}
	if len(segments) == 3 && segments[0] == "users" && segments[2] == "repos" {
		f.handleOwnerRepos(w, segments[1], false)
		return
	}
	if len(segments) >= 4 && segments[0] == "repos" {
		f.handleRepositoryRoutes(w, segments)
		return
	}

	f.writeJSON(w, http.StatusNotFound, map[string]string{"message": "not found"})
}

func (f *fakeGitHubAPI) handleCopilotRoutes(w http.ResponseWriter, r *http.Request, segments []string) bool {
	if len(segments) >= 6 &&
		segments[0] == "orgs" &&
		segments[2] == "copilot" &&
		segments[3] == "metrics" &&
		segments[4] == "reports" {
		owner := segments[1]
		reportPath := strings.Join(segments[5:], "/")
		day := strings.TrimSpace(r.URL.Query().Get("date"))
		f.writeCopilotReportLink(w, "orgs", owner, reportPath, day)
		return true
	}

	if len(segments) >= 6 &&
		segments[0] == "enterprises" &&
		segments[2] == "copilot" &&
		segments[3] == "metrics" &&
		segments[4] == "reports" {
		owner := segments[1]
		reportPath := strings.Join(segments[5:], "/")
		day := strings.TrimSpace(r.URL.Query().Get("date"))
		f.writeCopilotReportLink(w, "enterprises", owner, reportPath, day)
		return true
	}

	if len(segments) >= 4 && segments[0] == "copilot-download" {
		scope := segments[1]
		owner := segments[2]
		reportPath := strings.Join(segments[3:], "/")
		day := strings.TrimSpace(r.URL.Query().Get("date"))
		f.writeCopilotReportPayload(w, scope, owner, reportPath, day)
		return true
	}

	return false
}

func (f *fakeGitHubAPI) writeCopilotReportLink(
	w http.ResponseWriter,
	scope string,
	owner string,
	reportPath string,
	day string,
) {
	if scope == "orgs" {
		if _, ok := f.getReposForOrg(owner); !ok {
			f.writeJSON(w, http.StatusNotFound, map[string]string{"message": "not found"})
			return
		}
	}
	if strings.TrimSpace(reportPath) == "" {
		f.writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid report path"})
		return
	}

	nowUTC := time.Now().UTC()
	reportEnd := nowUTC.Truncate(24 * time.Hour)
	reportStart := reportEnd.Add(-27 * 24 * time.Hour)
	reportDay := day
	if strings.Contains(reportPath, "1-day") {
		if reportDay == "" {
			reportDay = nowUTC.Format("2006-01-02")
		}
		parsedDay, err := time.Parse("2006-01-02", reportDay)
		if err == nil {
			reportEnd = parsedDay.UTC()
			reportStart = parsedDay.UTC()
		}
	}

	downloadURL := fmt.Sprintf(
		"%s/copilot-download/%s/%s/%s",
		strings.TrimRight(f.server.URL, "/"),
		scope,
		owner,
		reportPath,
	)
	if reportDay != "" {
		downloadURL += "?date=" + reportDay
	}

	payload := map[string]any{
		"url":              downloadURL,
		"download_links":   []string{downloadURL},
		"report_start_day": reportStart.Format("2006-01-02"),
		"report_end_day":   reportEnd.Format("2006-01-02"),
		"expires_at":       nowUTC.Add(time.Hour).Format(time.RFC3339),
	}
	if reportDay != "" {
		payload["report_day"] = reportDay
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) writeCopilotReportPayload(
	w http.ResponseWriter,
	scope string,
	owner string,
	reportPath string,
	day string,
) {
	nowUTC := time.Now().UTC()
	reportDay := day
	if reportDay == "" {
		reportDay = nowUTC.Format("2006-01-02")
	}

	base := float64(len(strings.TrimSpace(owner)) + len(strings.TrimSpace(scope)))
	record := map[string]any{
		"total_code_acceptances":                  base + 3,
		"total_code_suggestions":                  base + 9,
		"total_code_lines_suggested":              base + 100,
		"total_code_lines_accepted":               base + 80,
		"total_pull_requests_created":             base + 2,
		"total_pull_requests_reviewed":            base + 1,
		"total_pull_requests_created_by_copilot":  base + 1,
		"total_pull_requests_reviewed_by_copilot": base + 1,
		"day": reportDay,
	}
	if strings.HasPrefix(reportPath, "users-") {
		record["user_login"] = strings.ToLower(owner) + "-copilot-user"
		record["user_id"] = strconv.Itoa(len(owner) + 1000)
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-RateLimit-Remaining", "4500")
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(nowUTC.Add(time.Hour).Unix(), 10))
	w.WriteHeader(http.StatusOK)
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	if _, err := w.Write(append(encoded, '\n')); err != nil {
		return
	}
}

func (f *fakeGitHubAPI) incrementCall(path string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount[path]++
}

func (f *fakeGitHubAPI) tryFailPath(path string, w http.ResponseWriter) bool {
	if f == nil {
		return false
	}

	f.mu.Lock()
	rule, ok := f.failures[path]
	if ok && rule.remaining > 0 {
		rule.remaining--
		status := rule.status
		body := rule.body
		f.mu.Unlock()
		f.writeJSON(w, status, body)
		return true
	}
	f.mu.Unlock()
	return false
}

func (f *fakeGitHubAPI) handleOwnerRepos(w http.ResponseWriter, owner string, orgRoute bool) {
	repos := []string{}
	var ok bool
	if orgRoute {
		repos, ok = f.getReposForOrg(owner)
	} else {
		repos, ok = f.getReposForUser(owner)
	}
	if !ok {
		f.writeJSON(w, http.StatusNotFound, map[string]string{"message": "not found"})
		return
	}

	payload := make([]map[string]any, 0, len(repos))
	for _, repo := range repos {
		payload = append(payload, map[string]any{
			"name":           repo,
			"full_name":      owner + "/" + repo,
			"default_branch": "main",
			"archived":       false,
			"disabled":       false,
			"fork":           false,
		})
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) handleRepositoryRoutes(w http.ResponseWriter, segments []string) {
	owner := segments[1]
	repo := segments[2]
	data, found := f.getRepository(owner, repo)
	if !found {
		f.writeJSON(w, http.StatusNotFound, map[string]string{"message": "repository not found"})
		return
	}

	if len(segments) == 5 && segments[3] == "stats" && segments[4] == "contributors" {
		f.writeContributorStats(w, data)
		return
	}
	if len(segments) == 4 && segments[3] == "commits" {
		f.writeCommitList(w, data)
		return
	}
	if len(segments) == 5 && segments[3] == "commits" {
		f.writeCommitDetail(w, data, segments[4])
		return
	}
	if len(segments) == 4 && segments[3] == "pulls" {
		f.writePullList(w, data)
		return
	}
	if len(segments) == 6 && segments[3] == "pulls" && segments[5] == "reviews" {
		pullNumber, err := strconv.Atoi(segments[4])
		if err != nil {
			f.writeJSON(w, http.StatusBadRequest, map[string]string{"message": "invalid pull number"})
			return
		}
		f.writePullReviews(w, data, pullNumber)
		return
	}
	if len(segments) == 5 && segments[3] == "issues" && segments[4] == "comments" {
		f.writeIssueComments(w, data)
		return
	}

	f.writeJSON(w, http.StatusNotFound, map[string]string{"message": "route not found"})
}

func (f *fakeGitHubAPI) getReposForOrg(org string) ([]string, bool) {
	if f == nil {
		return nil, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	repos, ok := f.orgRepos[strings.TrimSpace(org)]
	return append([]string(nil), repos...), ok
}

func (f *fakeGitHubAPI) getReposForUser(user string) ([]string, bool) {
	if f == nil {
		return nil, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	repos, ok := f.userRepos[strings.TrimSpace(user)]
	return append([]string(nil), repos...), ok
}

func (f *fakeGitHubAPI) getRepository(owner string, repo string) (repositoryFixture, bool) {
	if f == nil {
		return repositoryFixture{}, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	result, ok := f.repoData[repoKey(owner, repo)]
	return result, ok
}

func (f *fakeGitHubAPI) writeContributorStats(w http.ResponseWriter, data repositoryFixture) {
	payload := make([]map[string]any, 0, len(data.Contributors))
	for _, contributor := range data.Contributors {
		weeks := make([]map[string]any, 0, len(contributor.Weeks))
		for _, week := range contributor.Weeks {
			weeks = append(weeks, map[string]any{
				"w": week.WeekStart.UTC().Unix(),
				"a": week.Additions,
				"d": week.Deletions,
				"c": week.Commits,
			})
		}
		payload = append(payload, map[string]any{
			"total": contributor.Total,
			"author": map[string]any{
				"login": contributor.User,
			},
			"weeks": weeks,
		})
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) writeCommitList(w http.ResponseWriter, data repositoryFixture) {
	payload := make([]map[string]any, 0, len(data.Commits))
	for _, commit := range data.Commits {
		payload = append(payload, map[string]any{
			"sha": commit.SHA,
			"author": map[string]any{
				"login": commit.Author,
			},
			"committer": map[string]any{
				"login": commit.Committer,
			},
			"commit": map[string]any{
				"author": map[string]any{
					"date":  commit.CommittedAt.UTC().Format(time.RFC3339),
					"name":  commit.AuthorName,
					"email": commit.AuthorEmail,
				},
				"committer": map[string]any{
					"date":  commit.CommittedAt.UTC().Format(time.RFC3339),
					"name":  commit.CommitterName,
					"email": commit.CommitterEmail,
				},
			},
		})
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) writeCommitDetail(w http.ResponseWriter, data repositoryFixture, sha string) {
	for _, commit := range data.Commits {
		if commit.SHA != sha {
			continue
		}
		payload := map[string]any{
			"sha": commit.SHA,
			"author": map[string]any{
				"login": commit.Author,
			},
			"stats": map[string]any{
				"additions": commit.Additions,
				"deletions": commit.Deletions,
				"total":     commit.Additions + commit.Deletions,
			},
		}
		f.writeJSON(w, http.StatusOK, payload)
		return
	}
	f.writeJSON(w, http.StatusNotFound, map[string]string{"message": "commit not found"})
}

func (f *fakeGitHubAPI) writePullList(w http.ResponseWriter, data repositoryFixture) {
	payload := make([]map[string]any, 0, len(data.Pulls))
	for _, pull := range data.Pulls {
		item := map[string]any{
			"number":     pull.Number,
			"created_at": pull.CreatedAt.UTC().Format(time.RFC3339),
			"updated_at": pull.UpdatedAt.UTC().Format(time.RFC3339),
			"user": map[string]any{
				"login": pull.User,
			},
			"merged_at": nil,
		}
		if !pull.MergedAt.IsZero() {
			item["merged_at"] = pull.MergedAt.UTC().Format(time.RFC3339)
		}
		payload = append(payload, item)
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) writePullReviews(w http.ResponseWriter, data repositoryFixture, pullNumber int) {
	reviews := data.ReviewsByPR[pullNumber]
	payload := make([]map[string]any, 0, len(reviews))
	for _, review := range reviews {
		payload = append(payload, map[string]any{
			"id":    review.ID,
			"state": review.State,
			"user": map[string]any{
				"login": review.User,
			},
			"submitted_at": review.SubmittedAt.UTC().Format(time.RFC3339),
		})
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) writeIssueComments(w http.ResponseWriter, data repositoryFixture) {
	payload := make([]map[string]any, 0, len(data.Comments))
	for _, comment := range data.Comments {
		payload = append(payload, map[string]any{
			"id": comment.ID,
			"user": map[string]any{
				"login": comment.User,
			},
			"created_at": comment.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	f.writeJSON(w, http.StatusOK, payload)
}

func (f *fakeGitHubAPI) writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-RateLimit-Remaining", "4500")
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
	w.WriteHeader(statusCode)
	if payload == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return
	}
}

func splitPath(path string) []string {
	trimmed := strings.TrimSpace(path)
	trimmed = strings.Trim(trimmed, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func repoKey(owner string, repo string) string {
	return strings.TrimSpace(owner) + "/" + strings.TrimSpace(repo)
}
