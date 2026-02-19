package scrape

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
)

func TestShouldScrapeEnterpriseForOrg(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		org     string
		scraper *GitHubOrgScraper
		want    bool
	}{
		{
			name: "copilot_disabled",
			org:  "org-a",
			scraper: &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{},
				primaryOrg: "org-a",
			},
			want: false,
		},
		{
			name: "enterprise_disabled",
			org:  "org-a",
			scraper: &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					Enabled: true,
					Enterprise: config.CopilotEnterpriseConfig{
						Enabled: false,
					},
					IncludeEnterprise28d: true,
				},
				primaryOrg: "org-a",
			},
			want: false,
		},
		{
			name: "enterprise_windows_disabled",
			org:  "org-a",
			scraper: &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					Enabled: true,
					Enterprise: config.CopilotEnterpriseConfig{
						Enabled: true,
					},
				},
				primaryOrg: "org-a",
			},
			want: false,
		},
		{
			name: "primary_org_missing",
			org:  "org-a",
			scraper: &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					Enabled:                   true,
					IncludeEnterprise28d:      true,
					IncludeEnterpriseUsers28d: true,
					Enterprise: config.CopilotEnterpriseConfig{
						Enabled: true,
					},
				},
				primaryOrg: "",
			},
			want: false,
		},
		{
			name: "org_not_primary",
			org:  "org-b",
			scraper: &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					Enabled:              true,
					IncludeEnterprise28d: true,
					Enterprise: config.CopilotEnterpriseConfig{
						Enabled: true,
					},
				},
				primaryOrg: "org-a",
			},
			want: false,
		},
		{
			name: "primary_org_enabled",
			org:  "org-a",
			scraper: &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					Enabled:                   true,
					IncludeEnterprise28d:      true,
					IncludeEnterpriseUsers28d: true,
					Enterprise: config.CopilotEnterpriseConfig{
						Enabled: true,
					},
				},
				primaryOrg: "org-a",
			},
			want: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.scraper.shouldScrapeEnterpriseForOrg(tc.org); got != tc.want {
				t.Fatalf("shouldScrapeEnterpriseForOrg(%q) = %t, want %t", tc.org, got, tc.want)
			}
		})
	}
}

func TestResolveCopilotUserLabel(t *testing.T) {
	t.Parallel()

	hashedSeed := "alice"
	sum := sha256.Sum256([]byte(hashedSeed))
	expectedHash := hex.EncodeToString(sum[:])

	testCases := []struct {
		name   string
		mode   string
		scope  string
		record map[string]any
		want   string
	}{
		{
			name:   "non_user_scope_is_all",
			mode:   "login",
			scope:  "org",
			record: map[string]any{"user_login": "alice"},
			want:   "all",
		},
		{
			name:   "login_mode_uses_login",
			mode:   "login",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_login": "Alice_One"},
			want:   "Alice_One",
		},
		{
			name:   "id_mode_uses_user_id",
			mode:   "id",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_id": "12345"},
			want:   "12345",
		},
		{
			name:   "id_mode_falls_back_to_login",
			mode:   "id",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_login": "Bob"},
			want:   "Bob",
		},
		{
			name:   "hashed_mode_hashes_login",
			mode:   "hashed",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_login": "alice"},
			want:   expectedHash,
		},
		{
			name:   "hashed_mode_unknown_when_seed_missing",
			mode:   "hashed",
			scope:  copilotScopeUsers,
			record: map[string]any{},
			want:   UnknownLabelValue,
		},
		{
			name:   "none_mode_redacts",
			mode:   "none",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_login": "alice"},
			want:   "redacted",
		},
		{
			name:   "unknown_mode_falls_back_to_login",
			mode:   "custom",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_login": "carol"},
			want:   "carol",
		},
		{
			name:   "login_mode_falls_back_to_id",
			mode:   "login",
			scope:  copilotScopeUsers,
			record: map[string]any{"user_id": "987"},
			want:   "987",
		},
		{
			name:   "empty_data_unknown",
			mode:   "login",
			scope:  copilotScopeUsers,
			record: map[string]any{},
			want:   UnknownLabelValue,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scraper := &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					UserLabelMode: tc.mode,
				},
			}
			if got := scraper.resolveCopilotUserLabel(tc.record, tc.scope); got != tc.want {
				t.Fatalf("resolveCopilotUserLabel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCopilotDayLabel(t *testing.T) {
	t.Parallel()

	reportDay := time.Date(2026, time.February, 18, 0, 0, 0, 0, time.UTC)
	reportEnd := time.Date(2026, time.February, 19, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name       string
		emitDay    bool
		record     map[string]any
		linkResult githubapi.CopilotReportLinkResult
		want       string
	}{
		{
			name:    "emit_day_disabled",
			emitDay: false,
			record:  map[string]any{"day": "2026-02-20"},
			want:    "",
		},
		{
			name:    "uses_record_day",
			emitDay: true,
			record:  map[string]any{"day": "2026-02-20"},
			want:    "2026-02-20",
		},
		{
			name:    "uses_record_report_day",
			emitDay: true,
			record:  map[string]any{"report_day": "2026-02-21"},
			want:    "2026-02-21",
		},
		{
			name:    "falls_back_to_link_report_day",
			emitDay: true,
			record:  map[string]any{},
			linkResult: githubapi.CopilotReportLinkResult{
				ReportDay: reportDay,
			},
			want: reportDay.Format(dayFormat),
		},
		{
			name:    "falls_back_to_report_end_day",
			emitDay: true,
			record:  map[string]any{},
			linkResult: githubapi.CopilotReportLinkResult{
				ReportEndDay: reportEnd,
			},
			want: reportEnd.Format(dayFormat),
		},
		{
			name:       "no_day_available",
			emitDay:    true,
			record:     map[string]any{},
			linkResult: githubapi.CopilotReportLinkResult{},
			want:       "",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scraper := &GitHubOrgScraper{
				copilotCfg: config.CopilotConfig{
					EmitDayLabel: tc.emitDay,
				},
			}
			if got := scraper.copilotDayLabel(tc.record, tc.linkResult); got != tc.want {
				t.Fatalf("copilotDayLabel() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCopilotCheckpointRepoKeyParsing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		input     string
		wantScope string
		wantWin   string
		wantOK    bool
	}{
		{
			name:      "valid",
			input:     "__copilot__org__28d",
			wantScope: "org",
			wantWin:   "28d",
			wantOK:    true,
		},
		{
			name:   "missing_prefix",
			input:  "org__28d",
			wantOK: false,
		},
		{
			name:   "invalid_part_count",
			input:  "__copilot__org__28d__extra",
			wantOK: false,
		},
		{
			name:   "missing_scope",
			input:  "__copilot____28d",
			wantOK: false,
		},
		{
			name:   "missing_window",
			input:  "__copilot__org__",
			wantOK: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scope, win, ok := parseCopilotCheckpointRepoKey(tc.input)
			if ok != tc.wantOK {
				t.Fatalf("parseCopilotCheckpointRepoKey(%q) ok = %t, want %t", tc.input, ok, tc.wantOK)
			}
			if scope != tc.wantScope {
				t.Fatalf("scope = %q, want %q", scope, tc.wantScope)
			}
			if win != tc.wantWin {
				t.Fatalf("window = %q, want %q", win, tc.wantWin)
			}
		})
	}
}

func TestFirstStringAndNumbers(t *testing.T) {
	t.Parallel()

	record := map[string]any{
		"string":       "  value  ",
		"float_as_str": float64(42),
		"int_as_str":   int(8),
		"int64_as_str": int64(64),
		"float":        float64(10.5),
		"float32":      float32(11.5),
		"int":          int(12),
		"int64":        int64(13),
		"json_number":  json.Number("14.5"),
		"numeric_text": "15.5",
		"bad_text":     "not-a-number",
	}

	if got := firstString(record, "missing", "string"); got != "value" {
		t.Fatalf("firstString(string) = %q, want value", got)
	}
	if got := firstString(record, "float_as_str"); got != "42" {
		t.Fatalf("firstString(float) = %q, want 42", got)
	}
	if got := firstString(record, "int_as_str"); got != "8" {
		t.Fatalf("firstString(int) = %q, want 8", got)
	}
	if got := firstString(record, "int64_as_str"); got != "64" {
		t.Fatalf("firstString(int64) = %q, want 64", got)
	}
	if got := firstString(record, "missing"); got != "" {
		t.Fatalf("firstString(missing) = %q, want empty", got)
	}

	numberCases := []struct {
		key  string
		want float64
	}{
		{key: "float", want: 10.5},
		{key: "float32", want: 11.5},
		{key: "int", want: 12},
		{key: "int64", want: 13},
		{key: "json_number", want: 14.5},
		{key: "numeric_text", want: 15.5},
	}
	for _, tc := range numberCases {
		tc := tc
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()

			got, ok := firstNumber(record, tc.key)
			if !ok {
				t.Fatalf("firstNumber(%q) expected ok=true", tc.key)
			}
			if got != tc.want {
				t.Fatalf("firstNumber(%q) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}

	if got, ok := firstNumber(record, "bad_text"); ok || got != 0 {
		t.Fatalf("firstNumber(bad_text) = (%v,%t), want (0,false)", got, ok)
	}
	if got, ok := firstNumber(record, "missing"); ok || got != 0 {
		t.Fatalf("firstNumber(missing) = (%v,%t), want (0,false)", got, ok)
	}
	if got := firstNumberOrZero(record, "numeric_text"); got != 15.5 {
		t.Fatalf("firstNumberOrZero() = %v, want 15.5", got)
	}
	if !hasAnyNumber(record, "missing", "numeric_text") {
		t.Fatalf("hasAnyNumber() expected true")
	}
	if hasAnyNumber(record, "missing", "bad_text") {
		t.Fatalf("hasAnyNumber() expected false")
	}
	if got := sumNumbers(record, "float", "int", "missing"); got != 22.5 {
		t.Fatalf("sumNumbers() = %v, want 22.5", got)
	}
}

func TestGitHubOrgScraperSetCheckpointStoreAndBackfillValidation(t *testing.T) {
	t.Parallel()

	var nilScraper *GitHubOrgScraper
	nilScraper.SetCheckpointStore(&fakeCheckpointStore{})

	scraper := NewGitHubOrgScraper(map[string]GitHubDataClient{
		"org-a": &fakeGitHubDataClient{},
	}, GitHubOrgScraperConfig{
		Now: func() time.Time {
			return time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
		},
	})
	if scraper.checkpointStore() != nil {
		t.Fatalf("checkpointStore() expected nil before SetCheckpointStore")
	}
	checkpoints := &fakeCheckpointStore{}
	scraper.SetCheckpointStore(checkpoints)
	if got := scraper.checkpointStore(); got != checkpoints {
		t.Fatalf("checkpointStore() = %p, want %p", got, checkpoints)
	}

	_, err := scraper.ScrapeBackfill(
		context.Background(),
		config.GitHubOrgConfig{Org: "org-a"},
		"repo-a",
		time.Date(2026, time.February, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2026, time.February, 19, 0, 0, 0, 0, time.UTC),
		"scrape_error",
	)
	if err == nil {
		t.Fatalf("ScrapeBackfill() expected validation error, got nil")
	}
}
