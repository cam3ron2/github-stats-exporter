package scrape

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
)

// NewOrgScraperFromConfig builds a GitHubOrgScraper using per-org GitHub App credentials from config.
func NewOrgScraperFromConfig(cfg *config.Config) (OrgScraper, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is required")
	}

	timeout := cfg.GitHub.RequestTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	clients := make(map[string]GitHubDataClient, len(cfg.GitHub.Orgs))
	primaryOrg := ""
	for _, orgCfg := range cfg.GitHub.Orgs {
		orgName := strings.TrimSpace(orgCfg.Org)
		if orgName == "" {
			return nil, fmt.Errorf("organization name is required")
		}
		if primaryOrg == "" {
			primaryOrg = orgName
		}

		httpClient, err := githubapi.NewInstallationHTTPClient(githubapi.InstallationAuthConfig{
			AppID:          orgCfg.AppID,
			InstallationID: orgCfg.InstallationID,
			PrivateKeyPath: orgCfg.PrivateKeyPath,
			Timeout:        timeout,
			BaseTransport:  http.DefaultTransport,
		})
		if err != nil {
			return nil, fmt.Errorf("create installation client for org %q: %w", orgName, err)
		}

		requestClient := githubapi.NewClient(httpClient, githubapi.RetryConfig{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff,
			MaxBackoff:     cfg.Retry.MaxBackoff,
		}, githubapi.RateLimitPolicy{
			MinRemainingThreshold: cfg.RateLimit.MinRemainingThreshold,
			MinResetBuffer:        cfg.RateLimit.MinResetBuffer,
			SecondaryLimitBackoff: cfg.RateLimit.SecondaryLimitBackoff,
		})

		dataClient, err := githubapi.NewDataClient(cfg.GitHub.APIBaseURL, requestClient)
		if err != nil {
			return nil, fmt.Errorf("create data client for org %q: %w", orgName, err)
		}
		clients[orgName] = dataClient
	}

	var enterpriseClient GitHubDataClient
	if cfg.Copilot.Enabled && cfg.Copilot.Enterprise.Enabled {
		httpClient, err := githubapi.NewInstallationHTTPClient(githubapi.InstallationAuthConfig{
			AppID:          cfg.Copilot.Enterprise.AppID,
			InstallationID: cfg.Copilot.Enterprise.InstallationID,
			PrivateKeyPath: cfg.Copilot.Enterprise.PrivateKeyPath,
			Timeout:        timeout,
			BaseTransport:  http.DefaultTransport,
		})
		if err != nil {
			return nil, fmt.Errorf("create installation client for copilot enterprise: %w", err)
		}

		requestClient := githubapi.NewClient(httpClient, githubapi.RetryConfig{
			MaxAttempts:    cfg.Retry.MaxAttempts,
			InitialBackoff: cfg.Retry.InitialBackoff,
			MaxBackoff:     cfg.Retry.MaxBackoff,
		}, githubapi.RateLimitPolicy{
			MinRemainingThreshold: cfg.RateLimit.MinRemainingThreshold,
			MinResetBuffer:        cfg.RateLimit.MinResetBuffer,
			SecondaryLimitBackoff: cfg.RateLimit.SecondaryLimitBackoff,
		})

		dataClient, err := githubapi.NewDataClient(cfg.GitHub.APIBaseURL, requestClient)
		if err != nil {
			return nil, fmt.Errorf("create data client for copilot enterprise: %w", err)
		}
		enterpriseClient = dataClient
	}

	return NewGitHubOrgScraper(clients, GitHubOrgScraperConfig{
		LOCRefreshInterval:                        cfg.LOC.RefreshInterval,
		FallbackEnabled:                           cfg.LOC.FallbackEnabled,
		FallbackMaxCommitsPerRepoPerWeek:          cfg.LOC.FallbackMaxCommitsPerRepoPerWeek,
		FallbackMaxCommitDetailCallsPerOrgPerHour: cfg.LOC.FallbackMaxCommitDetailCallsPerOrgPerHour,
		LargeRepoZeroDetectionWindows:             cfg.LOC.LargeRepoZeroDetectionWindows,
		LargeRepoCooldown:                         cfg.LOC.LargeRepoCooldown,
		Copilot:                                   cfg.Copilot,
		CopilotEnterpriseClient:                   enterpriseClient,
		CopilotEnterprisePrimaryOrg:               primaryOrg,
	}), nil
}
