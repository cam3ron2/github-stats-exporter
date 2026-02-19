//go:build e2e && live

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/githubapi"
	"github.com/cam3ron2/github-stats-exporter/internal/scrape"
)

func TestLiveScrapeFromLocalConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("live scrape e2e is skipped in short mode")
	}

	cfg := loadLocalConfigForLiveScrape(t)
	if len(cfg.GitHub.Orgs) == 0 {
		t.Fatalf("config/local.yaml contains no github organizations")
	}

	orgScraper, err := scrape.NewOrgScraperFromConfig(cfg)
	if err != nil {
		t.Fatalf("create github org scraper from local config: %v", err)
	}

	successfulScrapes := 0
	for _, orgCfg := range cfg.GitHub.Orgs {
		orgCfg := orgCfg
		t.Run(orgCfg.Org, func(t *testing.T) {
			repoName, ok := selectSingleRepositoryForLiveScrape(t, cfg, orgCfg)
			if !ok {
				t.Skipf("org %q has no repositories visible to the app", orgCfg.Org)
				return
			}
			orgCfg.RepoAllowlist = []string{repoName}
			orgCfg.PerOrgConcurrency = 1

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			result, err := orgScraper.ScrapeOrg(ctx, orgCfg)
			if err != nil {
				t.Fatalf("scrape org %q: %v", orgCfg.Org, err)
			}
			if result.Summary.ReposDiscovered < 1 {
				t.Fatalf("org %q discovered no repositories", orgCfg.Org)
			}
			if result.Summary.ReposTargeted != 1 {
				t.Fatalf(
					"org %q expected one targeted repo, got %d",
					orgCfg.Org,
					result.Summary.ReposTargeted,
				)
			}
			if result.Summary.ReposProcessed < 1 {
				t.Fatalf("org %q processed no repositories", orgCfg.Org)
			}
			if len(result.Metrics) == 0 && len(result.MissedWindow) == 0 {
				t.Fatalf("org %q returned neither metrics nor missed windows", orgCfg.Org)
			}
			successfulScrapes++
		})
	}
	if successfulScrapes == 0 {
		t.Fatalf("live scrape ran zero organizations; all orgs were skipped")
	}
}

func TestLiveCopilotReportsFromLocalConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("live copilot e2e is skipped in short mode")
	}

	cfg := loadLocalConfigForLiveScrape(t)
	if !cfg.Copilot.Enabled {
		t.Skip("config/local.yaml has copilot.enabled=false")
	}
	if !cfg.Copilot.IncludeOrg28d && !cfg.Copilot.IncludeOrgUsers28d {
		t.Skip("config/local.yaml has no org copilot report endpoints enabled")
	}

	successfulReports := 0
	for _, orgCfg := range cfg.GitHub.Orgs {
		orgCfg := orgCfg
		t.Run(orgCfg.Org, func(t *testing.T) {
			dataClient := newDataClientForOrg(t, cfg, orgCfg)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			var (
				streamResult githubapi.CopilotReportStreamResult
				streamErr    error
				linkStatus   githubapi.EndpointStatus
			)

			if cfg.Copilot.IncludeOrgUsers28d {
				linkResult, err := dataClient.GetOrgCopilotUsers28DayLatestReportLink(ctx, orgCfg.Org)
				if err == nil && linkResult.Status == githubapi.EndpointStatusOK {
					linkStatus = linkResult.Status
					streamResult, streamErr = dataClient.StreamCopilotReportNDJSON(
						ctx,
						linkResult.URL,
						func(_ map[string]any) error { return nil },
					)
				} else if err != nil {
					t.Logf("users report link error: %v", err)
				} else {
					t.Logf("users report link status=%s", linkResult.Status)
				}
			}

			if streamErr == nil && streamResult.Status == githubapi.EndpointStatusOK && streamResult.RecordsParsed > 0 {
				successfulReports++
				return
			}

			if cfg.Copilot.IncludeOrg28d {
				linkResult, err := dataClient.GetOrgCopilotOrganization28DayLatestReportLink(ctx, orgCfg.Org)
				if err != nil {
					t.Logf("organization report link error: %v", err)
					return
				}
				linkStatus = linkResult.Status
				if linkResult.Status != githubapi.EndpointStatusOK {
					t.Logf("organization report link status=%s", linkResult.Status)
					return
				}
				streamResult, streamErr = dataClient.StreamCopilotReportNDJSON(
					ctx,
					linkResult.URL,
					func(_ map[string]any) error { return nil },
				)
				if streamErr != nil {
					t.Logf("organization report stream error: %v", streamErr)
					return
				}
				if streamResult.Status != githubapi.EndpointStatusOK {
					t.Logf("organization report stream status=%s", streamResult.Status)
					return
				}
				if streamResult.RecordsParsed == 0 {
					t.Logf("organization report stream parsed zero records")
					return
				}
				successfulReports++
				return
			}

			t.Logf(
				"copilot report unavailable (link_status=%s stream_status=%s records=%d)",
				linkStatus,
				streamResult.Status,
				streamResult.RecordsParsed,
			)
		})
	}

	if successfulReports > 0 {
		return
	}
	if strings.EqualFold(strings.TrimSpace(os.Getenv("E2E_LIVE_COPILOT_REQUIRED")), "true") {
		t.Fatalf("no org returned a successful copilot report")
	}
	t.Skip("no org returned a successful copilot report; set E2E_LIVE_COPILOT_REQUIRED=true to require success")
}

func loadLocalConfigForLiveScrape(t *testing.T) *config.Config {
	t.Helper()

	root := repositoryRoot(t)
	configPath := filepath.Join(root, "config", "local.yaml")
	file, err := os.Open(configPath)
	if err != nil {
		t.Fatalf("open config/local.yaml: %v", err)
	}
	defer file.Close()

	cfg, err := config.Load(file)
	if err != nil {
		t.Fatalf("load config/local.yaml: %v", err)
	}

	for index := range cfg.GitHub.Orgs {
		org := &cfg.GitHub.Orgs[index]
		resolvedPath, resolveErr := resolvePrivateKeyPath(root, org.PrivateKeyPath)
		if resolveErr != nil {
			t.Fatalf("resolve private key path for org %q: %v", org.Org, resolveErr)
		}
		org.PrivateKeyPath = resolvedPath
	}
	return cfg
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("resolve test file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func resolvePrivateKeyPath(root string, configuredPath string) (string, error) {
	trimmed := strings.TrimSpace(configuredPath)
	if trimmed == "" {
		return "", fmt.Errorf("private key path is required")
	}

	baseName := filepath.Base(trimmed)
	candidates := []string{
		trimmed,
		filepath.Join(root, trimmed),
		filepath.Join(root, "config", "keys", baseName),
	}

	if strings.HasPrefix(trimmed, "/app/keys/") {
		candidates = append(candidates, filepath.Join(root, "config", "keys", baseName))
	}

	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		info, statErr := os.Stat(abs)
		if statErr != nil {
			continue
		}
		if info.IsDir() {
			continue
		}
		return abs, nil
	}

	return "", fmt.Errorf("private key file not found for path %q", configuredPath)
}

func selectSingleRepositoryForLiveScrape(
	t *testing.T,
	cfg *config.Config,
	orgCfg config.GitHubOrgConfig,
) (string, bool) {
	t.Helper()

	repos := listOrganizationRepos(t, cfg, orgCfg)
	primary := make([]string, 0, len(repos))
	fallback := make([]string, 0, len(repos))
	for _, repo := range repos {
		if strings.TrimSpace(repo.Name) == "" {
			continue
		}
		fallback = append(fallback, repo.Name)
		if repo.Archived || repo.Disabled {
			continue
		}
		primary = append(primary, repo.Name)
	}

	if len(primary) > 0 {
		slices.Sort(primary)
		return primary[0], true
	}
	if len(fallback) > 0 {
		slices.Sort(fallback)
		return fallback[0], true
	}
	return "", false
}

func listOrganizationRepos(
	t *testing.T,
	cfg *config.Config,
	orgCfg config.GitHubOrgConfig,
) []githubapi.Repository {
	t.Helper()

	dataClient := newDataClientForOrg(t, cfg, orgCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	reposResult, err := dataClient.ListOrgRepos(ctx, orgCfg.Org)
	if err != nil {
		fingerprint, fingerprintErr := privateKeyFingerprint(orgCfg.PrivateKeyPath)
		if fingerprintErr != nil {
			fingerprint = "unavailable(" + fingerprintErr.Error() + ")"
		}
		t.Fatalf(
			"list repositories for org %q failed (app_id=%d installation_id=%d key=%q key_fingerprint=%s): %v",
			orgCfg.Org,
			orgCfg.AppID,
			orgCfg.InstallationID,
			orgCfg.PrivateKeyPath,
			fingerprint,
			err,
		)
	}
	if reposResult.Status != githubapi.EndpointStatusOK {
		t.Fatalf(
			"list repositories for org %q returned status %q",
			orgCfg.Org,
			reposResult.Status,
		)
	}
	return reposResult.Repos
}

func newDataClientForOrg(
	t *testing.T,
	cfg *config.Config,
	orgCfg config.GitHubOrgConfig,
) *githubapi.DataClient {
	t.Helper()

	timeout := cfg.GitHub.RequestTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	httpClient, err := githubapi.NewInstallationHTTPClient(githubapi.InstallationAuthConfig{
		AppID:          orgCfg.AppID,
		InstallationID: orgCfg.InstallationID,
		PrivateKeyPath: orgCfg.PrivateKeyPath,
		Timeout:        timeout,
		BaseTransport:  http.DefaultTransport,
	})
	if err != nil {
		t.Fatalf("create installation client for %q: %v", orgCfg.Org, err)
	}

	requestClient := githubapi.NewClient(httpClient, githubapi.RetryConfig{
		MaxAttempts:    1,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     time.Second,
	}, githubapi.RateLimitPolicy{})

	dataClient, err := githubapi.NewDataClient(cfg.GitHub.APIBaseURL, requestClient)
	if err != nil {
		t.Fatalf("create data client for %q: %v", orgCfg.Org, err)
	}
	return dataClient
}

func privateKeyFingerprint(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read key file: %w", err)
	}

	block, _ := pem.Decode(content)
	if block == nil {
		return "", fmt.Errorf("decode pem block")
	}

	var publicKey any
	switch block.Type {
	case "PRIVATE KEY":
		key, parseErr := x509.ParsePKCS8PrivateKey(block.Bytes)
		if parseErr != nil {
			return "", fmt.Errorf("parse pkcs8 key: %w", parseErr)
		}
		publicKey = publicKeyFromPrivateKey(key)
	case "RSA PRIVATE KEY":
		key, parseErr := x509.ParsePKCS1PrivateKey(block.Bytes)
		if parseErr != nil {
			return "", fmt.Errorf("parse pkcs1 rsa key: %w", parseErr)
		}
		publicKey = &key.PublicKey
	case "EC PRIVATE KEY":
		key, parseErr := x509.ParseECPrivateKey(block.Bytes)
		if parseErr != nil {
			return "", fmt.Errorf("parse ec key: %w", parseErr)
		}
		publicKey = &key.PublicKey
	default:
		return "", fmt.Errorf("unsupported pem type %q", block.Type)
	}

	if publicKey == nil {
		return "", fmt.Errorf("derive public key")
	}
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("marshal public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:]), nil
}

func publicKeyFromPrivateKey(privateKey any) any {
	switch key := privateKey.(type) {
	case *rsa.PrivateKey:
		return &key.PublicKey
	case *ecdsa.PrivateKey:
		return &key.PublicKey
	default:
		return nil
	}
}
