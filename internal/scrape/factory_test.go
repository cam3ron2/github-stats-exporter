package scrape

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
)

func TestNewOrgScraperFromConfig(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	keyA := writePrivateKey(t, tempDir, "a.pem")
	keyB := writePrivateKey(t, tempDir, "b.pem")

	testCases := []struct {
		name        string
		cfg         *config.Config
		wantErr     bool
		errContains string
		wantOrgs    []string
	}{
		{
			name:        "nil_config",
			cfg:         nil,
			wantErr:     true,
			errContains: "config is required",
		},
		{
			name: "single_org",
			cfg: &config.Config{
				GitHub: config.GitHubConfig{
					APIBaseURL:     "https://api.github.com",
					RequestTimeout: 15 * time.Second,
					Orgs: []config.GitHubOrgConfig{
						{
							Org:            "org-a",
							AppID:          1,
							InstallationID: 10,
							PrivateKeyPath: keyA,
						},
					},
				},
				Retry: config.RetryConfig{
					MaxAttempts:    2,
					InitialBackoff: time.Second,
					MaxBackoff:     3 * time.Second,
				},
				RateLimit: config.RateLimitConfig{
					MinRemainingThreshold: 100,
					MinResetBuffer:        10 * time.Second,
					SecondaryLimitBackoff: 30 * time.Second,
				},
			},
			wantOrgs: []string{"org-a"},
		},
		{
			name: "multiple_orgs",
			cfg: &config.Config{
				GitHub: config.GitHubConfig{
					APIBaseURL:     "https://api.github.com",
					RequestTimeout: 15 * time.Second,
					Orgs: []config.GitHubOrgConfig{
						{
							Org:            "org-a",
							AppID:          1,
							InstallationID: 10,
							PrivateKeyPath: keyA,
						},
						{
							Org:            "org-b",
							AppID:          2,
							InstallationID: 20,
							PrivateKeyPath: keyB,
						},
					},
				},
				Retry: config.RetryConfig{
					MaxAttempts:    2,
					InitialBackoff: time.Second,
					MaxBackoff:     3 * time.Second,
				},
				RateLimit: config.RateLimitConfig{
					MinRemainingThreshold: 100,
					MinResetBuffer:        10 * time.Second,
					SecondaryLimitBackoff: 30 * time.Second,
				},
			},
			wantOrgs: []string{"org-a", "org-b"},
		},
		{
			name: "invalid_key_path",
			cfg: &config.Config{
				GitHub: config.GitHubConfig{
					APIBaseURL:     "https://api.github.com",
					RequestTimeout: 15 * time.Second,
					Orgs: []config.GitHubOrgConfig{
						{
							Org:            "org-a",
							AppID:          1,
							InstallationID: 10,
							PrivateKeyPath: filepath.Join(tempDir, "missing.pem"),
						},
					},
				},
			},
			wantErr:     true,
			errContains: "create installation client",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scraper, err := NewOrgScraperFromConfig(tc.cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewOrgScraperFromConfig() expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewOrgScraperFromConfig() unexpected error: %v", err)
			}

			typed, ok := scraper.(*GitHubOrgScraper)
			if !ok {
				t.Fatalf("scraper type = %T, want *GitHubOrgScraper", scraper)
			}
			if len(typed.clients) != len(tc.wantOrgs) {
				t.Fatalf("len(clients) = %d, want %d", len(typed.clients), len(tc.wantOrgs))
			}
			for _, org := range tc.wantOrgs {
				if _, exists := typed.clients[org]; !exists {
					t.Fatalf("missing org client %q", org)
				}
			}
		})
	}
}

func writePrivateKey(t *testing.T, dir, fileName string) string {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() unexpected error: %v", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	path := filepath.Join(dir, fileName)
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("os.WriteFile() unexpected error: %v", err)
	}
	return path
}
