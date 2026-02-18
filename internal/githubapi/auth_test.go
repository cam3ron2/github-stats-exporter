package githubapi

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writePrivateKeyPEM(t *testing.T, dir string) string {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() unexpected error: %v", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	path := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("os.WriteFile() unexpected error: %v", err)
	}
	return path
}

func TestNewInstallationHTTPClient(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	validKeyPath := writePrivateKeyPEM(t, tempDir)
	invalidKeyPath := filepath.Join(tempDir, "invalid.pem")
	if err := os.WriteFile(invalidKeyPath, []byte("not-a-key"), 0o600); err != nil {
		t.Fatalf("os.WriteFile(invalid) unexpected error: %v", err)
	}

	testCases := []struct {
		name        string
		config      InstallationAuthConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "invalid_app_id",
			config: InstallationAuthConfig{
				AppID:          0,
				InstallationID: 1,
				PrivateKeyPath: validKeyPath,
			},
			wantErr:     true,
			errContains: "app id",
		},
		{
			name: "invalid_installation_id",
			config: InstallationAuthConfig{
				AppID:          1,
				InstallationID: 0,
				PrivateKeyPath: validKeyPath,
			},
			wantErr:     true,
			errContains: "installation id",
		},
		{
			name: "missing_private_key_path",
			config: InstallationAuthConfig{
				AppID:          1,
				InstallationID: 1,
				PrivateKeyPath: "",
			},
			wantErr:     true,
			errContains: "private key path",
		},
		{
			name: "invalid_private_key_file",
			config: InstallationAuthConfig{
				AppID:          1,
				InstallationID: 1,
				PrivateKeyPath: invalidKeyPath,
			},
			wantErr:     true,
			errContains: "create github app transport",
		},
		{
			name: "valid_configuration",
			config: InstallationAuthConfig{
				AppID:          1,
				InstallationID: 1,
				PrivateKeyPath: validKeyPath,
				Timeout:        15 * time.Second,
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewInstallationHTTPClient(tc.config)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewInstallationHTTPClient() expected error, got nil")
				}
				if tc.errContains != "" && !contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("NewInstallationHTTPClient() unexpected error: %v", err)
			}
			if client == nil {
				t.Fatalf("NewInstallationHTTPClient() returned nil client")
			}
			if client.Transport == nil {
				t.Fatalf("client.Transport is nil")
			}
			if client.Timeout != tc.config.Timeout {
				t.Fatalf("client.Timeout = %s, want %s", client.Timeout, tc.config.Timeout)
			}
		})
	}
}

func TestNewGitHubRESTClient(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		httpClient  *http.Client
		apiBaseURL  string
		wantErr     bool
		errContains string
		assert      func(t *testing.T, client *RESTClient)
	}{
		{
			name:       "default_base_url",
			httpClient: &http.Client{},
			assert: func(t *testing.T, client *RESTClient) {
				t.Helper()
				if client == nil || client.Client == nil {
					t.Fatalf("client is nil")
				}
			},
		},
		{
			name:        "invalid_base_url",
			httpClient:  &http.Client{},
			apiBaseURL:  "://bad-url",
			wantErr:     true,
			errContains: "parse github api base url",
		},
		{
			name:       "custom_enterprise_base_url",
			httpClient: &http.Client{},
			apiBaseURL: "https://github.example.com/api/v3",
			assert: func(t *testing.T, client *RESTClient) {
				t.Helper()
				if got := client.Client.BaseURL.String(); got != "https://github.example.com/api/v3/" {
					t.Fatalf("BaseURL = %q, want %q", got, "https://github.example.com/api/v3/")
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewGitHubRESTClient(tc.httpClient, tc.apiBaseURL)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewGitHubRESTClient() expected error, got nil")
				}
				if tc.errContains != "" && !contains(err.Error(), tc.errContains) {
					t.Fatalf("error = %q, missing %q", err.Error(), tc.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("NewGitHubRESTClient() unexpected error: %v", err)
			}
			if tc.assert != nil {
				tc.assert(t, client)
			}
		})
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
