package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/leader"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogLevel(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		input string
		want  zapcore.Level
	}{
		{name: "debug", input: "debug", want: zapcore.DebugLevel},
		{name: "warn", input: "warn", want: zapcore.WarnLevel},
		{name: "error", input: "error", want: zapcore.ErrorLevel},
		{name: "default_info", input: "other", want: zapcore.InfoLevel},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := logLevel(tc.input)
			if got != tc.want {
				t.Fatalf("logLevel(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestShouldIgnoreLoggerSyncError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil_error",
			err:  nil,
			want: false,
		},
		{
			name: "einval_direct",
			err:  syscall.EINVAL,
			want: true,
		},
		{
			name: "enotty_direct",
			err:  syscall.ENOTTY,
			want: true,
		},
		{
			name: "wrapped_einval",
			err:  fmt.Errorf("wrapped: %w", syscall.EINVAL),
			want: true,
		},
		{
			name: "other_error",
			err:  errors.New("boom"),
			want: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := shouldIgnoreLoggerSyncError(tc.err)
			if got != tc.want {
				t.Fatalf("shouldIgnoreLoggerSyncError(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestBuildElector(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *config.Config
		initialRole string
		wantLeader  bool
	}{
		{
			name:       "nil_config_defaults_to_leader",
			cfg:        nil,
			wantLeader: true,
		},
		{
			name: "leader_election_disabled_follower",
			cfg: &config.Config{
				LeaderElection: config.LeaderElectionConfig{Enabled: false},
			},
			initialRole: "follower",
			wantLeader:  false,
		},
		{
			name: "leader_election_disabled_defaults_to_leader",
			cfg: &config.Config{
				LeaderElection: config.LeaderElectionConfig{Enabled: false},
			},
			wantLeader: true,
		},
		{
			name: "leader_election_enabled_without_cluster_falls_back_to_leader",
			cfg: &config.Config{
				LeaderElection: config.LeaderElectionConfig{
					Enabled:   true,
					Namespace: "github-stats-exporter",
					LeaseName: "leader",
				},
			},
			wantLeader: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			previousRole := os.Getenv("GITHUB_STATS_INITIAL_ROLE")
			if tc.initialRole == "" {
				_ = os.Unsetenv("GITHUB_STATS_INITIAL_ROLE")
			} else {
				_ = os.Setenv("GITHUB_STATS_INITIAL_ROLE", tc.initialRole)
			}
			t.Cleanup(func() {
				if previousRole == "" {
					_ = os.Unsetenv("GITHUB_STATS_INITIAL_ROLE")
				} else {
					_ = os.Setenv("GITHUB_STATS_INITIAL_ROLE", previousRole)
				}
			})

			elector, err := buildElector(tc.cfg, zap.NewNop())
			if err != nil {
				t.Fatalf("buildElector() unexpected error: %v", err)
			}
			staticElector, ok := elector.(leader.StaticElector)
			if !ok {
				t.Fatalf("elector type = %T, want leader.StaticElector in this test", elector)
			}
			if staticElector.IsLeader != tc.wantLeader {
				t.Fatalf("StaticElector.IsLeader = %t, want %t", staticElector.IsLeader, tc.wantLeader)
			}
		})
	}
}

func TestBuildElectorLeaderElectionEnabledInCluster(t *testing.T) {
	tokenPath, caPath := writeServiceAccountFiles(t, "token-value\n")
	restoreServiceAccountPaths := setServiceAccountPaths(tokenPath, caPath)
	t.Cleanup(restoreServiceAccountPaths)

	restoreEnv := setInClusterEnv(t, "10.96.0.1", "443")
	t.Cleanup(restoreEnv)
	_ = os.Setenv("HOSTNAME", "pod-1")
	t.Cleanup(func() {
		_ = os.Unsetenv("HOSTNAME")
	})

	cfg := &config.Config{
		LeaderElection: config.LeaderElectionConfig{
			Enabled:       true,
			Namespace:     "default",
			LeaseName:     "github-stats-exporter",
			LeaseDuration: 15 * time.Second,
			RetryPeriod:   time.Second,
		},
	}

	elector, err := buildElector(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("buildElector() unexpected error: %v", err)
	}
	if _, ok := elector.(*leader.KubernetesLeaseElector); !ok {
		t.Fatalf("elector type = %T, want *leader.KubernetesLeaseElector", elector)
	}
}

func TestInClusterKubernetesClientMissingEnvironment(t *testing.T) {
	originalHost := os.Getenv("KUBERNETES_SERVICE_HOST")
	originalPort := os.Getenv("KUBERNETES_SERVICE_PORT")
	_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
	_ = os.Unsetenv("KUBERNETES_SERVICE_PORT")
	t.Cleanup(func() {
		if originalHost == "" {
			_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
		} else {
			_ = os.Setenv("KUBERNETES_SERVICE_HOST", originalHost)
		}
		if originalPort == "" {
			_ = os.Unsetenv("KUBERNETES_SERVICE_PORT")
		} else {
			_ = os.Setenv("KUBERNETES_SERVICE_PORT", originalPort)
		}
	})

	_, _, _, err := inClusterKubernetesClient()
	if err == nil {
		t.Fatalf("inClusterKubernetesClient() expected error, got nil")
	}
}

func TestInClusterKubernetesClientTokenReadFailure(t *testing.T) {
	restoreEnv := setInClusterEnv(t, "10.0.0.1", "443")
	t.Cleanup(restoreEnv)
	restoreServiceAccountPaths := setServiceAccountPaths(
		filepath.Join(t.TempDir(), "missing-token"),
		filepath.Join(t.TempDir(), "missing-ca"),
	)
	t.Cleanup(restoreServiceAccountPaths)

	_, _, _, err := inClusterKubernetesClient()
	if err == nil {
		t.Fatalf("inClusterKubernetesClient() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "service account token") {
		t.Fatalf("inClusterKubernetesClient() error = %q, want token read error", err.Error())
	}
}

func TestInClusterKubernetesClientTokenEmpty(t *testing.T) {
	tokenPath, caPath := writeServiceAccountFiles(t, " \n")
	restoreServiceAccountPaths := setServiceAccountPaths(tokenPath, caPath)
	t.Cleanup(restoreServiceAccountPaths)
	restoreEnv := setInClusterEnv(t, "10.0.0.2", "443")
	t.Cleanup(restoreEnv)

	_, _, _, err := inClusterKubernetesClient()
	if err == nil {
		t.Fatalf("inClusterKubernetesClient() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "token is empty") {
		t.Fatalf("inClusterKubernetesClient() error = %q, want empty token error", err.Error())
	}
}

func TestInClusterKubernetesClientSuccess(t *testing.T) {
	tokenPath, caPath := writeServiceAccountFiles(t, "token-value\n")
	restoreServiceAccountPaths := setServiceAccountPaths(tokenPath, caPath)
	t.Cleanup(restoreServiceAccountPaths)
	restoreEnv := setInClusterEnv(t, "10.0.0.3", "443")
	t.Cleanup(restoreEnv)

	apiBaseURL, token, client, err := inClusterKubernetesClient()
	if err != nil {
		t.Fatalf("inClusterKubernetesClient() unexpected error: %v", err)
	}
	if apiBaseURL != "https://10.0.0.3:443" {
		t.Fatalf("apiBaseURL = %q, want %q", apiBaseURL, "https://10.0.0.3:443")
	}
	if token != "token-value" {
		t.Fatalf("token = %q, want %q", token, "token-value")
	}
	if client == nil {
		t.Fatalf("http client is nil")
	}
	if client.Timeout != 10*time.Second {
		t.Fatalf("http client timeout = %s, want 10s", client.Timeout)
	}
}

func TestRunConfigLoadingAndScraperBuildFailures(t *testing.T) {
	testCases := []struct {
		name          string
		configContent string
		configPath    string
		wantErrSubstr string
	}{
		{
			name:          "missing_config_file",
			configPath:    filepath.Join(t.TempDir(), "missing.yaml"),
			wantErrSubstr: "open config file",
		},
		{
			name:          "invalid_config_yaml",
			configContent: "server: [",
			wantErrSubstr: "load config",
		},
		{
			name: "org_scraper_build_failure",
			configContent: `
server:
  listen_addr: ":0"
  log_level: "info"
github:
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: "/tmp/does-not-exist.pem"
      scrape_interval: "5m"
backfill:
  requeue_delays: ["1m"]
`,
			wantErrSubstr: "build org scraper",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			configPath := tc.configPath
			if configPath == "" {
				configPath = filepath.Join(t.TempDir(), "config.yaml")
			}
			if tc.configContent != "" {
				if err := os.WriteFile(configPath, []byte(tc.configContent), 0o600); err != nil {
					t.Fatalf("WriteFile(config) unexpected error: %v", err)
				}
			}

			err := runWithArgsForTest([]string{"-config", configPath})
			if err == nil {
				t.Fatalf("run() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("run() error = %q, missing %q", err.Error(), tc.wantErrSubstr)
			}
		})
	}
}

func TestRunHTTPServerStartupFailure(t *testing.T) {
	privateKeyPath := writeGitHubAppPrivateKey(t)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	configContent := fmt.Sprintf(`
server:
  listen_addr: "invalid-address"
  log_level: "info"
github:
  orgs:
    - org: "org-a"
      app_id: 1
      installation_id: 2
      private_key_path: %q
      scrape_interval: "5m"
leader_election:
  enabled: false
backfill:
  requeue_delays: ["1m"]
`, privateKeyPath)
	if err := os.WriteFile(configPath, []byte(configContent), 0o600); err != nil {
		t.Fatalf("WriteFile(config) unexpected error: %v", err)
	}

	previousRole := os.Getenv("GITHUB_STATS_INITIAL_ROLE")
	_ = os.Setenv("GITHUB_STATS_INITIAL_ROLE", "leader")
	t.Cleanup(func() {
		if previousRole == "" {
			_ = os.Unsetenv("GITHUB_STATS_INITIAL_ROLE")
		} else {
			_ = os.Setenv("GITHUB_STATS_INITIAL_ROLE", previousRole)
		}
	})

	err := runWithArgsForTest([]string{"-config", configPath})
	if err == nil {
		t.Fatalf("run() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "http server failed") {
		t.Fatalf("run() error = %q, want http server startup error", err.Error())
	}
}

func runWithArgsForTest(args []string) error {
	originalArgs := os.Args
	originalFlagSet := flag.CommandLine
	defer func() {
		os.Args = originalArgs
		flag.CommandLine = originalFlagSet
	}()

	os.Args = append([]string{"github-stats-exporter"}, args...)
	flagSet := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	flag.CommandLine = flagSet

	return run()
}

func setServiceAccountPaths(tokenPath, caPath string) func() {
	previousTokenPath := kubernetesServiceAccountTokenPath
	previousCAPath := kubernetesServiceAccountCAPath
	kubernetesServiceAccountTokenPath = tokenPath
	kubernetesServiceAccountCAPath = caPath
	return func() {
		kubernetesServiceAccountTokenPath = previousTokenPath
		kubernetesServiceAccountCAPath = previousCAPath
	}
}

func setInClusterEnv(t *testing.T, host, port string) func() {
	t.Helper()

	originalHost := os.Getenv("KUBERNETES_SERVICE_HOST")
	originalPort := os.Getenv("KUBERNETES_SERVICE_PORT")
	_ = os.Setenv("KUBERNETES_SERVICE_HOST", host)
	_ = os.Setenv("KUBERNETES_SERVICE_PORT", port)
	return func() {
		if originalHost == "" {
			_ = os.Unsetenv("KUBERNETES_SERVICE_HOST")
		} else {
			_ = os.Setenv("KUBERNETES_SERVICE_HOST", originalHost)
		}
		if originalPort == "" {
			_ = os.Unsetenv("KUBERNETES_SERVICE_PORT")
		} else {
			_ = os.Setenv("KUBERNETES_SERVICE_PORT", originalPort)
		}
	}
}

func writeServiceAccountFiles(t *testing.T, tokenContent string) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("GenerateKey() unexpected error: %v", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<20))
	if err != nil {
		t.Fatalf("rand.Int(serial) unexpected error: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate() unexpected error: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(tokenPath, []byte(tokenContent), 0o600); err != nil {
		t.Fatalf("WriteFile(token) unexpected error: %v", err)
	}
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatalf("WriteFile(ca) unexpected error: %v", err)
	}
	return tokenPath, caPath
}

func writeGitHubAppPrivateKey(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("GenerateKey() unexpected error: %v", err)
	}
	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: keyBytes,
	})
	keyPath := filepath.Join(t.TempDir(), "app-private-key.pem")
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("WriteFile(private key) unexpected error: %v", err)
	}
	return keyPath
}
