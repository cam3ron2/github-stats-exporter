package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cam3ron2/github-stats-exporter/internal/app"
	"github.com/cam3ron2/github-stats-exporter/internal/config"
	"github.com/cam3ron2/github-stats-exporter/internal/leader"
	"github.com/cam3ron2/github-stats-exporter/internal/scrape"
	"github.com/cam3ron2/github-stats-exporter/internal/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "github-stats-exporter: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var configPath string
	flag.StringVar(&configPath, "config", "config/local.yaml", "path to YAML config file")
	flag.Parse()

	configFile, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("open config file: %w", err)
	}
	defer func() {
		if closeErr := configFile.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "github-stats-exporter: config file close failed: %v\n", closeErr)
		}
	}()

	cfg, err := config.Load(configFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(logLevel(cfg.Server.LogLevel))
	logger, err := loggerConfig.Build()
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer func() {
		if syncErr := logger.Sync(); syncErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "github-stats-exporter: logger sync failed: %v\n", syncErr)
		}
	}()

	telemetryRuntime, err := telemetry.Setup(telemetry.Config{
		Enabled:          cfg.Telemetry.OTELEnabled,
		ServiceName:      "github-stats-exporter",
		OTLPEndpoint:     cfg.Telemetry.OTELExporterEndpoint,
		TraceMode:        cfg.Telemetry.OTELTraceMode,
		TraceSampleRatio: cfg.Telemetry.OTELTraceSampleRatio,
	})
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := telemetryRuntime.Shutdown(shutdownCtx); shutdownErr != nil {
			logger.Warn("telemetry shutdown failed", zap.Error(shutdownErr))
		}
	}()

	orgScraper, err := scrape.NewOrgScraperFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("build org scraper: %w", err)
	}

	runtime := app.NewRuntime(cfg, orgScraper, logger)
	handler := runtime.Handler()
	server := &http.Server{
		Addr:              cfg.Server.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	rootCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	elector, err := buildElector(cfg, logger)
	if err != nil {
		return fmt.Errorf("build leader elector: %w", err)
	}
	runner := leader.NewRunner(elector, logger)
	roleManager := app.NewRoleManager(runtime)
	roleEvents, electorErrs := runner.Start(rootCtx)
	roleManagerErrCh := make(chan error, 1)
	go func() {
		roleManagerErrCh <- roleManager.Run(rootCtx, roleEvents, electorErrs)
	}()

	serverErrCh := make(chan error, 1)
	go func() {
		logger.Info("http server starting", zap.String("addr", cfg.Server.ListenAddr))
		if serveErr := server.ListenAndServe(); serveErr != nil && serveErr != http.ErrServerClosed {
			serverErrCh <- serveErr
		}
		close(serverErrCh)
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case roleErr := <-roleManagerErrCh:
		if roleErr != nil {
			return fmt.Errorf("role manager failed: %w", roleErr)
		}
	case serveErr := <-serverErrCh:
		if serveErr != nil {
			return fmt.Errorf("http server failed: %w", serveErr)
		}
	}

	runtime.StopLeader()
	runtime.StopFollower()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}

	logger.Info("shutdown complete")
	return nil
}

func buildElector(cfg *config.Config, logger *zap.Logger) (leader.Elector, error) {
	if cfg == nil {
		return leader.StaticElector{IsLeader: true}, nil
	}

	if !cfg.LeaderElection.Enabled {
		initialRole := strings.ToLower(strings.TrimSpace(os.Getenv("GITHUB_STATS_INITIAL_ROLE")))
		if initialRole == "follower" {
			return leader.StaticElector{IsLeader: false}, nil
		}
		return leader.StaticElector{IsLeader: true}, nil
	}

	apiBaseURL, token, httpClient, err := inClusterKubernetesClient()
	if err != nil {
		logger.Warn("failed to initialize in-cluster kubernetes client; falling back to static leader mode", zap.Error(err))
		return leader.StaticElector{IsLeader: true}, nil
	}

	identity := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if identity == "" {
		hostname, hostErr := os.Hostname()
		if hostErr != nil {
			identity = fmt.Sprintf("github-stats-exporter-%d", time.Now().UnixNano())
		} else {
			identity = hostname
		}
	}

	return leader.NewKubernetesLeaseElector(leader.KubernetesLeaseConfig{
		APIBaseURL:    apiBaseURL,
		Namespace:     cfg.LeaderElection.Namespace,
		LeaseName:     cfg.LeaderElection.LeaseName,
		Identity:      identity,
		BearerToken:   token,
		LeaseDuration: cfg.LeaderElection.LeaseDuration,
		RetryPeriod:   cfg.LeaderElection.RetryPeriod,
		HTTPClient:    httpClient,
	})
}

func inClusterKubernetesClient() (string, string, *http.Client, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return "", "", nil, fmt.Errorf("kubernetes service host/port environment variables are not set")
	}

	// #nosec G101 -- Kubernetes service account token file path is fixed by cluster convention.
	tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return "", "", nil, fmt.Errorf("read kubernetes service account token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return "", "", nil, fmt.Errorf("kubernetes service account token is empty")
	}

	caPath := "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	caBytes, caErr := os.ReadFile(filepath.Clean(caPath))
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caErr == nil && len(caBytes) > 0 {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caBytes) {
			tlsConfig.RootCAs = pool
		}
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	return fmt.Sprintf("https://%s:%s", host, port), token, httpClient, nil
}

func logLevel(raw string) zapcore.Level {
	switch strings.ToLower(raw) {
	case "debug":
		return zapcore.DebugLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
