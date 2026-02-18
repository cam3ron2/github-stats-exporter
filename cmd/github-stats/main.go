package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cam3ron2/github-stats/internal/app"
	"github.com/cam3ron2/github-stats/internal/config"
	"github.com/cam3ron2/github-stats/internal/scrape"
	"github.com/cam3ron2/github-stats/internal/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "github-stats: %v\n", err)
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
		_ = configFile.Close()
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
		_ = logger.Sync()
	}()

	telemetryRuntime, err := telemetry.Setup(telemetry.Config{
		Enabled:          cfg.Telemetry.OTELEnabled,
		ServiceName:      "github-stats",
		TraceMode:        cfg.Telemetry.OTELTraceMode,
		TraceSampleRatio: cfg.Telemetry.OTELTraceSampleRatio,
	})
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = telemetryRuntime.Shutdown(shutdownCtx)
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

	initialRole := strings.ToLower(strings.TrimSpace(os.Getenv("GITHUB_STATS_INITIAL_ROLE")))
	switch initialRole {
	case "follower":
		runtime.StartFollower(rootCtx)
		logger.Info("runtime started in follower role")
	default:
		runtime.StartLeader(rootCtx)
		logger.Info("runtime started in leader role")
	}

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
