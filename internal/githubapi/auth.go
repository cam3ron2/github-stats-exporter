package githubapi

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v75/github"
)

// InstallationAuthConfig configures GitHub App installation authentication.
type InstallationAuthConfig struct {
	AppID          int64
	InstallationID int64
	PrivateKeyPath string
	Timeout        time.Duration
	BaseTransport  http.RoundTripper
}

// RESTClient wraps the go-github REST client.
type RESTClient struct {
	Client *github.Client
}

// NewInstallationHTTPClient creates an authenticated HTTP client for one GitHub App installation.
func NewInstallationHTTPClient(cfg InstallationAuthConfig) (*http.Client, error) {
	if cfg.AppID <= 0 {
		return nil, fmt.Errorf("app id must be > 0")
	}
	if cfg.InstallationID <= 0 {
		return nil, fmt.Errorf("installation id must be > 0")
	}
	if strings.TrimSpace(cfg.PrivateKeyPath) == "" {
		return nil, fmt.Errorf("private key path is required")
	}

	baseTransport := cfg.BaseTransport
	if baseTransport == nil {
		baseTransport = http.DefaultTransport
	}

	transport, err := ghinstallation.NewKeyFromFile(baseTransport, cfg.AppID, cfg.InstallationID, cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("create github app transport: %w", err)
	}

	return &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
	}, nil
}

// NewGitHubRESTClient creates a go-github client with optional API base URL override.
func NewGitHubRESTClient(httpClient *http.Client, apiBaseURL string) (*RESTClient, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	client := github.NewClient(httpClient)
	trimmedBaseURL := strings.TrimSpace(apiBaseURL)
	if trimmedBaseURL == "" {
		return &RESTClient{Client: client}, nil
	}

	parsedURL, err := url.Parse(trimmedBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse github api base url: %w", err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("parse github api base url: missing scheme or host")
	}
	if !strings.HasSuffix(parsedURL.Path, "/") {
		parsedURL.Path += "/"
	}

	client.BaseURL = parsedURL
	return &RESTClient{Client: client}, nil
}
