package leader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// KubernetesLeaseConfig configures Kubernetes Lease-based election.
type KubernetesLeaseConfig struct {
	APIBaseURL string
	Namespace  string
	LeaseName  string
	Identity   string
	//nolint:gosec // Bearer token is required for Kubernetes API authentication.
	BearerToken   string
	LeaseDuration time.Duration
	RetryPeriod   time.Duration
	HTTPClient    *http.Client
	Now           func() time.Time
	Sleep         func(time.Duration)
}

// KubernetesLeaseElector implements leader election with coordination.k8s.io Lease resources.
type KubernetesLeaseElector struct {
	apiBaseURL    string
	namespace     string
	leaseName     string
	identity      string
	bearerToken   string
	leaseDuration time.Duration
	retryPeriod   time.Duration
	httpClient    *http.Client
	now           func() time.Time
	sleep         func(time.Duration)
}

// NewKubernetesLeaseElector creates a Kubernetes Lease elector.
func NewKubernetesLeaseElector(cfg KubernetesLeaseConfig) (*KubernetesLeaseElector, error) {
	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return nil, fmt.Errorf("kubernetes api base url is required")
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		return nil, fmt.Errorf("kubernetes namespace is required")
	}
	if strings.TrimSpace(cfg.LeaseName) == "" {
		return nil, fmt.Errorf("kubernetes lease name is required")
	}
	if strings.TrimSpace(cfg.Identity) == "" {
		return nil, fmt.Errorf("kubernetes elector identity is required")
	}

	apiBaseURL := strings.TrimRight(strings.TrimSpace(cfg.APIBaseURL), "/")
	if _, err := url.Parse(apiBaseURL); err != nil {
		return nil, fmt.Errorf("parse kubernetes api base url: %w", err)
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	leaseDuration := cfg.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	retryPeriod := cfg.RetryPeriod
	if retryPeriod <= 0 {
		retryPeriod = 5 * time.Second
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	sleepFn := cfg.Sleep
	if sleepFn == nil {
		sleepFn = time.Sleep
	}

	return &KubernetesLeaseElector{
		apiBaseURL:    apiBaseURL,
		namespace:     strings.TrimSpace(cfg.Namespace),
		leaseName:     strings.TrimSpace(cfg.LeaseName),
		identity:      strings.TrimSpace(cfg.Identity),
		bearerToken:   strings.TrimSpace(cfg.BearerToken),
		leaseDuration: leaseDuration,
		retryPeriod:   retryPeriod,
		httpClient:    httpClient,
		now:           nowFn,
		sleep:         sleepFn,
	}, nil
}

// Run evaluates lease ownership in a retry loop and emits leadership state.
func (e *KubernetesLeaseElector) Run(ctx context.Context, emit func(isLeader bool)) error {
	if e == nil {
		return fmt.Errorf("kubernetes lease elector is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		acquired, err := e.TryAcquireOrRenew(ctx)
		if err != nil {
			return err
		}
		emit(acquired)

		select {
		case <-ctx.Done():
			return nil
		default:
		}
		e.sleep(e.retryPeriod)
	}
}

// TryAcquireOrRenew attempts to create, renew, or take over the configured lease.
func (e *KubernetesLeaseElector) TryAcquireOrRenew(ctx context.Context) (bool, error) {
	if e == nil {
		return false, fmt.Errorf("kubernetes lease elector is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	now := e.now().UTC()
	existingLease, getStatus, err := e.getLease(ctx)
	if err != nil {
		return false, err
	}

	if getStatus == http.StatusNotFound {
		createErr := e.createLease(ctx, leaseResource{
			Metadata: leaseMetadata{
				Name:      e.leaseName,
				Namespace: e.namespace,
			},
			Spec: leaseSpec{
				HolderIdentity:       e.identity,
				LeaseDurationSeconds: int32(e.leaseDuration.Seconds()),
				RenewTime:            now.Format(time.RFC3339),
			},
		})
		if createErr != nil {
			return false, createErr
		}
		return true, nil
	}

	holder := strings.TrimSpace(existingLease.Spec.HolderIdentity)
	durationSeconds := int(existingLease.Spec.LeaseDurationSeconds)
	if durationSeconds <= 0 {
		durationSeconds = int(e.leaseDuration.Seconds())
	}
	renewTime := parseLeaseTime(existingLease.Spec.RenewTime)
	isExpired := !renewTime.IsZero() && !renewTime.Add(time.Duration(durationSeconds)*time.Second).After(now)

	if holder != "" && holder != e.identity && !isExpired {
		return false, nil
	}

	existingLease.Spec.HolderIdentity = e.identity
	existingLease.Spec.LeaseDurationSeconds = int32(e.leaseDuration.Seconds())
	existingLease.Spec.RenewTime = now.Format(time.RFC3339)
	if err := e.updateLease(ctx, existingLease); err != nil {
		return false, err
	}
	return true, nil
}

func (e *KubernetesLeaseElector) getLease(ctx context.Context) (leaseResource, int, error) {
	resourceURL := e.leaseResourceURL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resourceURL, nil)
	if err != nil {
		return leaseResource{}, 0, fmt.Errorf("create lease get request: %w", err)
	}
	e.applyAuth(req)

	//nolint:gosec // Lease endpoint is derived from validated in-cluster Kubernetes API address.
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return leaseResource{}, 0, fmt.Errorf("execute lease get request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return leaseResource{}, http.StatusNotFound, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return leaseResource{}, resp.StatusCode, fmt.Errorf(
				"lease get request failed: status=%d body-read-error=%v",
				resp.StatusCode,
				readErr,
			)
		}
		return leaseResource{}, resp.StatusCode, fmt.Errorf("lease get request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var existing leaseResource
	if err := json.NewDecoder(resp.Body).Decode(&existing); err != nil {
		return leaseResource{}, resp.StatusCode, fmt.Errorf("decode lease get response: %w", err)
	}
	return existing, resp.StatusCode, nil
}

func (e *KubernetesLeaseElector) createLease(ctx context.Context, lease leaseResource) error {
	return e.writeLease(ctx, http.MethodPost, e.leaseCollectionURL(), lease)
}

func (e *KubernetesLeaseElector) updateLease(ctx context.Context, lease leaseResource) error {
	return e.writeLease(ctx, http.MethodPut, e.leaseResourceURL(), lease)
}

func (e *KubernetesLeaseElector) writeLease(ctx context.Context, method, endpoint string, lease leaseResource) error {
	payload, err := json.Marshal(lease)
	if err != nil {
		return fmt.Errorf("marshal lease payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create lease write request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	e.applyAuth(req)

	//nolint:gosec // Lease endpoint is derived from validated in-cluster Kubernetes API address.
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute lease write request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}

	bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if readErr != nil {
		return fmt.Errorf("lease write request failed: status=%d body-read-error=%v", resp.StatusCode, readErr)
	}
	return fmt.Errorf("lease write request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
}

func (e *KubernetesLeaseElector) leaseCollectionURL() string {
	return fmt.Sprintf("%s/apis/coordination.k8s.io/v1/namespaces/%s/leases", e.apiBaseURL, url.PathEscape(e.namespace))
}

func (e *KubernetesLeaseElector) leaseResourceURL() string {
	return fmt.Sprintf("%s/%s", e.leaseCollectionURL(), url.PathEscape(e.leaseName))
}

func (e *KubernetesLeaseElector) applyAuth(req *http.Request) {
	if strings.TrimSpace(e.bearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+e.bearerToken)
	}
}

func parseLeaseTime(raw string) time.Time {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

type leaseResource struct {
	APIVersion string        `json:"apiVersion,omitempty"`
	Kind       string        `json:"kind,omitempty"`
	Metadata   leaseMetadata `json:"metadata"`
	Spec       leaseSpec     `json:"spec"`
}

type leaseMetadata struct {
	Name            string `json:"name,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
	ResourceVersion string `json:"resourceVersion,omitempty"`
}

type leaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int32  `json:"leaseDurationSeconds,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
}
