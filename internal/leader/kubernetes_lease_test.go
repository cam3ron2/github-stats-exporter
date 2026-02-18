package leader

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKubernetesLeaseElectorTryAcquireOrRenew(t *testing.T) {
	t.Parallel()

	type callState struct {
		getStatus int
		getBody   map[string]any
		postCode  int
		putCode   int
		postCalls int
		putCalls  int
	}

	baseTime := time.Unix(1739836800, 0).UTC()
	testCases := []struct {
		name          string
		state         callState
		now           time.Time
		wantAcquired  bool
		wantPostCalls int
		wantPutCalls  int
	}{
		{
			name: "creates_lease_when_not_found",
			state: callState{
				getStatus: http.StatusNotFound,
				postCode:  http.StatusCreated,
				putCode:   http.StatusOK,
			},
			now:           baseTime,
			wantAcquired:  true,
			wantPostCalls: 1,
		},
		{
			name: "renews_existing_lease_when_holder_matches",
			state: callState{
				getStatus: http.StatusOK,
				getBody: map[string]any{
					"metadata": map[string]any{"resourceVersion": "9"},
					"spec": map[string]any{
						"holderIdentity":       "instance-a",
						"leaseDurationSeconds": float64(60),
						"renewTime":            baseTime.Format(time.RFC3339),
					},
				},
				postCode: http.StatusCreated,
				putCode:  http.StatusOK,
			},
			now:          baseTime,
			wantAcquired: true,
			wantPutCalls: 1,
		},
		{
			name: "does_not_acquire_active_foreign_lease",
			state: callState{
				getStatus: http.StatusOK,
				getBody: map[string]any{
					"metadata": map[string]any{"resourceVersion": "4"},
					"spec": map[string]any{
						"holderIdentity":       "instance-b",
						"leaseDurationSeconds": float64(120),
						"renewTime":            baseTime.Format(time.RFC3339),
					},
				},
				postCode: http.StatusCreated,
				putCode:  http.StatusOK,
			},
			now:          baseTime.Add(30 * time.Second),
			wantAcquired: false,
		},
		{
			name: "steals_expired_foreign_lease",
			state: callState{
				getStatus: http.StatusOK,
				getBody: map[string]any{
					"metadata": map[string]any{"resourceVersion": "4"},
					"spec": map[string]any{
						"holderIdentity":       "instance-b",
						"leaseDurationSeconds": float64(30),
						"renewTime":            baseTime.Add(-2 * time.Minute).Format(time.RFC3339),
					},
				},
				postCode: http.StatusCreated,
				putCode:  http.StatusOK,
			},
			now:          baseTime,
			wantAcquired: true,
			wantPutCalls: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			state := tc.state
			var mu sync.Mutex
			client := &http.Client{
				Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					mu.Lock()
					defer mu.Unlock()

					switch req.Method {
					case http.MethodGet:
						if state.getStatus == http.StatusOK {
							return jsonResponse(http.StatusOK, state.getBody), nil
						}
						return jsonResponse(state.getStatus, map[string]any{"error": "not found"}), nil
					case http.MethodPost:
						state.postCalls++
						return jsonResponse(state.postCode, map[string]any{}), nil
					case http.MethodPut:
						state.putCalls++
						return jsonResponse(state.putCode, map[string]any{}), nil
					default:
						return jsonResponse(http.StatusMethodNotAllowed, map[string]any{}), nil
					}
				}),
			}

			elector, err := NewKubernetesLeaseElector(KubernetesLeaseConfig{
				APIBaseURL:    "https://kubernetes.default.svc",
				Namespace:     "github-stats-exporter",
				LeaseName:     "leader",
				Identity:      "instance-a",
				BearerToken:   "token",
				LeaseDuration: time.Minute,
				RetryPeriod:   10 * time.Millisecond,
				HTTPClient:    client,
				Now: func() time.Time {
					return tc.now
				},
			})
			if err != nil {
				t.Fatalf("NewKubernetesLeaseElector() unexpected error: %v", err)
			}

			acquired, err := elector.TryAcquireOrRenew(context.Background())
			if err != nil {
				t.Fatalf("TryAcquireOrRenew() unexpected error: %v", err)
			}
			if acquired != tc.wantAcquired {
				t.Fatalf("TryAcquireOrRenew().acquired = %t, want %t", acquired, tc.wantAcquired)
			}

			mu.Lock()
			defer mu.Unlock()
			if state.postCalls != tc.wantPostCalls {
				t.Fatalf("postCalls = %d, want %d", state.postCalls, tc.wantPostCalls)
			}
			if state.putCalls != tc.wantPutCalls {
				t.Fatalf("putCalls = %d, want %d", state.putCalls, tc.wantPutCalls)
			}
		})
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonResponse(statusCode int, payload any) *http.Response {
	bodyBytes, _ := json.Marshal(payload)
	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(string(bodyBytes))),
	}
}
