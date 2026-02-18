package app

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

type fakeHooks struct {
	mu     sync.Mutex
	events []string
}

func (h *fakeHooks) StartLeader(_ context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, "start_leader")
}

func (h *fakeHooks) StopLeader() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, "stop_leader")
}

func (h *fakeHooks) StartFollower(_ context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, "start_follower")
}

func (h *fakeHooks) StopFollower() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, "stop_follower")
}

func (h *fakeHooks) Events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.events)
}

func TestRoleManager_Run(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		initialLeader bool
		transitions   []bool
		wantEvents    []string
	}{
		{
			name:          "starts_follower_by_default_and_transitions_to_leader",
			initialLeader: false,
			transitions:   []bool{true},
			wantEvents: []string{
				"start_follower",
				"stop_follower",
				"start_leader",
			},
		},
		{
			name:          "starts_leader_then_transitions_to_follower",
			initialLeader: true,
			transitions:   []bool{false},
			wantEvents: []string{
				"start_leader",
				"stop_leader",
				"start_follower",
			},
		},
		{
			name:          "ignores_duplicate_role_notifications",
			initialLeader: true,
			transitions:   []bool{true, true, false, false, true},
			wantEvents: []string{
				"start_leader",
				"stop_leader",
				"start_follower",
				"stop_follower",
				"start_leader",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			hooks := &fakeHooks{}
			events := make(chan bool, len(tc.transitions)+1)
			errs := make(chan error, 1)

			events <- tc.initialLeader
			for _, transition := range tc.transitions {
				events <- transition
			}
			close(events)

			manager := NewRoleManager(hooks)
			runErr := manager.Run(ctx, events, errs)
			if runErr != nil {
				t.Fatalf("Run() unexpected error: %v", runErr)
			}

			gotEvents := hooks.Events()
			if !slices.Equal(gotEvents, tc.wantEvents) {
				t.Fatalf("Run() events = %v, want %v", gotEvents, tc.wantEvents)
			}
		})
	}
}

func TestRoleManager_RunReturnsElectorError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	hooks := &fakeHooks{}
	events := make(chan bool, 1)
	errs := make(chan error, 1)
	manager := NewRoleManager(hooks)

	events <- true
	errs <- errors.New("elector failure")

	err := manager.Run(ctx, events, errs)
	if err == nil {
		t.Fatalf("Run() expected error, got nil")
	}
	if err.Error() != "elector failure" {
		t.Fatalf("Run() error = %q, want %q", err.Error(), "elector failure")
	}
}
