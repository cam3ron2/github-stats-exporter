package leader

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

type scriptedElector struct {
	events []bool
	err    error
	delay  time.Duration
}

func (e *scriptedElector) Run(ctx context.Context, emit func(bool)) error {
	for _, event := range e.events {
		if e.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(e.delay):
			}
		}
		emit(event)
	}
	if e.err != nil {
		return e.err
	}
	<-ctx.Done()
	return nil
}

func readRoleEvents(events <-chan bool) []bool {
	result := make([]bool, 0)
	for event := range events {
		result = append(result, event)
	}
	return result
}

func TestRunnerStart(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		elector   Elector
		cancelAt  time.Duration
		wantRoles []bool
		wantErr   string
	}{
		{
			name:      "nil_elector_defaults_to_leader",
			elector:   nil,
			cancelAt:  30 * time.Millisecond,
			wantRoles: []bool{true},
		},
		{
			name: "deduplicates_consecutive_role_events",
			elector: &scriptedElector{
				events: []bool{true, true, false, false, true},
			},
			cancelAt:  30 * time.Millisecond,
			wantRoles: []bool{true, false, true},
		},
		{
			name: "propagates_elector_error",
			elector: &scriptedElector{
				events: []bool{false},
				err:    errors.New("election failed"),
			},
			wantRoles: []bool{false},
			wantErr:   "election failed",
		},
		{
			name:      "static_follower_emits_follower",
			elector:   StaticElector{IsLeader: false},
			cancelAt:  30 * time.Millisecond,
			wantRoles: []bool{false},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			runner := NewRunner(tc.elector)
			roles, errs := runner.Start(ctx)

			if tc.cancelAt > 0 {
				time.AfterFunc(tc.cancelAt, cancel)
			}

			gotRoles := readRoleEvents(roles)
			if !slices.Equal(gotRoles, tc.wantRoles) {
				t.Fatalf("roles = %v, want %v", gotRoles, tc.wantRoles)
			}

			gotErr := ""
			for err := range errs {
				if err != nil {
					gotErr = err.Error()
				}
			}
			if tc.wantErr == "" && gotErr != "" {
				t.Fatalf("unexpected error: %v", gotErr)
			}
			if tc.wantErr != "" && gotErr != tc.wantErr {
				t.Fatalf("error = %q, want %q", gotErr, tc.wantErr)
			}
		})
	}
}

func TestChannelElectorRun(t *testing.T) {
	t.Parallel()

	events := make(chan bool, 3)
	events <- true
	events <- false
	events <- true
	close(events)

	elector := ChannelElector{Events: events}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	observed := make([]bool, 0)
	err := elector.Run(ctx, func(isLeader bool) {
		observed = append(observed, isLeader)
	})
	if err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	want := []bool{true, false, true}
	if !slices.Equal(observed, want) {
		t.Fatalf("observed = %v, want %v", observed, want)
	}
}
