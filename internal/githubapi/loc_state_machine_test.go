package githubapi

import (
	"testing"
	"time"
)

func TestLOCStateMachineApply(t *testing.T) {
	t.Parallel()

	baseTime := time.Unix(1739836800, 0)
	machine := LOCStateMachine{
		RefreshInterval:       24 * time.Hour,
		StaleWindowMultiplier: 2,
		ZeroDetectionWindows:  2,
		FallbackCooldown:      7 * 24 * time.Hour,
	}

	testCases := []struct {
		name  string
		state LOCState
		event LOCEvent
		want  LOCState
	}{
		{
			name: "initial_accepted_moves_to_warming",
			state: LOCState{
				Mode: LOCModeUnknown,
			},
			event: LOCEvent{
				ObservedAt: baseTime,
				HTTPStatus: 202,
			},
			want: LOCState{
				Mode:           LOCModeWarming,
				Consecutive202: 1,
				LastObservedAt: baseTime,
			},
		},
		{
			name: "warming_then_ready_resets_counters",
			state: LOCState{
				Mode:           LOCModeWarming,
				Consecutive202: 2,
				LastObservedAt: baseTime,
			},
			event: LOCEvent{
				ObservedAt:             baseTime.Add(1 * time.Hour),
				HTTPStatus:             200,
				StatsPresent:           true,
				ContributionsNonZero:   true,
				AdditionsDeletionsZero: false,
			},
			want: LOCState{
				Mode:                   LOCModeReady,
				Consecutive202:         0,
				ConsecutiveZeroWindows: 0,
				LastReadyAt:            baseTime.Add(1 * time.Hour),
				LastObservedAt:         baseTime.Add(1 * time.Hour),
			},
		},
		{
			name: "repeated_accepted_beyond_stale_window_marks_stale",
			state: LOCState{
				Mode:           LOCModeWarming,
				Consecutive202: 1,
				LastReadyAt:    baseTime,
				LastObservedAt: baseTime.Add(24 * time.Hour),
			},
			event: LOCEvent{
				ObservedAt: baseTime.Add(49 * time.Hour),
				HTTPStatus: 202,
			},
			want: LOCState{
				Mode:           LOCModeStale,
				Consecutive202: 2,
				LastReadyAt:    baseTime,
				LastObservedAt: baseTime.Add(49 * time.Hour),
			},
		},
		{
			name: "zeroed_window_increments_counter_but_keeps_ready_before_threshold",
			state: LOCState{
				Mode:                   LOCModeReady,
				ConsecutiveZeroWindows: 0,
				LastReadyAt:            baseTime,
				LastObservedAt:         baseTime,
			},
			event: LOCEvent{
				ObservedAt:             baseTime.Add(7 * 24 * time.Hour),
				HTTPStatus:             200,
				StatsPresent:           true,
				ContributionsNonZero:   true,
				AdditionsDeletionsZero: true,
			},
			want: LOCState{
				Mode:                   LOCModeReady,
				ConsecutiveZeroWindows: 1,
				LastReadyAt:            baseTime.Add(7 * 24 * time.Hour),
				LastObservedAt:         baseTime.Add(7 * 24 * time.Hour),
			},
		},
		{
			name: "zeroed_window_crossing_threshold_enables_fallback",
			state: LOCState{
				Mode:                   LOCModeReady,
				ConsecutiveZeroWindows: 1,
				LastReadyAt:            baseTime,
				LastObservedAt:         baseTime,
			},
			event: LOCEvent{
				ObservedAt:             baseTime.Add(8 * 24 * time.Hour),
				HTTPStatus:             200,
				StatsPresent:           true,
				ContributionsNonZero:   true,
				AdditionsDeletionsZero: true,
			},
			want: LOCState{
				Mode:                   LOCModeFallback,
				ConsecutiveZeroWindows: 2,
				LastReadyAt:            baseTime.Add(8 * 24 * time.Hour),
				FallbackUntil:          baseTime.Add(15 * 24 * time.Hour),
				LastObservedAt:         baseTime.Add(8 * 24 * time.Hour),
			},
		},
		{
			name: "fallback_remains_active_until_cooldown_expires",
			state: LOCState{
				Mode:           LOCModeFallback,
				FallbackUntil:  baseTime.Add(10 * 24 * time.Hour),
				LastReadyAt:    baseTime,
				LastObservedAt: baseTime,
			},
			event: LOCEvent{
				ObservedAt:             baseTime.Add(9 * 24 * time.Hour),
				HTTPStatus:             200,
				StatsPresent:           true,
				ContributionsNonZero:   true,
				AdditionsDeletionsZero: false,
			},
			want: LOCState{
				Mode:                   LOCModeFallback,
				ConsecutiveZeroWindows: 0,
				LastReadyAt:            baseTime.Add(9 * 24 * time.Hour),
				FallbackUntil:          baseTime.Add(10 * 24 * time.Hour),
				LastObservedAt:         baseTime.Add(9 * 24 * time.Hour),
			},
		},
		{
			name: "fallback_expires_and_returns_to_ready",
			state: LOCState{
				Mode:           LOCModeFallback,
				FallbackUntil:  baseTime.Add(9 * 24 * time.Hour),
				LastReadyAt:    baseTime,
				LastObservedAt: baseTime,
			},
			event: LOCEvent{
				ObservedAt:             baseTime.Add(10 * 24 * time.Hour),
				HTTPStatus:             200,
				StatsPresent:           true,
				ContributionsNonZero:   true,
				AdditionsDeletionsZero: false,
			},
			want: LOCState{
				Mode:                   LOCModeReady,
				ConsecutiveZeroWindows: 0,
				LastReadyAt:            baseTime.Add(10 * 24 * time.Hour),
				LastObservedAt:         baseTime.Add(10 * 24 * time.Hour),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := machine.Apply(tc.state, tc.event)
			if got.Mode != tc.want.Mode {
				t.Fatalf("Mode = %q, want %q", got.Mode, tc.want.Mode)
			}
			if got.Consecutive202 != tc.want.Consecutive202 {
				t.Fatalf("Consecutive202 = %d, want %d", got.Consecutive202, tc.want.Consecutive202)
			}
			if got.ConsecutiveZeroWindows != tc.want.ConsecutiveZeroWindows {
				t.Fatalf("ConsecutiveZeroWindows = %d, want %d", got.ConsecutiveZeroWindows, tc.want.ConsecutiveZeroWindows)
			}
			if !got.LastReadyAt.Equal(tc.want.LastReadyAt) {
				t.Fatalf("LastReadyAt = %v, want %v", got.LastReadyAt, tc.want.LastReadyAt)
			}
			if !got.FallbackUntil.Equal(tc.want.FallbackUntil) {
				t.Fatalf("FallbackUntil = %v, want %v", got.FallbackUntil, tc.want.FallbackUntil)
			}
			if !got.LastObservedAt.Equal(tc.want.LastObservedAt) {
				t.Fatalf("LastObservedAt = %v, want %v", got.LastObservedAt, tc.want.LastObservedAt)
			}
		})
	}
}
