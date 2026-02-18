package githubapi

import "time"

// LOCMode represents the contributor-stats processing mode.
type LOCMode string

const (
	// LOCModeUnknown is the initial mode before first observation.
	LOCModeUnknown LOCMode = "unknown"
	// LOCModeWarming indicates contributor stats are still being generated.
	LOCModeWarming LOCMode = "warming"
	// LOCModeReady indicates contributor stats are available and usable.
	LOCModeReady LOCMode = "ready"
	// LOCModeStale indicates contributor stats have remained unavailable too long.
	LOCModeStale LOCMode = "stale"
	// LOCModeFallback indicates commit-stat fallback mode is active.
	LOCModeFallback LOCMode = "fallback"
)

// LOCState tracks contributor-stats state transitions.
type LOCState struct {
	Mode                   LOCMode
	Consecutive202         int
	ConsecutiveZeroWindows int
	LastReadyAt            time.Time
	FallbackUntil          time.Time
	LastObservedAt         time.Time
}

// LOCEvent represents one contributor-stats observation.
type LOCEvent struct {
	ObservedAt             time.Time
	HTTPStatus             int
	StatsPresent           bool
	ContributionsNonZero   bool
	AdditionsDeletionsZero bool
}

// LOCStateMachine configures transition rules.
type LOCStateMachine struct {
	RefreshInterval       time.Duration
	StaleWindowMultiplier int
	ZeroDetectionWindows  int
	FallbackCooldown      time.Duration
}

// Apply applies an event to a previous state and returns a new state.
func (m LOCStateMachine) Apply(previous LOCState, event LOCEvent) LOCState {
	next := previous
	next.LastObservedAt = event.ObservedAt

	if event.HTTPStatus == 202 {
		next.Consecutive202++
		if previous.Mode == LOCModeUnknown {
			next.Mode = LOCModeWarming
			return next
		}
		if m.RefreshInterval > 0 && m.StaleWindowMultiplier > 0 && !next.LastReadyAt.IsZero() {
			staleWindow := time.Duration(m.StaleWindowMultiplier) * m.RefreshInterval
			if event.ObservedAt.Sub(next.LastReadyAt) > staleWindow {
				next.Mode = LOCModeStale
				return next
			}
		}
		if next.Mode != LOCModeFallback {
			next.Mode = LOCModeWarming
		}
		return next
	}

	if event.HTTPStatus != 200 || !event.StatsPresent {
		return next
	}

	next.Consecutive202 = 0
	next.LastReadyAt = event.ObservedAt
	if event.ContributionsNonZero && event.AdditionsDeletionsZero {
		next.ConsecutiveZeroWindows++
	} else {
		next.ConsecutiveZeroWindows = 0
	}

	if next.Mode == LOCModeFallback {
		if event.ObservedAt.Before(next.FallbackUntil) {
			return next
		}
		next.FallbackUntil = time.Time{}
	}

	if m.ZeroDetectionWindows > 0 && next.ConsecutiveZeroWindows >= m.ZeroDetectionWindows {
		next.Mode = LOCModeFallback
		next.FallbackUntil = event.ObservedAt.Add(m.FallbackCooldown)
		return next
	}

	next.Mode = LOCModeReady
	return next
}
