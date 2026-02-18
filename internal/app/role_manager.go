package app

import "context"

// RoleHooks defines lifecycle hooks for leader and follower responsibilities.
type RoleHooks interface {
	StartLeader(ctx context.Context)
	StopLeader()
	StartFollower(ctx context.Context)
	StopFollower()
}

// RoleManager handles role transitions based on leadership signals.
type RoleManager struct {
	hooks RoleHooks
}

// NewRoleManager creates a role manager.
func NewRoleManager(hooks RoleHooks) *RoleManager {
	return &RoleManager{hooks: hooks}
}

// Run processes role transition events until completion, context cancellation, or elector error.
func (m *RoleManager) Run(ctx context.Context, roleEvents <-chan bool, electorErrs <-chan error) error {
	if m == nil || m.hooks == nil {
		return nil
	}

	haveRole := false
	isLeader := false

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-electorErrs:
			if err != nil {
				return err
			}
		case nextRole, ok := <-roleEvents:
			if !ok {
				return nil
			}
			if !haveRole {
				haveRole = true
				isLeader = nextRole
				if isLeader {
					m.hooks.StartLeader(ctx)
					continue
				}
				m.hooks.StartFollower(ctx)
				continue
			}

			if nextRole == isLeader {
				continue
			}

			if isLeader {
				m.hooks.StopLeader()
				m.hooks.StartFollower(ctx)
				isLeader = false
				continue
			}

			m.hooks.StopFollower()
			m.hooks.StartLeader(ctx)
			isLeader = true
		}
	}
}
