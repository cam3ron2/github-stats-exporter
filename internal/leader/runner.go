package leader

import (
	"context"
	"errors"

	"go.uber.org/zap"
)

// Elector emits leadership transitions for a single process identity.
type Elector interface {
	Run(ctx context.Context, emit func(isLeader bool)) error
}

// Runner adapts an Elector into role and error channels used by runtime orchestration.
type Runner struct {
	elector Elector
	logger  *zap.Logger
}

// StaticElector emits a fixed role and then waits for context cancellation.
type StaticElector struct {
	IsLeader bool
}

// ChannelElector emits role events from an input channel.
type ChannelElector struct {
	Events <-chan bool
	Err    error
}

// NewRunner creates a leader-election runner.
func NewRunner(elector Elector, logger ...*zap.Logger) *Runner {
	baseLogger := zap.NewNop()
	if len(logger) > 0 && logger[0] != nil {
		baseLogger = logger[0]
	}
	return &Runner{
		elector: elector,
		logger:  baseLogger,
	}
}

// Start begins election observation and returns role and error channels.
func (r *Runner) Start(ctx context.Context) (<-chan bool, <-chan error) {
	if ctx == nil {
		ctx = context.Background()
	}

	roles := make(chan bool, 8)
	errs := make(chan error, 1)

	elector := r.elector
	if elector == nil {
		elector = StaticElector{IsLeader: true}
	}

	go func() {
		defer close(roles)
		defer close(errs)

		haveLast := false
		lastRole := false
		emit := func(isLeader bool) {
			if haveLast && lastRole == isLeader {
				return
			}
			haveLast = true
			lastRole = isLeader
			r.logger.Info("leadership transition", zap.Bool("is_leader", isLeader))
			select {
			case roles <- isLeader:
			case <-ctx.Done():
			}
		}

		err := elector.Run(ctx, emit)
		if err == nil {
			return
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}

		select {
		case errs <- err:
		default:
		}
	}()

	return roles, errs
}

// Run emits the configured static role until context cancellation.
func (e StaticElector) Run(ctx context.Context, emit func(isLeader bool)) error {
	emit(e.IsLeader)
	<-ctx.Done()
	return nil
}

// Run emits role events from a channel until closed or context cancellation.
func (e ChannelElector) Run(ctx context.Context, emit func(isLeader bool)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-e.Events:
			if !ok {
				return e.Err
			}
			emit(event)
		}
	}
}
