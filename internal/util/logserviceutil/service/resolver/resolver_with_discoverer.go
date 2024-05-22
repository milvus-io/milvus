package resolver

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"go.uber.org/zap"
)

var _ Resolver = (*resolverWithDiscoverer)(nil)

// newResolverWithDiscoverer creates a new resolver with discoverer.
func newResolverWithDiscoverer(scheme string, d discoverer.Discoverer, retryInterval time.Duration) *resolverWithDiscoverer {
	ctx, cancel := context.WithCancel(context.Background())
	r := &resolverWithDiscoverer{
		ctx:             ctx,
		cancel:          cancel,
		logger:          log.With(zap.String("scheme", scheme)),
		registerCh:      make(chan *watchBasedGRPCResolver),
		finishCh:        make(chan struct{}),
		discoverer:      d,
		retryInterval:   retryInterval,
		latestStateCond: syncutil.NewContextCond(&sync.Mutex{}),
		latestState:     d.NewVersionedState(),
	}
	r.doDiscoverOnBackground()
	return r
}

// versionStateWithError is the versionedState with error.
type versionStateWithError struct {
	state VersionedState
	err   error
}

// resolverWithDiscoverer is the resolver for bkproxy service.
type resolverWithDiscoverer struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *log.MLogger

	registerCh chan *watchBasedGRPCResolver
	finishCh   chan struct{}

	discoverer    discoverer.Discoverer // the discoverer method for the bkproxy service
	retryInterval time.Duration

	latestStateCond *syncutil.ContextCond
	latestState     discoverer.VersionedState
}

// GetLatestState returns the latest state of the resolver.
func (r *resolverWithDiscoverer) GetLatestState() VersionedState {
	r.latestStateCond.L.Lock()
	state := r.latestState
	r.latestStateCond.L.Unlock()
	return state
}

// Watch watch the state change of the resolver.
func (r *resolverWithDiscoverer) Watch(ctx context.Context, cb func(VersionedState) error) error {
	state := r.GetLatestState()
	if err := cb(state); err != nil {
		return errors.Mark(err, ErrInterrupted)
	}
	version := state.Version
	for {
		if err := r.watchStateChange(ctx, version); err != nil {
			return errors.Mark(err, ErrCanceled)
		}
		state := r.GetLatestState()
		if err := cb(state); err != nil {
			return errors.Mark(err, ErrInterrupted)
		}
		version = state.Version
	}
}

// Close closes the resolver.
func (r *resolverWithDiscoverer) Close() {
	// Cancel underlying task and close the discovery service.
	r.cancel()
	<-r.finishCh
}

// watchStateChange block util the state is changed.
func (r *resolverWithDiscoverer) watchStateChange(ctx context.Context, version util.Version) error {
	r.latestStateCond.L.Lock()
	for version.Equal(r.latestState.Version) {
		if err := r.latestStateCond.Wait(ctx); err != nil {
			return err
		}
	}
	r.latestStateCond.L.Unlock()
	return nil
}

// RegisterNewWatcher registers a new grpc resolver.
// RegisterNewWatcher should always be call before Close.
func (r *resolverWithDiscoverer) RegisterNewWatcher(grpcResolver *watchBasedGRPCResolver) error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case r.registerCh <- grpcResolver:
		return nil
	}
}

func (r *resolverWithDiscoverer) doDiscoverOnBackground() {
	go r.doDiscover()
}

// doDiscover do the discovery on background.
func (r *resolverWithDiscoverer) doDiscover() {
	grpcResolvers := make(map[*watchBasedGRPCResolver]struct{}, 0)
	defer func() {
		// Check if all grpc resolver is stopped.
		for r := range grpcResolvers {
			if err := lifetime.IsWorking(r.State()); err == nil {
				r.logger.Warn("resolver is stopped before grpc watcher exist, maybe bug here")
				break
			}
		}
		r.logger.Info("resolver stopped")
		close(r.finishCh)
	}()

	for {
		if r.ctx.Err() != nil {
			// resolver stopped.
			return
		}
		ch := r.asyncDiscover(r.ctx)
		r.logger.Info("service discover task started, listening...")
	L:
		for {
			select {
			case watcher := <-r.registerCh:
				// New grpc resolver registered.
				// Trigger the latest state to the new grpc resolver.
				if err := watcher.Update(r.GetLatestState()); err != nil {
					r.logger.Info("resolver is closed, ignore the new grpc resolver", zap.Error(err))
				} else {
					grpcResolvers[watcher] = struct{}{}
				}
			case stateWithError := <-ch:
				if stateWithError.err != nil {
					r.logger.Warn("service discover break down", zap.Error(stateWithError.err), zap.Duration("retryInterval", r.retryInterval))
					time.Sleep(r.retryInterval)
					break L
				}

				// Check if the state is the newer.
				state := stateWithError.state
				latestState := r.GetLatestState()
				if !state.Version.GT(latestState.Version) {
					// Ignore the old version.
					r.logger.Info("service discover update, ignore old version", zap.Any("state", state))
					continue
				}
				// Update all grpc resolver.
				r.logger.Info("service discover update, update resolver", zap.Any("state", state), zap.Int("resolver_count", len(grpcResolvers)))
				for watcher := range grpcResolvers {
					// update operation do not block.
					if err := watcher.Update(state); err != nil {
						r.logger.Info("resolver is closed, unregister the resolver", zap.Error(err))
						delete(grpcResolvers, watcher)
					}
				}
				r.logger.Info("update resolver done")
				// Update the latest state and notify all resolver watcher should be executed after the all grpc watcher updated.
				r.latestStateCond.LockAndBroadcast()
				r.latestState = state
				r.latestStateCond.L.Unlock()
			}
		}
	}
}

// asyncDiscover is a non-blocking version of Discover.
func (r *resolverWithDiscoverer) asyncDiscover(ctx context.Context) <-chan versionStateWithError {
	ch := make(chan versionStateWithError)
	go func() {
		err := r.discoverer.Discover(ctx, func(vs discoverer.VersionedState) error {
			select {
			case ch <- versionStateWithError{
				state: vs,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		ch <- versionStateWithError{err: err}
	}()
	return ch
}
