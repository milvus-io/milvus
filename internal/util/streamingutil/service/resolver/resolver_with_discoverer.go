package resolver

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ Resolver = (*resolverWithDiscoverer)(nil)

// newResolverWithDiscoverer creates a new resolver with discoverer.
func newResolverWithDiscoverer(d discoverer.Discoverer, retryInterval time.Duration, logger *log.MLogger) *resolverWithDiscoverer {
	r := &resolverWithDiscoverer{
		taskNotifier:    syncutil.NewAsyncTaskNotifier[struct{}](),
		registerCh:      make(chan *watchBasedGRPCResolver),
		discoverer:      d,
		retryInterval:   retryInterval,
		latestStateCond: syncutil.NewContextCond(&sync.Mutex{}),
		latestState:     d.NewVersionedState(),
	}
	r.SetLogger(logger)
	go r.doDiscover()
	return r
}

// versionStateWithError is the versionedState with error.
type versionStateWithError struct {
	state VersionedState
	err   error
}

// resolverWithDiscoverer is the resolver for bkproxy service.
type resolverWithDiscoverer struct {
	taskNotifier *syncutil.AsyncTaskNotifier[struct{}]
	log.Binder

	registerCh chan *watchBasedGRPCResolver

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
	r.taskNotifier.Cancel()
	r.taskNotifier.BlockUntilFinish()
}

// watchStateChange block util the state is changed.
func (r *resolverWithDiscoverer) watchStateChange(ctx context.Context, version typeutil.Version) error {
	r.latestStateCond.L.Lock()
	for version.EQ(r.latestState.Version) {
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
	case <-r.taskNotifier.Context().Done():
		return errors.Mark(r.taskNotifier.Context().Err(), ErrCanceled)
	case r.registerCh <- grpcResolver:
		return nil
	}
}

// doDiscover do the discovery on background.
func (r *resolverWithDiscoverer) doDiscover() {
	grpcResolvers := make(map[*watchBasedGRPCResolver]struct{}, 0)
	defer func() {
		// Check if all grpc resolver is stopped.
		for r := range grpcResolvers {
			if r.State() == typeutil.LifetimeStateWorking {
				r.Logger().Warn("resolver is stopped before grpc watcher exist, maybe bug here")
				break
			}
		}
		r.Logger().Info("resolver stopped")
		r.taskNotifier.Finish(struct{}{})
	}()

	for {
		ch := r.asyncDiscover(r.taskNotifier.Context())
		r.Logger().Info("service discover task started, listening...")
	L:
		for {
			select {
			case watcher := <-r.registerCh:
				watcher.Logger().Info("new grpc resolver registered")
				// New grpc resolver registered.
				// Trigger the latest state to the new grpc resolver.
				if err := watcher.Update(r.GetLatestState()); err != nil {
					r.Logger().Info("resolver is closed, ignore the new grpc resolver", zap.Error(err))
				} else {
					grpcResolvers[watcher] = struct{}{}
				}
			case stateWithError := <-ch:
				if stateWithError.err != nil {
					if r.taskNotifier.Context().Err() != nil {
						// resolver stopped.
						return
					}
					r.Logger().Warn("service discover break down", zap.Error(stateWithError.err), zap.Duration("retryInterval", r.retryInterval))
					time.Sleep(r.retryInterval)
					break L
				}

				// Check if the state is the newer.
				state := stateWithError.state
				latestState := r.GetLatestState()
				if !state.Version.GT(latestState.Version) {
					// Ignore the old version.
					r.Logger().Info("service discover update, ignore old version", zap.Any("state", state))
					continue
				}
				// Update all grpc resolver.
				r.Logger().Info("service discover update, update resolver", zap.Any("state", state), zap.Int("resolver_count", len(grpcResolvers)))
				for watcher := range grpcResolvers {
					// Update operation do not block.
					// Only return error if the resolver is closed, so just print a info log and delete the resolver.
					if err := watcher.Update(state); err != nil {
						// updateError is always context.Canceled.
						r.Logger().Info("resolver is closed, unregister the resolver", zap.NamedError("updateError", err))
						delete(grpcResolvers, watcher)
					}
				}
				r.Logger().Info("update resolver done")
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
	ch := make(chan versionStateWithError, 1)
	go func() {
		err := r.discoverer.Discover(ctx, func(vs discoverer.VersionedState) error {
			ch <- versionStateWithError{
				state: vs,
			}
			return nil
		})
		ch <- versionStateWithError{err: err}
	}()
	return ch
}
