package resolver

import (
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ resolver.Resolver = (*watchBasedGRPCResolver)(nil)

// newWatchBasedGRPCResolver creates a new watch based grpc resolver.
func newWatchBasedGRPCResolver(cc resolver.ClientConn) *watchBasedGRPCResolver {
	return &watchBasedGRPCResolver{
		lifetime: typeutil.NewLifetime(),
		cc:       cc,
	}
}

// watchBasedGRPCResolver is a watch based grpc resolver.
type watchBasedGRPCResolver struct {
	lifetime *typeutil.Lifetime

	cc resolver.ClientConn
	log.Binder
}

// ResolveNow will be called by gRPC to try to resolve the target name
// again. It's just a hint, resolver can ignore this if it's not necessary.
//
// It could be called multiple times concurrently.
func (r *watchBasedGRPCResolver) ResolveNow(_ resolver.ResolveNowOptions) {
}

// Close closes the resolver.
// Do nothing.
func (r *watchBasedGRPCResolver) Close() {
	r.lifetime.SetState(typeutil.LifetimeStateStopped)
	r.lifetime.Wait()
}

// Update updates the state of the resolver.
// Return error if the resolver is closed.
func (r *watchBasedGRPCResolver) Update(state VersionedState) error {
	if !r.lifetime.Add(typeutil.LifetimeStateWorking) {
		return errors.New("resolver is closed")
	}
	defer r.lifetime.Done()

	if err := r.cc.UpdateState(state.State); err != nil {
		// watch based resolver could ignore the error, just log and return nil
		r.Logger().Warn("fail to update resolver state", zap.Any("state", state.State), zap.Error(err))
		return nil
	}
	r.Logger().Info("update resolver state success", zap.Any("state", state.State))
	return nil
}

// State returns the state of the resolver.
func (r *watchBasedGRPCResolver) State() typeutil.LifetimeState {
	return r.lifetime.GetState()
}
