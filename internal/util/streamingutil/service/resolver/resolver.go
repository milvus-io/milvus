package resolver

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
)

type VersionedState = discoverer.VersionedState

var (
	ErrCanceled    = errors.New("canceled")
	ErrInterrupted = errors.New("interrupted")
)

// Builder is the interface for the grpc resolver builder.
// It owns a Resolver instance and build grpc.Resolver from it.
type Builder interface {
	resolver.Builder

	// Resolver returns the underlying resolver instance.
	Resolver() Resolver

	// Close the builder, release the underlying resolver instance.
	Close()
}

// Resolver is the interface for the service discovery in grpc.
// Allow the user to get the grpc service discovery results and watch the changes.
// Not all changes can be arrived by these api, only the newest state is guaranteed.
type Resolver interface {
	// GetLatestState returns the latest state of the resolver.
	// The returned state should be read only, applied any change to it will cause data race.
	GetLatestState(ctx context.Context) (VersionedState, error)

	// Watch watch the state change of the resolver.
	// cb will be called with latest state after call, and will be called with new state when state changed.
	// version may be skipped if the state is changed too fast, and latest version can be seen by cb.
	// Watch is keep running until ctx is canceled or cb first return error.
	// - Return error with ErrCanceled mark when ctx is canceled.
	// - Return error with ErrInterrupted when cb returns.
	Watch(ctx context.Context, cb func(VersionedState) error) error
}
