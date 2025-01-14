package resolver

import (
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	// targets: milvus-session:///streamingcoord.
	SessionResolverScheme = "milvus-session"
	// targets: channel-assignment://external-grpc-client
	ChannelAssignmentResolverScheme = "channel-assignment"
)

var idAllocator = typeutil.NewIDAllocator()

// NewChannelAssignmentBuilder creates a new resolver builder.
func NewChannelAssignmentBuilder(w types.AssignmentDiscoverWatcher) Builder {
	return newBuilder(ChannelAssignmentResolverScheme, discoverer.NewChannelAssignmentDiscoverer(w))
}

// NewSessionBuilder creates a new resolver builder.
// Multiple sessions are allowed, use the role as prefix.
func NewSessionBuilder(c *clientv3.Client, role string) Builder {
	return newBuilder(SessionResolverScheme, discoverer.NewSessionDiscoverer(c, role, false, "2.4.0"))
}

// NewSessionExclusiveBuilder creates a new resolver builder with exclusive.
// Only one session is allowed, not use the prefix, only use the role directly.
func NewSessionExclusiveBuilder(c *clientv3.Client, role string) Builder {
	return newBuilder(SessionResolverScheme, discoverer.NewSessionDiscoverer(c, role, true, "2.4.0"))
}

// newBuilder creates a new resolver builder.
func newBuilder(scheme string, d discoverer.Discoverer) Builder {
	resolver := newResolverWithDiscoverer(scheme, d, 1*time.Second) // configurable.
	return &builderImpl{
		lifetime: typeutil.NewLifetime(),
		scheme:   scheme,
		resolver: resolver,
	}
}

// builderImpl implements resolver.Builder.
type builderImpl struct {
	lifetime *typeutil.Lifetime
	scheme   string
	resolver *resolverWithDiscoverer
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
//
// In our implementation, resolver.Target is ignored, because the resolver results is determined by the discoverer.
// Resolver is built when a Builder constructed.
// So build operation just register a new watcher into the existed resolver to share the resolver result.
func (b *builderImpl) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if !b.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, errors.New("builder is closed")
	}
	defer b.lifetime.Done()

	r := newWatchBasedGRPCResolver(cc, b.resolver.logger.With(zap.Int64("id", idAllocator.Allocate())))
	b.resolver.RegisterNewWatcher(r)
	return r, nil
}

func (b *builderImpl) Resolver() Resolver {
	return b.resolver
}

// Scheme returns the scheme supported by this resolver.  Scheme is defined
// at https://github.com/grpc/grpc/blob/master/doc/naming.md.  The returned
// string should not contain uppercase characters, as they will not match
// the parsed target's scheme as defined in RFC 3986.
func (b *builderImpl) Scheme() string {
	return b.scheme
}

// Close closes the builder also close the underlying resolver.
func (b *builderImpl) Close() {
	b.lifetime.SetState(typeutil.LifetimeStateStopped)
	b.lifetime.Wait()
	b.resolver.Close()
}
