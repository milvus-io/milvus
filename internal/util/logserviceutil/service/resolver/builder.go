package resolver

import (
	"errors"
	"time"

	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

const (
	// targets: milvus-session:///logcoord.
	SessionResolverScheme = "milvus-session"
	// targets: channel-assignment://external-grpc-client
	ChannelAssignmentResolverScheme = "channel-assignment"
)

var idAllocator = util.NewIDAllocator()

// NewChannelAssignmentBuilder creates a new resolver builder.
func NewChannelAssignmentBuilder(w discoverer.AssignmentDiscoverWatcher) Builder {
	return newBuilder(ChannelAssignmentResolverScheme, discoverer.NewChannelAssignmentDiscoverer(w))
}

// NewSessionBuilder creates a new resolver builder.
func NewSessionBuilder(c *clientv3.Client, role string) Builder {
	return newBuilder(SessionResolverScheme, discoverer.NewSessionDiscoverer(c, role, "2.3.0"))
}

// newBuilder creates a new resolver builder.
func newBuilder(scheme string, d discoverer.Discoverer) Builder {
	resolver := newResolverWithDiscoverer(scheme, d, 1*time.Second) // configurable.
	return &builderImpl{
		lifetime: lifetime.NewLifetime(lifetime.Working),
		scheme:   scheme,
		resolver: resolver,
	}
}

// builderImpl implements resolver.Builder.
type builderImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	scheme   string
	resolver *resolverWithDiscoverer
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (b *builderImpl) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if err := b.lifetime.Add(lifetime.IsWorking); err != nil {
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

func (b *builderImpl) Close() {
	b.lifetime.SetState(lifetime.Stopped)
	b.lifetime.Wait()
	b.lifetime.Close()
	b.resolver.Close()
}
