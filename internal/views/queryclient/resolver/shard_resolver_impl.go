package resolver

import (
	"context"
)

var _ ShardResolver = (*ShardResolverImpl)(nil)

// ShardResolver resolves the shard topology of a collection.
type ShardResolver interface {
	// ResolveVChannels returns all vchannels of a collection, reflecting the
	// collection's shard layout as reported by the provider (the proxy's
	// GetCollection flow). Load state is not enforced here: an unloaded
	// collection still reports its vchannels. QueryViewClient confirms current
	// load state with Coordinator when the initialized replica directory is empty.
	ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error)
}

// CollectionVChannelProvider supplies the collection → vchannel mapping.
// It is implemented by the proxy via its GetCollection flow (metacache).
type CollectionVChannelProvider interface {
	// GetCollectionVChannels returns the vchannels of a collection.
	GetCollectionVChannels(ctx context.Context, collectionID int64) ([]string, error)
}

// NewShardResolverImpl creates a ShardResolverImpl.
// vchannels must not be nil.
func NewShardResolverImpl(vchannels CollectionVChannelProvider) *ShardResolverImpl {
	return &ShardResolverImpl{
		vchannels: vchannels,
	}
}

// ShardResolverImpl resolves shard topology from the collection → vchannel
// provider alone. It intentionally does not consult the channel assignment:
// vchannels are a static property of the collection. QueryViewClient selects
// explicit replicas from the SN directory and checks load state on empty results.
type ShardResolverImpl struct {
	vchannels CollectionVChannelProvider
}

func (t *ShardResolverImpl) ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error) {
	return t.vchannels.GetCollectionVChannels(ctx, collectionID)
}

// Close releases the resolver. It is a no-op: the resolver keeps no background
// goroutine or watcher.
func (t *ShardResolverImpl) Close() {}
