package resolver

import (
	"context"
)

// Shard is one entry of the serving list of a collection.
type Shard struct {
	ShardSetEpoch uint64
	VChannel      string
	// Lineage is set for a split source shard that is still served: rows it
	// holds are deleted through the shards named here. Nil for every other shard.
	Lineage *ShardLineage
}

// ShardLineage is the lineage of a split source shard.
type ShardLineage struct {
	// VChannels are the shards the source was split into. They are entries of
	// the same serving list.
	VChannels []string
	// SwitchTimeTick is the tick of the split on the source shard, its last event.
	SwitchTimeTick uint64
}

// LineageShardResolver is implemented by a resolver that knows the serving
// generation of a collection: the list of shards together with the lineage of
// split source shards. A ShardResolver that does not implement it serves every
// shard on its own, as before.
type LineageShardResolver interface {
	ShardResolver
	// ResolveShards returns the serving list. The vchannels of every lineage
	// must be entries of the returned list.
	ResolveShards(ctx context.Context, collectionID int64) ([]Shard, error)
}

// ResolveShards returns the serving list of a collection from r. Without
// lineage support the list is the resolver's vchannels, none with lineage.
func ResolveShards(ctx context.Context, r ShardResolver, collectionID int64) ([]Shard, error) {
	if lineage, ok := r.(LineageShardResolver); ok {
		return lineage.ResolveShards(ctx, collectionID)
	}
	vchannels, err := r.ResolveVChannels(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	shards := make([]Shard, len(vchannels))
	for i, vchannel := range vchannels {
		shards[i] = Shard{VChannel: vchannel}
	}
	return shards, nil
}
