package resolver

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// ShardReplicas is the discovery snapshot for one vchannel. The initial
// Query Client milestone consumes only PrimaryShardID.
type ShardReplicas struct {
	VChannel       string
	PrimaryShardID qviews.ShardID
}

// ShardResolver supplies collection and primary-shard topology. Its production
// implementation belongs to the later discovery integration change.
type ShardResolver interface {
	ResolveVChannels(ctx context.Context, collectionID int64) ([]string, error)
	ResolveShard(ctx context.Context, collectionID int64, vchannel string) (*ShardReplicas, error)
}
