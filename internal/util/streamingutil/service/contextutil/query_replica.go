package contextutil

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type queryReplicaKey struct{}

// WithQueryWALReplica routes the growing part of a shard plan to its MVCC source.
func WithQueryWALReplica(ctx context.Context, replica *types.PChannelInfoAssigned) context.Context {
	return context.WithValue(ctx, queryReplicaKey{}, replica)
}

func QueryWALReplica(ctx context.Context) *types.PChannelInfoAssigned {
	replica, _ := ctx.Value(queryReplicaKey{}).(*types.PChannelInfoAssigned)
	return replica
}
