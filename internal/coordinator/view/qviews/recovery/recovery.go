package recovery

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// RecoveryStorage is the interface to manage the recovery info for query views.
type RecoveryStorage interface {
	// List returns all the query view of shards.
	List(ctx context.Context) ([]*viewpb.QueryViewOfShard, error)

	// Save saves the recovery infos into underlying persist storage.
	// The save operation should be performed atomically.
	// The save operation should fail if and only if the context canceled.
	Save(ctx context.Context, saved ...*viewpb.QueryViewOfShard) error
}
