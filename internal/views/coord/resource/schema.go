package resource

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PreparationSchema selects Broadcaster's authorized target before consulting
// published metadata. The fallback is deliberately lazy: a pending target must
// remain usable while public metadata still exposes the preceding schema.
// Returned schemas are immutable for the caller.
func PreparationSchema(ctx context.Context, collectionID int64, published func(context.Context, int64) (*schemapb.CollectionSchema, error)) (*schemapb.CollectionSchema, error) {
	target, err := broadcast.GetPendingSchemaTarget(collectionID)
	if err != nil {
		return nil, err
	}
	if target != nil {
		return target.Schema, nil
	}
	return published(ctx, collectionID)
}

// PrepareSchema applies the acknowledged ALTER under its existing broadcast
// guard, and waits for every desired shard to serve the exact target schema.
// RootCoord must keep its published schema unchanged until this returns.
func (r *Resources) PrepareSchema(ctx context.Context, collectionID int64, published, target *schemapb.CollectionSchema, bound []*messagespb.LoadFieldConfig) error {
	if err := r.checkActive(ctx); err != nil {
		return err
	}
	if err := r.LoadConfigs.ApplySchema(ctx, collectionID, published, target, bound); err != nil {
		return err
	}
	if !r.LoadConfigs.Contains(collectionID) {
		return nil
	}
	r.Balancer.Trigger(balancer.TriggerScope{DirtyCollections: []int64{collectionID}})
	channels, err := r.VChannels(ctx, collectionID)
	if err != nil {
		return err
	}
	err = loadstatus.WaitAtSchema(ctx, r.LoadConfigs, r.ShardViews, r.Readiness, collectionID, channels, target.GetVersion())
	if errors.Is(err, merr.ErrCollectionNotLoaded) {
		return nil
	}
	return err
}
