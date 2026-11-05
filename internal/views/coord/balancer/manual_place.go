package balancer

import (
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PlacementScope is an index filter, not an operation policy. Zero means no
// restriction. Missing entries produce an empty snapshot for the callback.
type PlacementScope struct {
	CollectionID int64
	NodeID       int64
}

type ManualPlaceResult struct {
	Version qviews.QueryViewVersion
	Changed bool
	State   qviews.QueryViewState
	Err     error
}

// ManualPlace serializes planning, not waiting, with automatic reconciliation.
// The callback runs once and must not block, reenter the balancer, or retain its
// builders. Filtering belongs to the callback; retained builders must preserve
// their source data and metadata. Results identify exact accepted versions,
// including partial failure.
func (b *DefaultBalancer) ManualPlace(ctx context.Context, scope PlacementScope, modify func(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder) error) (map[qviews.ShardID]ManualPlaceResult, error) {
	if err := b.cache.WaitForReady(ctx); err != nil {
		return nil, err
	}
	results := make(map[qviews.ShardID]ManualPlaceResult)
	pending := make(map[qviews.ShardID]*coordview.Preparation)
	err := func() error {
		b.reconcileMu.Lock()
		defer b.reconcileMu.Unlock()
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		ids := b.manualShards(scope)
		builders := make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder, len(ids))
		originals := make(map[qviews.ShardID]*viewpb.QueryViewOfShard, len(ids))
		managers := make(map[qviews.ShardID]*coordview.ShardViewManager, len(ids))
		for _, id := range ids {
			manager := b.viewRegistry.Get(id)
			if manager == nil {
				continue
			}
			builder := manager.PlacementBuilder()
			if builder == nil {
				continue
			}
			original := builder.Build()
			managers[id], originals[id] = manager, original
			builders[id] = qviews.NewQueryViewAtCoordBuilderFromView(original)
		}
		if err := modify(builders); err != nil {
			return err
		}
		// A callback may filter the snapshot, but cannot submit a new shard
		// without a captured manager and source version.
		for id, builder := range builders {
			if builder != nil && originals[id] == nil {
				return merr.WrapErrServiceInternalMsg("manual placement has no source snapshot for shard %s", id.String())
			}
		}
		changed := make(map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder)
		for _, id := range ids {
			builder := builders[id]
			if builder == nil {
				continue
			}
			before, after := originals[id], builder.Build()
			if proto.Equal(before, after) {
				results[id] = ManualPlaceResult{Version: qviews.FromProtoQueryViewVersion(before.GetMeta().GetVersion())}
				continue
			}
			if err := validateManualPlacementData(before, after); err != nil {
				return err
			}
			// Detach all callback-owned memory before publishing to managers.
			changed[id] = qviews.NewQueryViewAtCoordBuilderFromView(after)
		}
		for _, id := range ids {
			builder := changed[id]
			if builder == nil {
				continue
			}
			before := originals[id]
			p, err := managers[id].AddManualPreparing(ctx, builder, qviews.FromProtoQueryViewVersion(before.GetMeta().GetVersion()))
			if err != nil {
				results[id] = ManualPlaceResult{Err: err}
				return err
			}
			pending[id] = p
			results[id] = ManualPlaceResult{Version: p.Version, Changed: true, State: qviews.QueryViewStatePreparing}
		}
		return nil
	}()
	// No lock is held here. Each completion retains the first outcome even if
	// subsequent handoffs already removed that version from the registry.
	for id, p := range pending {
		outcome := p.Wait(ctx)
		result := results[id]
		result.State, result.Err = outcome.State, outcome.Err
		results[id] = result
		if outcome.Err != nil {
			err = errors.CombineErrors(err, merr.Wrapf(outcome.Err, "manual placement shard %s version %v", id.String(), p.Version))
		}
	}
	return results, err
}

// manualShards only selects an indexed snapshot. Whether an empty or partial
// match is valid is entirely up to the operation callback.
func (b *DefaultBalancer) manualShards(scope PlacementScope) []qviews.ShardID {
	var ids []qviews.ShardID
	switch {
	case scope.NodeID != 0:
		ids = b.viewRegistry.NodeShards(scope.NodeID)
		if scope.CollectionID != 0 {
			collectionShards := make(map[qviews.ShardID]struct{})
			for _, id := range b.viewRegistry.CollectionShards(scope.CollectionID) {
				collectionShards[id] = struct{}{}
			}
			ids = slices.DeleteFunc(ids, func(id qviews.ShardID) bool {
				_, ok := collectionShards[id]
				return !ok
			})
		}
	case scope.CollectionID != 0:
		ids = b.viewRegistry.CollectionShards(scope.CollectionID)
	default:
		ids = b.viewRegistry.ShardIDs()
	}
	slices.SortFunc(ids, func(a, c qviews.ShardID) int {
		if a.ReplicaID != c.ReplicaID {
			return cmp.Compare(a.ReplicaID, c.ReplicaID)
		}
		return cmp.Compare(a.VChannel, c.VChannel)
	})
	return ids
}

// validateManualPlacementData allows placement changes only. Compare against the
// captured view, not the latest DataView: the source already encodes its selected
// partitions, schema and physical versions. Validate every changed shard before
// submitting any of them.
func validateManualPlacementData(before, after *viewpb.QueryViewOfShard) error {
	if !proto.Equal(before.GetMeta(), after.GetMeta()) || !proto.Equal(before.GetStreamingNode(), after.GetStreamingNode()) {
		return merr.WrapErrServiceInternalMsg("manual placement must preserve view metadata and streaming resources")
	}
	type member struct{ partition, manifest int64 }
	members := make(map[int64]member)
	for _, node := range before.GetQueryNode() {
		for _, partition := range node.GetPartitions() {
			for i, segment := range partition.GetSegmentIds() {
				var manifest int64
				if i < len(partition.GetSegmentManifestVersions()) {
					manifest = partition.GetSegmentManifestVersions()[i]
				}
				members[segment] = member{partition.GetPartitionId(), manifest}
			}
		}
	}
	for _, node := range after.GetQueryNode() {
		for _, partition := range node.GetPartitions() {
			for i, segment := range partition.GetSegmentIds() {
				var manifest int64
				if i < len(partition.GetSegmentManifestVersions()) {
					manifest = partition.GetSegmentManifestVersions()[i]
				}
				original, ok := members[segment]
				if !ok || original != (member{partition.GetPartitionId(), manifest}) {
					return merr.WrapErrServiceInternalMsg("manual placement changed or duplicated segment %d", segment)
				}
				delete(members, segment)
			}
		}
	}
	if len(members) != 0 {
		return merr.WrapErrServiceInternalMsg("manual placement removed segments")
	}
	return nil
}
