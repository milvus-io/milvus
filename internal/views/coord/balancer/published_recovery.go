package balancer

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// publishedRecoveryBase reuses ordinary placement policy with a retained read
// obligation instead of the target schema's newer materialized membership.
type publishedRecoveryBase struct {
	collectionID    int64
	shardID         qviews.ShardID
	dataVersion     qviews.DataVersion
	schemaVersion   int32
	loadInfoVersion uint64
	shard           *ShardDataView
}

func newPublishedRecoveryBase(id qviews.ShardID, recovery *coordview.PublishedRecovery) *publishedRecoveryBase {
	meta := recovery.View.GetMeta()
	shard := &ShardDataView{
		VChannel:                             id.VChannel,
		GrowingTransformStartAfterTimetick:   meta.GetTransformStartAfterTimetick(),
		CompactedTransformStartAfterTimetick: meta.GetTransformStartAfterTimetick(),
	}
	partitions := make(map[int64]*PartitionDataView)
	for _, node := range recovery.View.GetQueryNode() {
		for _, part := range node.GetPartitions() {
			partition := partitions[part.GetPartitionId()]
			if partition == nil {
				partition = &PartitionDataView{PartitionID: part.GetPartitionId()}
				partitions[part.GetPartitionId()] = partition
				shard.Partitions = append(shard.Partitions, partition)
			}
			for i, segmentID := range part.GetSegmentIds() {
				segment := &SegmentDataView{SegmentID: segmentID, LogicalSize: recovery.Sizes[segmentID]}
				if i < len(part.GetSegmentManifestVersions()) {
					segment.ManifestVersion = part.GetSegmentManifestVersions()[i]
				}
				partition.Segments = append(partition.Segments, segment)
			}
		}
	}
	api.PrepareCollectionDataView(&CollectionDataView{Shards: []*ShardDataView{shard}})
	return &publishedRecoveryBase{
		collectionID: meta.GetCollectionId(), shardID: id,
		dataVersion:   qviews.FromProtoDataVersion(meta.GetVersion().GetDataVersion()),
		schemaVersion: meta.GetSchemaVersion(), loadInfoVersion: meta.GetLoadInfoVersion(), shard: shard,
	}
}

func recoveryPlacementEligible(p *planningContext, id qviews.ShardID, view *viewpb.QueryViewOfShard) bool {
	cfg := p.ConfigForShard(id)
	for _, node := range view.GetQueryNode() {
		if !eligibleShardNode(p, cfg, id, node.GetNodeId()) {
			return false
		}
	}
	return true
}

// A target preparing view must not starve a published schema whose QN was lost.
// Each schema has its own preparation slot; publication retires the old slot.
func (b *DefaultBalancer) recoverPublishedSchemas(ctx context.Context, p *planningContext, dirty []qviews.ShardID) error {
	var result error
	for _, id := range dirty {
		cfg := p.ConfigForShard(id)
		if cfg == nil {
			continue
		}
		manager := b.viewRegistry.Get(id)
		if manager == nil {
			continue
		}
		recovery := manager.PublishedRecovery(p.SchemaVersion(cfg.CollectionID))
		if recovery == nil {
			continue
		}
		if recovery.View.GetMeta().GetState() == viewpb.QueryViewState_QueryViewStateUp && recoveryPlacementEligible(p, id, recovery.View) {
			continue
		}
		if recovery.Preparing != nil && recoveryPlacementEligible(p, id, recovery.Preparing) {
			continue
		}
		retained := *p
		retained.recovery = newPublishedRecoveryBase(id, recovery)
		baseRows := withoutRows(initialProjectedRows(p.NodesMap()), currentShardRows(p, id))
		allocation := allocate(&retained, id, baseRows, false)
		var err error
		if allocation == nil {
			err = merr.WrapErrServiceUnavailableMsg("published schema placement for shard %s is not ready", id.String())
		} else {
			source := qviews.FromProtoQueryViewVersion(recovery.View.GetMeta().GetVersion())
			err = manager.AddPublishedRecovery(ctx, allocation.builder, source)
		}
		if err != nil {
			result = errors.CombineErrors(result, err)
			b.queue.addRetry(TriggerScope{DirtyShards: []qviews.ShardID{id}})
		}
	}
	return result
}
