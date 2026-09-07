package balancer

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// dataViewSnapshotFromProto converts proto-shaped test DataViews into the
// native snapshot structure, merging the supplied per-segment footprint so
// tests can keep writing compact proto fixtures.
func dataViewSnapshotFromProto(
	collections []*viewpb.DataViewOfCollection,
	segments map[int64]*SegmentDataView,
) *DataViewSnapshot {
	native := make([]*CollectionDataView, 0, len(collections))
	for _, view := range collections {
		native = append(native, collectionDataViewFromProto(view, segments))
	}
	return NewDataViewSnapshot(1, native)
}

func collectionDataViewFromProto(
	view *viewpb.DataViewOfCollection,
	segments map[int64]*SegmentDataView,
) *CollectionDataView {
	if view == nil {
		return nil
	}
	coll := &CollectionDataView{
		CollectionID: view.GetCollectionId(),
		Shards:       make([]*ShardDataView, 0, len(view.GetShards())),
	}
	if dv := view.GetDataVersion(); dv != nil {
		coll.DataVersion = qviews.FromProtoDataVersion(dv)
	}
	for _, shard := range view.GetShards() {
		if shard == nil {
			continue
		}
		nativeShard := &ShardDataView{VChannel: shard.GetVchannel()}
		for _, partition := range shard.GetPartitions() {
			if partition == nil {
				continue
			}
			nativePartition := &PartitionDataView{PartitionID: partition.GetPartitionId()}
			for _, segmentID := range partition.GetSegmentIds() {
				info := segments[segmentID]
				if info == nil {
					info = &SegmentDataView{SegmentID: segmentID, PartitionID: partition.GetPartitionId()}
				}
				nativePartition.Segments = append(nativePartition.Segments, info)
			}
			nativeShard.Partitions = append(nativeShard.Partitions, nativePartition)
		}
		coll.Shards = append(coll.Shards, nativeShard)
	}
	return coll
}

// newMapSegmentSnapshot is retained as a thin alias so existing tests keep
// their "segments table" fixture shape; the native snapshot embeds the same
// footprint inline.
func newMapSegmentSnapshot(infos map[int64]*SegmentDataView) map[int64]*SegmentDataView {
	return infos
}
