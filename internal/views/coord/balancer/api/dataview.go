package api

import "github.com/milvus-io/milvus/internal/views/qviews"

// CollectionDataView is the native (non-proto) DataView of one collection.
type CollectionDataView struct {
	CollectionID int64
	TotalRows    int64
	LogicalSize  int64
	DataVersion  qviews.DataVersion
	Shards       []*ShardDataView
	shardIndex   map[string]*ShardDataView
	segments     map[int64]*SegmentDataView
}

// ShardDataView is one vchannel's DataView within a collection.
type ShardDataView struct {
	VChannel                             string
	GrowingTransformStartAfterTimetick   uint64
	CompactedTransformStartAfterTimetick uint64
	Partitions                           []*PartitionDataView
	TotalRows                            int64
	LogicalSize                          int64
	SegmentCount                         int
}

// PartitionDataView is one partition's segment list within a shard. The
// segments are embedded (not IDs plus an external lookup), so traversal
// paths read the published LogicalSize directly.
type PartitionDataView struct {
	PartitionID int64
	Segments    []*SegmentDataView
	TotalRows   int64
	LogicalSize int64
}

// SegmentDataView carries physical rows and the scheduling size frozen by the
// DataView manager. Neither statistic enters the viewpb wire format.
type SegmentDataView struct {
	SegmentID                   int64
	ManifestVersion             int64
	TransformStartAfterTimetick uint64
	PartitionID                 int64
	RowNum                      int64
	LogicalSize                 int64
}

// PrepareCollectionDataView takes ownership of an unpublished collection and
// builds its read indexes and summaries. Published values may be shared by
// listeners and cache readers, and must never be mutated by callers.
func PrepareCollectionDataView(coll *CollectionDataView) *CollectionDataView {
	if coll.shardIndex != nil {
		return coll
	}
	coll.TotalRows, coll.LogicalSize = 0, 0
	coll.shardIndex = make(map[string]*ShardDataView, len(coll.Shards))
	coll.segments = make(map[int64]*SegmentDataView)
	for _, shard := range coll.Shards {
		if shard == nil {
			continue
		}
		coll.shardIndex[shard.VChannel] = shard
		shard.TotalRows, shard.LogicalSize, shard.SegmentCount = 0, 0, 0
		for _, partition := range shard.Partitions {
			if partition == nil {
				continue
			}
			partition.TotalRows, partition.LogicalSize = 0, 0
			for _, segment := range partition.Segments {
				if segment == nil {
					continue
				}
				segment.PartitionID = partition.PartitionID
				coll.segments[segment.SegmentID] = segment
				partition.TotalRows += segment.RowNum
				shard.TotalRows += segment.RowNum
				coll.TotalRows += segment.RowNum
				partition.LogicalSize += segment.LogicalSize
				shard.LogicalSize += segment.LogicalSize
				coll.LogicalSize += segment.LogicalSize
				shard.SegmentCount++
			}
		}
	}
	return coll
}

func (c *CollectionDataView) Shard(vchannel string) *ShardDataView {
	return c.shardIndex[vchannel]
}

func (c *CollectionDataView) Segment(id int64) (*SegmentDataView, bool) {
	segment, ok := c.segments[id]
	return segment, ok
}

// DataViewListener runs synchronously at publication, including initial replay.
// A nil view means logical deletion. It must not re-enter its source or do I/O.
type DataViewListener func(collectionID int64, view *CollectionDataView)

// DataViewPublisher installs a listener and replays existing immutable entries
// without a missing-update window. The returned function unregisters it.
type DataViewPublisher interface {
	RegisterDataViewListener(DataViewListener) func()
}

// TransformStartAfterTimetick derives the QueryView subscription start from
// this immutable DataVersion's two persisted inputs.
func (s *ShardDataView) TransformStartAfterTimetick() uint64 {
	return min(s.GrowingTransformStartAfterTimetick, s.CompactedTransformStartAfterTimetick)
}
