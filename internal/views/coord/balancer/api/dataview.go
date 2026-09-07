package api

import "github.com/milvus-io/milvus/internal/views/qviews"

// DataViewSnapshot is the immutable DataView Manager output consumed by
// SnapshotBuilder and BalancePolicy. It is a native in-memory structure that
// is decoupled from the viewpb wire format: every segment of every included
// DataView carries its RowNum/MemSize inline, so the Balancer never needs a
// separate segment-metadata lookup during computation.
type DataViewSnapshot struct {
	version     uint64
	collections map[int64]*CollectionDataView
	segments    map[int64]*SegmentDataView
}

// CollectionDataView is the native (non-proto) DataView of one collection.
type CollectionDataView struct {
	CollectionID int64
	DataVersion  qviews.DataVersion
	Shards       []*ShardDataView
	shardIndex   map[string]*ShardDataView
}

// ShardDataView is one vchannel's DataView within a collection.
type ShardDataView struct {
	VChannel   string
	Partitions []*PartitionDataView
}

// PartitionDataView is one partition's segment list within a shard. The
// segments are embedded (not IDs plus an external lookup), so traversal
// paths read RowNum/MemSize directly.
type PartitionDataView struct {
	PartitionID int64
	Segments    []*SegmentDataView
}

// SegmentDataView carries the per-segment metadata the Balancer needs. RowNum
// and MemSize are maintained by the DataView manager and never enter the
// viewpb wire format.
type SegmentDataView struct {
	SegmentID   int64
	PartitionID int64
	RowNum      int64
	MemSize     int64
}

// NewDataViewSnapshot builds an immutable snapshot from the supplied native
// collection DataViews, indexing shards by vchannel and segments by ID for
// O(1) lookups.
func NewDataViewSnapshot(version uint64, collections []*CollectionDataView) *DataViewSnapshot {
	snapshot := &DataViewSnapshot{
		version:     version,
		collections: make(map[int64]*CollectionDataView, len(collections)),
		segments:    make(map[int64]*SegmentDataView),
	}
	for _, coll := range collections {
		if coll == nil {
			continue
		}
		snapshot.collections[coll.CollectionID] = coll
		coll.shardIndex = make(map[string]*ShardDataView, len(coll.Shards))
		for _, shard := range coll.Shards {
			if shard == nil {
				continue
			}
			coll.shardIndex[shard.VChannel] = shard
			for _, partition := range shard.Partitions {
				if partition == nil {
					continue
				}
				for _, segment := range partition.Segments {
					if segment == nil {
						continue
					}
					segment.PartitionID = partition.PartitionID
					snapshot.segments[segment.SegmentID] = segment
				}
			}
		}
	}
	return snapshot
}

func (s *DataViewSnapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

func (s *DataViewSnapshot) DataVersion(collectionID int64) (qviews.DataVersion, bool) {
	if s == nil {
		return qviews.DataVersion{}, false
	}
	coll := s.collections[collectionID]
	if coll == nil {
		return qviews.DataVersion{}, false
	}
	return coll.DataVersion, true
}

func (s *DataViewSnapshot) ShardView(collectionID int64, vchannel string) (*ShardDataView, bool) {
	if s == nil {
		return nil, false
	}
	coll := s.collections[collectionID]
	if coll == nil {
		return nil, false
	}
	shard, ok := coll.shardIndex[vchannel]
	return shard, ok
}

func (s *DataViewSnapshot) RangeShards(collectionID int64, fn func(*ShardDataView) bool) {
	if s == nil {
		return
	}
	coll := s.collections[collectionID]
	if coll == nil {
		return
	}
	for _, shard := range coll.Shards {
		if !fn(shard) {
			return
		}
	}
}

func (s *DataViewSnapshot) SegmentInfo(segmentID int64) (*SegmentDataView, bool) {
	if s == nil {
		return nil, false
	}
	segment, ok := s.segments[segmentID]
	return segment, ok
}
