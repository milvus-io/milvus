package api

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// DataViewSnapshot is the immutable DataView Manager output consumed by
// SnapshotBuilder and BalancePolicy.
type DataViewSnapshot struct {
	version           uint64
	collectionByID    map[int64]*viewpb.DataViewOfCollection
	shardsByCollVCh   map[int64]map[string]*viewpb.DataViewOfShard
	dataVersionByColl map[int64]qviews.DataVersion
	segments          SegmentSnapshot
}

func NewDataViewSnapshot(
	version uint64,
	collections []*viewpb.DataViewOfCollection,
	segments SegmentSnapshot,
) *DataViewSnapshot {
	if segments == nil {
		segments = emptySegmentSnapshot{}
	}
	snapshot := &DataViewSnapshot{
		version:           version,
		collectionByID:    make(map[int64]*viewpb.DataViewOfCollection, len(collections)),
		shardsByCollVCh:   make(map[int64]map[string]*viewpb.DataViewOfShard, len(collections)),
		dataVersionByColl: make(map[int64]qviews.DataVersion, len(collections)),
		segments:          segments,
	}
	for _, coll := range collections {
		if coll == nil {
			continue
		}
		collectionID := coll.GetCollectionId()
		snapshot.collectionByID[collectionID] = coll
		snapshot.dataVersionByColl[collectionID] = qviews.FromProtoDataVersion(coll.GetDataVersion())
		shards := make(map[string]*viewpb.DataViewOfShard, len(coll.GetShards()))
		for _, shard := range coll.GetShards() {
			if shard != nil {
				shards[shard.GetVchannel()] = shard
			}
		}
		snapshot.shardsByCollVCh[collectionID] = shards
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
	version, ok := s.dataVersionByColl[collectionID]
	return version, ok
}

func (s *DataViewSnapshot) ShardView(collectionID int64, vchannel string) (*viewpb.DataViewOfShard, bool) {
	if s == nil {
		return nil, false
	}
	shards := s.shardsByCollVCh[collectionID]
	if shards == nil {
		return nil, false
	}
	shard, ok := shards[vchannel]
	return shard, ok
}

func (s *DataViewSnapshot) RangeShards(collectionID int64, fn func(*viewpb.DataViewOfShard) bool) {
	if s == nil {
		return
	}
	coll := s.collectionByID[collectionID]
	if coll == nil {
		return
	}
	for _, shard := range coll.GetShards() {
		if !fn(shard) {
			return
		}
	}
}

func (s *DataViewSnapshot) SegmentInfo(segmentID int64) (*SegmentInfo, bool) {
	if s == nil || s.segments == nil {
		return nil, false
	}
	return s.segments.Get(segmentID)
}

// SegmentSnapshot is an immutable segment metadata lookup owned by the
// DataViewProvider.
type SegmentSnapshot interface {
	Get(segmentID int64) (*SegmentInfo, bool)
}

type emptySegmentSnapshot struct{}

func (emptySegmentSnapshot) Get(int64) (*SegmentInfo, bool) {
	return nil, false
}

// SegmentInfo carries the minimum per-segment metadata the Balancer needs.
// Index type, vector dimension, compression and other attributes are folded
// into DataCoord's MemSize estimate; the Balancer treats MemSize as opaque
// bytes.
type SegmentInfo struct {
	SegmentID   int64
	PartitionID int64
	// MemSize is the estimated in-memory footprint in bytes once this segment
	// is loaded onto a QueryNode. Primary load metric.
	MemSize int64
	// RowNum is the segment row count. Used as fallback when MemSize is zero
	// (e.g., DataCoord has not yet produced an estimate for a new segment).
	RowNum int64
}
