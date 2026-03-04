package balancer

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// NodeProvider supplies the identity / health / capacity of every QueryNode
// visible to this Coord. Backed by Node Manager + Replica Manager at the
// facade layer.
//
// The SnapshotBuilder combines these infos with cross-shard-aggregated load
// (from ShardViewRegistry) to produce the final *BalanceNode in the snapshot.
type NodeProvider interface {
	// Snapshot returns an immutable node snapshot.
	Snapshot() *NodeSnapshot
}

// NodeInfo carries the static portion of a QueryNode's state — identity,
// health, and capacity. Dynamic per-shard load is computed by the builder.
type NodeInfo struct {
	NodeID         int64
	Alive          bool
	Stopping       bool
	ResourceGroup  string
	MemoryCapacity int64
	// MemoryUsage is the last value reported by the node over SyncResponse.
	MemoryUsage int64
}

// NodeSnapshot is a provider-owned immutable node view.
type NodeSnapshot struct {
	version uint64
	infos   map[int64]*NodeInfo
}

func NewNodeSnapshot(version uint64, infos map[int64]*NodeInfo) *NodeSnapshot {
	return &NodeSnapshot{version: version, infos: infos}
}

func (s *NodeSnapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

func (s *NodeSnapshot) Range(fn func(int64, *NodeInfo) bool) {
	if s == nil {
		return
	}
	for id, info := range s.infos {
		if !fn(id, info) {
			return
		}
	}
}

// DataViewProvider supplies per-collection storage views and per-segment
// metadata. Segment lookup is intentionally part of the data-view snapshot so
// Balancer does not materialize a million-entry segment map per Build.
type DataViewProvider interface {
	// Snapshot returns an immutable data view + segment metadata snapshot.
	Snapshot() *DataViewSnapshot
}

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
		segments = EmptySegmentSnapshot{}
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

type EmptySegmentSnapshot struct{}

func (EmptySegmentSnapshot) Get(int64) (*SegmentInfo, bool) {
	return nil, false
}
