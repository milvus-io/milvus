package balancer

import (
	"context"

	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// NodeProvider supplies the identity / health / capacity / resource group of
// every QueryNode visible to this Coord. Backed by Node Manager and resource
// group metadata at the facade layer.
//
// The SnapshotBuilder combines these infos with cross-shard-aggregated load
// (from ShardViewRegistry) to produce the final *BalanceNode in the snapshot.
type NodeProvider interface {
	// Snapshot returns an immutable node snapshot.
	Snapshot() *NodeSnapshot
}

// NodeChangedNotifier lets Balancer subscribe to node membership changes from
// its NodeProvider. The notifier must be non-blocking.
type NodeChangedNotifier interface {
	RegisterNodeChangedNotifier(notifier func())
}

// NodeInfo carries the QueryNode state provided by the coordinator-facing
// node view. Dynamic per-shard load is computed by the builder.
type NodeInfo struct {
	NodeID        int64
	Alive         bool
	Stopping      bool
	ResourceGroup string
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

// DataViewProvider supplies the immutable data-view snapshot consumed by the
// Balancer. The method name intentionally does not collide with
// dataview.Manager.Snapshot(ctx, collectionIDs).
type DataViewProvider interface {
	DataViewSnapshot(ctx context.Context) *DataViewSnapshot
	DataViewSnapshotForCollections(ctx context.Context, collectionIDs map[int64]struct{}) *DataViewSnapshot
	SegmentSnapshot(ctx context.Context, segmentIDs []int64) SegmentSnapshot
}

type (
	DataViewSnapshot = balancerapi.DataViewSnapshot
	SegmentInfo      = balancerapi.SegmentInfo
	SegmentSnapshot  = balancerapi.SegmentSnapshot
)

func NewDataViewSnapshot(
	version uint64,
	collections []*viewpb.DataViewOfCollection,
	segments SegmentSnapshot,
) *DataViewSnapshot {
	return balancerapi.NewDataViewSnapshot(version, collections, segments)
}
