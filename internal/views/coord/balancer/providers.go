package balancer

import (
	"context"

	balancerapi "github.com/milvus-io/milvus/internal/views/coord/balancer/api"
)

// NodeProvider supplies the identity / health / capacity / resource group of
// every QueryNode visible to this Coord. Backed by Node Manager and resource
// group metadata at the facade layer.
//
// This pull interface is retained for compatibility. The Balancer runtime
// subscribes to cache.NodePublisher and reads the resident Cache.
type NodeProvider interface {
	// Snapshot returns an immutable node snapshot.
	Snapshot() *NodeSnapshot
}

// NodeChangedNotifier lets Balancer subscribe to node membership changes from
// its NodeProvider. The notifier must be non-blocking.
type NodeChangedNotifier interface {
	RegisterNodeChangedNotifier(notifier func())
}

type NodeInfo = balancerapi.NodeInfo

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
}

type (
	DataViewSnapshot   = balancerapi.DataViewSnapshot
	SegmentDataView    = balancerapi.SegmentDataView
	ShardDataView      = balancerapi.ShardDataView
	PartitionDataView  = balancerapi.PartitionDataView
	CollectionDataView = balancerapi.CollectionDataView
)

func NewDataViewSnapshot(
	version uint64,
	collections []*CollectionDataView,
) *DataViewSnapshot {
	return balancerapi.NewDataViewSnapshot(version, collections)
}
