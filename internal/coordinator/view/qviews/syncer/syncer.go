package syncer

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
)

// CoordSyncer is the interface to sync the query view at coord into the worknode.
type CoordSyncer interface {
	// Sync sync up a group of query view from the coord to related worknode.
	// Sync do a async operation at background.
	// It make sure that the query view will be sent to the work node (except node gone).
	// Once the target work node is gone, a work node view event created from `GenerateViewWhenNodeDown` will be returned from the receiver.
	Sync(g SyncGroup)

	// Receiver returns the channel to receive the query view coming from the worknode.
	Receiver() <-chan []events.SyncerEvent

	// Close closes the syncer.
	Close()
}

// SyncGroup is the group of query view to sync.
// A sync group will be sent as one message to the worknode to make an atomic apply operation at first time.
// But the resync operation at background doesn't promise the atomicity.
type SyncGroup struct {
	Views map[qviews.WorkNode][]QueryViewAtWorkNodeWithAck
}

// AddView adds a query view to the sync group.
func (g *SyncGroup) AddView(view QueryViewAtWorkNodeWithAck) {
	if g.Views == nil {
		g.Views = make(map[qviews.WorkNode][]QueryViewAtWorkNodeWithAck)
	}
	g.Views[view.WorkNode()] = append(g.Views[view.WorkNode()], view)
}

// QueryViewAtWorkNodeWithAck is the interface to interact with CoordSyncer.
type QueryViewAtWorkNodeWithAck interface {
	qviews.QueryViewAtWorkNode

	// GenerateViewWhenNodeDown is used to generate a query view if the target node is gone.
	GenerateViewWhenNodeDown() qviews.QueryViewAtWorkNode

	// ObserveSyncerEvent is used to observe the syncer event.
	// Every sync event sent by CoordSyncer with same (shardID, nodeID, version) can be observed by these method since it join the syncer.
	// The ObserveSyncerEvent need to track the events
	// Onces the view is acknowledged, the method should return true，
	// then the view can be removed from the syncer.
	// Otherwise, the view will be kept in the syncer, and resync if the underlying stream broken.
	ObserveSyncerEvent(event events.SyncerEventAck) (isAck bool)
}

// NodeReporter is the interface to sync the query view at worknode into the coord.
type NodeReporter[ViewAtWorkNode qviews.QueryViewAtWorkNode, BalanceInfoAtWorkNode qviews.BalanceAttrAtWorkNode] interface {
	// Report reports the query view state transition and balance info to the coord.
	// Report do a async operation at background.
	// It doesn't promise the report result will be sent to the coord.
	Report(views []ViewAtWorkNode, balanceInfo BalanceInfoAtWorkNode)

	Receiver() <-chan events.SyncerEvent
}
