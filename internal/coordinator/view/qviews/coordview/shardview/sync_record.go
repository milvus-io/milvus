package shardview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// newAllWorkNodeSyncRecord indicate to make sync opearation to all worknode.
func newAllWorkNodeSyncRecord(
	qv *viewpb.QueryViewOfShard,
	expectedState []qviews.QueryViewState, // The expected state for all worknode transit to.
	expectStateWhenNodeDown qviews.QueryViewState, // The expected returned state if the node is down.
) *workNodeSyncRecord {
	nodes := make(map[qviews.WorkNode]struct{}, len(qv.GetQueryNode())+1)
	pendingAckViews := make([]syncer.QueryViewAtWorkNodeWithAck, 0, len(qv.GetQueryNode())+1)
	nodes[qviews.NewStreamingNodeFromVChannel(qv.Meta.Vchannel)] = struct{}{}
	pendingAckViews = append(pendingAckViews, &queryViewAtWorkNodeWithAckImpl{
		QueryViewAtWorkNode:     qviews.NewQueryViewAtStreamingNode(qv.Meta, qv.StreamingNode),
		expectedStates:          expectedState,
		expectStateWhenNodeDown: expectStateWhenNodeDown,
	})
	for _, node := range qv.QueryNode {
		nodes[qviews.NewQueryNode(node.NodeId)] = struct{}{}
		pendingAckViews = append(pendingAckViews, &queryViewAtWorkNodeWithAckImpl{
			QueryViewAtWorkNode:     qviews.NewQueryViewAtQueryNode(qv.Meta, node),
			expectedStates:          expectedState,
			expectStateWhenNodeDown: expectStateWhenNodeDown,
		})
	}
	return &workNodeSyncRecord{
		nodes:           nodes,
		pendingAckViews: pendingAckViews,
	}
}

// newStreamingNodeSyncRecord indicate to make sync operation to related streaming node only.
func newStreamingNodeSyncRecord(
	qv *viewpb.QueryViewOfShard,
	expectedState []qviews.QueryViewState, // The expected state for all worknode transit to.
) *workNodeSyncRecord {
	return &workNodeSyncRecord{
		nodes: map[qviews.WorkNode]struct{}{qviews.NewStreamingNodeFromVChannel(qv.Meta.Vchannel): {}},
		pendingAckViews: []syncer.QueryViewAtWorkNodeWithAck{
			&queryViewAtWorkNodeWithAckImpl{
				QueryViewAtWorkNode:     qviews.NewQueryViewAtStreamingNode(qv.Meta, qv.StreamingNode),
				expectedStates:          expectedState,
				expectStateWhenNodeDown: qviews.QueryViewStateNil, // The streaming node is bind with vchannel, so it should never be down even the node is changed.
			},
		},
	}
}

// workNodeSyncRecord records the related node sync opeartion state of a query view.
type workNodeSyncRecord struct {
	nodes           map[qviews.WorkNode]struct{}
	pendingAckViews []syncer.QueryViewAtWorkNodeWithAck
}

// MarkNodeAcked marks the node sync state as acked.
func (r *workNodeSyncRecord) MarkNodeReady(node qviews.WorkNode) {
	delete(r.nodes, node)
}

// IsAllReady returns whether all nodes are ready.
func (r *workNodeSyncRecord) IsAllReady() bool {
	return len(r.nodes) == 0
}

// ConsumePendingAckViews returns the pending ack views.
func (r *workNodeSyncRecord) ConsumePendingAckViews() []syncer.QueryViewAtWorkNodeWithAck {
	views := r.pendingAckViews
	r.pendingAckViews = nil
	return views
}
