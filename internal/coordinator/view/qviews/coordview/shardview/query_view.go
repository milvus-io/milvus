package shardview

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/events"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// newPreparingQueryView creates a new query view of shard at coord.
func newPreparingQueryView(qv *viewpb.QueryViewOfShard) *queryView {
	return &queryView{
		inner:      qv,
		syncRecord: newPreparingSyncRecord(qv),
	}
}

// newRecoveredQueryView creates a new query view of shard at coord.
func newRecoveredQueryView(qv *viewpb.QueryViewOfShard) *queryView {
	var syncRecord *workNodeSyncRecord
	switch qv.Meta.State {
	case viewpb.QueryViewState_QueryViewStateUp, viewpb.QueryViewState_QueryViewStateUnrecoverable:
	case viewpb.QueryViewState_QueryViewStatePreparing:
		syncRecord = newPreparingSyncRecord(qv)
	case viewpb.QueryViewState_QueryViewStateDown:
		syncRecord = newDownSyncRecord(qv)
	default:
		panic(fmt.Sprintf("invalid presisted query view state %s", qv.Meta.State.String()))
	}
	return &queryView{
		inner:                    qv,
		stateBeforeUnrecoverable: qviews.QueryViewStateNil,
		syncRecord:               syncRecord,
	}
}

// queryView is the representation of a query view of one shard on coord.
// The state can be following values:
//
// | State | Persisted | Description |
// |-------|-----------|-------------|
// | Preparing | true | The view is on-preparing, need to be synced to all worknode(querynode, streamingnode). |
// | Ready | false | The view is ready at all worknodes, the streaming node is ready or up, all querynode are ready. |
// | Up | true | The view is up at streaming node for serving. |
// | Down | true | The view is going to be down, need to be synced to streamingnode to make the view unserving |
// | Dropping | false | The view is down at streamingnode or unavailable, no new incoming search/query operation will use it |
// | Dropped | deleted | The view is cleared at all worknode, so the view can be cleared at coord |
// | Unrecoverable | true | The view is unrecoverable, the related worknode resource is still kept, want for new incoming preparing |
//
// The state transition can be described as a following state machine:
// Preparing -> Ready: If streamingnode is in ready or up state, all querynode are ready state.
// Ready -> Up: If the streamingnode is up state.
// Preparing, Ready, Up -> Unrecoverable: If any worknodes is in unrecoverable state.
// Up -> Down: If the view is going to be down.
// Down -> Dropping: If the view is dropped at streamingnode.
// Unrecoverable -> Down: The view is going to be down if we don't know the previous state or previous state is ready.
// Unrecoverable -> Dropping: The view is going to be dropped.
// Dropping -> Dropped: If all worknodes has been dropped.
type queryView struct {
	inner                    *viewpb.QueryViewOfShard
	stateBeforeUnrecoverable qviews.QueryViewState // Record the state before the view is unrecoverable.
	syncRecord               *workNodeSyncRecord   // syncRecord is a record map to make record sync opeartion of worknode, help to achieve the 2PC.
}

// ShardID returns the shard id of the query view.
func (qv *queryView) ShardID() qviews.ShardID {
	return qviews.NewShardIDFromQVMeta(qv.inner.Meta)
}

// State returns the state of the query view.
func (qv *queryView) State() qviews.QueryViewState {
	return qviews.QueryViewState(qv.inner.Meta.State)
}

// Version return the version of the query view.
func (qv *queryView) Version() qviews.QueryViewVersion {
	return qviews.FromProtoQueryViewVersion(qv.inner.Meta.Version)
}

// ProtoWaitForPersist returns the proto of the query view that need to be persisted.
func (qv *queryView) ProtoWaitForPersist() *viewpb.QueryViewOfShard {
	if qv.State() != qviews.QueryViewStatePreparing &&
		qv.State() != qviews.QueryViewStateUp &&
		qv.State() != qviews.QueryViewStateDown &&
		qv.State() != qviews.QueryViewStateUnrecoverable &&
		qv.State() != qviews.QueryViewStateDropped {
		panic("invalid state to persist")
	}
	return proto.Clone(qv.inner).(*viewpb.QueryViewOfShard)
}

// ApplyViewFromWorkNode applies the node state view to the coord query view from the worknode.
// Return true if the view need to be sync with other.
func (qv *queryView) ApplyViewFromWorkNode(incomingQV qviews.QueryViewAtWorkNode) {
	t := qviews.NewStateTransition(qv.State())
	defer qv.afterTransition(t)

	// The version must be matched.
	if !qv.Version().EQ(incomingQV.Version()) {
		panic("version of query view not match")
	}
	previousState := qv.State()

	switch previousState {
	case qviews.QueryViewStatePreparing:
		qv.applyNodeStateViewAtPreparing(incomingQV)
	case qviews.QueryViewStateReady:
		if incomingQV.State() == qviews.QueryViewStateUp {
			// If the state is ready, we only receive the up state from streaming node.
			// make a assertion here.
			_ = incomingQV.(*qviews.QueryViewAtStreamingNode)
			qv.upView()
		}
	case qviews.QueryViewStateDown:
		if incomingQV.State() == qviews.QueryViewStateDown {
			// If the state is down, we only receive the down state from streaming node.
			// make a assertion here.
			_ = incomingQV.(*qviews.QueryViewAtStreamingNode)
			qv.dropView()
		}
	case qviews.QueryViewStateDropping:
		if incomingQV.State() == qviews.QueryViewStateDropped {
			// if the state is down, we only receive the dropped state from all worknode.
			qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
			// If all nodes are ready, then transit the state into ready.
			if qv.syncRecord.IsAllReady() {
				qv.deleteView()
			}
		}
	case qviews.QueryViewStateUp, qviews.QueryViewStateDropped, qviews.QueryViewStateUnrecoverable:
		// Those state can only be changed by coord itself, so we do nothing here.
		// Also see `Down` and `DropView` interface.
	default:
		panic("invalid query view state")
	}
}

// EnterUnrecoverable transits the query view state into unrecoverable actively by the coord.
func (qv *queryView) EnterUnrecoverable() {
	t := qviews.NewStateTransition(qv.State())
	defer qv.afterTransition(t)

	if qv.State() != qviews.QueryViewStatePreparing && qv.State() != qviews.QueryViewStateReady {
		panic(fmt.Sprintf("invalid state transition from %s to %s", qv.State().String(), qviews.QueryViewStateUnrecoverable.String()))
	}
	qv.unrecoverableView()
}

// DealUnrecoverable deal the unrecoverable query views.
func (qv *queryView) DealUnrecoverable() {
	if qv.State() != qviews.QueryViewStateUnrecoverable {
		panic(fmt.Sprintf("state should be unrecoverable, but %s", qv.State().String()))
	}
	if qv.stateBeforeUnrecoverable == qviews.QueryViewStateReady || qv.stateBeforeUnrecoverable == qviews.QueryViewStateNil {
		// If the previous state is ready or the state we don't know,
		// the view may be up at streamingnode, so we need to perform a two-phase drop.
		qv.EnterDown()
	} else {
		// If the previous state is not ready, the view should never be up at streamingnode,
		// so drop it directly without a two-phase drop.
		t := qviews.NewStateTransition(qv.State())
		defer qv.afterTransition(t)
		qv.dropView()
	}
}

// Down transits the query view state into down if current query view is reach the end of lifetime.
func (qv *queryView) EnterDown() {
	t := qviews.NewStateTransition(qv.State())
	defer qv.afterTransition(t)

	if qv.State() != qviews.QueryViewStateUp && qv.State() != qviews.QueryViewStateUnrecoverable {
		panic(fmt.Sprintf("invalid state transition from %s to %s", qv.State().String(), qviews.QueryViewStateDown.String()))
	}
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDown)
	qv.syncRecord = newDownSyncRecord(qv.inner)
}

// ConsumePendingAckViews consumes the pending ack views.
func (qv *queryView) ConsumePendingAckViews() []syncer.QueryViewAtWorkNodeWithAck {
	if qv.syncRecord == nil {
		return nil
	}
	return qv.syncRecord.ConsumePendingAckViews()
}

// afterTransition records the state after the transition.
func (qv *queryView) afterTransition(transition qviews.StateTransition) {
	transition.Done(qv.State())
	if !transition.IsStateTransition() {
		return
	}
	events.Notify(events.EventStateTransition{
		Version:    qv.Version(),
		Transition: transition,
	})
}

// applyNodeStateViewAtPreparing applies the node state view to the coord query view when the view is preparing.
func (qv *queryView) applyNodeStateViewAtPreparing(incomingQV qviews.QueryViewAtWorkNode) {
	// Update the view of related node parts.
	switch incomingQV := incomingQV.(type) {
	case *qviews.QueryViewAtQueryNode:
		qv.applyQueryNodeView(incomingQV)
	case *qviews.QueryViewAtStreamingNode:
		qv.applyStreamingNodeView(incomingQV)
	default:
		panic("invalid incoming query view type")
	}

	// Do a state transition
	qv.transitWhenPreparing(incomingQV)
}

// applyQueryNodeView applies the query node view to the coord query view.
func (qv *queryView) applyQueryNodeView(viewAtQueryNode *qviews.QueryViewAtQueryNode) {
	for idx, node := range qv.inner.QueryNode {
		if node.NodeId == viewAtQueryNode.NodeID() {
			qv.inner.QueryNode[idx] = viewAtQueryNode.ViewOfQueryNode()
			return
		}
	}
	panic("query node not found in query view")
}

// applyStreamingNodeView applies the streaming node view to the coord query view.
func (qv *queryView) applyStreamingNodeView(viewAtStreamingNode *qviews.QueryViewAtStreamingNode) {
	qv.inner.StreamingNode = viewAtStreamingNode.ViewOfStreamingNode()
}

// transitWhenPreparing transits the query view state when it is preparing.
func (qv *queryView) transitWhenPreparing(incomingQV qviews.QueryViewAtWorkNode) {
	// Check the state of the query view.
	switch incomingQV.State() {
	case qviews.QueryViewStatePreparing:
		// Do nothing.
	case qviews.QueryViewStateReady, qviews.QueryViewStateUp: // The querynode is ready, the streaming node may be ready or up.
		qv.syncRecord.MarkNodeReady(incomingQV.WorkNode())
		// If all nodes are ready, then transit the state into ready.
		if qv.syncRecord.IsAllReady() {
			qv.readyView()
		}
	case qviews.QueryViewStateUnrecoverable:
		// any node is unrecoverable, then transit the state into unrecoverable.
		qv.unrecoverableView()
	default:
		panic("found inconsistent state")
	}
}

// readyView marks the query view as ready.
func (qv *queryView) readyView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateReady)
	qv.syncRecord = newReadySyncRecord(qv.inner)
}

// upView marks the query view as up.
func (qv *queryView) upView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUp)
	qv.syncRecord = nil
}

// unrecoverableView marks the query view as unrecoverable.
// When the state is transited into unrecoverable, we need to do a persist operation.
func (qv *queryView) unrecoverableView() {
	qv.stateBeforeUnrecoverable = qviews.QueryViewState(qv.inner.Meta.State)
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
	qv.syncRecord = nil
}

// dropView marks the query view as dropping.
// When the state is transited into dropping, we need to sync to all nodes to notify them drop these view.
func (qv *queryView) dropView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDropping)
	qv.syncRecord = newDroppingSyncRecord(qv.inner)
}

// deleteView marks the query view can be deleted.
// When the state is transited into dropped, we can delete the view from the recovery storage.
func (qv *queryView) deleteView() {
	qv.inner.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateDropped)
	qv.syncRecord = nil
}
