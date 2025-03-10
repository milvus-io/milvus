package shardview

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// newPreparingSyncRecord creates a new sync record for preparing query view.
func newPreparingSyncRecord(qv *viewpb.QueryViewOfShard) *workNodeSyncRecord {
	if qv.Meta.State != viewpb.QueryViewState_QueryViewStatePreparing {
		panic(fmt.Sprintf("invalid query view state: %s", qv.Meta.State.String()))
	}
	return newAllWorkNodeSyncRecord(qv, []qviews.QueryViewState{
		qviews.QueryViewStateReady,
		qviews.QueryViewStateUnrecoverable,
	}, qviews.QueryViewStateUnrecoverable)
}

// newDroppingSyncRecord creates a new sync record for dropping query view.
func newDroppingSyncRecord(qv *viewpb.QueryViewOfShard) *workNodeSyncRecord {
	if qv.Meta.State != viewpb.QueryViewState_QueryViewStateDropping {
		panic(fmt.Sprintf("invalid query view state: %s", qv.Meta.State.String()))
	}
	return newAllWorkNodeSyncRecord(qv, []qviews.QueryViewState{qviews.QueryViewStateDropped}, qviews.QueryViewStateDropped)
}

// newDownSyncRecord creates a new sync record for down query view.
func newDownSyncRecord(qv *viewpb.QueryViewOfShard) *workNodeSyncRecord {
	if qv.Meta.State != viewpb.QueryViewState_QueryViewStateDown {
		panic(fmt.Sprintf("invalid query view state: %s", qv.Meta.State.String()))
	}
	return newStreamingNodeSyncRecord(qv, []qviews.QueryViewState{qviews.QueryViewStateDown})
}

// newReadySyncRecord creates a new sync record for ready query view.
func newReadySyncRecord(qv *viewpb.QueryViewOfShard) *workNodeSyncRecord {
	if qv.Meta.State != viewpb.QueryViewState_QueryViewStateReady {
		panic(fmt.Sprintf("invalid query view state: %s", qv.Meta.State.String()))
	}
	return newStreamingNodeSyncRecord(qv, []qviews.QueryViewState{qviews.QueryViewStateUp})
}
