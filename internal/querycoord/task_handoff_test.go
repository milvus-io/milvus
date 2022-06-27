package querycoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

func Test_handoffSegmentFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	node1.loadSegment = returnFailedResult

	infos := queryCoord.meta.showSegmentInfos(defaultCollectionID, nil)
	assert.NotEqual(t, 0, len(infos))
	segmentID := defaultSegmentID + 4
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_Handoff)

	segmentInfo := &querypb.SegmentInfo{
		SegmentID:    segmentID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID + 2,
		SegmentState: commonpb.SegmentState_Sealed,
	}
	handoffReq := &querypb.HandoffSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_HandoffSegments,
		},
		SegmentInfos: []*querypb.SegmentInfo{segmentInfo},
	}
	handoffTask := &handoffTask{
		baseTask:               baseTask,
		HandoffSegmentsRequest: handoffReq,
		broker:                 queryCoord.broker,
		cluster:                queryCoord.cluster,
		meta:                   queryCoord.meta,
	}
	err = queryCoord.scheduler.Enqueue(handoffTask)
	assert.Nil(t, err)

	waitTaskFinalState(handoffTask, taskFailed)

	node1.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}
