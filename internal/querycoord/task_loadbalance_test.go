package querycoord

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestLoadBalanceSegmentsTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	t.Run("Test LoadCollection", func(t *testing.T) {
		loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)

		err = queryCoord.scheduler.Enqueue(loadCollectionTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadCollectionTask, taskExpired)
	})

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	t.Run("Test LoadBalanceBySegmentID", func(t *testing.T) {
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_LoadBalance)
		loadBalanceTask := &loadBalanceTask{
			baseTask: baseTask,
			LoadBalanceRequest: &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
				},
				SourceNodeIDs:    []int64{node1.queryNodeID},
				SealedSegmentIDs: []UniqueID{defaultSegmentID},
			},
			broker:  queryCoord.broker,
			cluster: queryCoord.cluster,
			meta:    queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(loadBalanceTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadBalanceTask, taskExpired)
	})

	t.Run("Test LoadBalanceByNotExistSegmentID", func(t *testing.T) {
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_LoadBalance)
		loadBalanceTask := &loadBalanceTask{
			baseTask: baseTask,
			LoadBalanceRequest: &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
				},
				SourceNodeIDs:    []int64{node1.queryNodeID},
				SealedSegmentIDs: []UniqueID{defaultSegmentID + 100},
			},
			broker:  queryCoord.broker,
			cluster: queryCoord.cluster,
			meta:    queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(loadBalanceTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadBalanceTask, taskFailed)
	})

	t.Run("Test LoadBalanceByNode", func(t *testing.T) {
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_NodeDown)
		loadBalanceTask := &loadBalanceTask{
			baseTask: baseTask,
			LoadBalanceRequest: &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
				},
				SourceNodeIDs: []int64{node1.queryNodeID},
				CollectionID:  defaultCollectionID,
				BalanceReason: querypb.TriggerCondition_NodeDown,
			},
			broker:  queryCoord.broker,
			cluster: queryCoord.cluster,
			meta:    queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(loadBalanceTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadBalanceTask, taskExpired)
		assert.Equal(t, commonpb.ErrorCode_Success, loadBalanceTask.result.ErrorCode)
	})

	t.Run("Test LoadBalanceWithEmptySourceNode", func(t *testing.T) {
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_LoadBalance)
		loadBalanceTask := &loadBalanceTask{
			baseTask: baseTask,
			LoadBalanceRequest: &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
				},
			},
			broker:  queryCoord.broker,
			cluster: queryCoord.cluster,
			meta:    queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(loadBalanceTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadBalanceTask, taskFailed)
	})

	t.Run("Test LoadBalanceByNotExistNode", func(t *testing.T) {
		baseTask := newBaseTask(ctx, querypb.TriggerCondition_LoadBalance)
		loadBalanceTask := &loadBalanceTask{
			baseTask: baseTask,
			LoadBalanceRequest: &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
				},
				SourceNodeIDs: []int64{node1.queryNodeID + 100},
			},
			broker:  queryCoord.broker,
			cluster: queryCoord.cluster,
			meta:    queryCoord.meta,
		}
		err = queryCoord.scheduler.Enqueue(loadBalanceTask)
		assert.Nil(t, err)
		waitTaskFinalState(loadBalanceTask, taskFailed)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
		err = queryCoord.scheduler.processTask(releaseCollectionTask)
		assert.Nil(t, err)
	})

	node1.stop()
	node2.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalanceIndexedSegmentsTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)
	rootCoord := queryCoord.rootCoordClient.(*rootCoordMock)
	rootCoord.enableIndex = true

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)

	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	baseTask := newBaseTask(ctx, querypb.TriggerCondition_LoadBalance)
	loadBalanceTask := &loadBalanceTask{
		baseTask: baseTask,
		LoadBalanceRequest: &querypb.LoadBalanceRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadBalanceSegments,
			},
			SourceNodeIDs:    []int64{node1.queryNodeID},
			SealedSegmentIDs: []UniqueID{defaultSegmentID},
		},
		broker:  queryCoord.broker,
		cluster: queryCoord.cluster,
		meta:    queryCoord.meta,
	}
	err = queryCoord.scheduler.Enqueue(loadBalanceTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadBalanceTask, taskExpired)

	node1.stop()
	node2.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalanceIndexedSegmentsAfterNodeDown(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)
	defer queryCoord.Stop()

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)
	segments := queryCoord.meta.getSegmentInfosByNode(node1.queryNodeID)
	log.Debug("get segments by node",
		zap.Int64("nodeID", node1.queryNodeID),
		zap.Any("segments", segments))

	rootCoord := queryCoord.rootCoordClient.(*rootCoordMock)
	rootCoord.enableIndex = true

	node1.stop()

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)
	defer node2.stop()

	for {
		segments := queryCoord.meta.getSegmentInfosByNode(node1.queryNodeID)
		if len(segments) == 0 {
			break
		}
		log.Debug("node still has segments",
			zap.Int64("nodeID", node1.queryNodeID))
		time.Sleep(time.Second)
	}

	for {
		segments := queryCoord.meta.getSegmentInfosByNode(node2.queryNodeID)
		if len(segments) != 0 {
			log.Debug("get segments by node",
				zap.Int64("nodeID", node2.queryNodeID),
				zap.Any("segments", segments))
			break
		}
		log.Debug("node hasn't segments",
			zap.Int64("nodeID", node2.queryNodeID))
		time.Sleep(200 * time.Millisecond)
	}

	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalancePartitionAfterNodeDown(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)

	err = queryCoord.scheduler.Enqueue(loadPartitionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadPartitionTask, taskExpired)

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	removeNodeSession(node1.queryNodeID)
	for {
		if len(queryCoord.meta.getSegmentInfosByNode(node1.queryNodeID)) == 0 {
			break
		}
	}

	node2.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalanceAndReschedulSegmentTaskAfterNodeDown(t *testing.T) {
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

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2.loadSegment = returnFailedResult
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	removeNodeSession(node1.queryNodeID)
	for {
		_, activeTaskValues, err := queryCoord.scheduler.client.LoadWithPrefix(activeTaskPrefix)
		assert.Nil(t, err)
		if len(activeTaskValues) != 0 {
			break
		}
	}

	node3, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node3.queryNodeID)

	segmentInfos := queryCoord.meta.getSegmentInfosByNode(node3.queryNodeID)
	for _, segmentInfo := range segmentInfos {
		if nodeIncluded(node3.queryNodeID, segmentInfo.NodeIds) {
			break
		}
	}

	for {
		_, triggrtTaskValues, err := queryCoord.scheduler.client.LoadWithPrefix(triggerTaskPrefix)
		assert.Nil(t, err)
		if len(triggrtTaskValues) == 0 {
			break
		}
	}

	err = removeAllSession()
	assert.Nil(t, err)
}

func TestLoadBalanceAndRescheduleDmChannelTaskAfterNodeDown(t *testing.T) {
	defer removeAllSession()
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)
	defer queryCoord.Stop()

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	defer node1.stop()

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)

	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	defer node2.stop()
	node2.watchDmChannels = returnFailedResult
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	removeNodeSession(node1.queryNodeID)
	for {
		_, activeTaskValues, err := queryCoord.scheduler.client.LoadWithPrefix(activeTaskPrefix)
		assert.Nil(t, err)
		if len(activeTaskValues) != 0 {
			break
		}
	}

	node3, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	defer node3.stop()
	waitQueryNodeOnline(queryCoord.cluster, node3.queryNodeID)

	dmChannelInfos := queryCoord.meta.getDmChannelInfosByNodeID(node3.queryNodeID)
	for _, channelInfo := range dmChannelInfos {
		if channelInfo.NodeIDLoaded == node3.queryNodeID {
			break
		}
	}

	for {
		_, triggrtTaskValues, err := queryCoord.scheduler.client.LoadWithPrefix(triggerTaskPrefix)
		assert.Nil(t, err)
		if len(triggrtTaskValues) == 0 {
			break
		}
	}
}
