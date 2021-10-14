// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package querycoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func genLoadCollectionTask(ctx context.Context, queryCoord *QueryCoord) *LoadCollectionTask {
	req := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	loadCollectionTask := &LoadCollectionTask{
		BaseTask:              baseTask,
		LoadCollectionRequest: req,
		rootCoord:             queryCoord.rootCoordClient,
		dataCoord:             queryCoord.dataCoordClient,
		cluster:               queryCoord.cluster,
		meta:                  queryCoord.meta,
	}
	return loadCollectionTask
}

func genLoadPartitionTask(ctx context.Context, queryCoord *QueryCoord) *LoadPartitionTask {
	req := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	loadPartitionTask := &LoadPartitionTask{
		BaseTask:              baseTask,
		LoadPartitionsRequest: req,
		dataCoord:             queryCoord.dataCoordClient,
		cluster:               queryCoord.cluster,
		meta:                  queryCoord.meta,
	}
	return loadPartitionTask
}

func genReleaseCollectionTask(ctx context.Context, queryCoord *QueryCoord) *ReleaseCollectionTask {
	req := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	releaseCollectionTask := &ReleaseCollectionTask{
		BaseTask:                 baseTask,
		ReleaseCollectionRequest: req,
		rootCoord:                queryCoord.rootCoordClient,
		cluster:                  queryCoord.cluster,
		meta:                     queryCoord.meta,
	}

	return releaseCollectionTask
}

func genReleasePartitionTask(ctx context.Context, queryCoord *QueryCoord) *ReleasePartitionTask {
	req := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	releasePartitionTask := &ReleasePartitionTask{
		BaseTask:                 baseTask,
		ReleasePartitionsRequest: req,
		cluster:                  queryCoord.cluster,
	}

	return releasePartitionTask
}

func genReleaseSegmentTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *ReleaseSegmentTask {
	req := &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseSegments,
		},
		NodeID:       nodeID,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		SegmentIDs:   []UniqueID{defaultSegmentID},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	releaseSegmentTask := &ReleaseSegmentTask{
		BaseTask:               baseTask,
		ReleaseSegmentsRequest: req,
		cluster:                queryCoord.cluster,
	}
	return releaseSegmentTask
}

func genWatchDmChannelTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *WatchDmChannelTask {
	schema := genCollectionSchema(defaultCollectionID, false)
	vChannelInfo := &datapb.VchannelInfo{
		CollectionID: defaultCollectionID,
		ChannelName:  "testDmChannel",
	}
	req := &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDmChannels,
		},
		NodeID:       nodeID,
		CollectionID: defaultCollectionID,
		PartitionID:  defaultPartitionID,
		Schema:       schema,
		Infos:        []*datapb.VchannelInfo{vChannelInfo},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	baseTask.taskID = 100
	watchDmChannelTask := &WatchDmChannelTask{
		BaseTask:               baseTask,
		WatchDmChannelsRequest: req,
		cluster:                queryCoord.cluster,
		meta:                   queryCoord.meta,
		excludeNodeIDs:         []int64{},
	}

	parentReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	}
	baseParentTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	baseParentTask.taskID = 10
	parentTask := &LoadCollectionTask{
		BaseTask:              baseParentTask,
		LoadCollectionRequest: parentReq,
		rootCoord:             queryCoord.rootCoordClient,
		dataCoord:             queryCoord.dataCoordClient,
		meta:                  queryCoord.meta,
		cluster:               queryCoord.cluster,
	}
	parentTask.setState(taskDone)
	parentTask.setResultInfo(nil)
	parentTask.addChildTask(watchDmChannelTask)
	watchDmChannelTask.setParentTask(parentTask)

	queryCoord.meta.addCollection(defaultCollectionID, schema)
	return watchDmChannelTask
}
func genLoadSegmentTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *LoadSegmentTask {
	schema := genCollectionSchema(defaultCollectionID, false)
	segmentInfo := &querypb.SegmentLoadInfo{
		SegmentID:    defaultSegmentID,
		PartitionID:  defaultPartitionID,
		CollectionID: defaultCollectionID,
	}
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
		},
		NodeID: nodeID,
		Schema: schema,
		Infos:  []*querypb.SegmentLoadInfo{segmentInfo},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	baseTask.taskID = 100
	loadSegmentTask := &LoadSegmentTask{
		BaseTask:            baseTask,
		LoadSegmentsRequest: req,
		cluster:             queryCoord.cluster,
		meta:                queryCoord.meta,
		excludeNodeIDs:      []int64{},
	}

	parentReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	}
	baseParentTask := newBaseTask(ctx, querypb.TriggerCondition_grpcRequest)
	baseParentTask.taskID = 10
	parentTask := &LoadCollectionTask{
		BaseTask:              baseParentTask,
		LoadCollectionRequest: parentReq,
		rootCoord:             queryCoord.rootCoordClient,
		dataCoord:             queryCoord.dataCoordClient,
		meta:                  queryCoord.meta,
		cluster:               queryCoord.cluster,
	}
	parentTask.setState(taskDone)
	parentTask.setResultInfo(nil)
	parentTask.addChildTask(loadSegmentTask)
	loadSegmentTask.setParentTask(parentTask)

	queryCoord.meta.addCollection(defaultCollectionID, schema)
	return loadSegmentTask
}

func waitTaskFinalState(t task, state taskState) {
	for {
		if t.getState() == state {
			break
		}
	}
}

func TestTriggerTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)

	t.Run("Test LoadCollection", func(t *testing.T) {
		loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)

		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection", func(t *testing.T) {
		releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
		err = queryCoord.scheduler.processTask(releaseCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)

		err = queryCoord.scheduler.processTask(loadPartitionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleasePartition", func(t *testing.T) {
		releasePartitionTask := genReleaseCollectionTask(ctx, queryCoord)

		err = queryCoord.scheduler.processTask(releasePartitionTask)
		assert.Nil(t, err)
	})

	err = node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadCollectionAfterLoadPartition(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)

	loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadPartitionTask)
	assert.Nil(t, err)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)

	releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(releaseCollectionTask)
	assert.Nil(t, err)

	err = releaseCollectionTask.waitToFinish()
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RepeatLoadCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)

	loadCollectionTask1 := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask1)
	assert.Nil(t, err)

	createDefaultPartition(ctx, queryCoord)
	loadCollectionTask2 := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask2)
	assert.Nil(t, err)

	releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(releaseCollectionTask)
	assert.Nil(t, err)

	err = releaseCollectionTask.waitToFinish()
	assert.Nil(t, err)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadCollectionAssignTaskFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)

	err = loadCollectionTask.waitToFinish()
	assert.NotNil(t, err)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadCollectionExecuteFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	node.loadSegment = returnFailedResult
	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)

	waitTaskFinalState(loadCollectionTask, taskFailed)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadPartitionAssignTaskFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadPartitionTask)
	assert.Nil(t, err)

	err = loadPartitionTask.waitToFinish()
	assert.NotNil(t, err)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadPartitionExecuteFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	node.loadSegment = returnFailedResult

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadPartitionTask)
	assert.Nil(t, err)

	waitTaskFinalState(loadPartitionTask, taskFailed)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadPartitionExecuteFailAfterLoadCollection(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)

	waitTaskFinalState(loadCollectionTask, taskExpired)

	createDefaultPartition(ctx, queryCoord)
	node.watchDmChannels = returnFailedResult

	loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadPartitionTask)
	assert.Nil(t, err)

	waitTaskFinalState(loadPartitionTask, taskFailed)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_ReleaseCollectionExecuteFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node.releaseCollection = returnFailedResult

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(releaseCollectionTask)
	assert.Nil(t, err)

	waitTaskFinalState(releaseCollectionTask, taskFailed)

	node.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_LoadSegmentReschedule(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node1.loadSegment = returnFailedResult

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)

	waitTaskFinalState(loadCollectionTask, taskExpired)

	node1.stop()
	node2.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_WatchDmChannelReschedule(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node1.watchDmChannels = returnFailedResult

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(loadCollectionTask)
	assert.Nil(t, err)

	waitTaskFinalState(loadCollectionTask, taskExpired)

	node1.stop()
	node2.stop()
	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_ReleaseSegmentTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	releaseSegmentTask := genReleaseSegmentTask(ctx, queryCoord, node1.queryNodeID)
	queryCoord.scheduler.activateTaskChan <- releaseSegmentTask

	waitTaskFinalState(releaseSegmentTask, taskDone)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RescheduleDmChannelWithWatchQueryChannel(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	node1.watchDmChannels = returnFailedResult
	watchDmChannelTask := genWatchDmChannelTask(ctx, queryCoord, node1.queryNodeID)
	loadCollectionTask := watchDmChannelTask.parentTask
	queryCoord.scheduler.triggerTaskQueue.addTask(loadCollectionTask)

	waitTaskFinalState(loadCollectionTask, taskExpired)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RescheduleSegmentWithWatchQueryChannel(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	node1.loadSegment = returnFailedResult
	loadSegmentTask := genLoadSegmentTask(ctx, queryCoord, node1.queryNodeID)
	loadCollectionTask := loadSegmentTask.parentTask
	queryCoord.scheduler.triggerTaskQueue.addTask(loadCollectionTask)

	waitTaskFinalState(loadCollectionTask, taskExpired)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RescheduleSegmentEndWithFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node1.loadSegment = returnFailedResult
	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2.loadSegment = returnFailedResult

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	loadSegmentTask := genLoadSegmentTask(ctx, queryCoord, node1.queryNodeID)
	loadCollectionTask := loadSegmentTask.parentTask
	queryCoord.scheduler.triggerTaskQueue.addTask(loadCollectionTask)

	waitTaskFinalState(loadCollectionTask, taskFailed)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_RescheduleDmChannelsEndWithFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node1.watchDmChannels = returnFailedResult
	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2.watchDmChannels = returnFailedResult

	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	watchDmChannelTask := genWatchDmChannelTask(ctx, queryCoord, node1.queryNodeID)
	loadCollectionTask := watchDmChannelTask.parentTask
	queryCoord.scheduler.triggerTaskQueue.addTask(loadCollectionTask)

	waitTaskFinalState(loadCollectionTask, taskFailed)

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}
