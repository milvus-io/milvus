// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func genLoadCollectionTask(ctx context.Context, queryCoord *QueryCoord) *loadCollectionTask {
	queryCoord.meta.setDeltaChannel(defaultCollectionID, nil)
	req := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID:  defaultCollectionID,
		Schema:        genDefaultCollectionSchema(false),
		ReplicaNumber: 1,
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	loadCollectionTask := &loadCollectionTask{
		baseTask:              baseTask,
		LoadCollectionRequest: req,
		broker:                queryCoord.broker,
		cluster:               queryCoord.cluster,
		meta:                  queryCoord.meta,
	}
	return loadCollectionTask
}

func genLoadPartitionTask(ctx context.Context, queryCoord *QueryCoord) *loadPartitionTask {
	queryCoord.meta.setDeltaChannel(defaultCollectionID, nil)
	req := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID:  defaultCollectionID,
		PartitionIDs:  []UniqueID{defaultPartitionID},
		Schema:        genDefaultCollectionSchema(false),
		ReplicaNumber: 1,
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	loadPartitionTask := &loadPartitionTask{
		baseTask:              baseTask,
		LoadPartitionsRequest: req,
		broker:                queryCoord.broker,
		cluster:               queryCoord.cluster,
		meta:                  queryCoord.meta,
	}
	return loadPartitionTask
}

func genReleaseCollectionTask(ctx context.Context, queryCoord *QueryCoord) *releaseCollectionTask {
	req := &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	releaseCollectionTask := &releaseCollectionTask{
		baseTask:                 baseTask,
		ReleaseCollectionRequest: req,
		broker:                   queryCoord.broker,
		cluster:                  queryCoord.cluster,
		meta:                     queryCoord.meta,
	}

	return releaseCollectionTask
}

func genReleasePartitionTask(ctx context.Context, queryCoord *QueryCoord) *releasePartitionTask {
	req := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	releasePartitionTask := &releasePartitionTask{
		baseTask:                 baseTask,
		ReleasePartitionsRequest: req,
		cluster:                  queryCoord.cluster,
		meta:                     queryCoord.meta,
	}

	return releasePartitionTask
}

func genReleaseSegmentTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *releaseSegmentTask {
	req := &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseSegments,
		},
		NodeID:       nodeID,
		CollectionID: defaultCollectionID,
		PartitionIDs: []UniqueID{defaultPartitionID},
		SegmentIDs:   []UniqueID{defaultSegmentID},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	releaseSegmentTask := &releaseSegmentTask{
		baseTask:               baseTask,
		ReleaseSegmentsRequest: req,
		cluster:                queryCoord.cluster,
	}
	return releaseSegmentTask
}

func genWatchDmChannelTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *watchDmChannelTask {
	schema := genDefaultCollectionSchema(false)
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
		PartitionIDs: []UniqueID{defaultPartitionID},
		Schema:       schema,
		Infos:        []*datapb.VchannelInfo{vChannelInfo},
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: defaultCollectionID,
			PartitionIDs: []int64{defaultPartitionID},
		},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	baseTask.taskID = 100
	watchDmChannelTask := &watchDmChannelTask{
		baseTask:               baseTask,
		WatchDmChannelsRequest: req,
		cluster:                queryCoord.cluster,
		meta:                   queryCoord.meta,
		excludeNodeIDs:         []int64{},
	}

	parentReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID:  defaultCollectionID,
		Schema:        genDefaultCollectionSchema(false),
		ReplicaNumber: 1,
	}
	baseParentTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	baseParentTask.taskID = 10
	baseParentTask.setState(taskDone)
	parentTask := &loadCollectionTask{
		baseTask:              baseParentTask,
		LoadCollectionRequest: parentReq,
		broker:                queryCoord.broker,
		meta:                  queryCoord.meta,
		cluster:               queryCoord.cluster,
	}
	parentTask.setState(taskDone)
	parentTask.setResultInfo(nil)
	parentTask.addChildTask(watchDmChannelTask)
	watchDmChannelTask.setParentTask(parentTask)

	queryCoord.meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, schema)
	return watchDmChannelTask
}
func genLoadSegmentTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *loadSegmentTask {
	queryCoord.meta.setDeltaChannel(defaultCollectionID, nil)
	schema := genDefaultCollectionSchema(false)
	segmentInfo := &querypb.SegmentLoadInfo{
		SegmentID:    defaultSegmentID,
		PartitionID:  defaultPartitionID,
		CollectionID: defaultCollectionID,
	}
	req := &querypb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
		},
		DstNodeID:    nodeID,
		Schema:       schema,
		Infos:        []*querypb.SegmentLoadInfo{segmentInfo},
		CollectionID: defaultCollectionID,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: defaultCollectionID,
			PartitionIDs: []int64{defaultPartitionID},
		},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	baseTask.taskID = 100
	loadSegmentTask := &loadSegmentTask{
		baseTask:            baseTask,
		LoadSegmentsRequest: req,
		cluster:             queryCoord.cluster,
		meta:                queryCoord.meta,
		excludeNodeIDs:      []int64{},
	}

	parentReq := &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID:  defaultCollectionID,
		Schema:        genDefaultCollectionSchema(false),
		ReplicaNumber: 1,
	}
	baseParentTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	baseParentTask.taskID = 10
	baseParentTask.setState(taskDone)
	parentTask := &loadCollectionTask{
		baseTask:              baseParentTask,
		LoadCollectionRequest: parentReq,
		broker:                queryCoord.broker,
		meta:                  queryCoord.meta,
		cluster:               queryCoord.cluster,
	}
	parentTask.setState(taskDone)
	parentTask.setResultInfo(nil)
	parentTask.addChildTask(loadSegmentTask)
	loadSegmentTask.setParentTask(parentTask)

	queryCoord.meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, schema)
	return loadSegmentTask
}

func genWatchQueryChannelTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *watchQueryChannelTask {
	queryChannelInfo := queryCoord.meta.getQueryChannelInfoByID(defaultCollectionID)
	req := &querypb.AddQueryChannelRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
		},
		NodeID:             nodeID,
		CollectionID:       defaultCollectionID,
		QueryChannel:       queryChannelInfo.QueryChannel,
		QueryResultChannel: queryChannelInfo.QueryResultChannel,
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	baseTask.taskID = 200
	return &watchQueryChannelTask{
		baseTask:               baseTask,
		AddQueryChannelRequest: req,
		cluster:                queryCoord.cluster,
	}
}

func genWatchDeltaChannelTask(ctx context.Context, queryCoord *QueryCoord, nodeID int64) *watchDeltaChannelTask {
	req := &querypb.WatchDeltaChannelsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchDeltaChannels,
		},
		NodeID:       nodeID,
		CollectionID: defaultCollectionID,
		LoadMeta: &querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: defaultCollectionID,
			PartitionIDs: []int64{defaultPartitionID},
		},
	}
	baseTask := newBaseTask(ctx, querypb.TriggerCondition_GrpcRequest)
	baseTask.taskID = 300
	return &watchDeltaChannelTask{
		baseTask:                  baseTask,
		WatchDeltaChannelsRequest: req,
		cluster:                   queryCoord.cluster,
	}
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

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node3, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node3.queryNodeID)

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

	t.Run("Test LoadCollection With Replicas", func(t *testing.T) {
		loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
		loadCollectionTask.ReplicaNumber = 3
		err = queryCoord.scheduler.processTask(loadCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleaseCollection With Replicas", func(t *testing.T) {
		releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
		err = queryCoord.scheduler.processTask(releaseCollectionTask)
		assert.Nil(t, err)
	})

	t.Run("Test LoadPartition With Replicas", func(t *testing.T) {
		loadPartitionTask := genLoadPartitionTask(ctx, queryCoord)
		loadPartitionTask.ReplicaNumber = 3
		err = queryCoord.scheduler.processTask(loadPartitionTask)
		assert.Nil(t, err)
	})

	t.Run("Test ReleasePartition With Replicas", func(t *testing.T) {
		releasePartitionTask := genReleaseCollectionTask(ctx, queryCoord)

		err = queryCoord.scheduler.processTask(releasePartitionTask)
		assert.Nil(t, err)
	})

	assert.NoError(t, node1.stop())
	assert.NoError(t, node2.stop())
	assert.NoError(t, node3.stop())
	assert.NoError(t, queryCoord.Stop())
	assert.NoError(t, removeAllSession())
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

func TestLoadCollectionNoEnoughNodeFail(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	defer removeAllSession()

	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)
	defer queryCoord.Stop()

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)
	defer node1.stop()
	defer node2.stop()

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	loadCollectionTask.ReplicaNumber = 3
	err = queryCoord.scheduler.processTask(loadCollectionTask)
	assert.Error(t, err)
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
	node.setRPCInterface(&node.releaseCollection, returnFailedResult)

	waitQueryNodeOnline(queryCoord.cluster, node.queryNodeID)
	releaseCollectionTask := genReleaseCollectionTask(ctx, queryCoord)
	err = queryCoord.scheduler.Enqueue(releaseCollectionTask)
	assert.Nil(t, err)

	waitTaskFinalState(releaseCollectionTask, taskDone)

	node.setRPCInterface(&node.releaseCollection, returnSuccessResult)
	waitTaskFinalState(releaseCollectionTask, taskExpired)

	node.stop()
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

func Test_RescheduleDmChannel(t *testing.T) {
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

	waitTaskFinalState(loadCollectionTask, taskFailed)

	queryCoord.Stop()
	err = removeAllSession()

	assert.Nil(t, err)
}

func Test_RescheduleSegment(t *testing.T) {
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
	loadSegmentTask.meta.setDeltaChannel(defaultCollectionID, []*datapb.VchannelInfo{})
	loadCollectionTask := loadSegmentTask.parentTask
	queryCoord.scheduler.triggerTaskQueue.addTask(loadCollectionTask)

	waitTaskFinalState(loadCollectionTask, taskFailed)

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

func Test_AssignInternalTask(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	schema := genDefaultCollectionSchema(false)
	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	loadSegmentRequests := make([]*querypb.LoadSegmentsRequest, 0)
	binlogs := make([]*datapb.FieldBinlog, 0)
	binlogs = append(binlogs, &datapb.FieldBinlog{
		FieldID: 0,
		Binlogs: []*datapb.Binlog{{LogPath: funcutil.RandomString(1000)}},
	})
	for id := 0; id < 3000; id++ {
		segmentInfo := &querypb.SegmentLoadInfo{
			SegmentID:    UniqueID(id),
			PartitionID:  defaultPartitionID,
			CollectionID: defaultCollectionID,
			BinlogPaths:  binlogs,
		}
		segmentInfo.SegmentSize = estimateSegmentSize(segmentInfo)
		req := &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_LoadSegments,
			},
			DstNodeID:    node1.queryNodeID,
			Schema:       schema,
			Infos:        []*querypb.SegmentLoadInfo{segmentInfo},
			CollectionID: defaultCollectionID,
		}
		loadSegmentRequests = append(loadSegmentRequests, req)
	}

	internalTasks, err := assignInternalTask(queryCoord.loopCtx, loadCollectionTask, queryCoord.meta, queryCoord.cluster, loadSegmentRequests, nil, false, nil, nil, -1)
	assert.Nil(t, err)

	assert.NotEqual(t, 1, len(internalTasks))

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_reverseSealedSegmentChangeInfo(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)

	loadCollectionTask := genLoadCollectionTask(ctx, queryCoord)
	queryCoord.scheduler.Enqueue(loadCollectionTask)
	waitTaskFinalState(loadCollectionTask, taskExpired)

	node2, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node2.queryNodeID)

	loadSegmentTask := genLoadSegmentTask(ctx, queryCoord, node2.queryNodeID)
	parentTask := loadSegmentTask.parentTask

	kv := &testKv{
		returnFn: failedResult,
	}
	queryCoord.meta.setKvClient(kv)

	assert.Panics(t, func() {
		updateSegmentInfoFromTask(ctx, parentTask, queryCoord.meta)
	})

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

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

	queryCoord.Stop()
	err = removeAllSession()
	assert.Nil(t, err)
}

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

	rootCoord := queryCoord.rootCoordClient.(*rootCoordMock)
	rootCoord.enableIndex = true
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
		if segmentInfo.NodeID == node3.queryNodeID {
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

func TestMergeWatchDeltaChannelInfo(t *testing.T) {
	infos := []*datapb.VchannelInfo{
		{
			ChannelName: "test-1",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-1",
				Timestamp:   9,
			},
		},
		{
			ChannelName: "test-2",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-2",
				Timestamp:   10,
			},
		},
		{
			ChannelName: "test-1",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-1",
				Timestamp:   15,
			},
		},
		{
			ChannelName: "test-2",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-2",
				Timestamp:   16,
			},
		},
		{
			ChannelName: "test-1",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-1",
				Timestamp:   5,
			},
		},
		{
			ChannelName: "test-2",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-2",
				Timestamp:   4,
			},
		},
		{
			ChannelName: "test-1",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-1",
				Timestamp:   3,
			},
		},
		{
			ChannelName: "test-2",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-2",
				Timestamp:   5,
			},
		},
	}

	results := mergeWatchDeltaChannelInfo(infos)
	expected := []*datapb.VchannelInfo{
		{
			ChannelName: "test-1",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-1",
				Timestamp:   3,
			},
		},
		{
			ChannelName: "test-2",
			SeekPosition: &internalpb.MsgPosition{
				ChannelName: "test-2",
				Timestamp:   4,
			},
		},
	}
	assert.ElementsMatch(t, expected, results)
}

func TestUpdateTaskProcessWhenLoadSegment(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	queryCoord.meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, genDefaultCollectionSchema(false))

	loadSegmentTask := genLoadSegmentTask(ctx, queryCoord, node1.queryNodeID)
	loadCollectionTask := loadSegmentTask.getParentTask()

	queryCoord.scheduler.processTask(loadSegmentTask)
	collectionInfo, err := queryCoord.meta.getCollectionInfoByID(defaultCollectionID)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), collectionInfo.InMemoryPercentage)

	watchQueryChannel := genWatchQueryChannelTask(ctx, queryCoord, node1.queryNodeID)
	watchQueryChannel.setParentTask(loadCollectionTask)
	watchDeltaChannel := genWatchDeltaChannelTask(ctx, queryCoord, node1.queryNodeID)
	watchDeltaChannel.setParentTask(loadCollectionTask)
	queryCoord.scheduler.processTask(watchQueryChannel)
	queryCoord.scheduler.processTask(watchDeltaChannel)
	collectionInfo, err = queryCoord.meta.getCollectionInfoByID(defaultCollectionID)
	assert.Nil(t, err)
	assert.Equal(t, int64(100), collectionInfo.InMemoryPercentage)

	err = removeAllSession()
	assert.Nil(t, err)
}

func TestUpdateTaskProcessWhenWatchDmChannel(t *testing.T) {
	refreshParams()
	ctx := context.Background()
	queryCoord, err := startQueryCoord(ctx)
	assert.Nil(t, err)

	node1, err := startQueryNodeServer(ctx)
	assert.Nil(t, err)
	waitQueryNodeOnline(queryCoord.cluster, node1.queryNodeID)
	queryCoord.meta.addCollection(defaultCollectionID, querypb.LoadType_LoadCollection, genDefaultCollectionSchema(false))

	watchDmChannel := genWatchDmChannelTask(ctx, queryCoord, node1.queryNodeID)
	loadCollectionTask := watchDmChannel.getParentTask()

	queryCoord.scheduler.processTask(watchDmChannel)
	collectionInfo, err := queryCoord.meta.getCollectionInfoByID(defaultCollectionID)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), collectionInfo.InMemoryPercentage)

	watchQueryChannel := genWatchQueryChannelTask(ctx, queryCoord, node1.queryNodeID)
	watchQueryChannel.setParentTask(loadCollectionTask)
	queryCoord.scheduler.processTask(watchQueryChannel)
	collectionInfo, err = queryCoord.meta.getCollectionInfoByID(defaultCollectionID)
	assert.Nil(t, err)
	assert.Equal(t, int64(100), collectionInfo.InMemoryPercentage)

	err = removeAllSession()
	assert.Nil(t, err)
}
