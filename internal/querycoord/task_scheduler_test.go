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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type testTask struct {
	BaseTask
	baseMsg *commonpb.MsgBase
	cluster Cluster
	meta    Meta
	nodeID  int64
}

func (tt *testTask) MsgBase() *commonpb.MsgBase {
	return tt.baseMsg
}

func (tt *testTask) Marshal() ([]byte, error) {
	return []byte{}, nil
}

func (tt *testTask) Type() commonpb.MsgType {
	return tt.baseMsg.MsgType
}

func (tt *testTask) Timestamp() Timestamp {
	return tt.baseMsg.Timestamp
}

func (tt *testTask) PreExecute(ctx context.Context) error {
	log.Debug("test task preExecute...")
	return nil
}

func (tt *testTask) Execute(ctx context.Context) error {
	log.Debug("test task execute...")

	switch tt.baseMsg.MsgType {
	case commonpb.MsgType_LoadSegments:
		childTask := &LoadSegmentTask{
			BaseTask: BaseTask{
				ctx:              tt.ctx,
				Condition:        NewTaskCondition(tt.ctx),
				triggerCondition: tt.triggerCondition,
			},
			LoadSegmentsRequest: &querypb.LoadSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadSegments,
				},
				NodeID: tt.nodeID,
			},
			meta:    tt.meta,
			cluster: tt.cluster,
		}
		tt.AddChildTask(childTask)
	case commonpb.MsgType_WatchDmChannels:
		childTask := &WatchDmChannelTask{
			BaseTask: BaseTask{
				ctx:              tt.ctx,
				Condition:        NewTaskCondition(tt.ctx),
				triggerCondition: tt.triggerCondition,
			},
			WatchDmChannelsRequest: &querypb.WatchDmChannelsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_WatchDmChannels,
				},
				NodeID: tt.nodeID,
			},
			cluster: tt.cluster,
			meta:    tt.meta,
		}
		tt.AddChildTask(childTask)
	case commonpb.MsgType_WatchQueryChannels:
		childTask := &WatchQueryChannelTask{
			BaseTask: BaseTask{
				ctx:              tt.ctx,
				Condition:        NewTaskCondition(tt.ctx),
				triggerCondition: tt.triggerCondition,
			},
			AddQueryChannelRequest: &querypb.AddQueryChannelRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_WatchQueryChannels,
				},
				NodeID: tt.nodeID,
			},
			cluster: tt.cluster,
		}
		tt.AddChildTask(childTask)
	}

	return nil
}

func (tt *testTask) PostExecute(ctx context.Context) error {
	log.Debug("test task postExecute...")
	return nil
}

func TestWatchQueryChannel_ClearEtcdInfoAfterAssignedNodeDown(t *testing.T) {
	refreshParams()
	baseCtx := context.Background()
	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)
	activeTaskIDKeys, _, err := queryCoord.scheduler.client.LoadWithPrefix(activeTaskPrefix)
	assert.Nil(t, err)
	queryNode, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	queryNode.addQueryChannels = returnFailedResult

	nodeID := queryNode.queryNodeID
	for {
		_, err = queryCoord.cluster.getNodeByID(nodeID)
		if err == nil {
			break
		}
	}
	testTask := &testTask{
		BaseTask: BaseTask{
			ctx:              baseCtx,
			Condition:        NewTaskCondition(baseCtx),
			triggerCondition: querypb.TriggerCondition_grpcRequest,
		},
		baseMsg: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_WatchQueryChannels,
		},
		cluster: queryCoord.cluster,
		meta:    queryCoord.meta,
		nodeID:  nodeID,
	}
	queryCoord.scheduler.Enqueue([]task{testTask})

	time.Sleep(time.Second)
	queryCoord.cluster.stopNode(nodeID)
	for {
		newActiveTaskIDKeys, _, err := queryCoord.scheduler.client.LoadWithPrefix(activeTaskPrefix)
		assert.Nil(t, err)
		if len(newActiveTaskIDKeys) == len(activeTaskIDKeys) {
			break
		}
	}
	queryCoord.Stop()
	queryNode.stop()
}

func TestUnMarshalTask(t *testing.T) {
	refreshParams()
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	taskScheduler := &TaskScheduler{}

	t.Run("Test LoadCollectionTask", func(t *testing.T) {
		loadTask := &LoadCollectionTask{
			LoadCollectionRequest: &querypb.LoadCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadCollection,
				},
			},
		}
		blobs, err := loadTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalLoadCollection", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalLoadCollection")
		value, err := kv.Load("testMarshalLoadCollection")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_LoadCollection)
	})

	t.Run("Test LoadPartitionsTask", func(t *testing.T) {
		loadTask := &LoadPartitionTask{
			LoadPartitionsRequest: &querypb.LoadPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadPartitions,
				},
			},
		}
		blobs, err := loadTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalLoadPartition", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalLoadPartition")
		value, err := kv.Load("testMarshalLoadPartition")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_LoadPartitions)
	})

	t.Run("Test ReleaseCollectionTask", func(t *testing.T) {
		releaseTask := &ReleaseCollectionTask{
			ReleaseCollectionRequest: &querypb.ReleaseCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ReleaseCollection,
				},
			},
		}
		blobs, err := releaseTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalReleaseCollection", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalReleaseCollection")
		value, err := kv.Load("testMarshalReleaseCollection")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_ReleaseCollection)
	})

	t.Run("Test ReleasePartitionTask", func(t *testing.T) {
		releaseTask := &ReleasePartitionTask{
			ReleasePartitionsRequest: &querypb.ReleasePartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ReleasePartitions,
				},
			},
		}
		blobs, err := releaseTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalReleasePartition", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalReleasePartition")
		value, err := kv.Load("testMarshalReleasePartition")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_ReleasePartitions)
	})

	t.Run("Test LoadSegmentTask", func(t *testing.T) {
		loadTask := &LoadSegmentTask{
			LoadSegmentsRequest: &querypb.LoadSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadSegments,
				},
			},
		}
		blobs, err := loadTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalLoadSegment", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalLoadSegment")
		value, err := kv.Load("testMarshalLoadSegment")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_LoadSegments)
	})

	t.Run("Test ReleaseSegmentTask", func(t *testing.T) {
		releaseTask := &ReleaseSegmentTask{
			ReleaseSegmentsRequest: &querypb.ReleaseSegmentsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ReleaseSegments,
				},
			},
		}
		blobs, err := releaseTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalReleaseSegment", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalReleaseSegment")
		value, err := kv.Load("testMarshalReleaseSegment")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_ReleaseSegments)
	})

	t.Run("Test WatchDmChannelTask", func(t *testing.T) {
		watchTask := &WatchDmChannelTask{
			WatchDmChannelsRequest: &querypb.WatchDmChannelsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_WatchDmChannels,
				},
			},
		}
		blobs, err := watchTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalWatchDmChannel", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalWatchDmChannel")
		value, err := kv.Load("testMarshalWatchDmChannel")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_WatchDmChannels)
	})

	t.Run("Test WatchQueryChannelTask", func(t *testing.T) {
		watchTask := &WatchQueryChannelTask{
			AddQueryChannelRequest: &querypb.AddQueryChannelRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_WatchQueryChannels,
				},
			},
		}
		blobs, err := watchTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalWatchQueryChannel", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalWatchQueryChannel")
		value, err := kv.Load("testMarshalWatchQueryChannel")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_WatchQueryChannels)
	})

	t.Run("Test LoadBalanceTask", func(t *testing.T) {
		loadBalanceTask := &LoadBalanceTask{
			LoadBalanceRequest: &querypb.LoadBalanceRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_LoadBalanceSegments,
				},
			},
		}

		blobs, err := loadBalanceTask.Marshal()
		assert.Nil(t, err)
		err = kv.Save("testMarshalLoadBalanceTask", string(blobs))
		assert.Nil(t, err)
		defer kv.RemoveWithPrefix("testMarshalLoadBalanceTask")
		value, err := kv.Load("testMarshalLoadBalanceTask")
		assert.Nil(t, err)

		task, err := taskScheduler.unmarshalTask(value)
		assert.Nil(t, err)
		assert.Equal(t, task.Type(), commonpb.MsgType_LoadBalanceSegments)
	})
}

func TestReloadTaskFromKV(t *testing.T) {
	refreshParams()
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	taskScheduler := &TaskScheduler{
		client:           kv,
		triggerTaskQueue: NewTaskQueue(),
	}

	kvs := make(map[string]string)
	triggerTask := &LoadCollectionTask{
		LoadCollectionRequest: &querypb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				Timestamp: 1,
				MsgType:   commonpb.MsgType_LoadCollection,
			},
		},
	}
	triggerBlobs, err := triggerTask.Marshal()
	assert.Nil(t, err)
	triggerTaskKey := fmt.Sprintf("%s/%d", triggerTaskPrefix, 100)
	kvs[triggerTaskKey] = string(triggerBlobs)

	activeTask := &LoadSegmentTask{
		LoadSegmentsRequest: &querypb.LoadSegmentsRequest{
			Base: &commonpb.MsgBase{
				Timestamp: 2,
				MsgType:   commonpb.MsgType_LoadSegments,
			},
		},
	}
	activeBlobs, err := activeTask.Marshal()
	assert.Nil(t, err)
	activeTaskKey := fmt.Sprintf("%s/%d", activeTaskPrefix, 101)
	kvs[activeTaskKey] = string(activeBlobs)

	stateKey := fmt.Sprintf("%s/%d", taskInfoPrefix, 100)
	kvs[stateKey] = strconv.Itoa(int(taskDone))
	err = kv.MultiSave(kvs)
	assert.Nil(t, err)

	taskScheduler.reloadFromKV()

	task := taskScheduler.triggerTaskQueue.PopTask()
	assert.Equal(t, taskDone, task.State())
	assert.Equal(t, 1, len(task.GetChildTask()))
}
