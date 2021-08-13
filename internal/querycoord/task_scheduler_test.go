package querycoord

import (
	"context"
	"testing"
	"time"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/assert"
)

type testTask struct {
	BaseTask
	baseMsg *commonpb.MsgBase
	cluster *queryNodeCluster
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
	baseCtx := context.Background()
	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)
	activeTaskIDKeys, _, err := queryCoord.scheduler.client.LoadWithPrefix(activeTaskPrefix)
	assert.Nil(t, err)
	queryNode, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	queryNode.addQueryChannels = returnFailedResult

	time.Sleep(time.Second)
	nodes, err := queryCoord.cluster.onServiceNodes()
	assert.Nil(t, err)
	assert.Equal(t, len(nodes), 1)
	var nodeID int64
	for id := range nodes {
		nodeID = id
		break
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
	queryNode.stop()

	for {
		_, err = queryCoord.cluster.getNodeByID(nodeID)
		if err == nil {
			time.Sleep(time.Second)
			break
		}
	}

	time.Sleep(time.Second)
	newActiveTaskIDKeys, _, err := queryCoord.scheduler.client.LoadWithPrefix(activeTaskPrefix)
	assert.Nil(t, err)
	assert.Equal(t, len(newActiveTaskIDKeys), len(activeTaskIDKeys))
	queryCoord.Stop()
}

func TestUnMarshalTask_LoadCollection(t *testing.T) {
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)

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

	taskScheduler := &TaskScheduler{}
	task, err := taskScheduler.unmarshalTask(value)
	assert.Nil(t, err)
	assert.Equal(t, task.Type(), commonpb.MsgType_LoadCollection)
}
