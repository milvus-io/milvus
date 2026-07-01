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

package task

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type distribution struct {
	NodeID   int64
	channels typeutil.Set[string]
	segments typeutil.Set[int64]
}

type TaskSuite struct {
	suite.Suite

	// Data
	collection      int64
	replica         *meta.Replica
	subChannels     []string
	unsubChannels   []string
	moveChannels    []string
	growingSegments map[string]int64
	loadSegments    []int64
	releaseSegments []int64
	moveSegments    []int64
	distributions   map[int64]*distribution

	// Dependencies
	kv      kv.MetaKv
	store   metastore.QueryCoordCatalog
	meta    *meta.Meta
	dist    *meta.DistributionManager
	target  *meta.TargetManager
	broker  *meta.MockBroker
	nodeMgr *session.NodeManager
	cluster *session.MockCluster

	// Test object
	scheduler *taskScheduler
	ctx       context.Context
}

func (suite *TaskSuite) SetupSuite() {
	paramtable.Init()
	suite.collection = 1000
	suite.replica = meta.NewReplica(&querypb.Replica{
		CollectionID:  suite.collection,
		ID:            10,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{1, 2, 3},
	}, typeutil.NewUniqueSet(1, 2, 3))

	suite.subChannels = []string{
		"sub-0",
		"sub-1",
	}
	suite.unsubChannels = []string{
		"unsub-2",
		"unsub-3",
	}
	suite.moveChannels = []string{
		"move-4",
		"move-5",
	}
	suite.growingSegments = map[string]int64{
		"sub-0": 10,
		"sub-1": 11,
	}
	suite.loadSegments = []int64{1, 2}
	suite.releaseSegments = []int64{3, 4}
	suite.moveSegments = []int64{5, 6}
	suite.distributions = map[int64]*distribution{
		1: {
			NodeID:   1,
			channels: typeutil.NewSet("unsub-2", "move-4"),
			segments: typeutil.NewSet[int64](3, 5),
		},
		2: {
			NodeID:   2,
			channels: typeutil.NewSet("unsub-3", "move-5"),
			segments: typeutil.NewSet[int64](4, 6),
		},
		3: {
			NodeID:   3,
			channels: typeutil.NewSet[string](),
			segments: typeutil.NewSet[int64](),
		},
	}
}

func (suite *TaskSuite) TearDownSuite() {
	paramtable.Get().Reset(paramtable.Get().EtcdCfg.Endpoints.Key)
}

func (suite *TaskSuite) SetupTest() {
	snmanager.ResetDoNothingStreamingNodeManager(suite.T())

	config := GenerateEtcdConfig()
	suite.ctx = context.Background()
	cli, _ := kvfactory.GetEtcdAndPath()

	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.store = querycoord.NewCatalog(suite.kv)
	suite.meta = meta.NewMeta(RandomIncrementIDAllocator(), suite.store, session.NewNodeManager())
	suite.meta.Put(suite.ctx, suite.replica)
	suite.nodeMgr = session.NewNodeManager()
	suite.dist = meta.NewDistributionManager(suite.nodeMgr)
	suite.broker = meta.NewMockBroker(suite.T())
	suite.target = meta.NewTargetManager(suite.broker, suite.meta)
	suite.cluster = session.NewMockCluster(suite.T())

	suite.scheduler = suite.newScheduler()
	suite.scheduler.Start()
	suite.scheduler.AddExecutor(1)
	suite.scheduler.AddExecutor(2)
	suite.scheduler.AddExecutor(3)
	meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
}

func (suite *TaskSuite) BeforeTest(suiteName, testName string) {
	for node := range suite.distributions {
		suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "localhost",
			Hostname: "localhost",
		}))
	}

	switch testName {
	case "TestSubscribeChannelTask",
		"TestUnsubscribeChannelTask",
		"TestLoadSegmentTask",
		"TestLoadSegmentTaskNotIndex",
		"TestSegmentTaskWaitsDistAfterLoadRPC",
		"TestLoadSegmentTaskFailed",
		"TestTaskCanceled",
		"TestMoveSegmentTask",
		"TestSubmitDuplicateLoadSegmentTask",
		"TestSubmitDuplicateSubscribeChannelTask",
		"TestLeaderTaskSet",
		"TestLeaderTaskRemove",
		"TestNoExecutor",
		"TestTaskStaleByRONode",
		"TestTaskStaleBySegmentInDist",
		"TestLeaderTaskStaleByRONode":
		suite.meta.PutCollection(suite.ctx, &meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  suite.collection,
				ReplicaNumber: 1,
				Status:        querypb.LoadStatus_Loading,
			},
		})
		suite.meta.PutPartition(suite.ctx, &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: suite.collection,
				PartitionID:  1,
			},
		})
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	}
}

func (suite *TaskSuite) TestSubscribeChannelTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partitions := []int64{100, 101}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).
		RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
			return &milvuspb.DescribeCollectionResponse{
				Schema: &schemapb.CollectionSchema{
					Name: "TestSubscribeChannelTask",
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
					},
				},
			}, nil
		})
	suite.broker.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(&rootcoordpb.DescribeDatabaseResponse{}, nil)
	for channel, segment := range suite.growingSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).
			Return([]*datapb.SegmentInfo{
				{
					ID:            segment,
					CollectionID:  suite.collection,
					PartitionID:   partitions[0],
					InsertChannel: channel,
				},
			}, nil)
	}
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
			FieldID:      100,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MetricTypeKey,
					Value: "L2",
				},
			},
		},
	}, nil)
	suite.cluster.EXPECT().WatchDmChannels(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test subscribe channel task
	tasks := []Task{}
	dmChannels := make([]*datapb.VchannelInfo, 0)
	for _, channel := range suite.subChannels {
		dmChannels = append(dmChannels, &datapb.VchannelInfo{
			CollectionID:        suite.collection,
			ChannelName:         channel,
			UnflushedSegmentIds: []int64{suite.growingSegments[channel]},
		})
		task, err := NewChannelTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return(dmChannels, nil, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	suite.AssertTaskNum(0, len(suite.subChannels), len(suite.subChannels), 0)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(len(suite.subChannels), 0, len(suite.subChannels), 0)

	// Process tasks done
	// Dist contains channels
	channels := []*meta.DmChannel{}
	for _, channel := range suite.subChannels {
		channels = append(channels, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel,
			},
			Node:    targetNode,
			Version: 1,
			View: &meta.LeaderView{
				ID:           targetNode,
				CollectionID: suite.collection,
				Channel:      channel,
			},
		})
	}
	suite.dist.ChannelDistManager.Update(targetNode, channels...)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	Wait(ctx, timeout, tasks...)
	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestSubmitDuplicateSubscribeChannelTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)

	tasks := []Task{}
	for _, channel := range suite.subChannels {
		task, err := NewChannelTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
	}

	channels := []*meta.DmChannel{}
	for _, channel := range suite.subChannels {
		channels = append(channels, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel,
			},
			Node:    targetNode,
			Version: 1,
			View: &meta.LeaderView{
				ID:           targetNode,
				CollectionID: suite.collection,
				Channel:      channel,
			},
		})
	}
	suite.dist.ChannelDistManager.Update(targetNode, channels...)

	for _, task := range tasks {
		err := suite.scheduler.Add(task)
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.Error(err)
	}
}

func (suite *TaskSuite) TestUnsubscribeChannelTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(1)

	// Expect
	suite.cluster.EXPECT().UnsubDmChannel(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test unsubscribe channel task
	tasks := []Task{}
	dmChannels := make([]*datapb.VchannelInfo, 0)
	for _, channel := range suite.unsubChannels {
		dmChannels = append(dmChannels, &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel,
		})
		task, err := NewChannelTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			meta.NilReplica,
			NewChannelAction(targetNode, ActionTypeReduce, channel),
		)

		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return(dmChannels, nil, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)

	// Only first channel exists
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  suite.unsubChannels[0],
		},
		Node:    targetNode,
		Version: 1,
		View: &meta.LeaderView{
			ID:           targetNode,
			CollectionID: suite.collection,
			Channel:      suite.unsubChannels[0],
		},
	})
	suite.AssertTaskNum(0, len(suite.unsubChannels), len(suite.unsubChannels), 0)

	// ProcessTasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 1, 0)

	// Update dist
	suite.dist.ChannelDistManager.Update(targetNode)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestLoadSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestLoadSegmentTask",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
		},
	}, nil)
	for _, segment := range suite.loadSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).Return([]*datapb.SegmentInfo{
			{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		}, nil)
		suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segment).Return(nil, nil)
	}
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: channel,
		Node:         targetNode,
		Version:      1,
		View: &meta.LeaderView{
			ID:           targetNode,
			CollectionID: suite.collection,
			Channel:      channel.ChannelName,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	})
	tasks := []Task{}
	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.loadSegments {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segment,
			InsertChannel: channel.ChannelName,
			PartitionID:   1,
		})
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	// Dist contains channels
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Segments:     map[int64]*querypb.SegmentDist{},
		Channel:      channel.ChannelName,
	}
	for _, segment := range suite.loadSegments {
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
	}
	distSegments := lo.Map(segments, func(info *datapb.SegmentInfo, _ int) *meta.Segment {
		return meta.SegmentFromInfo(info)
	})
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.ChannelName,
		},
		Node:    targetNode,
		Version: 1,
		View:    view,
	})
	suite.dist.SegmentDistManager.Update(targetNode, distSegments...)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestLoadSegmentTaskNotIndex() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestLoadSegmentTaskNotIndex",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
		},
	}, nil)
	for _, segment := range suite.loadSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).Return([]*datapb.SegmentInfo{
			{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		}, nil)
		suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segment).Return(nil, merr.WrapErrIndexNotFoundForSegments([]int64{segment}))
	}
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.ChannelName,
		},
		Node:    targetNode,
		Version: 1,
		View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})
	tasks := []Task{}
	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.loadSegments {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segment,
			InsertChannel: channel.ChannelName,
			PartitionID:   1,
		})
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	// Dist contains channels
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Segments:     map[int64]*querypb.SegmentDist{},
		Channel:      channel.ChannelName,
	}
	for _, segment := range suite.loadSegments {
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
	}
	distSegments := lo.Map(segments, func(info *datapb.SegmentInfo, _ int) *meta.Segment {
		return meta.SegmentFromInfo(info)
	})
	suite.dist.SegmentDistManager.Update(targetNode, distSegments...)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestLoadSegmentTaskFailed() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestLoadSegmentTaskNotIndex",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	for _, segment := range suite.loadSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).Return([]*datapb.SegmentInfo{
			{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		}, nil)
		suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segment).Return(nil, errors.New("index not ready"))
	}

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.ChannelName,
		},
		Node:    targetNode,
		Version: 1,
		View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})

	tasks := []Task{}
	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.loadSegments {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segment,
			PartitionID:   1,
			InsertChannel: channel.ChannelName,
		})
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	// Dist contains channels
	time.Sleep(timeout)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusFailed, task.Status())
	}
}

func (suite *TaskSuite) TestReleaseSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test load segment task
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
	}
	segments := make([]*meta.Segment, 0)
	tasks := []Task{}
	for _, segment := range suite.releaseSegments {
		segments = append(segments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		})
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeReduce, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.dist.SegmentDistManager.Update(targetNode, segments...)

	segmentsNum := len(suite.releaseSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	suite.dist.SegmentDistManager.Update(targetNode)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestReleaseGrowingSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)

	// Expect
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	tasks := []Task{}
	for _, segment := range suite.releaseSegments {
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentActionWithScope(targetNode, ActionTypeReduce, "", segment, querypb.DataScope_Streaming, 0),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}

	segmentsNum := len(suite.releaseSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks and Release done
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Tasks removed
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestSegmentTaskWaitsDistAfterLoadRPC() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	segmentID := suite.loadSegments[0]
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestSegmentTaskWaitsDistAfterLoadRPC",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{{CollectionID: suite.collection}}, nil)
	suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segmentID).Return([]*datapb.SegmentInfo{
		{
			ID:            segmentID,
			CollectionID:  suite.collection,
			PartitionID:   partition,
			InsertChannel: channel.ChannelName,
		},
	}, nil)
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segmentID).Return(nil, nil)
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil).Once()

	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: channel,
		Node:         targetNode,
		Version:      1,
		View: &meta.LeaderView{
			ID:           targetNode,
			CollectionID: suite.collection,
			Channel:      channel.ChannelName,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	task, err := NewSegmentTask(
		ctx,
		timeout,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segmentID),
	)
	suite.NoError(err)

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, []*datapb.SegmentInfo{
		{
			ID:            segmentID,
			CollectionID:  suite.collection,
			PartitionID:   partition,
			InsertChannel: channel.ChannelName,
		},
	}, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)

	suite.NoError(suite.scheduler.Add(task))
	suite.AssertTaskNum(0, 1, 0, 1)

	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 0, 1)
	suite.Equal(TaskStatusStarted, task.Status())

	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 0, 1)
	suite.Equal(TaskStatusStarted, task.Status())

	suite.dist.SegmentDistManager.Update(targetNode, utils.CreateTestSegment(suite.collection, partition, segmentID, targetNode, 1, channel.ChannelName))
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)
	suite.Equal(TaskStatusSucceeded, task.Status())
	suite.NoError(task.Err())
}

func (suite *TaskSuite) TestSegmentTaskWaitsDistAfterReleaseRPC() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	segmentID := suite.releaseSegments[0]
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil).Once()

	task, err := NewSegmentTask(
		ctx,
		timeout,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(targetNode, ActionTypeReduce, channel.GetChannelName(), segmentID),
	)
	suite.NoError(err)
	suite.NoError(suite.scheduler.Add(task))
	suite.dist.SegmentDistManager.Update(targetNode, utils.CreateTestSegment(suite.collection, partition, segmentID, targetNode, 1, channel.ChannelName))
	suite.AssertTaskNum(0, 1, 0, 1)

	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 0, 1)
	suite.Equal(TaskStatusStarted, task.Status())

	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 0, 1)
	suite.Equal(TaskStatusStarted, task.Status())

	suite.dist.SegmentDistManager.Update(targetNode)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)
	suite.Equal(TaskStatusSucceeded, task.Status())
	suite.NoError(task.Err())
}

func (suite *TaskSuite) TestSegmentTaskChecksTimeoutWhileWaitingDist() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	action := NewSegmentAction(3, ActionTypeGrow, "test-channel", suite.loadSegments[0])
	action.rpcReturned.Store(true)
	task, err := NewSegmentTask(
		ctx,
		time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		action,
	)
	suite.NoError(err)
	time.Sleep(time.Millisecond)

	shouldProcess := suite.scheduler.preProcess(task)
	suite.False(shouldProcess)
	suite.Equal(TaskStatusCanceled, task.Status())
	suite.ErrorIs(task.Err(), context.DeadlineExceeded)
}

func (suite *TaskSuite) TestStreamingReduceWaitsForGrowingSegmentDist() {
	nodeID := int64(3)
	channelName := "test-channel"
	segmentID := suite.growingSegments["sub-0"]
	suite.dist.ChannelDistManager.Update(nodeID, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channelName,
		},
		Node:    nodeID,
		Version: 1,
		View: &meta.LeaderView{
			ID:           nodeID,
			CollectionID: suite.collection,
			Channel:      channelName,
			GrowingSegments: map[int64]*meta.Segment{
				segmentID: utils.CreateTestSegment(suite.collection, 1, segmentID, nodeID, 1, channelName),
			},
		},
	})

	action := NewSegmentActionWithScope(nodeID, ActionTypeReduce, channelName, segmentID, querypb.DataScope_Streaming, 0)
	action.rpcReturned.Store(true)
	suite.False(action.IsFinished(suite.dist))

	suite.dist.ChannelDistManager.Update(nodeID)
	suite.True(action.IsFinished(suite.dist))
}

func (suite *TaskSuite) TestMoveSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	leader := int64(1)
	sourceNode := int64(2)
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestMoveSegmentTask",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
		},
	}, nil)
	for _, segment := range suite.moveSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).Return([]*datapb.SegmentInfo{
			{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		}, nil)
		suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segment).Return(nil, nil)
	}
	suite.cluster.EXPECT().LoadSegments(mock.Anything, leader, mock.Anything).Return(merr.Success(), nil)
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, leader, mock.Anything).Return(merr.Success(), nil)
	vchannel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}
	suite.dist.ChannelDistManager.Update(leader, &meta.DmChannel{
		VchannelInfo: vchannel,
		Node:         leader,
		Version:      1,
		View: &meta.LeaderView{
			ID:           leader,
			CollectionID: suite.collection,
			Channel:      channel.ChannelName,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	})
	view := &meta.LeaderView{
		ID:           leader,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
		Status:       &querypb.LeaderViewStatus{Serviceable: true},
	}
	tasks := []Task{}
	segments := make([]*meta.Segment, 0)
	segmentInfos := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.moveSegments {
		segments = append(segments,
			utils.CreateTestSegment(suite.collection, partition, segment, sourceNode, 1, channel.ChannelName))
		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID:            segment,
			PartitionID:   1,
			InsertChannel: channel.ChannelName,
		})
		view.Segments[segment] = &querypb.SegmentDist{NodeID: sourceNode, Version: 0}

		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
			NewSegmentAction(sourceNode, ActionTypeReduce, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{vchannel}, segmentInfos, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	suite.target.UpdateCollectionCurrentTarget(ctx, suite.collection)
	suite.dist.SegmentDistManager.Update(sourceNode, segments...)
	suite.dist.ChannelDistManager.Update(leader, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.ChannelName,
		},
		Node: leader,
		View: view,
	})
	for _, task := range tasks {
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}

	segmentsNum := len(suite.moveSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks, target node contains the segment
	view = view.Clone()
	for _, segment := range suite.moveSegments {
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
	}
	distSegments := lo.Map(segmentInfos, func(info *datapb.SegmentInfo, _ int) *meta.Segment {
		return meta.SegmentFromInfo(info)
	})

	suite.dist.SegmentDistManager.Update(targetNode, distSegments...)
	// First action done, execute the second action
	suite.dispatchAndWait(sourceNode)
	suite.dist.SegmentDistManager.Update(sourceNode)
	// Check second action
	suite.dispatchAndWait(sourceNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestTaskCanceled() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestSubscribeChannelTask",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
		},
	}, nil)
	for _, segment := range suite.loadSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).Return([]*datapb.SegmentInfo{
			{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		}, nil)
		suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segment).Return(nil, nil)
	}
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.ChannelName,
		},
		Node:    targetNode,
		Version: 1,
		View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})

	tasks := []Task{}
	segmentInfos := []*datapb.SegmentInfo{}
	for _, segment := range suite.loadSegments {
		segmentInfos = append(segmentInfos, &datapb.SegmentInfo{
			ID:            segment,
			PartitionID:   partition,
			InsertChannel: channel.GetChannelName(),
		})
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segmentInfos, nil)
	suite.meta.PutPartition(ctx, utils.CreateTestPartition(suite.collection, partition))
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Cancel all tasks
	for _, task := range tasks {
		task.Cancel(errors.New("mock error"))
	}

	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.Error(task.Err())
	}
}

func (suite *TaskSuite) TestChannelTaskReplace() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)

	for _, channel := range suite.subChannels {
		task, err := NewChannelTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		suite.NoError(err)
		task.SetPriority(TaskPriorityNormal)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}

	// Task with the same replica and segment,
	// but without higher priority can't be added
	for _, channel := range suite.subChannels {
		task, err := NewChannelTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		suite.NoError(err)
		task.SetPriority(TaskPriorityNormal)
		err = suite.scheduler.Add(task)
		suite.Error(err)
		task.SetPriority(TaskPriorityLow)
		err = suite.scheduler.Add(task)
		suite.Error(err)
	}

	// Replace the task with one with higher priority
	for _, channel := range suite.subChannels {
		task, err := NewChannelTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		suite.NoError(err)
		task.SetPriority(TaskPriorityHigh)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	channelNum := len(suite.subChannels)
	suite.AssertTaskNum(0, channelNum, channelNum, 0)
}

func (suite *TaskSuite) TestLeaderTaskSet() {
	ctx := context.Background()
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestLoadSegmentTask",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
		},
	}, nil)
	for _, segment := range suite.loadSegments {
		suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segment).Return([]*datapb.SegmentInfo{
			{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		}, nil)
		suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segment).Return(nil, nil)
	}
	suite.cluster.EXPECT().SyncDistribution(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: channel,
		Node:         targetNode,
		Version:      1,
		View:         &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})
	tasks := []Task{}
	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.loadSegments {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segment,
			InsertChannel: channel.ChannelName,
			PartitionID:   1,
		})
		task := NewLeaderSegmentTask(
			ctx,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			targetNode,
			NewLeaderAction(targetNode, targetNode, ActionTypeGrow, channel.GetChannelName(), segment, 0),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      channel.GetChannelName(),
		Segments:     map[int64]*querypb.SegmentDist{},
		Status:       &querypb.LeaderViewStatus{Serviceable: true},
	}
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.GetChannelName(),
		},
		Node: targetNode,
		View: view,
	})

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	// Dist contains channels
	view = &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      channel.GetChannelName(),
		Segments:     map[int64]*querypb.SegmentDist{},
	}
	for _, segment := range suite.loadSegments {
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
	}
	distSegments := lo.Map(segments, func(info *datapb.SegmentInfo, _ int) *meta.Segment {
		return meta.SegmentFromInfo(info)
	})
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.GetChannelName(),
		},
		Node: targetNode,
		View: view,
	})
	suite.dist.SegmentDistManager.Update(targetNode, distSegments...)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestCreateTaskBehavior() {
	chanelTask, err := NewChannelTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(chanelTask)

	action := NewSegmentAction(0, 0, "", 0)
	chanelTask, err = NewChannelTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, action)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(chanelTask)

	action1 := NewChannelAction(0, 0, "fake-channel1")
	action2 := NewChannelAction(0, 0, "fake-channel2")
	chanelTask, err = NewChannelTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, action1, action2)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(chanelTask)

	segmentTask, err := NewSegmentTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, commonpb.LoadPriority_LOW)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(segmentTask)

	channelAction := NewChannelAction(0, 0, "fake-channel1")
	segmentTask, err = NewSegmentTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, commonpb.LoadPriority_LOW, channelAction)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(segmentTask)

	segmentAction1 := NewSegmentAction(0, 0, "", 0)
	segmentAction2 := NewSegmentAction(0, 0, "", 1)

	segmentTask, err = NewSegmentTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, commonpb.LoadPriority_LOW, segmentAction1, segmentAction2)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(segmentTask)

	leaderAction := NewLeaderAction(1, 2, ActionTypeGrow, "fake-channel1", 100, 0)
	leaderTask := NewLeaderSegmentTask(context.TODO(), WrapIDSource(0), 0, meta.NilReplica, 1, leaderAction)
	suite.NotNil(leaderTask)
}

func (suite *TaskSuite) TestSegmentTaskReplace() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)

	for _, segment := range suite.loadSegments {
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, "", segment),
		)
		suite.NoError(err)
		task.SetPriority(TaskPriorityNormal)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}

	// Task with the same replica and segment,
	// but without higher priority can't be added
	for _, segment := range suite.loadSegments {
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, "", segment),
		)
		suite.NoError(err)
		task.SetPriority(TaskPriorityNormal)
		err = suite.scheduler.Add(task)
		suite.Error(err)
		task.SetPriority(TaskPriorityLow)
		err = suite.scheduler.Add(task)
		suite.Error(err)
	}

	// Replace the task with one with higher priority
	for _, segment := range suite.loadSegments {
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, "", segment),
		)
		suite.NoError(err)
		task.SetPriority(TaskPriorityHigh)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	segmentNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentNum, 0, segmentNum)
}

func (suite *TaskSuite) TestNoExecutor() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(-1)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	suite.meta.Put(ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3, -1}))

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: channel,
		Node:         targetNode,
		Version:      1,
		View:         &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
	})
	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.loadSegments {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segment,
			PartitionID:   1,
			InsertChannel: channel.ChannelName,
		})
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)
}

func (suite *TaskSuite) TestRemoveByNodeWaitsForSchedule() {
	ctx := context.Background()
	targetNode := int64(1)

	task, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(targetNode, ActionTypeGrow, "ch-0", 999),
	)
	suite.NoError(err)
	suite.NoError(suite.scheduler.Add(task))
	suite.AssertTaskNum(0, 1, 0, 1)

	suite.scheduler.scheduleMu.Lock()
	locked := true
	defer func() {
		if locked {
			suite.scheduler.scheduleMu.Unlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		suite.scheduler.RemoveByNode(targetNode)
	}()

	select {
	case <-done:
		suite.FailNow("RemoveByNode should wait for scheduleMu")
	case <-time.After(50 * time.Millisecond):
	}
	suite.AssertTaskNum(0, 1, 0, 1)

	suite.scheduler.scheduleMu.Unlock()
	locked = false

	select {
	case <-done:
	case <-time.After(time.Second):
		suite.FailNow("RemoveByNode blocked after scheduleMu was released")
	}
	suite.AssertTaskNum(0, 0, 0, 0)
}

func (suite *TaskSuite) AssertTaskNum(process, wait, channel, segment int) {
	scheduler := suite.scheduler

	suite.Equal(process, scheduler.processQueue.Len())
	suite.Equal(wait, scheduler.waitQueue.Len())
	suite.Equal(scheduler.segmentTasks.Len(), segment)
	suite.Equal(scheduler.channelTasks.Len(), channel)
	suite.Equal(scheduler.tasks.Len(), process+wait)
	suite.Equal(scheduler.tasks.Len(), segment+channel)
}

func (suite *TaskSuite) dispatchAndWait(node int64) {
	timeout := 10 * time.Second
	suite.scheduler.Dispatch(node)
	var keys []any
	count := 0
	for start := time.Now(); time.Since(start) < timeout; {
		count = 0
		keys = make([]any, 0)

		suite.scheduler.executors.Range(func(_ int64, executor *Executor) bool {
			executor.executingTasks.Range(func(taskIndex string) bool {
				keys = append(keys, taskIndex)
				count++
				return true
			})
			return true
		})

		if count == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	suite.FailNow("executor hangs in executing tasks", "count=%d keys=%+v", count, keys)
}

func (suite *TaskSuite) TestLeaderTaskRemove() {
	ctx := context.Background()
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Expect
	suite.cluster.EXPECT().SyncDistribution(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)

	// Test remove segment task
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
		Status:       &querypb.LeaderViewStatus{Serviceable: true},
	}
	segments := make([]*meta.Segment, 0)
	tasks := []Task{}
	for _, segment := range suite.releaseSegments {
		segments = append(segments, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segment,
				CollectionID:  suite.collection,
				PartitionID:   partition,
				InsertChannel: channel.ChannelName,
			},
		})
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
		task := NewLeaderSegmentTask(
			ctx,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			targetNode,
			NewLeaderAction(targetNode, targetNode, ActionTypeReduce, channel.GetChannelName(), segment, 0),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.dist.SegmentDistManager.Update(targetNode, segments...)
	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel.ChannelName,
		},
		Node:    targetNode,
		Version: 1,
		View:    view,
	})

	segmentsNum := len(suite.releaseSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// mock leader view which has removed all segments
	view.Segments = make(map[int64]*querypb.SegmentDist)
	// Process tasks done
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestLeaderTaskUsesLeaderExecutor() {
	ctx := context.Background()
	leaderID := int64(1)
	workerID := int64(2)
	segmentID := suite.releaseSegments[0]
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-leader-executor",
	}

	suite.scheduler.RemoveExecutor(workerID)
	suite.cluster.EXPECT().SyncDistribution(mock.Anything, leaderID, mock.MatchedBy(func(req *querypb.SyncDistributionRequest) bool {
		return req.GetCollectionID() == suite.collection &&
			req.GetChannel() == channel.GetChannelName() &&
			len(req.GetActions()) == 1 &&
			req.GetActions()[0].GetType() == querypb.SyncType_Remove &&
			req.GetActions()[0].GetSegmentID() == segmentID &&
			req.GetActions()[0].GetNodeID() == workerID
	})).Return(merr.Success(), nil).Once()

	task := NewLeaderSegmentTask(
		ctx,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		leaderID,
		NewLeaderAction(leaderID, workerID, ActionTypeReduce, channel.GetChannelName(), segmentID, 0),
	)
	suite.NoError(suite.scheduler.Add(task))

	suite.dispatchAndWait(leaderID)

	suite.dispatchAndWait(leaderID)
	suite.Equal(TaskStatusSucceeded, task.Status())
	suite.NoError(task.Err())
	suite.AssertTaskNum(0, 0, 0, 0)
}

func (suite *TaskSuite) newScheduler() *taskScheduler {
	return NewScheduler(
		context.Background(),
		suite.meta,
		suite.dist,
		suite.target,
		suite.broker,
		suite.cluster,
		suite.nodeMgr,
	)
}

func createReplica(collection int64, nodes ...int64) *meta.Replica {
	return meta.NewReplica(
		&querypb.Replica{
			ID:           rand.Int63()/2 + 1,
			CollectionID: collection,
			Nodes:        nodes,
		},
		typeutil.NewUniqueSet(nodes...),
	)
}

func (suite *TaskSuite) TestBalanceChannelTask() {
	ctx := context.Background()
	collectionID := suite.collection
	partitionID := int64(1)
	channel := "channel-1"
	vchannel := &datapb.VchannelInfo{
		CollectionID: collectionID,
		ChannelName:  channel,
	}

	segments := []*datapb.SegmentInfo{
		{
			ID:            1,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
		},
		{
			ID:            2,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
		},
		{
			ID:            3,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
		},
	}
	suite.meta.PutCollection(ctx, utils.CreateTestCollection(collectionID, 1), utils.CreateTestPartition(collectionID, 1))
	suite.broker.ExpectedCalls = nil
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return([]*datapb.VchannelInfo{vchannel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, collectionID)
	suite.target.UpdateCollectionCurrentTarget(ctx, collectionID)
	suite.target.UpdateCollectionNextTarget(ctx, collectionID)

	suite.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node:    2,
		Version: 1,
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: collectionID,
			Channel:      channel,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	})
	task, err := NewChannelTask(context.Background(),
		10*time.Second,
		WrapIDSource(2),
		collectionID,
		suite.replica,
		NewChannelAction(1, ActionTypeGrow, channel),
		NewChannelAction(2, ActionTypeReduce, channel),
	)
	suite.NoError(err)

	// new delegator distribution hasn't updated, block balance
	suite.scheduler.preProcess(task)
	suite.Equal(0, task.step)

	suite.dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  channel,
		},
		Node:    1,
		Version: 2,
		View: &meta.LeaderView{
			ID:           1,
			CollectionID: collectionID,
			Channel:      channel,
			Version:      2,
			Status:       &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	// new delegator distribution updated, task step up
	suite.scheduler.preProcess(task)
	suite.Equal(1, task.step)

	suite.dist.ChannelDistManager.Update(2)
	// old delegator removed
	suite.scheduler.preProcess(task)
	suite.Equal(2, task.step)
}

func (suite *TaskSuite) TestGetTasksJSON() {
	ctx := context.Background()
	scheduler := suite.newScheduler()

	// Add some tasks to the scheduler
	task1, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(1, ActionTypeGrow, "", 1),
	)
	suite.NoError(err)
	err = scheduler.Add(task1)
	suite.NoError(err)

	task2, err := NewChannelTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		NewChannelAction(1, ActionTypeGrow, "channel-1"),
	)
	suite.NoError(err)
	err = scheduler.Add(task2)
	suite.NoError(err)

	actualJSON := scheduler.GetTasksJSON()

	var tasks []*metricsinfo.QueryCoordTask
	err = json.Unmarshal([]byte(actualJSON), &tasks)
	suite.NoError(err)
	suite.Equal(2, len(tasks))
}

func (suite *TaskSuite) TestCalculateTaskDelta() {
	ctx := context.Background()
	scheduler := suite.newScheduler()

	coll := int64(1001)
	nodeID := int64(1)
	channelName := "channel-1"
	segmentID := int64(1)
	// add segment task for collection
	task1, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeGrow, "", segmentID, querypb.DataScope_Historical, 100),
	)
	task1.SetID(1)
	suite.NoError(err)
	err = scheduler.Add(task1)
	suite.NoError(err)
	task2, err := NewChannelTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		NewChannelAction(nodeID, ActionTypeGrow, channelName),
	)
	task2.SetID(2)
	suite.NoError(err)
	err = scheduler.Add(task2)
	suite.NoError(err)

	coll2 := int64(1005)
	nodeID2 := int64(2)
	channelName2 := "channel-2"
	segmentID2 := int64(2)
	task3, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll2,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID2, ActionTypeGrow, "", segmentID2, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	task3.SetID(3)
	err = scheduler.Add(task3)
	suite.NoError(err)
	task4, err := NewChannelTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll2,
		suite.replica,
		NewChannelAction(nodeID2, ActionTypeGrow, channelName2),
	)
	suite.NoError(err)
	task4.SetID(4)
	err = scheduler.Add(task4)
	suite.NoError(err)

	snapshot := scheduler.GetSegmentTaskDeltaSnapshot([]int64{nodeID, nodeID2}, coll)
	snapshot2 := scheduler.GetSegmentTaskDeltaSnapshot([]int64{nodeID, nodeID2}, coll2)

	// check task delta with collectionID and nodeID
	suite.Equal(100, snapshot.GetByNodeInCollection(nodeID))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID, coll))
	suite.Equal(100, snapshot2.GetByNodeInCollection(nodeID2))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID2, coll2))

	// check task delta with collectionID=-1
	suite.Equal(100, snapshot.GetByNode(nodeID))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID, -1))
	suite.Equal(100, snapshot.GetByNode(nodeID2))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID2, -1))

	// check task delta with nodeID=-1
	suite.Equal(100, snapshot.GetByNodeInCollection(nodeID)+snapshot.GetByNodeInCollection(nodeID2))
	suite.Equal(1, scheduler.GetChannelTaskDelta(-1, coll))
	suite.Equal(100, snapshot.GetByNodeInCollection(nodeID)+snapshot.GetByNodeInCollection(nodeID2))
	suite.Equal(1, scheduler.GetChannelTaskDelta(-1, coll))

	// check task delta with nodeID=-1 and collectionID=-1
	suite.Equal(200, snapshot.GetByNode(nodeID)+snapshot.GetByNode(nodeID2))
	suite.Equal(2, scheduler.GetChannelTaskDelta(-1, -1))
	suite.Equal(200, snapshot.GetByNode(nodeID)+snapshot.GetByNode(nodeID2))
	suite.Equal(2, scheduler.GetChannelTaskDelta(-1, -1))

	scheduler.remove(task1)
	scheduler.remove(task2)
	scheduler.remove(task3)
	scheduler.remove(task4)
	snapshot = scheduler.GetSegmentTaskDeltaSnapshot([]int64{nodeID, nodeID2}, coll)
	snapshot2 = scheduler.GetSegmentTaskDeltaSnapshot([]int64{nodeID, nodeID2}, coll2)
	suite.Equal(0, snapshot.GetByNodeInCollection(nodeID))
	suite.Equal(0, scheduler.GetChannelTaskDelta(nodeID, coll))
	suite.Equal(0, snapshot2.GetByNodeInCollection(nodeID2))
	suite.Equal(0, scheduler.GetChannelTaskDelta(nodeID2, coll2))

	task5, err := NewChannelTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll2,
		suite.replica,
		NewChannelAction(nodeID2, ActionTypeGrow, channelName2),
	)
	suite.NoError(err)
	task4.SetID(5)
	scheduler.incExecutingTaskDelta(task5)
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID2, coll2))
	scheduler.decExecutingTaskDelta(task5)
	suite.Equal(0, scheduler.GetChannelTaskDelta(nodeID2, coll2))
}

func (suite *TaskSuite) TestSegmentTaskDeltaWithDistFilter() {
	ctx := context.Background()
	scheduler := suite.newScheduler()

	coll := int64(1001)
	partition := int64(100)
	channel := "channel-1"
	sourceNode := int64(1)
	targetNode := int64(2)
	growSegmentID := int64(101)
	reduceSegmentID := int64(102)
	rowCount := 100

	growTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, "", growSegmentID, querypb.DataScope_Historical, rowCount),
	)
	suite.NoError(err)
	growTask.SetID(1)
	scheduler.incExecutingTaskDelta(growTask)

	snapshot := scheduler.GetSegmentTaskDeltaSnapshot([]int64{targetNode}, coll)
	suite.Equal(rowCount, snapshot.GetByNode(targetNode))
	suite.Equal(rowCount, snapshot.GetByNodeInCollection(targetNode))
	snapshot = scheduler.GetSegmentTaskDeltaSnapshot([]int64{targetNode + 1}, coll)
	suite.Equal(0, snapshot.GetByNode(targetNode))

	suite.dist.SegmentDistManager.Update(targetNode,
		utils.CreateTestSegment(coll, partition, growSegmentID, targetNode, 1, channel))
	snapshot = scheduler.GetSegmentTaskDeltaSnapshot([]int64{targetNode}, coll)
	suite.Equal(0, snapshot.GetByNode(targetNode))
	suite.Equal(0, snapshot.GetByNodeInCollection(targetNode))

	scheduler.decExecutingTaskDelta(growTask)

	suite.dist.SegmentDistManager.Update(sourceNode,
		utils.CreateTestSegment(coll, partition, reduceSegmentID, sourceNode, 1, channel))
	reduceTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(sourceNode, ActionTypeReduce, "", reduceSegmentID, querypb.DataScope_Historical, rowCount),
	)
	suite.NoError(err)
	reduceTask.SetID(2)
	scheduler.incExecutingTaskDelta(reduceTask)

	snapshot = scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode}, coll)
	suite.Equal(-rowCount, snapshot.GetByNode(sourceNode))
	suite.Equal(-rowCount, snapshot.GetByNodeInCollection(sourceNode))

	suite.dist.SegmentDistManager.Update(sourceNode)
	snapshot = scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode}, coll)
	suite.Equal(0, snapshot.GetByNode(sourceNode))
	suite.Equal(0, snapshot.GetByNodeInCollection(sourceNode))

	channelTask, err := NewChannelTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		NewChannelAction(targetNode, ActionTypeGrow, channel),
	)
	suite.NoError(err)
	channelTask.SetID(3)
	scheduler.incExecutingTaskDelta(channelTask)
	suite.Equal(1, scheduler.GetChannelTaskDelta(targetNode, coll))
}

func (suite *TaskSuite) TestSegmentTaskDeltaSnapshotKeepsStreamingReduceUntilGrowingDistGone() {
	ctx := context.Background()
	scheduler := suite.newScheduler()

	coll := int64(1001)
	partition := int64(100)
	channel := "channel-1"
	sourceNode := int64(1)
	segmentID := int64(102)
	rowCount := 100

	suite.dist.ChannelDistManager.Update(sourceNode, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{
			CollectionID: coll,
			ChannelName:  channel,
		},
		Node:    sourceNode,
		Version: 1,
		View: &meta.LeaderView{
			ID:           sourceNode,
			CollectionID: coll,
			Channel:      channel,
			GrowingSegments: map[int64]*meta.Segment{
				segmentID: utils.CreateTestSegment(coll, partition, segmentID, sourceNode, 1, channel),
			},
		},
	})

	reduceTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(sourceNode, ActionTypeReduce, channel, segmentID, querypb.DataScope_Streaming, rowCount),
	)
	suite.NoError(err)
	reduceTask.SetID(1)
	scheduler.incExecutingTaskDelta(reduceTask)

	snapshot := scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode}, coll)
	suite.Equal(-rowCount, snapshot.GetByNode(sourceNode))
	suite.Equal(-rowCount, snapshot.GetByNodeInCollection(sourceNode))

	suite.dist.ChannelDistManager.Update(sourceNode)
	snapshot = scheduler.GetSegmentTaskDeltaSnapshot([]int64{sourceNode}, coll)
	suite.Equal(0, snapshot.GetByNode(sourceNode))
	suite.Equal(0, snapshot.GetByNodeInCollection(sourceNode))

	scheduler.decExecutingTaskDelta(reduceTask)
}

func (suite *TaskSuite) TestChannelTaskDeltaCache() {
	delta := NewChannelTaskDelta()

	taskDelta := []int{1, 2, 3, 4, 5, -6, -7, -8, -9, -10}

	nodeID := int64(1)
	collectionID := int64(100)

	tasks := make([]Task, 0)
	for i := 0; i < len(taskDelta); i++ {
		task, _ := NewChannelTask(
			context.TODO(),
			10*time.Second,
			WrapIDSource(0),
			collectionID,
			suite.replica,
			NewChannelAction(nodeID, ActionTypeGrow, "channel"),
		)
		task.SetID(int64(i))
		tasks = append(tasks, task)
	}

	tasks = lo.Shuffle(tasks)
	for i := 0; i < len(taskDelta); i++ {
		delta.Add(tasks[i].(*ChannelTask))
	}

	tasks = lo.Shuffle(tasks)
	for i := 0; i < len(taskDelta); i++ {
		delta.Sub(tasks[i].(*ChannelTask))
	}
	suite.Equal(0, delta.Get(nodeID, collectionID))
	suite.Equal(0, delta.Get(nodeID, -1))
	suite.Equal(0, delta.Get(-1, -1))
}

func (suite *TaskSuite) TestAutoscaleInflightLoadMetrics() {
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.AutoscaleEnabled.Key, "false")
	suite.T().Cleanup(func() {
		params.Reset(params.QueryCoordCfg.AutoscaleEnabled.Key)
	})

	rgName := "rg-inflight"
	ctx := context.Background()
	scheduler := suite.newScheduler()
	collectionID := int64(2001)
	segmentID := int64(3001)
	targetNode := int64(1)
	targetNodeLabel := "1"
	targetNodeHost := "querynode-1"
	otherNodeLabel := "2"
	otherNodeHost := "querynode-2"
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: targetNode, Hostname: targetNodeHost}))
	metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost).Set(0)
	metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost).Set(0)
	metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(otherNodeLabel, otherNodeHost).Set(0)
	metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(otherNodeLabel, otherNodeHost).Set(0)
	partitionID := int64(100)
	replica := meta.NewReplica(&querypb.Replica{
		CollectionID:  collectionID,
		ID:            20,
		ResourceGroup: rgName,
		Nodes:         []int64{targetNode},
	}, typeutil.NewUniqueSet(targetNode))
	collection := utils.CreateTestCollection(collectionID, 1)
	collection.LoadFields = []int64{100}
	collection.FieldIndexID = map[int64]int64{100: 11}
	suite.NoError(suite.meta.PutCollection(ctx, collection))
	suite.NoError(suite.meta.PutPartition(ctx, utils.CreateTestPartition(collectionID, partitionID)))
	suite.meta.Put(ctx, replica)

	segment := &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 100, LogSize: 80}}},
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 300, LogSize: 240}}},
		},
		Statslogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 20, LogSize: 20}}},
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 50, LogSize: 50}}},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{Binlogs: []*datapb.Binlog{{MemorySize: 10, LogSize: 8}}},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nil, []*datapb.SegmentInfo{segment}, nil)
	suite.NoError(suite.target.UpdateCollectionNextTarget(ctx, collectionID))
	suite.broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "4"},
					},
				},
				{FieldID: 101, Name: "scalar", DataType: schemapb.DataType_Int64},
			},
		},
	}, nil).Twice()
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, collectionID, segmentID).Return(map[int64][]*querypb.FieldIndexInfo{
		segmentID: {
			{FieldID: 100, IndexID: 10, IndexSize: 60},
			{
				FieldID:        100,
				IndexID:        11,
				IndexSize:      30,
				NumRows:        100,
				IndexFilePaths: []string{"index/11"},
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "HNSW"},
					{Key: common.MetricTypeKey, Value: "L2"},
					{Key: common.DimKey, Value: "4"},
					{Key: "M", Value: "8"},
				},
			},
			{FieldID: 101, IndexID: 20, IndexSize: 90},
		},
	}, nil).Twice()

	loadTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		collectionID,
		replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, "", segmentID, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	loadTask.SetReason(ReasonLacksOfSegment)
	suite.NoError(scheduler.Add(loadTask))

	duplicateTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		collectionID,
		replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, "", segmentID, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	duplicateTask.SetReason(ReasonLacksOfSegment)
	suite.Error(scheduler.Add(duplicateTask))

	ignoredTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		collectionID,
		replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, "", segmentID+1, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	ignoredTask.SetReason("balance")
	suite.NoError(scheduler.Add(ignoredTask))

	suite.Eventually(func() bool {
		return testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)) == float64(60) &&
			testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost)) == float64(0)
	}, time.Second, 10*time.Millisecond)
	suite.Equal(int64(60), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).DiskBytes)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{2}).MemoryBytes)
	suite.Equal(int64(60), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode, 2}).MemoryBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(otherNodeLabel, otherNodeHost)))
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(otherNodeLabel, otherNodeHost)))

	loadTask.Actions()[0].(*SegmentAction).Finish()
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).DiskBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))

	scheduler.remove(loadTask)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).DiskBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))

	removedTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		collectionID,
		replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, "", segmentID, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	removedTask.SetReason(ReasonLacksOfSegment)
	suite.NoError(scheduler.Add(removedTask))
	suite.Equal(int64(60), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)

	scheduler.remove(removedTask)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))

	removedTask.Actions()[0].(*SegmentAction).Finish()
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))
}

func TestAutoscaleInflightLoadResourceSnapshot(t *testing.T) {
	const (
		node1      = int64(91001)
		node2      = int64(91002)
		node1Label = "91001"
		node2Label = "91002"
		node1Host  = "inflight-snapshot-node-1"
		node2Host  = "inflight-snapshot-node-2"
	)
	scheduler := NewScheduler(context.Background(), nil, nil, nil, nil, nil, nil)
	t.Cleanup(func() {
		metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.DeleteLabelValues(node1Label, node1Host)
		metrics.QueryCoordAutoscaleInflightLoadDiskBytes.DeleteLabelValues(node1Label, node1Host)
		metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.DeleteLabelValues(node2Label, node2Host)
		metrics.QueryCoordAutoscaleInflightLoadDiskBytes.DeleteLabelValues(node2Label, node2Host)
	})

	scheduler.updateAutoscaleInflightLoadResource(node1, node1Host, 10, 20)
	scheduler.updateAutoscaleInflightLoadResource(node2, node2Host, 30, 40)

	assert.Equal(t, int64(10), scheduler.GetAutoscaleInflightLoadResource([]int64{node1}).MemoryBytes)
	assert.Equal(t, int64(20), scheduler.GetAutoscaleInflightLoadResource([]int64{node1}).DiskBytes)
	assert.Equal(t, int64(40), scheduler.GetAutoscaleInflightLoadResource([]int64{node1, node2}).MemoryBytes)
	assert.Equal(t, int64(60), scheduler.GetAutoscaleInflightLoadResource([]int64{node1, node2}).DiskBytes)

	scheduler.updateAutoscaleInflightLoadResource(node1, node1Host, -10, -20)
	assert.Equal(t, int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{node1}).MemoryBytes)
	assert.Equal(t, int64(30), scheduler.GetAutoscaleInflightLoadResource([]int64{node1, node2}).MemoryBytes)
}

func (suite *TaskSuite) TestAutoscaleInflightLoadMetricsSkipEstimateFailure() {
	ctx := context.Background()
	scheduler := suite.newScheduler()
	collectionID := int64(2101)
	segmentID := int64(3101)
	targetNode := int64(1)
	targetNodeLabel := "1"
	targetNodeHost := "querynode-1"
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: targetNode, Hostname: targetNodeHost}))
	metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost).Set(0)
	metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost).Set(0)
	partitionID := int64(100)
	replica := meta.NewReplica(&querypb.Replica{
		CollectionID:  collectionID,
		ID:            21,
		ResourceGroup: meta.DefaultResourceGroupName,
		Nodes:         []int64{targetNode},
	}, typeutil.NewUniqueSet(targetNode))
	collection := utils.CreateTestCollection(collectionID, 1)
	suite.NoError(suite.meta.PutCollection(ctx, collection))
	suite.NoError(suite.meta.PutPartition(ctx, utils.CreateTestPartition(collectionID, partitionID)))
	suite.meta.Put(ctx, replica)

	segment := &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 100, LogSize: 80}}},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nil, []*datapb.SegmentInfo{segment}, nil)
	suite.NoError(suite.target.UpdateCollectionNextTarget(ctx, collectionID))
	suite.broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "field_100", DataType: schemapb.DataType_Int64}},
		},
	}, nil)
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, collectionID, segmentID).Return(map[int64][]*querypb.FieldIndexInfo{
		segmentID: {{
			FieldID:        100,
			IndexFilePaths: []string{"/path/that/must/not/exist/index_file"},
		}},
	}, nil)

	loadTask, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		collectionID,
		replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(targetNode, ActionTypeGrow, "", segmentID, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	loadTask.SetReason(ReasonLacksOfSegment)
	suite.NoError(scheduler.Add(loadTask))

	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).DiskBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))

	loadTask.Actions()[0].(*SegmentAction).Finish()
	scheduler.remove(loadTask)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).MemoryBytes)
	suite.Equal(int64(0), scheduler.GetAutoscaleInflightLoadResource([]int64{targetNode}).DiskBytes)
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadMemoryBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))
	suite.Equal(float64(0), testutil.ToFloat64(metrics.QueryCoordAutoscaleInflightLoadDiskBytes.WithLabelValues(targetNodeLabel, targetNodeHost)))
}

func (suite *TaskSuite) TestRemoveTaskWithError() {
	ctx := context.Background()
	scheduler := suite.newScheduler()

	mockTarget := meta.NewMockTargetManager(suite.T())
	mockTarget.EXPECT().UpdateCollectionNextTarget(mock.Anything, mock.Anything).Return(nil)
	scheduler.targetMgr = mockTarget

	coll := int64(1001)
	nodeID := int64(1)
	// add segment task for collection
	task1, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeGrow, "", 1, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	err = scheduler.Add(task1)
	suite.NoError(err)

	task1.Fail(merr.ErrSegmentNotFound)
	// when try to remove task with ErrSegmentNotFound, should trigger UpdateNextTarget
	scheduler.remove(task1)
	mockTarget.AssertExpectations(suite.T())

	// test remove task with ErrSegmentRequestResourceFailed
	task2, err := NewSegmentTask(
		ctx,
		10*time.Second,
		WrapIDSource(0),
		coll,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(nodeID, ActionTypeGrow, "", 1, querypb.DataScope_Historical, 100),
	)
	suite.NoError(err)
	err = scheduler.Add(task2)
	suite.NoError(err)

	task2.Fail(merr.ErrSegmentRequestResourceFailed)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ResourceExhaustionPenaltyDuration.Key, "3")
	scheduler.remove(task2)
	suite.True(suite.nodeMgr.IsResourceExhausted(nodeID))
	// expect the penalty duration is expired
	time.Sleep(3 * time.Second)
	suite.False(suite.nodeMgr.IsResourceExhausted(nodeID))
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskSuite))
}

func TestSegmentActionFinishStates(t *testing.T) {
	dist := meta.NewDistributionManager(nil)
	action := NewSegmentActionWithScope(1, ActionTypeGrow, "ch", 10, querypb.DataScope_Historical, 100)

	assert.False(t, action.IsFinished(dist))

	action.rpcReturned.Store(true)
	assert.False(t, action.IsFinished(dist))

	dist.SegmentDistManager.Update(1, utils.CreateTestSegment(100, 1, 10, 1, 1, "ch"))
	assert.True(t, action.IsFinished(dist))

	updateAction := NewSegmentActionWithScope(1, ActionTypeUpdate, "ch", 10, querypb.DataScope_Historical, 0)
	updateAction.rpcReturned.Store(true)
	assert.True(t, updateAction.IsFinished(dist))
}

func TestSegmentTaskDeltaSnapshotDefaults(t *testing.T) {
	snapshot := NewSegmentTaskDeltaSnapshot(nil, nil)
	assert.Equal(t, 0, snapshot.GetByNode(1))
	assert.Equal(t, 0, snapshot.GetByNodeInCollection(1))

	var nilSnapshot *SegmentTaskDeltaSnapshot
	assert.Equal(t, 0, nilSnapshot.GetByNode(1))
	assert.Equal(t, 0, nilSnapshot.GetByNodeInCollection(1))
}

func TestSegmentTaskDeltaDefensiveBranches(t *testing.T) {
	replica := newReplicaDefaultRG(10)
	segmentTask, err := NewSegmentTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		100,
		replica,
		commonpb.LoadPriority_LOW,
		NewSegmentActionWithScope(1, ActionTypeGrow, "ch", 10, querypb.DataScope_Historical, 100),
	)
	assert.NoError(t, err)
	segmentTask.SetID(1)

	delta := NewSegmentTaskDelta()
	delta.Add(segmentTask)
	delta.printDetailInfos()
	delta.Add(segmentTask)
	assert.Len(t, delta.records[segmentTask.ID()], 1)

	delta.Sub(segmentTask)
	delta.Sub(segmentTask)
	assert.Empty(t, delta.records)

	base := newBaseTask(context.Background(), WrapIDSource(0), 100, replica, "ch", "MalformedSegmentTask")
	base.SetID(2)
	base.actions = []Action{NewChannelAction(1, ActionTypeGrow, "ch")}
	malformedTask := &SegmentTask{baseTask: base, segmentID: 10}

	delta.Add(malformedTask)
	assert.Empty(t, delta.records[malformedTask.ID()])

	dist := meta.NewDistributionManager(nil)
	assert.False(t, segmentDeltaRecord{segmentID: 0}.isSegmentDistMatched(dist))
	assert.True(t, segmentDeltaRecord{nodeID: 1, segmentID: 10, actionType: ActionTypeUpdate}.isSegmentDistMatched(dist))
}

func TestChannelTaskDeltaDefensiveBranches(t *testing.T) {
	replica := newReplicaDefaultRG(10)
	channelTask, err := NewChannelTask(
		context.Background(),
		time.Second,
		WrapIDSource(0),
		100,
		replica,
		NewChannelAction(1, ActionTypeGrow, "ch"),
	)
	assert.NoError(t, err)
	channelTask.SetID(1)

	delta := NewChannelTaskDelta()
	delta.Add(channelTask)
	delta.printDetailInfos()
	delta.Add(channelTask)
	assert.Equal(t, 1, delta.Get(1, 100))

	delta.Sub(channelTask)
	delta.Sub(channelTask)
	assert.Equal(t, 0, delta.Get(1, 100))

	delta.taskIDRecords.Insert(channelTask.ID())
	delete(delta.data, int64(1))
	delta.Sub(channelTask)
	assert.Equal(t, -1, delta.Get(1, 100))
}

func TestMockSchedulerGetSegmentTaskDeltaSnapshot(t *testing.T) {
	nodes := []int64{1, 2}
	expected := NewSegmentTaskDeltaSnapshot(map[int64]int{1: 10}, map[int64]int{1: 5})

	mockScheduler := NewMockScheduler(t)
	mockScheduler.EXPECT().
		GetSegmentTaskDeltaSnapshot(nodes, int64(100)).
		Run(func(nodeIDs []int64, collectionID int64) {
			assert.Equal(t, nodes, nodeIDs)
			assert.Equal(t, int64(100), collectionID)
		}).
		Return(expected).
		Once()
	assert.Same(t, expected, mockScheduler.GetSegmentTaskDeltaSnapshot(nodes, 100))

	mockScheduler.EXPECT().
		GetSegmentTaskDeltaSnapshot(mock.Anything, int64(101)).
		RunAndReturn(func(nodeIDs []int64, collectionID int64) *SegmentTaskDeltaSnapshot {
			assert.Equal(t, nodes, nodeIDs)
			assert.Equal(t, int64(101), collectionID)
			return NewSegmentTaskDeltaSnapshot(map[int64]int{2: 20}, map[int64]int{2: 15})
		}).
		Once()

	snapshot := mockScheduler.GetSegmentTaskDeltaSnapshot(nodes, 101)
	assert.Equal(t, 20, snapshot.GetByNode(2))
	assert.Equal(t, 15, snapshot.GetByNodeInCollection(2))
}

func TestMockSchedulerGetSegmentTaskDeltaSnapshotPanicsWithoutReturn(t *testing.T) {
	mockScheduler := NewMockScheduler(t)
	mockScheduler.On("GetSegmentTaskDeltaSnapshot", mock.Anything, int64(100))

	assert.Panics(t, func() {
		mockScheduler.GetSegmentTaskDeltaSnapshot([]int64{1}, 100)
	})
}

func newReplicaDefaultRG(replicaID int64) *meta.Replica {
	return meta.NewReplica(
		&querypb.Replica{
			ID:            replicaID,
			ResourceGroup: meta.DefaultResourceGroupName,
		},
		typeutil.NewUniqueSet(),
	)
}

func (suite *TaskSuite) TestSegmentTaskShardLeaderID() {
	ctx := context.Background()
	timeout := 10 * time.Second

	// Create a segment task
	action := NewSegmentActionWithScope(1, ActionTypeGrow, "", 100, querypb.DataScope_Historical, 100)
	segmentTask, err := NewSegmentTask(
		ctx,
		timeout,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		action,
	)
	suite.NoError(err)

	// Test initial shard leader ID (should be -1)
	suite.Equal(int64(-1), segmentTask.ShardLeaderID())

	// Test setting shard leader ID
	expectedLeaderID := int64(123)
	segmentTask.SetShardLeaderID(expectedLeaderID)
	suite.Equal(expectedLeaderID, segmentTask.ShardLeaderID())

	// Test setting another value
	anotherLeaderID := int64(456)
	segmentTask.SetShardLeaderID(anotherLeaderID)
	suite.Equal(anotherLeaderID, segmentTask.ShardLeaderID())

	// Test with zero value
	segmentTask.SetShardLeaderID(0)
	suite.Equal(int64(0), segmentTask.ShardLeaderID())
}

func (suite *TaskSuite) TestExecutor_MoveSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	sourceNode := int64(2)
	targetNode := int64(3)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	suite.meta.PutCollection(ctx, utils.CreateTestCollection(suite.collection, 1))
	suite.meta.Put(ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{sourceNode, targetNode}))

	// Create move task with both grow and reduce actions to simulate TaskTypeMove
	segmentID := suite.loadSegments[0]
	growAction := NewSegmentAction(targetNode, ActionTypeGrow, channel.ChannelName, segmentID)
	reduceAction := NewSegmentAction(sourceNode, ActionTypeReduce, channel.ChannelName, segmentID)

	// Create a move task that has both actions
	moveTask, err := NewSegmentTask(
		ctx,
		timeout,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		growAction,
		reduceAction,
	)
	suite.NoError(err)

	// Mock cluster expectations for load segment
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(merr.Success(), nil)
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, mock.Anything, mock.Anything).Return(merr.Success(), nil)

	suite.broker.EXPECT().DescribeCollection(mock.Anything, suite.collection).RunAndReturn(func(ctx context.Context, i int64) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Schema: &schemapb.CollectionSchema{
				Name: "TestMoveSegmentTask",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "vec", DataType: schemapb.DataType_FloatVector},
				},
			},
		}, nil
	})
	suite.broker.EXPECT().ListIndexes(mock.Anything, suite.collection).Return([]*indexpb.IndexInfo{
		{
			CollectionID: suite.collection,
		},
	}, nil)
	suite.broker.EXPECT().GetSegmentInfo(mock.Anything, segmentID).Return([]*datapb.SegmentInfo{
		{
			ID:            segmentID,
			CollectionID:  suite.collection,
			PartitionID:   -1,
			InsertChannel: channel.ChannelName,
		},
	}, nil)
	suite.broker.EXPECT().GetIndexInfo(mock.Anything, suite.collection, segmentID).Return(nil, nil)

	// Set up distribution with leader view
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
		Status:       &querypb.LeaderViewStatus{Serviceable: true},
	}

	suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
		VchannelInfo: channel,
		Node:         targetNode,
		Version:      1,
		View:         view,
	})

	// Add segments to original node distribution for release
	segments := []*meta.Segment{
		{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            segmentID,
				CollectionID:  suite.collection,
				PartitionID:   1,
				InsertChannel: channel.ChannelName,
			},
		},
	}
	suite.dist.SegmentDistManager.Update(sourceNode, segments...)

	// Set up broker expectations
	segmentInfos := []*datapb.SegmentInfo{
		{
			ID:            segmentID,
			CollectionID:  suite.collection,
			PartitionID:   1,
			InsertChannel: channel.ChannelName,
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segmentInfos, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)

	// Test that move task sets shard leader ID during load step
	suite.Equal(TaskTypeMove, GetTaskType(moveTask))
	suite.Equal(int64(-1), moveTask.ShardLeaderID()) // Initial value

	// Set up task executor
	executor := NewExecutor(targetNode,
		suite.meta,
		suite.dist,
		suite.broker,
		suite.target,
		suite.cluster,
		suite.nodeMgr,
	)

	// Verify shard leader ID was set for load action in move task
	executor.executeSegmentAction(moveTask, 0)
	suite.Equal(targetNode, moveTask.ShardLeaderID())
	suite.NoError(moveTask.Err())
	suite.dist.SegmentDistManager.Update(targetNode, utils.CreateTestSegment(suite.collection, 1, segmentID, targetNode, 1, channel.ChannelName))
	suite.True(moveTask.actions[0].IsFinished(suite.dist))

	// expect release action will execute successfully
	executor.executeSegmentAction(moveTask, 1)
	suite.Equal(targetNode, moveTask.ShardLeaderID())
	suite.dist.SegmentDistManager.Update(sourceNode)
	suite.True(moveTask.actions[1].IsFinished(suite.dist))
	suite.NoError(moveTask.Err())

	// test shard leader change before release action
	newLeaderID := sourceNode
	view1 := &meta.LeaderView{
		ID:           newLeaderID,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
		Status:       &querypb.LeaderViewStatus{Serviceable: true},
		Version:      100,
	}

	suite.dist.ChannelDistManager.Update(newLeaderID, &meta.DmChannel{
		VchannelInfo: channel,
		Node:         newLeaderID,
		Version:      100,
		View:         view1,
	})

	// expect release action will skip and task will fail
	suite.broker.ExpectedCalls = nil
	executor.executeSegmentAction(moveTask, 1)
	suite.True(moveTask.actions[1].IsFinished(suite.dist))
	suite.ErrorContains(moveTask.Err(), "shard leader changed")
}

func (suite *TaskSuite) TestLeaderTaskStaleByRONode() {
	ctx := context.Background()
	leaderNode := int64(1)
	workerNode := int64(3)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Test case 1: LeaderAction should NOT be stale when worker node is RO but leader is RW
	// This is the fix for issue #46737: the checkStale should use leaderID instead of Node() for LeaderAction
	suite.Run("WorkerRONodeLeaderRW", func() {
		// Create replica with worker node (node 3) as RO node, leader node (node 1) as RW
		replicaWithRONode := meta.NewReplica(&querypb.Replica{
			ID:            suite.replica.GetID(),
			CollectionID:  suite.collection,
			Nodes:         []int64{1, 2}, // RW nodes (leader is RW)
			RoNodes:       []int64{3},    // RO nodes (worker is RO)
			ResourceGroup: meta.DefaultResourceGroupName,
		}, typeutil.NewUniqueSet(1, 2))
		suite.meta.Put(suite.ctx, replicaWithRONode)

		// Set up channel distribution
		suite.dist.ChannelDistManager.Update(leaderNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    leaderNode,
			Version: 1,
			View:    &meta.LeaderView{ID: leaderNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		// Create a LeaderAction that syncs segment from worker (RO node) to leader (RW node)
		// leaderID=1 (RW), workerID=3 (RO)
		// Before fix: checkStale used action.Node() which returns workerNode (3, RO), causing false stale
		// After fix: checkStale uses leaderID (1, RW), task should NOT be stale
		task := NewLeaderSegmentTask(
			ctx,
			WrapIDSource(0),
			suite.collection,
			replicaWithRONode,
			leaderNode,
			NewLeaderAction(leaderNode, workerNode, ActionTypeGrow, channel.GetChannelName(), suite.loadSegments[0], 0),
		)

		// Add task should succeed because leader node is RW
		err := suite.scheduler.Add(task)
		suite.NoError(err)

		// Task should NOT be stale because we check leader node status, not worker node
		suite.Equal(TaskStatusStarted, task.Status())
		suite.NoError(task.Err())

		// Clean up
		suite.scheduler.remove(task)
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	})

	// Test case 2: LeaderAction should be stale when leader node becomes RO
	suite.Run("LeaderRONode", func() {
		// Set up channel distribution with leader on node 1
		suite.dist.ChannelDistManager.Update(leaderNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    leaderNode,
			Version: 1,
			View:    &meta.LeaderView{ID: leaderNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		// Create task with original replica (all RW nodes)
		task := NewLeaderSegmentTask(
			ctx,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			leaderNode,
			NewLeaderAction(leaderNode, workerNode, ActionTypeGrow, channel.GetChannelName(), suite.loadSegments[0], 0),
		)

		// Add task should succeed
		err := suite.scheduler.Add(task)
		suite.NoError(err)

		// Now change replica to make leader node (node 1) as RO node
		replicaWithLeaderRO := meta.NewReplica(&querypb.Replica{
			ID:            suite.replica.GetID(),
			CollectionID:  suite.collection,
			Nodes:         []int64{2, 3}, // RW nodes
			RoNodes:       []int64{1},    // Leader becomes RO
			ResourceGroup: meta.DefaultResourceGroupName,
		}, typeutil.NewUniqueSet(2, 3))
		suite.meta.Put(suite.ctx, replicaWithLeaderRO)

		// Dispatch will trigger promote which calls checkStale
		suite.dispatchAndWait(leaderNode)

		// Task should be canceled because leader node is RO
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.ErrorContains(task.Err(), "node becomes ro node")

		// Restore original replica
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	})

	// Test case 3: LeaderAction with Reduce type should NOT be affected by RO node
	suite.Run("LeaderReduceActionNotAffected", func() {
		// Create replica with leader node as RO
		replicaWithLeaderRO := meta.NewReplica(&querypb.Replica{
			ID:            suite.replica.GetID(),
			CollectionID:  suite.collection,
			Nodes:         []int64{2, 3}, // RW nodes
			RoNodes:       []int64{1},    // Leader is RO
			ResourceGroup: meta.DefaultResourceGroupName,
		}, typeutil.NewUniqueSet(2, 3))
		suite.meta.Put(suite.ctx, replicaWithLeaderRO)

		// Set up channel distribution
		suite.dist.ChannelDistManager.Update(leaderNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    leaderNode,
			Version: 1,
			View:    &meta.LeaderView{ID: leaderNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		// Create LeaderAction with Reduce type
		task := NewLeaderSegmentTask(
			ctx,
			WrapIDSource(0),
			suite.collection,
			replicaWithLeaderRO,
			leaderNode,
			NewLeaderAction(leaderNode, workerNode, ActionTypeReduce, channel.GetChannelName(), suite.releaseSegments[0], 0),
		)

		// Add task should succeed because Reduce action is not affected by RO node check
		err := suite.scheduler.Add(task)
		suite.NoError(err)

		// Clean up
		suite.scheduler.remove(task)
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	})
}

func (suite *TaskSuite) TestTaskStaleByRONode() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Test case 1: Task stale due to target node becomes RO node
	suite.Run("RONode", func() {
		// Set up channel distribution first with RW replica
		suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    targetNode,
			Version: 1,
			View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		// Create task with original replica (all RW nodes)
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), suite.loadSegments[0]),
		)
		suite.NoError(err)

		// Add task should succeed
		err = suite.scheduler.Add(task)
		suite.NoError(err)

		// Now change replica to make node 3 as RO node (simulating node state change)
		replicaWithRONode := meta.NewReplica(&querypb.Replica{
			ID:            suite.replica.GetID(),
			CollectionID:  suite.collection,
			Nodes:         []int64{1, 2}, // RW nodes
			RoNodes:       []int64{3},    // RO node
			ResourceGroup: meta.DefaultResourceGroupName,
		}, typeutil.NewUniqueSet(1, 2))
		suite.meta.Put(suite.ctx, replicaWithRONode)

		// Dispatch will trigger promote which calls checkStale
		suite.dispatchAndWait(targetNode)

		// Task should be canceled due to RO node
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.ErrorContains(task.Err(), "node becomes ro node")

		// Restore original replica for other tests
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	})

	// Test case 2: Task stale due to target node becomes RO SQ node
	suite.Run("ROSQNode", func() {
		// Set up channel distribution
		suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    targetNode,
			Version: 1,
			View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		// Create task with original replica (all RW nodes)
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), suite.loadSegments[1]),
		)
		suite.NoError(err)

		// Add task should succeed
		err = suite.scheduler.Add(task)
		suite.NoError(err)

		// Now change replica to make node 3 as RO SQ node
		replicaWithROSQNode := meta.NewReplica(&querypb.Replica{
			ID:            suite.replica.GetID(),
			CollectionID:  suite.collection,
			Nodes:         []int64{1, 2}, // RW nodes
			RoSqNodes:     []int64{3},    // RO SQ node
			ResourceGroup: meta.DefaultResourceGroupName,
		}, typeutil.NewUniqueSet(1, 2))
		suite.meta.Put(suite.ctx, replicaWithROSQNode)

		// Dispatch will trigger promote which calls checkStale
		suite.dispatchAndWait(targetNode)

		// Task should be canceled due to RO SQ node
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.ErrorContains(task.Err(), "node becomes ro node")

		// Restore original replica
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	})

	// Test case 3: ActionTypeReduce should not be affected by RO node
	suite.Run("ReduceActionNotAffected", func() {
		// Create replica with node 3 as RO node
		replicaWithRONode := meta.NewReplica(&querypb.Replica{
			ID:            suite.replica.GetID(),
			CollectionID:  suite.collection,
			Nodes:         []int64{1, 2}, // RW nodes
			RoNodes:       []int64{3},    // RO node
			ResourceGroup: meta.DefaultResourceGroupName,
		}, typeutil.NewUniqueSet(1, 2))
		suite.meta.Put(suite.ctx, replicaWithRONode)

		// Set up channel distribution
		suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    targetNode,
			Version: 1,
			View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		// Create a segment task with Reduce action targeting the RO node
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			replicaWithRONode,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeReduce, channel.GetChannelName(), suite.releaseSegments[0]),
		)
		suite.NoError(err)

		// Add task should succeed because Reduce action is not affected by RO node check
		err = suite.scheduler.Add(task)
		suite.NoError(err)

		// Clean up
		suite.scheduler.remove(task)
		suite.meta.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
	})
}

func (suite *TaskSuite) TestTaskStaleBySegmentInDist() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	// Test case: Grow task should be stale when segment already exists in dist
	suite.Run("SegmentAlreadyInDist", func() {
		// Set up channel distribution
		suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    targetNode,
			Version: 1,
			View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		segmentID := suite.loadSegments[0]

		// Create a Grow task for the segment
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segmentID),
		)
		suite.NoError(err)

		// Add task should succeed (segment not in dist yet)
		err = suite.scheduler.Add(task)
		suite.NoError(err)

		// Now simulate segment being loaded: add segment to dist
		suite.dist.SegmentDistManager.Update(targetNode, utils.CreateTestSegment(suite.collection, 1, segmentID, targetNode, 1, channel.ChannelName))

		// Dispatch will trigger promote which calls checkStale
		suite.dispatchAndWait(targetNode)

		// Task should be canceled because segment is already in dist
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.ErrorContains(task.Err(), "segment already loaded in dist")

		// Clean up
		suite.dist.SegmentDistManager.Update(targetNode)
	})

	// Test case: Reduce task should not be affected by segment in dist check
	suite.Run("ReduceTaskNotAffected", func() {
		// Set up channel distribution
		suite.dist.ChannelDistManager.Update(targetNode, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: suite.collection,
				ChannelName:  channel.ChannelName,
			},
			Node:    targetNode,
			Version: 1,
			View:    &meta.LeaderView{ID: targetNode, CollectionID: suite.collection, Channel: channel.ChannelName, Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})

		segmentID := suite.releaseSegments[0]

		// Add segment to dist first
		suite.dist.SegmentDistManager.Update(targetNode, utils.CreateTestSegment(suite.collection, 1, segmentID, targetNode, 1, channel.ChannelName))

		// Create a Reduce task for the segment (should not be affected by dist check)
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			commonpb.LoadPriority_LOW,
			NewSegmentAction(targetNode, ActionTypeReduce, channel.GetChannelName(), segmentID),
		)
		suite.NoError(err)

		// Add task should succeed - Reduce task is not affected by "segment in dist" check
		err = suite.scheduler.Add(task)
		suite.NoError(err)

		// Clean up without dispatching (to avoid mock issues)
		suite.scheduler.remove(task)
		suite.dist.SegmentDistManager.Update(targetNode)
	})
}

func (suite *TaskSuite) TestTaskQueueRangePriority() {
	queue := newTaskQueue()
	nodeID := int64(1)

	taskLow, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch-0", 400),
	)
	suite.NoError(err)
	taskLow.SetID(31)
	taskLow.SetPriority(TaskPriorityLow)

	taskNormal, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch-0", 401),
	)
	suite.NoError(err)
	taskNormal.SetID(32)
	taskNormal.SetPriority(TaskPriorityNormal)

	taskHigh, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch-0", 402),
	)
	suite.NoError(err)
	taskHigh.SetID(33)
	taskHigh.SetPriority(TaskPriorityHigh)

	queue.Add(taskLow)
	queue.Add(taskHigh)
	queue.Add(taskNormal)

	suite.Equal(3, queue.Len())

	var visited []Priority
	queue.Range(func(t Task) bool {
		visited = append(visited, t.Priority())
		return true
	})
	suite.Equal([]Priority{TaskPriorityHigh, TaskPriorityNormal, TaskPriorityLow}, visited)

	queue.Remove(taskHigh)
	suite.Equal(2, queue.Len())
}

func (suite *TaskSuite) TestNodeTaskQueueNodeBucketing() {
	queue := newNodeTaskQueue()

	task1, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(1, ActionTypeGrow, "ch-0", 100),
	)
	suite.NoError(err)
	task1.SetID(1)

	task2, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(2, ActionTypeGrow, "ch-0", 101),
	)
	suite.NoError(err)
	task2.SetID(2)

	task3, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(1, ActionTypeGrow, "ch-0", 102),
	)
	suite.NoError(err)
	task3.SetID(3)

	queue.Add(task1)
	queue.Add(task2)
	queue.Add(task3)

	suite.Equal(3, queue.Len())
	suite.Equal(2, queue.LenByNode(1))
	suite.Equal(1, queue.LenByNode(2))
	suite.Equal(0, queue.LenByNode(999))

	var node1Tasks []Task
	queue.RangeByNode(1, func(task Task) bool {
		node1Tasks = append(node1Tasks, task)
		return true
	})
	suite.Equal(2, len(node1Tasks))

	var node2Tasks []Task
	queue.RangeByNode(2, func(task Task) bool {
		node2Tasks = append(node2Tasks, task)
		return true
	})
	suite.Equal(1, len(node2Tasks))
	suite.Equal(int64(2), node2Tasks[0].ID())

	queue.Remove(task1)
	suite.Equal(2, queue.Len())
	suite.Equal(1, queue.LenByNode(1))
	suite.Equal(1, queue.LenByNode(2))
}

func (suite *TaskSuite) TestNodeTaskQueueMoveTaskDualNode() {
	queue := newNodeTaskQueue()

	destNode := int64(1)
	srcNode := int64(2)
	segmentID := int64(200)

	task, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(destNode, ActionTypeGrow, "ch-0", segmentID),
		NewSegmentAction(srcNode, ActionTypeReduce, "ch-0", segmentID),
	)
	suite.NoError(err)
	task.SetID(10)

	queue.Add(task)

	suite.Equal(1, queue.Len())
	suite.Equal(1, queue.LenByNode(destNode))
	suite.Equal(1, queue.LenByNode(srcNode))

	var destTasks []Task
	queue.RangeByNode(destNode, func(t Task) bool {
		destTasks = append(destTasks, t)
		return true
	})
	suite.Equal(1, len(destTasks))
	suite.Equal(int64(10), destTasks[0].ID())

	var srcTasks []Task
	queue.RangeByNode(srcNode, func(t Task) bool {
		srcTasks = append(srcTasks, t)
		return true
	})
	suite.Equal(1, len(srcTasks))
	suite.Equal(int64(10), srcTasks[0].ID())

	queue.Remove(task)
	suite.Equal(0, queue.Len())
	suite.Equal(0, queue.LenByNode(destNode))
	suite.Equal(0, queue.LenByNode(srcNode))
}

func (suite *TaskSuite) TestNodeTaskQueueLeaderActionDualNode() {
	queue := newNodeTaskQueue()

	leaderID := int64(1)
	workerID := int64(2)
	segmentID := int64(300)

	action := NewLeaderAction(leaderID, workerID, ActionTypeGrow, "ch-0", segmentID, 1)
	task := NewLeaderSegmentTask(suite.ctx, WrapIDSource(0), suite.collection, suite.replica, leaderID, action)
	task.SetID(20)

	queue.Add(task)

	suite.Equal(1, queue.Len())
	suite.Equal(1, queue.LenByNode(workerID))
	suite.Equal(1, queue.LenByNode(leaderID))

	var workerTasks []Task
	queue.RangeByNode(workerID, func(t Task) bool {
		workerTasks = append(workerTasks, t)
		return true
	})
	suite.Equal(1, len(workerTasks))
	suite.Equal(int64(20), workerTasks[0].ID())

	var leaderTasks []Task
	queue.RangeByNode(leaderID, func(t Task) bool {
		leaderTasks = append(leaderTasks, t)
		return true
	})
	suite.Equal(1, len(leaderTasks))
	suite.Equal(int64(20), leaderTasks[0].ID())

	queue.Remove(task)
	suite.Equal(0, queue.Len())
	suite.Equal(0, queue.LenByNode(workerID))
	suite.Equal(0, queue.LenByNode(leaderID))
}

func (suite *TaskSuite) TestNodeTaskQueueRangeByNodePriority() {
	queue := newNodeTaskQueue()
	nodeID := int64(1)

	taskLow, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch-0", 400),
	)
	suite.NoError(err)
	taskLow.SetID(31)
	taskLow.SetPriority(TaskPriorityLow)

	taskNormal, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch-0", 401),
	)
	suite.NoError(err)
	taskNormal.SetID(32)
	taskNormal.SetPriority(TaskPriorityNormal)

	taskHigh, err := NewSegmentTask(
		suite.ctx,
		5*time.Second,
		WrapIDSource(0),
		suite.collection,
		suite.replica,
		commonpb.LoadPriority_LOW,
		NewSegmentAction(nodeID, ActionTypeGrow, "ch-0", 402),
	)
	suite.NoError(err)
	taskHigh.SetID(33)
	taskHigh.SetPriority(TaskPriorityHigh)

	queue.Add(taskLow)
	queue.Add(taskHigh)
	queue.Add(taskNormal)

	suite.Equal(3, queue.LenByNode(nodeID))

	var visited []Priority
	queue.RangeByNode(nodeID, func(t Task) bool {
		visited = append(visited, t.Priority())
		return true
	})
	suite.Equal([]Priority{TaskPriorityHigh, TaskPriorityNormal, TaskPriorityLow}, visited)
}
