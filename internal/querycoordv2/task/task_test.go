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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type distribution struct {
	NodeID   int64
	channels typeutil.Set[string]
	segments typeutil.Set[int64]
}

type TaskSuite struct {
	suite.Suite
	testutils.EmbedEtcdUtil

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
	addressList, err := suite.SetupEtcd()
	suite.Require().NoError(err)
	params := paramtable.Get()
	params.Save(params.EtcdCfg.Endpoints.Key, strings.Join(addressList, ","))

	suite.collection = 1000
	suite.replica = newReplicaDefaultRG(10)
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
	suite.TearDownEmbedEtcd()
	paramtable.Get().Reset(paramtable.Get().EtcdCfg.Endpoints.Key)
}

func (suite *TaskSuite) SetupTest() {
	config := GenerateEtcdConfig()
	suite.ctx = context.Background()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)

	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())
	suite.store = querycoord.NewCatalog(suite.kv)
	suite.meta = meta.NewMeta(RandomIncrementIDAllocator(), suite.store, session.NewNodeManager())
	suite.dist = meta.NewDistributionManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.target = meta.NewTargetManager(suite.broker, suite.meta)
	suite.nodeMgr = session.NewNodeManager()
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
		"TestLoadSegmentTaskFailed",
		"TestSegmentTaskStale",
		"TestTaskCanceled",
		"TestMoveSegmentTask",
		"TestMoveSegmentTaskStale",
		"TestSubmitDuplicateLoadSegmentTask",
		"TestSubmitDuplicateSubscribeChannelTask",
		"TestLeaderTaskSet",
		"TestLeaderTaskRemove",
		"TestNoExecutor":
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
		suite.meta.ReplicaManager.Put(suite.ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3}))
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
	views := make([]*meta.LeaderView, 0)
	for _, channel := range suite.subChannels {
		views = append(views, &meta.LeaderView{
			ID:           targetNode,
			CollectionID: suite.collection,
			Channel:      channel,
		})
	}
	suite.dist.LeaderViewManager.Update(targetNode, views...)
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

	views := make([]*meta.LeaderView, 0)
	for _, channel := range suite.subChannels {
		views = append(views, &meta.LeaderView{
			ID:           targetNode,
			CollectionID: suite.collection,
			Channel:      channel,
		})
	}
	suite.dist.LeaderViewManager.Update(targetNode, views...)

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
	suite.dist.LeaderViewManager.Update(targetNode, &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      suite.unsubChannels[0],
	})
	suite.AssertTaskNum(0, len(suite.unsubChannels), len(suite.unsubChannels), 0)

	// ProcessTasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 1, 0)

	// Update dist
	suite.dist.LeaderViewManager.Update(targetNode)
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
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	suite.dist.LeaderViewManager.Update(targetNode, utils.CreateTestLeaderView(targetNode, suite.collection, channel.ChannelName, map[int64]int64{}, map[int64]*meta.Segment{}))
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
	suite.assertExecutedFlagChan(targetNode)
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
	suite.dist.LeaderViewManager.Update(targetNode, view)
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
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	suite.dist.LeaderViewManager.Update(targetNode, utils.CreateTestLeaderView(targetNode, suite.collection, channel.ChannelName, map[int64]int64{}, map[int64]*meta.Segment{}))
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
	suite.dist.LeaderViewManager.Update(targetNode, view)
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
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	suite.dist.LeaderViewManager.Update(targetNode, utils.CreateTestLeaderView(targetNode, suite.collection, channel.ChannelName, map[int64]int64{}, map[int64]*meta.Segment{}))
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
			NewSegmentAction(targetNode, ActionTypeReduce, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.dist.SegmentDistManager.Update(targetNode, segments...)
	suite.dist.LeaderViewManager.Update(targetNode, view)

	segmentsNum := len(suite.releaseSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	suite.dist.LeaderViewManager.Update(targetNode)
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
			NewSegmentActionWithScope(targetNode, ActionTypeReduce, "", segment, querypb.DataScope_Streaming),
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
	suite.dist.ChannelDistManager.Update(leader, meta.DmChannelFromVChannel(vchannel))
	view := &meta.LeaderView{
		ID:           leader,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
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
	suite.dist.LeaderViewManager.Update(leader, view)
	for _, task := range tasks {
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}

	segmentsNum := len(suite.moveSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(leader)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks, target node contains the segment
	view = view.Clone()
	for _, segment := range suite.moveSegments {
		view.Segments[segment] = &querypb.SegmentDist{NodeID: targetNode, Version: 0}
	}
	distSegments := lo.Map(segmentInfos, func(info *datapb.SegmentInfo, _ int) *meta.Segment {
		return meta.SegmentFromInfo(info)
	})

	suite.dist.LeaderViewManager.Update(leader, view)
	suite.dist.SegmentDistManager.Update(targetNode, distSegments...)
	// First action done, execute the second action
	suite.dispatchAndWait(leader)
	// Check second action
	suite.dispatchAndWait(leader)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestMoveSegmentTaskStale() {
	ctx := context.Background()
	timeout := 10 * time.Second
	leader := int64(1)
	sourceNode := int64(2)
	targetNode := int64(3)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml.GetValue() + "-test",
	}

	vchannel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}
	suite.dist.ChannelDistManager.Update(leader, meta.DmChannelFromVChannel(vchannel))
	view := &meta.LeaderView{
		ID:           leader,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]*querypb.SegmentDist),
	}
	tasks := []Task{}
	segmentInfos := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.moveSegments {
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
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
			NewSegmentAction(sourceNode, ActionTypeReduce, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{vchannel}, segmentInfos, nil)
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)
	suite.target.UpdateCollectionCurrentTarget(ctx, suite.collection)
	suite.dist.LeaderViewManager.Update(leader, view)
	for _, task := range tasks {
		err := suite.scheduler.Add(task)
		suite.Error(err)
		suite.Equal(TaskStatusCanceled, task.Status())
		suite.Error(task.Err())
	}
	suite.AssertTaskNum(0, 0, 0, 0)
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
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	suite.dist.LeaderViewManager.Update(targetNode, utils.CreateTestLeaderView(targetNode, suite.collection, channel.ChannelName, map[int64]int64{}, map[int64]*meta.Segment{}))
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
	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(suite.collection, partition))
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

func (suite *TaskSuite) TestSegmentTaskStale() {
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
				Name: "TestSegmentTaskStale",
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
	for _, segment := range suite.loadSegments[1:] {
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
	suite.meta.ReplicaManager.Put(ctx, createReplica(suite.collection, targetNode))
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	suite.dist.LeaderViewManager.Update(targetNode, utils.CreateTestLeaderView(targetNode, suite.collection, channel.ChannelName, map[int64]int64{}, map[int64]*meta.Segment{}))
	tasks := []Task{}
	for _, segment := range suite.loadSegments {
		task, err := NewSegmentTask(
			ctx,
			timeout,
			WrapIDSource(0),
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, channel.GetChannelName(), segment),
		)
		suite.NoError(err)
		tasks = append(tasks, task)
		err = suite.scheduler.Add(task)
		suite.NoError(err)
	}

	segments := make([]*datapb.SegmentInfo, 0)
	for _, segment := range suite.loadSegments[1:] {
		segments = append(segments, &datapb.SegmentInfo{
			ID:            segment,
			PartitionID:   2,
			InsertChannel: channel.GetChannelName(),
		})
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, suite.collection).Return([]*datapb.VchannelInfo{channel}, segments, nil)

	suite.meta.CollectionManager.PutPartition(ctx, utils.CreateTestPartition(suite.collection, 2))
	suite.target.UpdateCollectionNextTarget(ctx, suite.collection)

	// process done
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(1, 0, 0, 1)

	// task removed
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for i, task := range tasks {
		if i == 0 {
			suite.Equal(TaskStatusCanceled, task.Status())
			suite.Error(task.Err())
		} else {
			suite.Equal(TaskStatusSucceeded, task.Status())
			suite.NoError(task.Err())
		}
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
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
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
	}
	suite.dist.LeaderViewManager.Update(targetNode, view)

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
	suite.dist.LeaderViewManager.Update(targetNode, view)
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

	segmentTask, err := NewSegmentTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(segmentTask)

	channelAction := NewChannelAction(0, 0, "fake-channel1")
	segmentTask, err = NewSegmentTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, channelAction)
	suite.ErrorIs(err, merr.ErrParameterInvalid)
	suite.Nil(segmentTask)

	segmentAction1 := NewSegmentAction(0, 0, "", 0)
	segmentAction2 := NewSegmentAction(0, 0, "", 1)

	segmentTask, err = NewSegmentTask(context.TODO(), 5*time.Second, WrapIDSource(0), 0, meta.NilReplica, segmentAction1, segmentAction2)
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

	suite.meta.ReplicaManager.Put(ctx, utils.CreateTestReplica(suite.replica.GetID(), suite.collection, []int64{1, 2, 3, -1}))

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
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

func (suite *TaskSuite) AssertTaskNum(process, wait, channel, segment int) {
	scheduler := suite.scheduler

	suite.Equal(process, scheduler.processQueue.Len())
	suite.Equal(wait, scheduler.waitQueue.Len())
	suite.Len(scheduler.segmentTasks, segment)
	suite.Len(scheduler.channelTasks, channel)
	suite.Equal(len(scheduler.tasks), process+wait)
	suite.Equal(len(scheduler.tasks), segment+channel)
}

func (suite *TaskSuite) dispatchAndWait(node int64) {
	timeout := 10 * time.Second
	suite.scheduler.Dispatch(node)
	var keys []any
	count := 0
	for start := time.Now(); time.Since(start) < timeout; {
		count = 0
		keys = make([]any, 0)

		for _, executor := range suite.scheduler.executors {
			executor.executingTasks.Range(func(taskIndex string) bool {
				keys = append(keys, taskIndex)
				count++
				return true
			})
		}

		if count == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	suite.FailNow("executor hangs in executing tasks", "count=%d keys=%+v", count, keys)
}

func (suite *TaskSuite) assertExecutedFlagChan(targetNode int64) {
	flagChan := suite.scheduler.GetExecutedFlag(targetNode)
	if flagChan != nil {
		select {
		case <-flagChan:
		default:
			suite.FailNow("task not executed")
		}
	}
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
	suite.dist.LeaderViewManager.Update(targetNode, view)

	segmentsNum := len(suite.releaseSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	view.Segments = make(map[int64]*querypb.SegmentDist)
	suite.dist.LeaderViewManager.Update(targetNode, view)
	// Process tasks done
	// suite.dist.LeaderViewManager.Update(targetNode)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
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
	collectionID := int64(1)
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

	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: collectionID,
		Channel:      channel,
		Segments: map[int64]*querypb.SegmentDist{
			1: {},
			2: {},
			3: {},
		},
	})
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:                 1,
		CollectionID:       collectionID,
		Channel:            channel,
		UnServiceableError: merr.ErrSegmentLack,
	})
	task, err := NewChannelTask(context.Background(),
		10*time.Second,
		WrapIDSource(2),
		collectionID,
		meta.NilReplica,
		NewChannelAction(1, ActionTypeGrow, channel),
		NewChannelAction(2, ActionTypeReduce, channel),
	)
	suite.NoError(err)

	// new delegator distribution hasn't updated, block balance
	suite.scheduler.preProcess(task)
	suite.Equal(0, task.step)

	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: collectionID,
		Channel:      channel,
		Segments: map[int64]*querypb.SegmentDist{
			1: {},
			2: {},
			3: {},
		},
	})

	// new delegator distribution updated, task step up
	suite.scheduler.preProcess(task)
	suite.Equal(1, task.step)

	suite.dist.LeaderViewManager.Update(2)
	// old delegator removed
	suite.scheduler.preProcess(task)
	suite.Equal(2, task.step)
}

func (suite *TaskSuite) TestBalanceChannelWithL0SegmentTask() {
	ctx := context.Background()
	collectionID := int64(1)
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
			Level:         datapb.SegmentLevel_L0,
		},
		{
			ID:            2,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			Level:         datapb.SegmentLevel_L0,
		},
		{
			ID:            3,
			CollectionID:  collectionID,
			PartitionID:   partitionID,
			InsertChannel: channel,
			Level:         datapb.SegmentLevel_L0,
		},
	}
	suite.meta.PutCollection(ctx, utils.CreateTestCollection(collectionID, 1), utils.CreateTestPartition(collectionID, 1))
	suite.broker.ExpectedCalls = nil
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return([]*datapb.VchannelInfo{vchannel}, segments, nil)
	suite.target.UpdateCollectionNextTarget(ctx, collectionID)
	suite.target.UpdateCollectionCurrentTarget(ctx, collectionID)
	suite.target.UpdateCollectionNextTarget(ctx, collectionID)

	suite.dist.LeaderViewManager.Update(2, &meta.LeaderView{
		ID:           2,
		CollectionID: collectionID,
		Channel:      channel,
		Segments: map[int64]*querypb.SegmentDist{
			1: {NodeID: 2},
			2: {NodeID: 2},
			3: {NodeID: 2},
		},
	})
	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: collectionID,
		Channel:      channel,
		Segments: map[int64]*querypb.SegmentDist{
			1: {NodeID: 2},
			2: {NodeID: 2},
			3: {NodeID: 2},
		},
		UnServiceableError: merr.ErrSegmentLack,
	})

	task, err := NewChannelTask(context.Background(),
		10*time.Second,
		WrapIDSource(2),
		collectionID,
		meta.NewReplica(
			&querypb.Replica{
				ID: 1,
			},
			typeutil.NewUniqueSet(),
		),
		NewChannelAction(1, ActionTypeGrow, channel),
		NewChannelAction(2, ActionTypeReduce, channel),
	)
	suite.NoError(err)

	// l0 hasn't been loaded into delegator, block balance
	suite.scheduler.preProcess(task)
	suite.Equal(0, task.step)

	suite.dist.LeaderViewManager.Update(1, &meta.LeaderView{
		ID:           1,
		CollectionID: collectionID,
		Channel:      channel,
		Segments: map[int64]*querypb.SegmentDist{
			1: {NodeID: 1},
			2: {NodeID: 1},
			3: {NodeID: 1},
		},
	})

	// new delegator distribution updated, task step up
	suite.scheduler.preProcess(task)
	suite.Equal(1, task.step)

	suite.dist.LeaderViewManager.Update(2)
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

	mockTarget := meta.NewMockTargetManager(suite.T())
	mockTarget.EXPECT().GetSealedSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datapb.SegmentInfo{
		NumOfRows: 100,
	})
	scheduler.targetMgr = mockTarget

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
		NewSegmentActionWithScope(nodeID, ActionTypeGrow, "", segmentID, querypb.DataScope_Historical),
	)
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
		NewSegmentActionWithScope(nodeID2, ActionTypeGrow, "", segmentID2, querypb.DataScope_Historical),
	)
	suite.NoError(err)
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
	err = scheduler.Add(task4)
	suite.NoError(err)

	// check task delta with collectionID and nodeID
	suite.Equal(100, scheduler.GetSegmentTaskDelta(nodeID, coll))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID, coll))
	suite.Equal(100, scheduler.GetSegmentTaskDelta(nodeID2, coll2))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID2, coll2))

	// check task delta with collectionID=-1
	suite.Equal(100, scheduler.GetSegmentTaskDelta(nodeID, -1))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID, -1))
	suite.Equal(100, scheduler.GetSegmentTaskDelta(nodeID2, -1))
	suite.Equal(1, scheduler.GetChannelTaskDelta(nodeID2, -1))

	// check task delta with nodeID=-1
	suite.Equal(100, scheduler.GetSegmentTaskDelta(-1, coll))
	suite.Equal(1, scheduler.GetChannelTaskDelta(-1, coll))
	suite.Equal(100, scheduler.GetSegmentTaskDelta(-1, coll))
	suite.Equal(1, scheduler.GetChannelTaskDelta(-1, coll))

	// check task delta with nodeID=-1 and collectionID=-1
	suite.Equal(200, scheduler.GetSegmentTaskDelta(-1, -1))
	suite.Equal(2, scheduler.GetChannelTaskDelta(-1, -1))
	suite.Equal(200, scheduler.GetSegmentTaskDelta(-1, -1))
	suite.Equal(2, scheduler.GetChannelTaskDelta(-1, -1))
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskSuite))
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
