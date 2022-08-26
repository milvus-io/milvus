package task

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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
	replica         int64
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
	store   meta.Store
	meta    *meta.Meta
	dist    *meta.DistributionManager
	target  *meta.TargetManager
	broker  *meta.MockBroker
	nodeMgr *session.NodeManager
	cluster *session.MockCluster

	// Test object
	scheduler *taskScheduler
}

func (suite *TaskSuite) SetupSuite() {
	Params.Init()
	suite.collection = 1000
	suite.replica = 10
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

func (suite *TaskSuite) SetupTest() {
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)

	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)
	suite.store = meta.NewMetaStore(suite.kv)
	suite.meta = meta.NewMeta(RandomIncrementIDAllocator(), suite.store)
	suite.dist = meta.NewDistributionManager()
	suite.target = meta.NewTargetManager()
	suite.broker = meta.NewMockBroker(suite.T())
	suite.nodeMgr = session.NewNodeManager()
	suite.cluster = session.NewMockCluster(suite.T())

	suite.scheduler = suite.newScheduler()
}

func (suite *TaskSuite) BeforeTest(suiteName, testName string) {
	for node := range suite.distributions {
		suite.nodeMgr.Add(session.NewNodeInfo(node, "localhost"))
	}

	switch testName {
	case "TestSubscribeChannelTask",
		"TestLoadSegmentTask",
		"TestSegmentTaskStale",
		"TestMoveSegmentTask":
		suite.meta.PutCollection(&meta.Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  suite.collection,
				ReplicaNumber: 1,
				Status:        querypb.LoadStatus_Loading,
			},
		})
		suite.meta.ReplicaManager.Put(
			utils.CreateTestReplica(suite.replica, suite.collection, []int64{1, 2, 3}))
	}
}

func (suite *TaskSuite) TestSubscribeChannelTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partitions := []int64{100, 101}

	// Expect
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, suite.collection).
		Return(&schemapb.CollectionSchema{
			Name: "TestSubscribeChannelTask",
		}, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, suite.collection).
		Return([]int64{100, 101}, nil)
	channels := make([]*datapb.VchannelInfo, 0, len(suite.subChannels))
	for _, channel := range suite.subChannels {
		channels = append(channels, &datapb.VchannelInfo{
			CollectionID:        suite.collection,
			ChannelName:         channel,
			UnflushedSegmentIds: []int64{suite.growingSegments[channel]},
		})
	}
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
	// for _, partition := range partitions {
	// 	suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, suite.collection, partition).
	// 		Return(channels, nil, nil)
	// }
	suite.cluster.EXPECT().WatchDmChannels(mock.Anything, targetNode, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test subscribe channel task
	tasks := []Task{}
	for _, channel := range suite.subChannels {
		suite.target.AddDmChannel(meta.DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID:        suite.collection,
			ChannelName:         channel,
			UnflushedSegmentIds: []int64{suite.growingSegments[channel]},
		}))
		task := NewChannelTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.AssertTaskNum(0, len(suite.subChannels), len(suite.subChannels), 0)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(len(suite.subChannels), 0, len(suite.subChannels), 0)

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(targetNode + 1)
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

func (suite *TaskSuite) TestUnsubscribeChannelTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(1)

	// Expect
	suite.cluster.EXPECT().UnsubDmChannel(mock.Anything, targetNode, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test unsubscribe channel task
	tasks := []Task{}
	for _, channel := range suite.unsubChannels {
		suite.target.AddDmChannel(meta.DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel,
		}))
		task := NewChannelTask(
			ctx,
			timeout,
			0,
			suite.collection,
			-1,
			NewChannelAction(targetNode, ActionTypeReduce, channel),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
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

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(targetNode + 1)
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
		ChannelName:  Params.CommonCfg.RootCoordDml + "-test",
	}

	// Expect
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, suite.collection).Return(&schemapb.CollectionSchema{
		Name: "TestSubscribeChannelTask",
	}, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, suite.collection).Return([]int64{100, 101}, nil)
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
	// suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, suite.collection, partition).
	// 	Return([]*datapb.VchannelInfo{channel}, nil, nil)
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test load segment task
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	tasks := []Task{}
	for _, segment := range suite.loadSegments {
		suite.target.AddSegment(&datapb.SegmentInfo{
			ID:            segment,
			CollectionID:  suite.collection,
			PartitionID:   partition,
			InsertChannel: channel.ChannelName,
		})
		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, segment),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(targetNode + 1)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	// Dist contains channels
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Segments:     map[int64]int64{},
	}
	for _, segment := range suite.loadSegments {
		view.Segments[segment] = targetNode
	}
	suite.dist.LeaderViewManager.Update(targetNode, view)
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for _, task := range tasks {
		suite.Equal(TaskStatusSucceeded, task.Status())
		suite.NoError(task.Err())
	}
}

func (suite *TaskSuite) TestReleaseSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml + "-test",
	}

	// Expect
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, targetNode, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test load segment task
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]int64),
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
		view.Segments[segment] = targetNode
		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeReduce, segment),
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

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(targetNode + 1)
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

func (suite *TaskSuite) TestMoveSegmentTask() {
	ctx := context.Background()
	timeout := 10 * time.Second
	leader := int64(1)
	sourceNode := int64(2)
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml + "-test",
	}

	// Expect
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, suite.collection).Return(&schemapb.CollectionSchema{
		Name: "TestMoveSegmentTask",
	}, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, suite.collection).Return([]int64{100, 101}, nil)
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
	// suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, suite.collection, partition).
	// 	Return([]*datapb.VchannelInfo{channel}, nil, nil)
	suite.cluster.EXPECT().LoadSegments(mock.Anything, leader, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)
	suite.cluster.EXPECT().ReleaseSegments(mock.Anything, leader, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test move segment task
	suite.dist.ChannelDistManager.Update(leader, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	view := &meta.LeaderView{
		ID:           leader,
		CollectionID: suite.collection,
		Channel:      channel.ChannelName,
		Segments:     make(map[int64]int64),
	}
	tasks := []Task{}
	segments := make([]*meta.Segment, 0)
	for _, segment := range suite.moveSegments {
		segments = append(segments,
			utils.CreateTestSegment(suite.collection, partition, segment, sourceNode, 1, channel.ChannelName))
		suite.target.AddSegment(&datapb.SegmentInfo{
			ID:            segment,
			CollectionID:  suite.collection,
			PartitionID:   partition,
			InsertChannel: channel.ChannelName,
		})
		view.Segments[segment] = sourceNode

		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, segment),
			NewSegmentAction(sourceNode, ActionTypeReduce, segment),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	suite.dist.SegmentDistManager.Update(sourceNode, segments...)
	suite.dist.LeaderViewManager.Update(leader, view)
	segmentsNum := len(suite.moveSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(leader)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(-1)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks, target node contains the segment
	view = view.Clone()
	for _, segment := range suite.moveSegments {
		view.Segments[segment] = targetNode
	}
	suite.dist.LeaderViewManager.Update(leader, view)
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

func (suite *TaskSuite) TestTaskCanceled() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(1)

	// Expect
	suite.cluster.EXPECT().UnsubDmChannel(mock.Anything, targetNode, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test unsubscribe channel task
	tasks := []Task{}
	for _, channel := range suite.unsubChannels {
		suite.target.AddDmChannel(meta.DmChannelFromVChannel(&datapb.VchannelInfo{
			CollectionID: suite.collection,
			ChannelName:  channel,
		}))
		task := NewChannelTask(
			ctx,
			timeout,
			0,
			suite.collection,
			-1,
			NewChannelAction(targetNode, ActionTypeReduce, channel),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
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

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(targetNode + 1)
	suite.AssertTaskNum(1, 0, 1, 0)

	// Cancel first task
	tasks[0].Cancel()
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for i, task := range tasks {
		if i == 0 {
			suite.Equal(TaskStatusCanceled, task.Status())
			suite.ErrorIs(task.Err(), ErrTaskCanceled)
		} else {
			suite.Equal(TaskStatusSucceeded, task.Status())
			suite.NoError(task.Err())
		}
	}
}

func (suite *TaskSuite) TestSegmentTaskStale() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)
	partition := int64(100)
	channel := &datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  Params.CommonCfg.RootCoordDml + "-test",
	}

	// Expect
	suite.broker.EXPECT().GetCollectionSchema(mock.Anything, suite.collection).Return(&schemapb.CollectionSchema{
		Name: "TestSegmentTaskStale",
	}, nil)
	suite.broker.EXPECT().GetPartitions(mock.Anything, suite.collection).Return([]int64{100, 101}, nil)
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
	// suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, suite.collection, partition).
	// 	Return([]*datapb.VchannelInfo{channel}, nil, nil)
	suite.cluster.EXPECT().LoadSegments(mock.Anything, targetNode, mock.Anything).Return(utils.WrapStatus(commonpb.ErrorCode_Success, ""), nil)

	// Test load segment task
	suite.meta.ReplicaManager.Put(
		createReplica(suite.collection, targetNode))
	suite.dist.ChannelDistManager.Update(targetNode, meta.DmChannelFromVChannel(&datapb.VchannelInfo{
		CollectionID: suite.collection,
		ChannelName:  channel.ChannelName,
	}))
	tasks := []Task{}
	for _, segment := range suite.loadSegments {
		suite.target.AddSegment(&datapb.SegmentInfo{
			ID:            segment,
			CollectionID:  suite.collection,
			PartitionID:   partition,
			InsertChannel: channel.ChannelName,
		})
		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, segment),
		)
		tasks = append(tasks, task)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	segmentsNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentsNum, 0, segmentsNum)

	// Process tasks
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Other nodes' HB can't trigger the procedure of tasks
	suite.dispatchAndWait(targetNode + 1)
	suite.AssertTaskNum(segmentsNum, 0, 0, segmentsNum)

	// Process tasks done
	// Dist contains channels, first task stale
	view := &meta.LeaderView{
		ID:           targetNode,
		CollectionID: suite.collection,
		Segments:     map[int64]int64{},
	}
	for _, segment := range suite.loadSegments[1:] {
		view.Segments[segment] = targetNode
	}
	suite.dist.LeaderViewManager.Update(targetNode, view)
	suite.target.RemoveSegment(suite.loadSegments[0])
	suite.dispatchAndWait(targetNode)
	suite.AssertTaskNum(0, 0, 0, 0)

	for i, task := range tasks {
		if i == 0 {
			suite.Equal(TaskStatusStale, task.Status())
			suite.ErrorIs(ErrTaskStale, task.Err())
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
		task := NewChannelTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		task.SetPriority(TaskPriorityNormal)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}

	// Task with the same replica and segment,
	// but without higher priority can't be added
	for _, channel := range suite.subChannels {
		task := NewChannelTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		task.SetPriority(TaskPriorityNormal)
		err := suite.scheduler.Add(task)
		suite.ErrorIs(err, ErrConflictTaskExisted)
		task.SetPriority(TaskPriorityLow)
		err = suite.scheduler.Add(task)
		suite.ErrorIs(err, ErrConflictTaskExisted)
	}

	// Replace the task with one with higher priority
	for _, channel := range suite.subChannels {
		task := NewChannelTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewChannelAction(targetNode, ActionTypeGrow, channel),
		)
		task.SetPriority(TaskPriorityHigh)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	channelNum := len(suite.subChannels)
	suite.AssertTaskNum(0, channelNum, channelNum, 0)
}

func (suite *TaskSuite) TestSegmentTaskReplace() {
	ctx := context.Background()
	timeout := 10 * time.Second
	targetNode := int64(3)

	for _, segment := range suite.loadSegments {
		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, segment),
		)
		task.SetPriority(TaskPriorityNormal)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}

	// Task with the same replica and segment,
	// but without higher priority can't be added
	for _, segment := range suite.loadSegments {
		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, segment),
		)
		task.SetPriority(TaskPriorityNormal)
		err := suite.scheduler.Add(task)
		suite.ErrorIs(err, ErrConflictTaskExisted)
		task.SetPriority(TaskPriorityLow)
		err = suite.scheduler.Add(task)
		suite.ErrorIs(err, ErrConflictTaskExisted)
	}

	// Replace the task with one with higher priority
	for _, segment := range suite.loadSegments {
		task := NewSegmentTask(
			ctx,
			timeout,
			0,
			suite.collection,
			suite.replica,
			NewSegmentAction(targetNode, ActionTypeGrow, segment),
		)
		task.SetPriority(TaskPriorityHigh)
		err := suite.scheduler.Add(task)
		suite.NoError(err)
	}
	segmentNum := len(suite.loadSegments)
	suite.AssertTaskNum(0, segmentNum, 0, segmentNum)
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
	suite.scheduler.Dispatch(node)
	for {
		count := 0
		suite.scheduler.executor.executingActions.Range(func(key, value any) bool {
			count++
			return true
		})
		if count == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
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
	return &meta.Replica{
		Replica: &querypb.Replica{
			ID:           rand.Int63()/2 + 1,
			CollectionID: collection,
			Nodes:        nodes,
		},
		Nodes: typeutil.NewUniqueSet(nodes...),
	}
}

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskSuite))
}
