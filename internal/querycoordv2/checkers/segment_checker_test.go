package checkers

import (
	"context"
	"testing"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type SegmentCheckerTestSuite struct {
	suite.Suite
	kv      *etcdkv.EtcdKV
	checker *SegmentChecker
}

func (suite *SegmentCheckerTestSuite) SetupSuite() {
	Params.Init()
}

func (suite *SegmentCheckerTestSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(&config)
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath)

	// meta
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	testMeta := meta.NewMeta(idAllocator, store)

	distManager := meta.NewDistributionManager()
	targetManager := meta.NewTargetManager()

	balancer := suite.createMockBalancer()
	suite.checker = NewSegmentChecker(testMeta, distManager, targetManager, balancer)
}

func (suite *SegmentCheckerTestSuite) TearDownTest() {
	suite.kv.Close()
}

func (suite *SegmentCheckerTestSuite) createMockBalancer() balance.Balance {
	balancer := balance.NewMockBalancer(suite.T())
	balancer.EXPECT().AssignSegment(mock.Anything, mock.Anything).Maybe().Return(func(segments []*meta.Segment, nodes []int64) []balance.SegmentAssignPlan {
		plans := make([]balance.SegmentAssignPlan, 0, len(segments))
		for i, s := range segments {
			plan := balance.SegmentAssignPlan{
				Segment:   s,
				From:      -1,
				To:        nodes[i%len(nodes)],
				ReplicaID: -1,
			}
			plans = append(plans, plan)
		}
		return plans
	})
	return balancer
}

func (suite *SegmentCheckerTestSuite) TestLoadSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set target
	checker.targetMgr.AddSegment(utils.CreateTestSegmentInfo(1, 1, 1, "test-insert-channel"))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, []int64{}))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeGrow, action.Type())
	suite.EqualValues(1, action.SegmentID())

}

func (suite *SegmentCheckerTestSuite) TestReleaseSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{}, []int64{}))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.SegmentID())
}

func (suite *SegmentCheckerTestSuite) TestReleaseRepeatedSegments() {
	checker := suite.checker
	// set meta
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	// set target
	checker.targetMgr.AddSegment(utils.CreateTestSegmentInfo(1, 1, 1, "test-insert-channel"))

	// set dist
	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 2}, []int64{}))
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 1, 1, 2, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(1, action.Node())

	// test less version exist on leader
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{1: 1}, []int64{}))
	tasks = checker.Check(context.TODO())
	suite.Len(tasks, 0)
}

func (suite *SegmentCheckerTestSuite) TestReleaseGrowingSegments() {
	checker := suite.checker
	// segment3 is compacted from segment2, and node2 has growing segments 2 and 3. checker should generate
	// 2 tasks to reduce segment 2 and 3.
	checker.meta.CollectionManager.PutCollection(utils.CreateTestCollection(1, 1))
	checker.meta.ReplicaManager.Put(utils.CreateTestReplica(1, 1, []int64{1, 2}))

	segment := utils.CreateTestSegmentInfo(1, 1, 3, "test-insert-channel")
	segment.CompactionFrom = append(segment.CompactionFrom, 2)
	checker.targetMgr.AddSegment(segment)

	checker.dist.ChannelDistManager.Update(2, utils.CreateTestChannel(1, 2, 1, "test-insert-channel"))
	checker.dist.LeaderViewManager.Update(2, utils.CreateTestLeaderView(2, 1, "test-insert-channel", map[int64]int64{3: 2}, []int64{2, 3}))
	checker.dist.SegmentDistManager.Update(2, utils.CreateTestSegment(1, 1, 3, 2, 1, "test-insert-channel"))

	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 2)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(2, action.SegmentID())
	suite.EqualValues(2, action.Node())

	suite.Len(tasks[1].Actions(), 1)
	action, ok = tasks[1].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(1, tasks[1].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(3, action.SegmentID())
	suite.EqualValues(2, action.Node())
}

func (suite *SegmentCheckerTestSuite) TestReleaseDroppedSegments() {
	checker := suite.checker
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 1, 1, 1, "test-insert-channel"))
	tasks := checker.Check(context.TODO())
	suite.Len(tasks, 1)
	suite.Len(tasks[0].Actions(), 1)
	action, ok := tasks[0].Actions()[0].(*task.SegmentAction)
	suite.True(ok)
	suite.EqualValues(-1, tasks[0].ReplicaID())
	suite.Equal(task.ActionTypeReduce, action.Type())
	suite.EqualValues(1, action.SegmentID())
	suite.EqualValues(1, action.Node())
}

func TestSegmentCheckerSuite(t *testing.T) {
	suite.Run(t, new(SegmentCheckerTestSuite))
}
