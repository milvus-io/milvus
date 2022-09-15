package dist

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
)

type DistControllerTestSuite struct {
	suite.Suite
	controller    *Controller
	mockCluster   *session.MockCluster
	mockScheduler *task.MockScheduler
}

func (suite *DistControllerTestSuite) SetupTest() {
	Params.Init()

	suite.mockCluster = session.NewMockCluster(suite.T())
	nodeManager := session.NewNodeManager()
	distManager := meta.NewDistributionManager()
	targetManager := meta.NewTargetManager()
	suite.mockScheduler = task.NewMockScheduler(suite.T())
	suite.controller = NewDistController(suite.mockCluster, nodeManager, distManager, targetManager, suite.mockScheduler)
}

func (suite *DistControllerTestSuite) TestStart() {
	dispatchCalled := atomic.NewBool(false)
	suite.mockCluster.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Return(
		&querypb.GetDataDistributionResponse{NodeID: 1},
		nil,
	)
	suite.mockScheduler.EXPECT().Dispatch(int64(1)).Run(func(node int64) { dispatchCalled.Store(true) })
	suite.controller.StartDistInstance(context.TODO(), 1)
	suite.Eventually(
		func() bool {
			return dispatchCalled.Load()
		},
		10*time.Second,
		500*time.Millisecond,
	)

	suite.controller.Remove(1)
	dispatchCalled.Store(false)
	suite.Never(
		func() bool {
			return dispatchCalled.Load()
		},
		3*time.Second,
		500*time.Millisecond,
	)
}

func (suite *DistControllerTestSuite) TestStop() {
	suite.controller.StartDistInstance(context.TODO(), 1)
	called := atomic.NewBool(false)
	suite.mockCluster.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Maybe().Return(
		&querypb.GetDataDistributionResponse{NodeID: 1},
		nil,
	).Run(func(args mock.Arguments) {
		called.Store(true)
	})
	suite.mockScheduler.EXPECT().Dispatch(mock.Anything).Maybe()
	suite.controller.Stop()
	called.Store(false)
	suite.Never(
		func() bool {
			return called.Load()
		},
		3*time.Second,
		500*time.Millisecond,
	)
}

func (suite *DistControllerTestSuite) TestSyncAll() {
	suite.controller.StartDistInstance(context.TODO(), 1)
	suite.controller.StartDistInstance(context.TODO(), 2)

	calledSet := typeutil.NewConcurrentSet[int64]()
	suite.mockCluster.EXPECT().GetDataDistribution(mock.Anything, mock.Anything, mock.Anything).Call.Return(
		func(ctx context.Context, nodeID int64, req *querypb.GetDataDistributionRequest) *querypb.GetDataDistributionResponse {
			return &querypb.GetDataDistributionResponse{
				NodeID: nodeID,
			}
		},
		nil,
	).Run(func(args mock.Arguments) {
		calledSet.Insert(args[1].(int64))
	})
	suite.mockScheduler.EXPECT().Dispatch(mock.Anything)

	// stop inner loop
	suite.controller.handlers[1].stop()
	suite.controller.handlers[2].stop()

	calledSet.Remove(1)
	calledSet.Remove(2)

	suite.controller.SyncAll(context.TODO())
	suite.Eventually(
		func() bool {
			return calledSet.Contain(1) && calledSet.Contain(2)
		},
		5*time.Second,
		500*time.Millisecond,
	)
}

func TestDistControllerSuite(t *testing.T) {
	suite.Run(t, new(DistControllerTestSuite))
}
