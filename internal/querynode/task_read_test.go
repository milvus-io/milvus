package querynode

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type baseReadTaskSuite struct {
	suite.Suite

	qs    *queryShard
	tsafe *MockTSafeReplicaInterface

	task *baseReadTask
}

func (s *baseReadTaskSuite) SetupSuite() {
	meta := newMockReplicaInterface()
	meta.getCollectionByIDFunc = func(collectionID UniqueID) (*Collection, error) {
		return &Collection{
			id: defaultCollectionID,
		}, nil
	}
	rcm := &mocks.ChunkManager{}
	lcm := &mocks.ChunkManager{}

	tsafe := &MockTSafeReplicaInterface{}
	s.tsafe = tsafe

	qs, err := newQueryShard(context.Background(), defaultCollectionID, defaultDMLChannel, defaultReplicaID, nil, meta, tsafe, lcm, rcm, false)
	s.Require().NoError(err)

	s.qs = qs
}

func (s *baseReadTaskSuite) TearDownSuite() {
	s.qs.Close()
}

func (s *baseReadTaskSuite) SetupTest() {
	s.task = &baseReadTask{QS: s.qs, tr: timerecord.NewTimeRecorder("baseReadTaskTest")}
}

func (s *baseReadTaskSuite) TearDownTest() {
	s.task = nil
}

func (s *baseReadTaskSuite) TestPreExecute() {
	ctx := context.Background()
	err := s.task.PreExecute(ctx)
	s.Assert().NoError(err)
	s.Assert().Equal(TaskStepPreExecute, s.task.step)
}

func (s *baseReadTaskSuite) TestExecute() {
	ctx := context.Background()
	err := s.task.Execute(ctx)
	s.Assert().NoError(err)
	s.Assert().Equal(TaskStepExecute, s.task.step)
}

func (s *baseReadTaskSuite) TestTimeout() {
	s.Run("background ctx", func() {
		s.task.ctx = context.Background()
		s.Assert().False(s.task.Timeout())
	})

	s.Run("context canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s.task.ctx = ctx

		s.Assert().True(s.task.Timeout())
	})

	s.Run("deadline exceeded", func() {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Minute))
		defer cancel()
		s.task.ctx = ctx

		s.Assert().True(s.task.Timeout())
	})
}

func (s *baseReadTaskSuite) TestTimeoutError() {
	s.Run("background ctx", func() {
		s.task.ctx = context.Background()
		s.Assert().Nil(s.task.TimeoutError())
	})

	s.Run("context canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		s.task.ctx = ctx

		s.Assert().ErrorIs(s.task.TimeoutError(), context.Canceled)
	})

	s.Run("deadline exceeded", func() {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Minute))
		defer cancel()
		s.task.ctx = ctx

		s.Assert().ErrorIs(s.task.TimeoutError(), context.DeadlineExceeded)
	})
}

func (s *baseReadTaskSuite) TestReady() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.task.ctx = ctx
	baseTime := time.Now()
	serviceable := tsoutil.ComposeTSByTime(baseTime, 0)
	s.tsafe.EXPECT().getTSafe(mock.AnythingOfType("string")).Return(serviceable, nil)
	s.Run("lag too large", func() {
		tooLargeGuarantee := baseTime.Add(Params.QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)).Add(time.Second)
		guaranteeTs := tsoutil.ComposeTSByTime(tooLargeGuarantee, 0)
		s.task.GuaranteeTimestamp = guaranteeTs
		s.task.DataScope = querypb.DataScope_Historical

		ready, err := s.task.Ready()
		s.False(ready)
		s.Error(err)
		s.ErrorIs(err, ErrTsLagTooLarge)
	})

	s.Run("not ready", func() {
		guarantee := baseTime.Add(Params.QueryNodeCfg.MaxTimestampLag.GetAsDuration(time.Second)).Add(-time.Second)
		guaranteeTs := tsoutil.ComposeTSByTime(guarantee, 0)
		s.task.GuaranteeTimestamp = guaranteeTs
		s.task.DataScope = querypb.DataScope_Historical

		ready, err := s.task.Ready()
		s.False(ready)
		s.NoError(err)
	})

	s.Run("ready", func() {
		guarantee := baseTime.Add(-time.Second)
		guaranteeTs := tsoutil.ComposeTSByTime(guarantee, 0)
		s.task.GuaranteeTimestamp = guaranteeTs
		s.task.DataScope = querypb.DataScope_Historical

		ready, err := s.task.Ready()
		s.True(ready)
		s.NoError(err)
	})
}

func TestBaseReadTask(t *testing.T) {
	suite.Run(t, new(baseReadTaskSuite))
}
