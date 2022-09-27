package querynode

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/timerecord"
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

func TestBaseReadTask(t *testing.T) {
	suite.Run(t, new(baseReadTaskSuite))
}
