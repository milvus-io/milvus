package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestScheduler(t *testing.T) {
	paramtable.Init()
	t.Run("user-task-polling", func(t *testing.T) {
		testScheduler(t, newUserTaskPollingPolicy())
	})
	t.Run("fifo", func(t *testing.T) {
		testScheduler(t, newFIFOPolicy())
	})
	t.Run("scheduler_not_working", func(t *testing.T) {
		scheduler := newScheduler(newFIFOPolicy())

		task := newMockTask(mockTaskConfig{
			nq:          1,
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				return nil
			},
		})

		err := scheduler.Add(task)
		assert.Error(t, err)

		scheduler.Stop()

		err = scheduler.Add(task)
		assert.Error(t, err)
	})

	suite.Run(t, new(SchedulerSuite))
}

func testScheduler(t *testing.T, policy schedulePolicy) {
	// start a new scheduler
	scheduler := newScheduler(policy)
	scheduler.Start()

	var cnt atomic.Int32
	n := 100
	nq := 0
	userN := 10
	// Test Push
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", rand.Int31n(int32(userN)))
		task := newMockTask(mockTaskConfig{
			username:    username,
			nq:          int64(i),
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				cnt.Inc()
				return nil
			},
		})
		nq += i
		assert.NoError(t, scheduler.Add(task))
		total := int(scheduler.GetWaitingTaskTotal())
		nqNow := int(scheduler.GetWaitingTaskTotalNQ())
		assert.LessOrEqual(t, total, i)
		assert.LessOrEqual(t, nqNow, nq)
	}
	time.Sleep(2 * time.Second)
	assert.Equal(t, cnt.Load(), int32(n))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotal()))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotalNQ()))

	// Test Push
	for i := 1; i <= n; i++ {
		username := fmt.Sprintf("user_%d", rand.Int31n(int32(userN)))
		task := newMockTask(mockTaskConfig{
			username:    username,
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				cnt.Inc()
				return nil
			},
		})
		assert.NoError(t, scheduler.Add(task))
		total := int(scheduler.GetWaitingTaskTotal())
		nqNow := int(scheduler.GetWaitingTaskTotalNQ())
		assert.LessOrEqual(t, total, i)
		assert.LessOrEqual(t, nqNow, i)
	}

	time.Sleep(2 * time.Second)
	assert.Equal(t, cnt.Load(), int32(2*n))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotal()))
	assert.Equal(t, 0, int(scheduler.GetWaitingTaskTotalNQ()))
}

type SchedulerSuite struct {
	suite.Suite
}

func (s *SchedulerSuite) TestConsumeRecvChan() {
	s.Run("consume_chan_closed", func() {
		ch := make(chan addTaskReq, 10)
		close(ch)
		scheduler := &scheduler{
			policy:           newFIFOPolicy(),
			receiveChan:      ch,
			execChan:         make(chan Task),
			pool:             conc.NewPool[any](10, conc.WithPreAlloc(true)),
			schedulerCounter: schedulerCounter{},
			lifetime:         lifetime.NewLifetime(lifetime.Initializing),
		}

		task := newMockTask(mockTaskConfig{
			nq:          1,
			executeCost: 10 * time.Millisecond,
			execution: func(ctx context.Context) error {
				return nil
			},
		})

		s.NotPanics(func() {
			scheduler.consumeRecvChan(addTaskReq{
				task: task,
				err:  make(chan error, 1),
			}, maxReceiveChanBatchConsumeNum)
		})
	})
}
