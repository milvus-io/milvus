package tasks

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestScheduler(t *testing.T) {
	paramtable.Init()
	t.Run("user-task-polling", func(t *testing.T) {
		testScheduler(t, newUserTaskPollingPolicy())
	})
	t.Run("fifo", func(t *testing.T) {
		testScheduler(t, newFIFOPolicy())
	})
}

func testScheduler(t *testing.T, policy schedulePolicy) {
	// start a new scheduler
	scheduler := newScheduler(policy)
	go scheduler.Start(context.Background())

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
