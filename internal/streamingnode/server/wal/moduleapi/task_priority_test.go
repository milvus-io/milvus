package moduleapi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestDataTaskPressurePromotion(t *testing.T) {
	for _, beforeSubmit := range []bool{false, true} {
		scheduler := nodescheduler.New(4)
		started := make(chan struct{}, 4)
		hold := taskFunc(func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			return nil
		})
		for range 3 {
			scheduler.Submit(hold, nodescheduler.WithPriority(PriorityData))
		}
		for range 3 {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("data task did not start")
			}
		}
		var priority DataTaskPriority
		if beforeSubmit {
			priority.Promote()
		}
		priority.Submit(scheduler, hold)
		if !beforeSubmit {
			require.Never(t, func() bool { return len(started) != 0 }, 20*time.Millisecond, time.Millisecond)
			priority.Promote()
		}
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("pressure promotion failed to admit queued data task")
		}
		scheduler.Close()
		priority.Promote() // A completed handle rejects the update safely.
	}
}

func TestDataTaskConcurrentSubmitAndPromotion(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	for range 100 {
		var priority DataTaskPriority
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			priority.Submit(scheduler, taskFunc(func(context.Context) error { return nil }))
		}()
		go func() { defer wg.Done(); priority.Promote() }()
		wg.Wait()
		require.True(t, priority.urgent)
		require.NotNil(t, priority.handle)
	}
}

type taskFunc func(context.Context) error

func (f taskFunc) Execute(ctx context.Context) error { return f(ctx) }
