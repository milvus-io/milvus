package qnview

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestSegmentResourcePreparationPriorityAndEligibility(t *testing.T) {
	runResourceSchedulingTest(t, "only eligible unmet bindings compete", func(t *testing.T, f *resourceSchedulingFixture) {
		low := f.r.bindings[0]
		low.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: -20}
		high := f.addBinding(2)
		high.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: -10}
		require.EqualValues(t, -10, f.r.pendingPriorityLocked())
		var order []int32
		f.onPrepare = func(b *segmentBinding) error { order = append(order, b.view.req.Meta.SchemaVersion); return nil }
		f.wake()
		f.runNext()
		require.True(t, high.reported)
		require.False(t, low.reported)
		require.EqualValues(t, -20, f.r.pendingPriorityLocked(), "ready high binding no longer contributes")
		f.runNext()
		require.Equal(t, []int32{2, 1}, order)
		require.Zero(t, f.r.pendingPriorityLocked())

		delayed := f.addBinding(3)
		delayed.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: 20}
		delayed.retryAfter = time.Now().Add(time.Hour)
		other := f.addBinding(4)
		other.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: -15}
		require.EqualValues(t, -15, f.r.pendingPriorityLocked())
		f.wake()
		f.runNext()
		require.True(t, other.reported)
		require.False(t, delayed.reported)
	})
}

func TestSegmentResourcePriorityBindingFairness(t *testing.T) {
	runResourceSchedulingTest(t, "a low binding eventually receives a turn", func(t *testing.T, f *resourceSchedulingFixture) {
		low := f.r.bindings[0]
		low.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: -20}
		high := f.addBinding(2)
		high.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: 10}
		found := false
		for range 9 {
			selected, _ := f.r.nextBindingLocked()
			if selected == low {
				found = true
				break
			}
		}
		require.True(t, found)
	})
}

func TestSegmentResourceQueuedPriorityWithdrawal(t *testing.T) {
	runResourceSchedulingTest(t, "queued resource follows remaining dependencies", func(t *testing.T, f *resourceSchedulingFixture) {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		started, release := make(chan struct{}), make(chan struct{})
		scheduler.Submit(schedulerTaskFunc(func(ctx context.Context) error {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		}))
		<-started
		f.r.manager.scheduler = scheduler
		low := f.r.bindings[0]
		low.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: -20}
		high := f.addBinding(2)
		high.view.req.Meta.Control = &viewpb.QueryViewControl{PreparationPriority: 10}
		order := make(chan string, 2)
		f.onPrepare = func(*segmentBinding) error { order <- "resource"; return nil }
		f.wake()
		other := scheduler.Submit(schedulerTaskFunc(func(context.Context) error { order <- "other"; return nil }), nodescheduler.WithPriority(-15))
		f.r.manager.mu.Lock()
		high.view.failed = true
		f.r.wakeLocked()
		handle := f.r.taskHandle
		f.r.manager.mu.Unlock()
		close(release)
		require.NoError(t, other.Wait(t.Context()))
		require.NoError(t, handle.Wait(t.Context()))
		require.Equal(t, "other", <-order)
		require.Equal(t, "resource", <-order)
	})
}
