package transformlogbuffer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Hold the only replay slot before SyncUp. Other tasks on this PChannel must
// finish cancellation without releasing this registration or receiving data.
func occupyCatchupWorker(t *testing.T, owner *Buffer) {
	t.Helper()
	buf := newVChannelBuffer(owner, "p", "p_1v0", 0)
	reg := newRegistration(buf, &fakeSegment{id: 1, vchannel: buf.vchannel})
	buf.pending[1] = reg
	waiting := make(chan struct{})
	var once sync.Once
	var original func(*vchannelBuffer, *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error)
	patch := mockey.Mock((*vchannelBuffer).nextCatchupBatch).To(func(b *vchannelBuffer, r *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error) {
		batch, done, notify, err := original(b, r)
		if r == reg {
			once.Do(func() { close(waiting) })
		}
		return batch, done, notify, err
	}).Origin(&original).Build()
	done := startCatchup(reg)
	t.Cleanup(func() {
		reg.Unregister()
		require.ErrorIs(t, awaitCatchup(t, done), context.Canceled)
		waitDrainIdle(t, owner)
		patch.UnPatch()
	})
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach SyncUp wait")
	}
}

func TestQueuedCatchupTerminatesWithoutReplaySlot(t *testing.T) {
	for _, reason := range []string{"context", "unregister", "buffer failure", "failure before submission"} {
		t.Run(reason, func(t *testing.T) {
			owner := newBuffer(nil, 1)
			occupyCatchupWorker(t, owner)
			buf := newVChannelBuffer(owner, "p", "p_2v0", 0)
			reg := newRegistration(buf, &fakeSegment{id: 2, vchannel: buf.vchannel})
			defer reg.Unregister()
			buf.pending[2] = reg
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := merr.Wrap(merr.ErrDataIntegrity, "retained transform chunk is corrupt")
			if reason == "failure before submission" {
				buf.fail(failure)
			}
			done := make(chan error, 2)
			reg.Catchup(ctx, func(err error) {
				// Completion must be outside queue/buffer/Apply locks and permit
				// the same re-entrant cleanup performed by the segment manager.
				owner.mu.Lock()
				queued := owner.drainQueues["p"].tasks.Len()
				owner.mu.Unlock()
				require.Zero(t, queued)
				reg.Unregister()
				done <- err
			})
			want := failure
			switch reason {
			case "context":
				cancel()
				want = context.Canceled
			case "unregister":
				reg.Unregister()
				want = context.Canceled
			case "buffer failure":
				buf.fail(failure)
			}
			require.ErrorIs(t, awaitCatchup(t, done), want)
			owner.mu.Lock()
			require.Zero(t, owner.drainQueues["p"].tasks.Len())
			require.Equal(t, 1, owner.drainQueues["p"].workers, "A must still occupy its slot")
			owner.mu.Unlock()
			require.Empty(t, buf.pending)
			require.Empty(t, done, "completion must run exactly once")
		})
	}
}

func TestCatchupDequeueRacesTermination(t *testing.T) {
	owner := newBuffer(nil, 1)
	for i := 0; i < 100; i++ {
		buf := newVChannelBuffer(owner, "p", "p_1v0", 0)
		reg := newRegistration(buf, &fakeSegment{id: 1, vchannel: buf.vchannel})
		buf.pending[1] = reg
		buf.syncUp = true
		ctx, cancel := context.WithCancel(context.Background())
		var calls atomic.Int32
		done := make(chan error, 4)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, f := range []func(){
			func() { reg.Catchup(ctx, func(err error) { calls.Add(1); done <- err }) },
			cancel,
			reg.Unregister,
			func() { buf.fail(merr.ErrDataIntegrity) },
		} {
			wg.Add(1)
			go func(f func()) { defer wg.Done(); <-start; f() }(f)
		}
		close(start)
		wg.Wait()
		cancel()
		_ = awaitCatchup(t, done) // Completion may win before cancellation.
		waitDrainIdle(t, owner)
		require.EqualValues(t, 1, calls.Load())
		require.Empty(t, done)
		require.Empty(t, buf.pending)
		require.Empty(t, buf.live)
	}
}

func TestQueuedCatchupReleasesPhysicalSegment(t *testing.T) {
	for _, failBuffer := range []bool{false, true} {
		name := "view released"
		if failBuffer {
			name = "buffer failed"
		}
		t.Run(name, func(t *testing.T) {
			buffer := newBuffer(newFakeStreamManager(), 1)
			occupyCatchupWorker(t, buffer)
			segment := &fakeSegment{id: 2, partitionID: 10, vchannel: "p_2v0"}
			load := mockey.Mock((*poisonPhysicalLoader).Load).Return(segment, nil).Build()
			defer load.UnPatch()
			metadata := mockey.Mock((*poisonLoadInfoStream).Subscribe).To(func(_ *poisonLoadInfoStream, opt qnview.SegmentLoadInfoSubscriptionOption) qnview.SegmentLoadInfoSubscription {
				require.NoError(t, opt.Handler.Handle(qnview.SegmentLoadInfoSnapshot{
					CollectionID: opt.CollectionID, SegmentID: opt.SegmentID, DataVersion: opt.DataVersion,
					Revision: qnview.SegmentLoadInfoRevision{Revision: 1}, LoadInfo: &querypb.SegmentLoadInfo{SegmentID: opt.SegmentID},
				}))
				return &poisonLoadInfoSubscription{}
			}).Build()
			defer metadata.UnPatch()
			released := make(chan error, 2)
			free := mockey.Mock((*fakeSegment).Release).To(func(s *fakeSegment, _ context.Context) error {
				require.Same(t, segment, s)
				released <- nil
				return nil
			}).Build()
			defer free.UnPatch()
			scheduler := nodescheduler.New(1)
			defer scheduler.Close()
			manager := qnview.NewQueryViewSegmentManager(qnview.QueryViewSegmentManagerConfig{
				Scheduler: scheduler, Loader: &poisonPhysicalLoader{}, LoadInfoStream: &poisonLoadInfoStream{}, Buffer: buffer,
			})
			meta := &viewpb.QueryViewMeta{CollectionId: 1, ReplicaId: 1, Vchannel: segment.vchannel, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{}, QueryVersion: 1}}
			assignment := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{2}}}}
			view := qviews.NewQueryViewAtQueryNode(meta, assignment)
			failed := make(chan error, 2)
			manager.Acquire(qnview.AcquireSegments{
				Key: view.QueryViewKey(), Meta: meta, View: assignment,
				OnReady:         func(map[int64][]int64) { t.Error("queued segment cannot be Ready") },
				OnUnrecoverable: func() { failed <- nil },
			})
			defer manager.Release(qnview.ReleaseSegments{Key: view.QueryViewKey()})
			require.Eventually(t, func() bool {
				buffer.mu.Lock()
				defer buffer.mu.Unlock()
				return buffer.drainQueues["p"].tasks.Len() == 1
			}, time.Second, time.Millisecond)
			if failBuffer {
				buffer.mu.Lock()
				buf := buffer.channels[segment.vchannel]
				buffer.mu.Unlock()
				buf.fail(merr.ErrDataIntegrity)
				require.NoError(t, awaitCatchup(t, failed), "failure must reach the waiting view without a replay slot")
			}
			dropped := make(chan error, 1)
			manager.Release(qnview.ReleaseSegments{Key: view.QueryViewKey(), OnDropped: func() { dropped <- nil }})
			require.NoError(t, awaitCatchup(t, dropped))
			require.NoError(t, awaitCatchup(t, released), "taskRefs must be returned while A still waits for SyncUp")
			require.Empty(t, released, "physical segment must be freed only once")
		})
	}
}
