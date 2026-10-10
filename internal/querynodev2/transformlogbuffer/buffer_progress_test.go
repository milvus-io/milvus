package transformlogbuffer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCatchupCompletionWaitsForApply(t *testing.T) {
	for _, cancelTask := range []bool{true, false} {
		name := "buffer failure"
		if cancelTask {
			name = "task canceled"
		}
		t.Run(name, func(t *testing.T) {
			owner := New(nil, 1)
			buf := newVChannelBuffer(owner, "p1", "v1", 50)
			reg := newRegistration(buf, &fakeSegment{id: 10, vchannel: "v1", startAfter: 50})
			defer reg.Unregister()
			buf.pending[10] = reg
			buf.syncUp = true
			buf.entries = []*streamingpb.TransformLogEntry{{TimeTick: 60}}
			started, proceed := make(chan struct{}), make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(proceed) })
			patch := mockey.Mock((*fakeSegment).ApplyTransform).To(func(*fakeSegment, context.Context, *streamingpb.TransformLogEntry) error {
				close(started)
				<-proceed
				return nil
			}).Build()
			defer patch.UnPatch()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			completed := make(chan error, 2)
			reg.Catchup(ctx, func(err error) {
				// Completion runs outside the Apply and buffer locks, and may unregister.
				reg.Unregister()
				completed <- err
			})
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("Apply did not start")
			}
			failure := merr.WrapErrServiceUnavailableMsg("injected stream failure")
			expected := failure
			if cancelTask {
				cancel()
				expected = context.Canceled
			} else {
				buf.fail(failure)
			}
			select {
			case <-completed:
				t.Fatal("completion ran while native Apply was in flight")
			default:
			}
			release.Do(func() { close(proceed) })
			require.ErrorIs(t, awaitCatchup(t, completed), expected)
			waitDrainIdle(t, owner)
			require.Empty(t, completed, "completion must run exactly once")
			require.Empty(t, buf.pending)
			require.Empty(t, buf.live)
		})
	}
}

func TestCatchupCanceledBeforeSubmission(t *testing.T) {
	// Cancellation known before submission must complete inline.
	owner := New(nil, 1)
	buf := newVChannelBuffer(owner, "p1", "v1", 50)
	reg := newRegistration(buf, &fakeSegment{id: 10, vchannel: "v1", startAfter: 50})
	buf.pending[10] = reg
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	reg.Catchup(ctx, func(err error) {
		calls++
		reg.Unregister()
		require.ErrorIs(t, err, context.Canceled)
	})
	require.Equal(t, 1, calls)
	require.Empty(t, buf.pending)
}

func TestCatchupUnregisterBeforeSyncUp(t *testing.T) {
	owner := New(nil, 1)
	buf := newVChannelBuffer(owner, "p1", "v1", 50)
	reg := newRegistration(buf, &fakeSegment{id: 10, vchannel: "v1", startAfter: 50})
	buf.pending[10] = reg
	waiting := make(chan struct{})
	var origin func(*vchannelBuffer, *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error)
	patch := mockey.Mock((*vchannelBuffer).nextCatchupBatch).To(func(b *vchannelBuffer, r *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error) {
		batch, done, notify, err := origin(b, r)
		close(waiting)
		return batch, done, notify, err
	}).Origin(&origin).Build()
	defer patch.UnPatch()
	caughtUp := startCatchup(reg)
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach SyncUp wait")
	}
	// Only registration cancellation occurs; the task context remains alive.
	reg.Unregister()
	require.ErrorIs(t, awaitCatchup(t, caughtUp), context.Canceled)
	waitDrainIdle(t, owner)
}

func waitDrainIdle(t *testing.T, buffer *Buffer) {
	t.Helper()
	require.Eventually(t, func() bool {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		return len(buffer.drainQueues) == 0
	}, time.Second, time.Millisecond)
}

func TestCatchupSchedulingPerPChannel(t *testing.T) {
	buffer := New(nil, 1)
	a := newVChannelBuffer(buffer, "pa", "pa_1v0", 0)
	a2 := newVChannelBuffer(buffer, "pa", "pa_2v0", 0)
	b := newVChannelBuffer(buffer, "pb", "pb_1v0", 0)
	regA := newRegistration(a, &fakeSegment{id: 1, vchannel: a.vchannel})
	regA2 := newRegistration(a2, &fakeSegment{id: 2, vchannel: a2.vchannel})
	regB := newRegistration(b, &fakeSegment{id: 3, vchannel: b.vchannel})
	a.pending[1], a2.pending[2], b.pending[3] = regA, regA2, regB
	a2.syncUp, b.syncUp = true, true
	defer regA.Unregister()
	defer regA2.Unregister()
	defer regB.Unregister()
	waiting := make(chan struct{})
	var once sync.Once
	var original func(*vchannelBuffer, *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error)
	patch := mockey.Mock((*vchannelBuffer).nextCatchupBatch).To(func(v *vchannelBuffer, r *registration) ([]*streamingpb.TransformLogEntry, bool, <-chan struct{}, error) {
		batch, done, notify, err := original(v, r)
		if r == regA {
			once.Do(func() { close(waiting) })
		}
		return batch, done, notify, err
	}).Origin(&original).Build()
	defer patch.UnPatch()
	doneA := startCatchup(regA)
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("A did not start")
	}
	doneA2 := startCatchup(regA2)
	doneB := startCatchup(regB)
	require.NoError(t, awaitCatchup(t, doneB), "another PChannel must complete while A awaits SyncUp")
	select {
	case <-doneA2:
		t.Fatal("VChannels on the same PChannel exceeded the shared concurrency limit")
	default:
	}
	regA.Unregister()
	require.ErrorIs(t, awaitCatchup(t, doneA), context.Canceled)
	require.NoError(t, awaitCatchup(t, doneA2))
	waitDrainIdle(t, buffer)
	// An idle PChannel must restart without retaining a pool or replacing a stream.
	regNext := newRegistration(a2, &fakeSegment{id: 4, vchannel: a2.vchannel})
	a2.pending[4] = regNext
	defer regNext.Unregister()
	require.NoError(t, awaitCatchup(t, startCatchup(regNext)))
	waitDrainIdle(t, buffer)
}
