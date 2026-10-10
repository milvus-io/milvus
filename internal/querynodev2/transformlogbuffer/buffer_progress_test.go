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
			owner := &Buffer{drainTasks: make(chan catchupTask, 1)}
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
			close(owner.drainTasks)
			workerDone := make(chan struct{})
			go func() { owner.drainWorker(); close(workerDone) }()
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
			select {
			case <-workerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("worker did not finish")
			}
			require.Empty(t, completed, "completion must run exactly once")
			require.Empty(t, buf.pending)
			require.Empty(t, buf.live)
		})
	}
}

func TestCatchupCanceledBeforeSubmission(t *testing.T) {
	// With no worker and no queue capacity, cancellation must complete inline.
	owner := &Buffer{drainTasks: make(chan catchupTask)}
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
	owner := &Buffer{drainTasks: make(chan catchupTask, 1)}
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
	close(owner.drainTasks)
	workerDone := make(chan struct{})
	go func() { owner.drainWorker(); close(workerDone) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach SyncUp wait")
	}
	// Only registration cancellation occurs; the task context remains alive.
	reg.Unregister()
	require.ErrorIs(t, awaitCatchup(t, caughtUp), context.Canceled)
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not finish")
	}
}
