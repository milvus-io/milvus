package transformlogbuffer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestTransformApplyFailureIsIsolatedToPhysicalInstance(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "live"
		if replay {
			name = "replay"
		}
		t.Run(name, func(t *testing.T) {
			mockey.PatchConvey("failed instance cannot poison healthy or same-ID replacement instances", t, func() {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				streams := newFakeStreamManager()
				buffer := New(streams, 2)
				guard, err := buffer.Acquire(ctx, newTestQueryView("v1", 50))
				require.NoError(t, err)
				defer guard.Release()
				stream := streams.stream("v1")
				broken := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
				failure := merr.WrapErrServiceInternalMsg("partial transform application")
				var attempts atomic.Int32
				mockey.Mock((*fakeSegment).ApplyTransform).To(func(s *fakeSegment, _ context.Context, entry *streamingpb.TransformLogEntry) error {
					if s == broken {
						attempts.Add(1)
						return failure
					}
					s.mu.Lock()
					defer s.mu.Unlock()
					s.applied = append(s.applied, entry.GetTimeTick())
					return nil
				}).Build()
				emit := func(tick uint64) {
					stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: tick}})
				}
				if replay {
					emit(60)
				}
				stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
				a, err := buffer.RegisterSegment(ctx, broken)
				require.NoError(t, err)
				defer a.Unregister()
				healthy := &fakeSegment{id: 2, vchannel: "v1", startAfter: 50}
				b, err := buffer.RegisterSegment(ctx, healthy)
				require.NoError(t, err)
				defer b.Unregister()
				require.NoError(t, b.WaitCatchup(ctx))
				if !replay {
					require.NoError(t, a.WaitCatchup(ctx))
					emit(60)
				}
				require.ErrorIs(t, a.WaitFailure(ctx), failure)
				require.ErrorIs(t, a.WaitCatchup(ctx), failure)
				for _, tick := range []uint64{0, 50, 60, 100} {
					require.ErrorIs(t, a.WaitTransformVisible(ctx, tick), failure)
				}
				require.NoError(t, guard.WaitTransformVisible(ctx, 60))
				require.NoError(t, b.WaitTransformVisible(ctx, 60))

				// A later instance of the same logical Segment replays independently.
				replacement := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
				c, err := buffer.RegisterSegment(ctx, replacement)
				require.NoError(t, err)
				defer c.Unregister()
				require.NoError(t, c.WaitCatchup(ctx))
				emit(80)
				stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 90}})
				require.NoError(t, b.WaitTransformVisible(ctx, 90))
				require.NoError(t, c.WaitTransformVisible(ctx, 90))
				require.Equal(t, []uint64{60, 80}, healthy.appliedTicks())
				require.Equal(t, []uint64{60, 80}, replacement.appliedTicks())
				require.EqualValues(t, 1, attempts.Load())
				require.EqualValues(t, 50, a.(*registration).drainedTo.Load())

				// A stale reference cannot retry the failed base or advance its watermark.
				require.ErrorIs(t, a.(*registration).apply(ctx, &streamingpb.TransformLogEntry{TimeTick: 100}), failure)
				a.(*registration).publishProgress(100)
				require.EqualValues(t, 50, a.(*registration).drainedTo.Load())
				require.EqualValues(t, 1, attempts.Load())

				// Actual shared history failure still reaches every healthy consumer.
				transportErr := merr.WrapErrServiceUnavailableMsg("history unavailable")
				stream.failSubscription(transportErr)
				require.ErrorIs(t, b.WaitFailure(ctx), transportErr)
				require.ErrorIs(t, c.WaitTransformVisible(ctx, 100), transportErr)
				require.ErrorIs(t, guard.WaitTransformVisible(ctx, 100), transportErr)
				require.ErrorIs(t, a.WaitFailure(ctx), failure)
			})
		})
	}
}

func TestTransformCancellationDoesNotPoisonReplacement(t *testing.T) {
	mockey.PatchConvey("release drains canceled Apply without failing the shared stream", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		streams := newFakeStreamManager()
		buffer := New(streams, 2)
		guard, err := buffer.Acquire(ctx, newTestQueryView("v1", 50))
		require.NoError(t, err)
		defer guard.Release()
		stream := streams.stream("v1")
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
		old := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
		reg, err := buffer.RegisterSegment(ctx, old)
		require.NoError(t, err)
		defer reg.Unregister()
		require.NoError(t, reg.WaitCatchup(ctx))
		entered, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(resume) })
		mockey.Mock((*fakeSegment).ApplyTransform).To(func(s *fakeSegment, applyCtx context.Context, _ *streamingpb.TransformLogEntry) error {
			if s == old {
				close(entered)
				<-resume
				return applyCtx.Err()
			}
			return nil
		}).Build()
		applied := make(chan struct{})
		go func() {
			stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
			close(applied)
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("Apply did not start")
		}
		released := make(chan struct{})
		go func() { reg.Unregister(); close(released) }()
		select {
		case <-reg.(*registration).ctx.Done():
		case <-ctx.Done():
			t.Fatal("registration was not canceled")
		}
		select {
		case <-released:
			t.Fatal("released before in-flight Apply returned")
		default:
		}
		next, err := buffer.RegisterSegment(ctx, &fakeSegment{id: 1, vchannel: "v1", startAfter: 50})
		require.NoError(t, err)
		defer next.Unregister()
		require.NoError(t, next.WaitCatchup(ctx))
		once.Do(func() { close(resume) })
		for _, done := range []chan struct{}{applied, released} {
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("release/application did not drain")
			}
		}
		require.NoError(t, guard.WaitTransformVisible(ctx, 60))
		require.NoError(t, next.WaitTransformVisible(ctx, 60))
		reg.(*registration).errMu.Lock()
		terminalErr := reg.(*registration).err
		reg.(*registration).errMu.Unlock()
		require.NoError(t, terminalErr)
	})
}

func TestLateTransformFailureDoesNotPoisonSameIDInstance(t *testing.T) {
	mockey.PatchConvey("an old Apply failure belongs to its registration even after same-ID rebinding", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		streams := newFakeStreamManager()
		buffer := New(streams, 2)
		guard, err := buffer.Acquire(ctx, newTestQueryView("v1", 50))
		require.NoError(t, err)
		defer guard.Release()
		stream := streams.stream("v1")
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
		old := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
		reg, err := buffer.RegisterSegment(ctx, old)
		require.NoError(t, err)
		defer reg.Unregister()
		require.NoError(t, reg.WaitCatchup(ctx))
		entered, resume := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(resume) })
		failure := merr.WrapErrServiceInternalMsg("old instance failed")
		mockey.Mock((*fakeSegment).ApplyTransform).To(func(s *fakeSegment, _ context.Context, _ *streamingpb.TransformLogEntry) error {
			if s == old {
				close(entered)
				<-resume
				return failure
			}
			return nil
		}).Build()
		applied := make(chan struct{})
		go func() {
			stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
			close(applied)
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("old Apply did not start")
		}
		next, err := buffer.RegisterSegment(ctx, &fakeSegment{id: 1, vchannel: "v1", startAfter: 50})
		require.NoError(t, err)
		defer next.Unregister()
		require.NoError(t, next.WaitCatchup(ctx))
		once.Do(func() { close(resume) })
		select {
		case <-applied:
		case <-ctx.Done():
			t.Fatal("old Apply did not finish")
		}
		require.ErrorIs(t, reg.WaitFailure(ctx), failure)
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 70}})
		require.NoError(t, next.WaitTransformVisible(ctx, 70))
		require.NoError(t, guard.WaitTransformVisible(ctx, 70))
	})
}
