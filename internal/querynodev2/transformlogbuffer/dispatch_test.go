package transformlogbuffer

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestDispatchWaitingChannelDoesNotOccupyCatchupWorker(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, vc := range []string{"p_1v0", "p_2v0"} {
		guard, err := buffer.Acquire(ctx, newTestQueryView(vc, 50))
		require.NoError(t, err)
		defer guard.Release()
	}
	// Register more than a worker's capacity before either channel has SyncUp.
	var waiting []qnview.TransformRegistration
	for i := 0; i < 20; i++ {
		reg, err := buffer.RegisterSegment(ctx, &fakeSegment{id: int64(i), vchannel: "p_1v0", startAfter: 50})
		require.NoError(t, err)
		waiting = append(waiting, reg)
		defer reg.Unregister()
	}
	ready, err := buffer.RegisterSegment(ctx, &fakeSegment{id: 100, vchannel: "p_2v0", startAfter: 50})
	require.NoError(t, err)
	defer ready.Unregister()
	stream := streams.stream("p")
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_2v0", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_2v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 70}})
	require.NoError(t, ready.WaitCatchup(ctx), "another channel waiting for SyncUp must not starve this one")
	stream.emit(wal.TransformLogStreamEvent{VChannel: "p_1v0", SyncUp: &wal.TransformLogSyncUp{TimeTick: 80}})
	for _, reg := range waiting {
		require.NoError(t, reg.WaitCatchup(ctx), "arrival must wake every parked registration")
	}
}

func TestDispatchIdleSegmentsHaveNoWorkers(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 2)
	guard, err := buffer.Acquire(t.Context(), newTestQueryView("v", 50))
	require.NoError(t, err)
	defer guard.Release()
	streams.stream("v").emit(wal.TransformLogStreamEvent{VChannel: "v", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
	before := runtime.NumGoroutine()
	const n = 10000
	regs := make([]qnview.TransformRegistration, 0, n)
	for i := 0; i < n; i++ {
		reg, err := buffer.RegisterSegment(t.Context(), &fakeSegment{id: int64(i), vchannel: "v", startAfter: 50})
		require.NoError(t, err)
		regs = append(regs, reg)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, reg := range regs {
		require.NoError(t, reg.WaitCatchup(ctx))
	}
	require.Less(t, runtime.NumGoroutine()-before, 20, "live registrations must not own waiting goroutines")
	for _, reg := range regs {
		reg.Unregister()
	}
}

func TestDispatchFailureCallbackCatchesAlreadyFailedRegistration(t *testing.T) {
	buffer := newVChannelBuffer(nil, "p", "v", 0)
	for _, failFirst := range []bool{false, true} {
		reg := newRegistration(buffer, &fakeSegment{id: 1, vchannel: "v"})
		failure := merr.WrapErrServiceInternalMsg("application failed")
		if failFirst {
			reg.finish(failure)
		}
		called := 0
		reg.OnFailure(func(err error) {
			// Callback is outside errMu, so reconciliation can inspect state.
			require.ErrorIs(t, reg.WaitFailure(t.Context()), failure)
			require.ErrorIs(t, err, failure)
			called++
		})
		reg.finish(failure)
		reg.finish(context.Canceled)
		require.Equal(t, 1, called)
		reg.cancel()
	}
}

func TestDispatchHonorsExclusiveSegmentCursor(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 1)
	guard, err := buffer.Acquire(t.Context(), newTestQueryView("v", 50))
	require.NoError(t, err)
	defer guard.Release()
	stream := streams.stream("v")
	stream.emit(wal.TransformLogStreamEvent{VChannel: "v", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
	segment := &fakeSegment{id: 1, vchannel: "v", startAfter: 100}
	reg, err := buffer.RegisterSegment(t.Context(), segment)
	require.NoError(t, err)
	defer reg.Unregister()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, reg.WaitCatchup(ctx))
	for _, tick := range []uint64{60, 100, 110} {
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v", Entry: &streamingpb.TransformLogEntry{TimeTick: tick}})
	}
	require.NoError(t, reg.WaitTransformVisible(ctx, 110))
	require.Equal(t, []uint64{110}, segment.appliedTicks())
}
