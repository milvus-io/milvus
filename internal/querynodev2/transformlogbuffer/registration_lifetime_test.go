package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestBindRetainsRangeWithoutStartingTransport(t *testing.T) {
	streams := newFakeStreamManager()
	b := New(streams, 2)
	first, err := b.Bind(newTestQueryView("v1", 50))
	require.NoError(t, err)
	next, err := b.Bind(newTestQueryView("v1", 80))
	require.NoError(t, err)
	require.Zero(t, streams.callCount("v1"))
	first.Release()
	_, err = next.Prepare(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, streams.callCount("v1"))
	next.Release()
}

func TestSameIDRegistrationsHaveIndependentLifetime(t *testing.T) {
	mockey.PatchConvey("stopping one physical base cannot remove another base with the same ID", t, func() {
		streams := newFakeStreamManager()
		b := New(streams, 2)
		g, err := b.Acquire(t.Context(), newTestQueryView("v1", 50))
		require.NoError(t, err)
		defer g.Release()
		first := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
		next := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
		// Both remain pending until SyncUp, reproducing the previous ID-keyed
		// removal race without relying on an extra metadata or View push.
		a, err := b.RegisterSegment(t.Context(), first)
		require.NoError(t, err)
		c, err := b.RegisterSegment(t.Context(), next)
		require.NoError(t, err)
		a.Unregister()
		streams.stream("v1").emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
		streams.stream("v1").emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 60}})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorIs(t, a.WaitCatchup(ctx), context.Canceled)
		require.NoError(t, c.WaitCatchup(ctx))
		require.Equal(t, []uint64{60}, next.appliedTicks())
		c.Unregister()
	})
}

func TestUnregisterWaitsForLiveApplication(t *testing.T) {
	mockey.PatchConvey("copied live registration cannot access a destroyed resource", t, func() {
		streams := newFakeStreamManager()
		b := New(streams, 2)
		g, err := b.Acquire(t.Context(), newTestQueryView("v1", 50))
		require.NoError(t, err)
		defer g.Release()
		streams.stream("v1").emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 50}})
		segment := &fakeSegment{id: 1, vchannel: "v1", startAfter: 50}
		reg, err := b.RegisterSegment(t.Context(), segment)
		require.NoError(t, err)
		require.NoError(t, reg.WaitCatchup(t.Context()))
		entered, resume := make(chan struct{}), make(chan struct{})
		mockey.Mock((*fakeSegment).ApplyTransform).To(func(*fakeSegment, context.Context, *streamingpb.TransformLogEntry) error {
			close(entered)
			<-resume
			return nil
		}).Build()
		applied := make(chan struct{})
		go func() {
			streams.stream("v1").emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
			close(applied)
		}()
		<-entered
		released := make(chan struct{})
		go func() { reg.Unregister(); close(released) }()
		select {
		case <-released:
			t.Fatal("unregistered before application stopped")
		case <-time.After(20 * time.Millisecond):
		}
		close(resume)
		<-released
		<-applied
	})
}

func TestFiniteCatchupFrontierAndPerInstanceVisibility(t *testing.T) {
	mockey.PatchConvey("Ready is bounded by SyncUp while queries wait for their own application", t, func() {
		streams := newFakeStreamManager()
		b := New(streams, 1)
		g, err := b.Acquire(t.Context(), newTestQueryView("v1", 50))
		require.NoError(t, err)
		defer g.Release()
		stream := streams.stream("v1")
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 60}})
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 70}})
		first, resumeFirst := make(chan struct{}), make(chan struct{})
		later, resumeLater := make(chan struct{}), make(chan struct{})
		mockey.Mock((*fakeSegment).ApplyTransform).To(func(_ *fakeSegment, _ context.Context, entry *streamingpb.TransformLogEntry) error {
			if entry.TimeTick == 60 {
				close(first)
				<-resumeFirst
			}
			if entry.TimeTick == 80 {
				close(later)
				<-resumeLater
			}
			return nil
		}).Build()
		reg, err := b.RegisterSegment(t.Context(), &fakeSegment{id: 1, vchannel: "v1", startAfter: 50})
		require.NoError(t, err)
		<-first
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", Entry: &streamingpb.TransformLogEntry{TimeTick: 80}})
		stream.emit(wal.TransformLogStreamEvent{VChannel: "v1", SyncUp: &wal.TransformLogSyncUp{TimeTick: 90}})
		close(resumeFirst)
		<-later
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.NoError(t, reg.WaitCatchup(ctx), "the finite 70 frontier is complete although later work is pending")
		require.NoError(t, g.WaitTransformVisible(ctx, 90), "transport already received the later empty interval")
		short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
		require.ErrorIs(t, reg.WaitTransformVisible(short, 90), context.DeadlineExceeded)
		stop()
		close(resumeLater)
		require.NoError(t, reg.WaitTransformVisible(ctx, 90), "empty interval advances after preceding application")
		reg.Unregister()
	})
}
