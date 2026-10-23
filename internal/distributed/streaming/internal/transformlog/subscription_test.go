package transformlog

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestSubscriptionFiltersBeforeDelivery(t *testing.T) {
	for _, start := range []uint64{0, 10} {
		t.Run(strconv.FormatUint(start, 10), func(t *testing.T) {
			h := newRecordingHandler()
			sub := newResumableSubscription(&resumableStream{}, 1, wal.TransformLogSubscriptionOption{
				VChannel: "v", StartAfterTimeTick: start, Handler: h,
			})
			steps := []struct {
				syncUp bool
				tick   uint64
				accept bool
			}{
				{false, start, false},
				{true, start, true}, // Initial SyncUp, including zero, is meaningful.
				{true, start, false},
				{false, start + 2, true},
				{true, start, false}, // Advancing Entry must not admit an old SyncUp.
				{false, start + 1, false},
				{false, start + 2, false},
				{true, start + 1, false}, // Even a new SyncUp must not regress past Entry.
				{true, start + 2, true},  // Entry(T) must not suppress SyncUp(T).
				{true, start + 2, false},
				{true, start + 4, true},
				{false, start + 3, false},
				{false, start + 4, false}, // SyncUp(T) covers entries through T.
				{true, start + 3, false},
				{false, start + 5, true},
				{true, start + 5, true},
			}
			cursor := start
			for i, step := range steps {
				event := wal.TransformLogStreamEvent{}
				if step.syncUp {
					event.SyncUp = &wal.TransformLogSyncUp{TimeTick: step.tick}
				} else {
					event.Entry = &streamingpb.TransformLogEntry{TimeTick: step.tick}
				}
				sub.handler.Handle(event)
				if step.accept {
					event.SubscriptionID, event.VChannel = sub.ID(), sub.VChannel()
					require.Equal(t, event, recvStreamEvent(t, h.events), "step %d", i)
					cursor = step.tick
				}
				require.Empty(t, h.events, "filtered event reached Handle at step %d", i)
				require.Equal(t, cursor, sub.option().StartAfterTimeTick)
			}
		})
	}
}

func TestSubscriptionCheckpointsAfterDelivery(t *testing.T) {
	sub := newResumableSubscription(&resumableStream{}, 1, wal.TransformLogSubscriptionOption{
		VChannel: "v", StartAfterTimeTick: 10, Handler: newRecordingHandler(),
	})
	seen := 0
	patch := mockey.Mock((*recordingHandler).Handle).To(func(_ *recordingHandler, event wal.TransformLogStreamEvent) {
		seen++
		if event.Entry != nil {
			require.EqualValues(t, 10, sub.option().StartAfterTimeTick)
		} else {
			require.EqualValues(t, 11, sub.option().StartAfterTimeTick)
		}
		require.False(t, sub.handler.hasSyncUp, "delivery must finish before updating SyncUp state")
	}).Build()
	defer patch.UnPatch()
	sub.handler.Handle(wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 11}})
	sub.handler.Handle(wal.TransformLogStreamEvent{SyncUp: &wal.TransformLogSyncUp{TimeTick: 12}})
	require.Equal(t, 2, seen)
	require.EqualValues(t, 12, sub.option().StartAfterTimeTick)
	require.True(t, sub.handler.hasSyncUp)
}

func TestBoundedSubscriptionFiltersAcrossResume(t *testing.T) {
	owners := make(chan *fakeTransformLogStream, 2)
	stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		owner := newFakeTransformLogStream()
		owners <- owner
		return owner, nil
	})
	defer stream.Close()
	h := newRecordingHandler()
	opt := wal.TransformLogSubscriptionOption{VChannel: "v", StartAfterTimeTick: 10, EndTimeTick: 10, Handler: h}
	sub := subscribeStreamForTest(t, t.Context(), stream, opt)
	first := receiveSubscribeClose(t, owners)
	first.emit(wal.TransformLogStreamEvent{SubscriptionID: first.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 10}})
	require.EqualValues(t, 10, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	first.finish(wal.ErrTransformLogVChannelUnavailable)
	second := receiveSubscribeClose(t, owners)
	// Subscribe is acknowledged asynchronously after the factory returns.
	require.Eventually(t, func() bool { return second.subscriptionID("v") != 0 }, time.Second, time.Millisecond)
	require.EqualValues(t, 10, second.startAfter("v"))
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), Entry: &streamingpb.TransformLogEntry{TimeTick: 9}})
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 9}})
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 10}})
	require.Empty(t, h.events, "completed bounded subscription must suppress replayed SyncUp")
	require.NoError(t, sub.Close())

	// A new logical subscription must deliver its own initial completion.
	next := subscribeStreamForTest(t, t.Context(), stream, opt)
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 10}})
	event := recvStreamEvent(t, h.events)
	require.Equal(t, next.ID(), event.SubscriptionID)
	require.EqualValues(t, 10, event.SyncUp.TimeTick)
	require.NoError(t, next.Close())
}

func TestSubscriptionPreservesUnsyncedCursorAcrossResume(t *testing.T) {
	owners := make(chan *fakeTransformLogStream, 2)
	stream := NewResumableStream(t.Context(), "p", func(context.Context, string) (wal.TransformLogStream, error) {
		owner := newFakeTransformLogStream()
		owners <- owner
		return owner, nil
	})
	defer stream.Close()
	h := newRecordingHandler()
	sub := subscribeStreamForTest(t, t.Context(), stream, wal.TransformLogSubscriptionOption{VChannel: "v", Handler: h})
	defer sub.Close()
	first := receiveSubscribeClose(t, owners)
	first.emit(wal.TransformLogStreamEvent{SubscriptionID: first.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 10}})
	require.EqualValues(t, 10, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	first.emit(wal.TransformLogStreamEvent{SubscriptionID: first.subscriptionID("v"), Entry: &streamingpb.TransformLogEntry{TimeTick: 11}})
	require.EqualValues(t, 11, recvStreamEvent(t, h.events).Entry.GetTimeTick())
	first.finish(wal.ErrTransformLogVChannelUnavailable)
	second := receiveSubscribeClose(t, owners)
	require.Eventually(t, func() bool { return second.subscriptionID("v") != 0 }, time.Second, time.Millisecond)
	require.EqualValues(t, 11, second.startAfter("v"))
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 10}})
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), Entry: &streamingpb.TransformLogEntry{TimeTick: 11}})
	require.Empty(t, h.events)
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 11}})
	require.EqualValues(t, 11, recvStreamEvent(t, h.events).SyncUp.TimeTick)
	second.emit(wal.TransformLogStreamEvent{SubscriptionID: second.subscriptionID("v"), SyncUp: &wal.TransformLogSyncUp{TimeTick: 11}})
	require.Empty(t, h.events)
}
