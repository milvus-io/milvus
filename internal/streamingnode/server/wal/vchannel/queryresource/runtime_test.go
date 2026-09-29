package queryresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestQueryRuntimeAdvanceRejectsNonMonotonicWatermark(t *testing.T) {
	runtime := NewQueryRuntime(&recordingModule{})
	runtime.Advance(qviews.DataVersion{StreamingVersion: 10})
	require.Panics(t, func() {
		runtime.Advance(qviews.DataVersion{StreamingVersion: 9})
	})
}

func TestQueryRuntimeCloseRejectsLiveEvents(t *testing.T) {
	runtime := NewQueryRuntime(&recordingModule{})
	runtime.Close()
	require.False(t, runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{}))
}

func TestQueryRuntimeAdvanceBeforeReadyBroadcastsAfterInitialize(t *testing.T) {
	module := &recordingModule{}
	runtime := NewQueryRuntime(module)

	advance := qviews.DataVersion{StreamingVersion: 12}
	runtime.Advance(advance)
	require.NoError(t, runtime.Initialize(context.Background(), testWALView(1, "ch", qviews.DataVersion{StreamingVersion: 10})))
	require.Equal(t, []qviews.DataVersion{advance}, module.advancedVersions())
	runtime.Close()
}

func TestQueryRuntimeCloseUnblocksFullLiveEventBuffer(t *testing.T) {
	runtime := NewQueryRuntime(&recordingModule{})
	runtime.pendingLimit = 1
	require.True(t, runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{
		SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 1},
	}))

	accepted := make(chan bool, 1)
	go func() {
		accepted <- runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{
			SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 2},
		})
	}()

	select {
	case <-accepted:
		t.Fatal("second live event should wait for buffer capacity")
	case <-time.After(20 * time.Millisecond):
	}
	runtime.Close()
	select {
	case ok := <-accepted:
		require.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked observer")
	}
}

func TestQueryRuntimeInitialBatchAndReadyEventsUseSameConsumer(t *testing.T) {
	module := &recordingModule{}
	runtime := NewQueryRuntime(module)

	require.True(t, runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{
		SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 1},
	}))
	require.NoError(t, runtime.Initialize(context.Background(), testWALView(1, "ch", qviews.DataVersion{StreamingVersion: 10})))
	require.Equal(t, []int64{1}, module.segmentIDs())

	require.True(t, runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{
		SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 2},
	}))
	require.Eventually(t, func() bool {
		return len(module.segmentIDs()) == 2
	}, time.Second, time.Millisecond)
	require.Equal(t, []int64{1, 2}, module.segmentIDs())
	runtime.Close()
}

func TestManagerQueuedMessageDoesNotRetainAck(t *testing.T) {
	id := walimplstest.NewTestMessageID(10)
	insert := message.CreateTestInsertMessage(t, 1, 2, 20, id).IntoImmutableMessage(id)
	txnContext := message.TxnContext{TxnID: 1}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(10).
		WithLastConfirmed(id).IntoImmutableMessage(id)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnContext).WithTimeTick(30).
		WithLastConfirmed(id).IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	builder.Add(insert)
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)

	for _, raw := range []message.ImmutableMessage{insert, txn} {
		t.Run(raw.MessageType().String(), func(t *testing.T) {
			module := &messageRecordingModule{}
			runtime := NewQueryRuntime(module)
			defer runtime.Close()
			manager := &Manager{runtime: runtime}
			tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
			owner := tracker.Track(raw)
			defer owner.Release()
			manager.ObserveEvent(context.Background(), walview.VChannelResourceEvent{Message: owner.Message()})
			require.Len(t, runtime.pending, 1)
			queued := runtime.pending[0].Message
			require.Same(t, raw, queued)
			owner.Release()
			require.Zero(t, tracker.Pending(), "query backlog must not retain persistence Ack")
			require.Equal(t, raw.TimeTick(), tracker.CompletedPoint().TimeTick)

			// Payload decoding and txn commit timestamps remain valid after Ack.
			count := 0
			require.NoError(t, walview.ForEachSegmentInsertMessage(queued, 1, func(selected walview.SegmentInsertMessage) error {
				count++
				require.Equal(t, raw.TimeTick(), selected.TimeTick)
				require.Equal(t, uint64(2), selected.Message.MustBody().GetNumRows())
				return nil
			}))
			require.Equal(t, 1, count)
			require.NoError(t, runtime.Initialize(context.Background(), testWALView(1, "v1", qviews.DataVersion{})))
			require.Equal(t, raw.TimeTick(), module.timeTick)
		})
	}
}

func testWALView(collectionID int64, vchannel string, version qviews.DataVersion) walview.VChannelWALView {
	return walview.VChannelWALView{
		CollectionID: collectionID,
		VChannel:     vchannel,
		SegmentSnapshot: walview.VisibleSegmentSnapshot{
			CollectionID: collectionID,
			VChannel:     vchannel,
			DataVersion:  version,
		},
	}
}

type recordingModule struct {
	mu       sync.Mutex
	segments []int64
	advances []qviews.DataVersion
}

func (m *recordingModule) Prepare(context.Context, walview.VChannelWALView) error { return nil }
func (m *recordingModule) ApplyLiveEvent(_ context.Context, event walview.VChannelResourceEvent) {
	if event.SegmentSealed == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.segments = append(m.segments, event.SegmentSealed.SegmentID)
}

func (m *recordingModule) Advance(version qviews.DataVersion) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.advances = append(m.advances, version)
}
func (m *recordingModule) Close() {}
func (m *recordingModule) segmentIDs() []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int64(nil), m.segments...)
}

func (m *recordingModule) advancedVersions() []qviews.DataVersion {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]qviews.DataVersion(nil), m.advances...)
}

type messageRecordingModule struct {
	timeTick uint64
}

func (*messageRecordingModule) Prepare(context.Context, walview.VChannelWALView) error { return nil }
func (m *messageRecordingModule) ApplyLiveEvent(_ context.Context, event walview.VChannelResourceEvent) {
	m.timeTick = event.Message.TimeTick()
}
func (*messageRecordingModule) Advance(qviews.DataVersion) {}
func (*messageRecordingModule) Close()                     {}

func TestQueryRuntimeBarrierFollowsObservedEvents(t *testing.T) {
	module := &recordingModule{}
	runtime := NewQueryRuntime(module)
	defer runtime.Close()
	var captured walview.VChannelWALView
	patch := mockey.Mock((*recordingModule).Prepare).To(func(_ *recordingModule, _ context.Context, view walview.VChannelWALView) error {
		captured = view
		return nil
	}).Build()
	defer patch.UnPatch()
	require.True(t, runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 1}}))
	view := testWALView(1, "ch", qviews.DataVersion{})
	locked := false
	view.WithResourceEventLock = func(fn func()) { locked = true; fn() }
	require.NoError(t, runtime.Initialize(context.Background(), view))
	require.True(t, runtime.ObserveEvent(context.Background(), walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 2}}))
	require.NoError(t, captured.ResourceEventBarrier(context.Background()))
	require.True(t, locked)
	require.Equal(t, []int64{1, 2}, module.segmentIDs())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, captured.ResourceEventBarrier(ctx), context.Canceled)
}
