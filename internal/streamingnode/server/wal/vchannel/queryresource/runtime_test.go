package queryresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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

func testWALView(collectionID int64, vchannel string, version qviews.DataVersion) walview.VChannelWALView {
	return walview.VChannelWALView{
		CollectionID: collectionID,
		VChannel:     vchannel,
		LoadConfig:   &streamingpb.VChannelLoadConfig{},
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
