package viewresource

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestManagerAcquireWaitsForQueryRuntimeInitialization(t *testing.T) {
	manager := NewManager(testModuleBuilder{}).(*queryRuntimeManager)
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	observer := manager.OnAlterLoadConfig(testWALView(1, "ch", version))
	require.NotNil(t, observer)

	ready := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:     key,
		Meta:    meta,
		OnReady: func() { close(ready) },
	})

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ready callback")
	}

	state := manager.resourceState("ch")
	require.NotNil(t, state)
	require.False(t, state.initRef)
	require.Nil(t, state.task)
	require.NoError(t, state.err)
	require.NotNil(t, state.runtime)
	require.Len(t, state.queryViewRefs, 1)
}

func TestManagerReleaseClosesUnreferencedRuntime(t *testing.T) {
	manager := NewManager(testModuleBuilder{}).(*queryRuntimeManager)
	version := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}
	meta, key := testQueryViewMetaAndKey(1, 2, "ch", version, 3)

	require.NotNil(t, manager.OnAlterLoadConfig(testWALView(1, "ch", version)))
	waitReady(t, manager, key, meta)

	dropped := make(chan struct{})
	manager.Release(snview.ReleaseResource{
		Key:       key,
		OnDropped: func() { close(dropped) },
	})
	select {
	case <-dropped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dropped callback")
	}

	require.Nil(t, manager.resourceState("ch"))
}

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

func waitReady(t *testing.T, manager *queryRuntimeManager, key qviews.QueryViewKey, meta *viewpb.QueryViewMeta) {
	t.Helper()
	ready := make(chan struct{})
	manager.Acquire(snview.AcquireResource{
		Key:     key,
		Meta:    meta,
		OnReady: func() { close(ready) },
	})
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ready callback")
	}
}

func (m *queryRuntimeManager) resourceState(vchannel string) *resourceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resources[vchannel]
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

func testQueryViewMetaAndKey(
	collectionID int64,
	replicaID int64,
	vchannel string,
	dataVersion qviews.DataVersion,
	queryVersion int64,
) (*viewpb.QueryViewMeta, qviews.QueryViewKey) {
	version := qviews.QueryViewVersion{DataVersion: dataVersion, QueryVersion: queryVersion}
	meta := &viewpb.QueryViewMeta{
		CollectionId: collectionID,
		ReplicaId:    replicaID,
		Vchannel:     vchannel,
		Version:      version.IntoProto(),
	}
	key := qviews.QueryViewKey{
		ShardID:          qviews.ShardID{ReplicaID: replicaID, VChannel: vchannel},
		QueryViewVersion: version,
	}
	return meta, key
}

type recordingModule struct {
	mu       sync.Mutex
	segments []int64
	advances []qviews.DataVersion
}

type testModuleBuilder struct{}

func (testModuleBuilder) NewRuntime() (QueryRuntimeModule, error) {
	return &recordingModule{}, nil
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
