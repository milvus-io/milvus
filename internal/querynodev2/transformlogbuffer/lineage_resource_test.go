//go:build test && dynamic

package transformlogbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// The fakes below let the real SegmentResourceManager run on the real Buffer.

type lineageResourceLoader struct {
	// failApply makes one segment fail to apply one entry.
	failSegment int64
	failTick    uint64
}

type failingApplySegment struct {
	*fakeSegment
	failTick uint64
}

func (s *failingApplySegment) ApplyTransform(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	if entry.GetTimeTick() == s.failTick {
		return errors.New("apply failed")
	}
	return s.fakeSegment.ApplyTransform(ctx, entry)
}

func (l *lineageResourceLoader) Load(_ context.Context, info *querypb.SegmentLoadInfo, _ qnview.CollectionRuntime) (qnview.TransformSegment, error) {
	segment := &fakeSegment{id: info.GetSegmentID(), vchannel: lineageSource, partitionID: info.GetPartitionID(), startAfter: info.GetTransformStartAfterTimetick()}
	if info.GetSegmentID() == l.failSegment {
		return &failingApplySegment{fakeSegment: segment, failTick: l.failTick}, nil
	}
	return segment, nil
}

func (*lineageResourceLoader) PrepareReadState(context.Context, qnview.TransformSegment, segments.ReadState, qnview.CollectionRuntime, qnview.SegmentLoadInfoSnapshot) (segments.ReadState, error) {
	return &lineageResourceRead{}, nil
}

func (*lineageResourceLoader) AdoptReadState(qnview.TransformSegment, segments.ReadState) error {
	return nil
}

type lineageResourceRead struct{ segments.Segment }

func (*lineageResourceRead) RetainReadState() func() { return func() {} }
func (*lineageResourceRead) ReleaseReadState()       {}

type lineageResourceMetadata struct{}

func (lineageResourceMetadata) GetQueryViewSegmentLoadInfo(_ context.Context, collectionID, segmentID int64) (qnview.SegmentLoadInfoSnapshot, error) {
	return qnview.SegmentLoadInfoSnapshot{CollectionID: collectionID, SegmentID: segmentID, LoadInfo: &querypb.SegmentLoadInfo{
		CollectionID: collectionID, SegmentID: segmentID, PartitionID: 10, NumOfRows: 10,
		ManifestPath: packed.MarshalManifestPath("test", 1), StorageVersion: storage.StorageV3, TransformStartAfterTimetick: 50,
	}}, nil
}

type lineageResourceCollections struct{}

func (lineageResourceCollections) Acquire(context.Context, *qviews.QueryViewAtQueryNode) (qnview.CollectionRuntimeGuard, bool, error) {
	return lineageResourceCollection{}, false, nil
}

type lineageResourceCollection struct{}

func (lineageResourceCollection) CollectionID() int64  { return 1 }
func (lineageResourceCollection) DatabaseName() string { return "" }
func (lineageResourceCollection) Schema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{}
}
func (lineageResourceCollection) SchemaVersion() int64              { return 0 }
func (lineageResourceCollection) CCollection() *segcore.CCollection { return nil }
func (lineageResourceCollection) IndexInfos() []*indexpb.IndexInfo  { return nil }
func (lineageResourceCollection) Collection() *segments.Collection  { return nil }
func (lineageResourceCollection) Release()                          {}

type lineageResourceView struct {
	req           qnview.AcquireSegments
	ready         chan struct{}
	unrecoverable chan struct{}
}

// newLineageResourceView is a view of the source shard over segments 1000 and
// 1001, with or without lineage {T1, T2, 100}.
func newLineageResourceView(version int64, lineage bool) *lineageResourceView {
	meta := &viewpb.QueryViewMeta{
		CollectionId: 1, ReplicaId: 1, Vchannel: lineageSource, TransformStartAfterTimetick: 50,
		Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: version, CompactVersion: 1}, QueryVersion: version},
	}
	if lineage {
		meta.Lineage = &viewpb.QueryViewLineage{Vchannels: []string{lineageTarget1, lineageTarget2}, SwitchTimetick: 100}
	}
	node := &viewpb.QueryViewOfQueryNode{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 10, SegmentIds: []int64{1000, 1001}, SegmentManifestVersions: []int64{1, 1}}}}
	v := &lineageResourceView{ready: make(chan struct{}, 8), unrecoverable: make(chan struct{}, 8)}
	v.req = qnview.AcquireSegments{
		Key: qviews.NewQueryViewAtQueryNode(meta, node).QueryViewKey(), Meta: meta, View: node,
		OnReady:         func(map[int64][]int64) { v.ready <- struct{}{} },
		OnUnrecoverable: func() { v.unrecoverable <- struct{}{} },
	}
	return v
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func releaseResourceView(t *testing.T, manager *qnview.SegmentResourceManager, view *lineageResourceView) {
	t.Helper()
	dropped := make(chan struct{})
	manager.Release(qnview.ReleaseSegments{Key: view.req.Key, OnDropped: func() { close(dropped) }})
	awaitSignal(t, dropped, "view release")
}

// A lineage stream fails for good while a view without lineage keeps the
// source's segments loaded. Once the failed lineage view is released, nothing
// may keep a range on the failed buffer: the target's own views and a rebuilt
// lineage view must be able to bind the stream again.
func TestFailedLineageBufferIsReplacedWhileSegmentsStayLoaded(t *testing.T) {
	streams := newFakeStreamManager()
	buffer := New(streams, 4)
	scheduler := nodescheduler.New(2)
	defer scheduler.Close()
	// Segment 1000 cannot apply the entry at 110; segment 1001 can.
	manager := qnview.NewSegmentResourceManager(t.Context(), scheduler, &lineageResourceLoader{failSegment: 1000, failTick: 110}, nil, lineageResourceMetadata{}, buffer, lineageResourceCollections{})
	syncUp := func(pchannel, vchannel string, tick uint64) {
		require.Eventually(t, func() bool { return streams.stream(pchannel) != nil }, 5*time.Second, time.Millisecond)
		requireSubscriptionVChannels(t, streams.stream(pchannel), []string{vchannel})
		streams.stream(pchannel).emit(wal.TransformLogStreamEvent{VChannel: vchannel, SyncUp: &wal.TransformLogSyncUp{TimeTick: tick}})
	}

	plain := newLineageResourceView(1, false)
	manager.Acquire(plain.req)
	syncUp("ps", lineageSource, 100)
	awaitSignal(t, plain.ready, "plain view segment")
	awaitSignal(t, plain.ready, "plain view segment")

	lineage := newLineageResourceView(2, true)
	manager.Acquire(lineage.req)
	syncUp("pa", lineageTarget1, 100)
	syncUp("pb", lineageTarget2, 100)
	awaitSignal(t, lineage.ready, "lineage view segment")
	awaitSignal(t, lineage.ready, "lineage view segment")

	// The failure reaches segment 1000 first and fails the view.
	target2 := streams.stream("pb")
	target2.emit(wal.TransformLogStreamEvent{VChannel: lineageTarget2, Entry: &streamingpb.TransformLogEntry{TimeTick: 110}})
	awaitSignal(t, lineage.unrecoverable, "lineage view failure")
	// Then the stream terminates: segment 1001 learns of it after the view failed.
	target2.failStream(wal.ErrTransformLogStartPointTruncated)

	// While the failed view exists the failed buffer refuses new ranges, as on master.
	_, err := buffer.Bind(newTestQueryView(lineageTarget2, 100))
	require.ErrorIs(t, err, wal.ErrTransformLogStartPointTruncated)

	releaseResourceView(t, manager, lineage)
	var rebound qnview.TransformLogGuard
	require.Eventually(t, func() bool {
		rebound, err = buffer.Bind(newTestQueryView(lineageTarget2, 100))
		return err == nil
	}, 5*time.Second, 5*time.Millisecond, "a loaded segment kept its range on the failed lineage buffer")
	rebound.Release()

	// With no lineage view left, the segments also gave up the healthy target
	// stream, although the view without lineage keeps them loaded.
	awaitSignal(t, streams.stream("pa").Done(), "release of the healthy lineage stream")
	require.Equal(t, 1, channelCount(buffer))

	// A rebuilt lineage view binds fresh buffers and replays from the switch tick.
	streams.mu.Lock()
	delete(streams.streams, "pa")
	delete(streams.streams, "pb")
	streams.mu.Unlock()
	rebuilt := newLineageResourceView(3, true)
	manager.Acquire(rebuilt.req)
	syncUp("pa", lineageTarget1, 100)
	syncUp("pb", lineageTarget2, 100)
	awaitSignal(t, rebuilt.ready, "rebuilt lineage view segment")
	awaitSignal(t, rebuilt.ready, "rebuilt lineage view segment")
	select {
	case <-plain.unrecoverable:
		t.Fatal("the view without lineage failed")
	default:
	}

	releaseResourceView(t, manager, rebuilt)
	releaseResourceView(t, manager, plain)
	require.Zero(t, channelCount(buffer))
	manager.Close()
}
