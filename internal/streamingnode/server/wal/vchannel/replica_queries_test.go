package vchannel

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	sncatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// The first sealed version switches ownership of the rows. A new RO can
// observe that binding in DataCoord before its local assignment changes state.
func TestReplicaViewFirstSealedVersion(t *testing.T) {
	assignment := &streamingpb.SegmentAssignmentMeta{
		Vchannel: "v1", CollectionId: 1, PartitionId: 10, SegmentId: 101,
		State:               streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2},
		CheckpointTimeTick:  30,
	}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{101: assignment}
	v := NewVChannelViewFromMeta(restoreSchemaTestMeta())
	for _, ver := range []int64{1, 2, 3} {
		meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v1", SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: ver}}}
		s, err := buildReplicaView(meta, v, segments, nil, 40)
		require.NoError(t, err)
		require.EqualValues(t, 40, s.SegmentSnapshot.BaseGrowingTimeTick)
		if ver < 2 {
			require.Len(t, s.SegmentSnapshot.Segments, 1)
			require.EqualValues(t, 101, s.SegmentSnapshot.Segments[0].SegmentID)
			require.Empty(t, s.SegmentSnapshot.FlushedSegments)
		} else {
			require.Empty(t, s.SegmentSnapshot.Segments)
			require.Len(t, s.SegmentSnapshot.FlushedSegments, 1)
			require.EqualValues(t, 101, s.SegmentSnapshot.FlushedSegments[0].SegmentID)
			require.EqualValues(t, 40, s.DeleteReplayStartAfterTimeTick)
		}
		_, err = buildReplicaView(meta, nil, segments, nil, 40)
		require.Error(t, err)
	}
}

func TestReplicaQueryPrefixIsMonotonic(t *testing.T) {
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	defer q.Close()
	q.Observe(nil, 15)
	q.Observe(nil, 12)
	require.EqualValues(t, 15, q.through)
}

func newTestReplicaQueries(t *testing.T, canCapture <-chan struct{}) (*ReplicaQueries, *queryresource.Manager) {
	t.Helper()
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	t.Cleanup(q.Close)
	return q, addTestQueryChannel(q, canCapture)
}

func addTestQueryChannel(q *ReplicaQueries, canCapture <-chan struct{}) *queryresource.Manager {
	ch := &queryChannel{
		refs: make(map[qviews.QueryViewKey]*snview.AcquireResource),
		resources: queryresource.NewManager(queryresource.Config{
			Scheduler: nodescheduler.Get(), Dispatcher: q.dispatch,
			Builders: []queryresource.QueryRuntimeModuleBuilder{nil},
			CaptureView: func(ctx context.Context, _ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) error {
				if canCapture != nil {
					select {
					case <-canCapture:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				install(walview.VChannelWALView{})
				return nil
			},
		}),
	}
	q.active.Insert("v1", ch)
	return ch.resources
}

func acquireReplicaView(q *ReplicaQueries, version int64) (qviews.QueryViewKey, <-chan bool) {
	meta := &viewpb.QueryViewMeta{
		Vchannel: "v1", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: version}},
	}
	key := qviews.QueryViewKey{ShardID: qviews.NewShardIDFromQVMeta(meta), QueryViewVersion: qviews.FromProtoQueryViewVersion(meta.Version)}
	outcome := make(chan bool, 1)
	q.Acquire(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { outcome <- true }, OnUnrecoverable: func() { outcome <- false }})
	return key, outcome
}

func expectReplicaAdmission(t *testing.T, result <-chan bool, ready bool) {
	t.Helper()
	select {
	case got := <-result:
		require.Equal(t, ready, got)
	case <-time.After(5 * time.Second):
		t.Fatal("query resource preparation did not report")
	}
}

func TestReplicaQueryAdmissionDuringForwardRecovery(t *testing.T) {
	q, _ := newTestReplicaQueries(t, nil)
	high, first := acquireReplicaView(q, 8)
	expectReplicaAdmission(t, first, true)
	expired, obsolete := acquireReplicaView(q, 2)
	_, third := acquireReplicaView(q, 3)
	_, fourth := acquireReplicaView(q, 4)
	q.Release(snview.ReleaseResource{Key: expired})
	q.Release(snview.ReleaseResource{Key: high})
	expectReplicaAdmission(t, third, true)
	expectReplicaAdmission(t, fourth, true)
	select {
	case <-obsolete:
		t.Fatal("a dropped pending acquisition was revived")
	default:
	}
	_, waiting := acquireReplicaView(q, 1)
	q.SetWriter(true)
	expectReplicaAdmission(t, waiting, false)
}

func TestReplicaQueryRetentionAcrossWriterLifetimes(t *testing.T) {
	canCapture := make(chan struct{})
	q, resources := newTestReplicaQueries(t, canCapture)
	key, ready := acquireReplicaView(q, 10)

	seg := &streamingpb.SegmentAssignmentMeta{
		CollectionId: 1, PartitionId: 10, SegmentId: 100, Vchannel: "v1",
		State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		CheckpointTimeTick: 10, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 11},
	}
	openWriter := func() (*PChannelRecoveryManager, func()) {
		writer, err := NewPChannelRecoveryManager(PChannelManagerConfig{
			PChannel: "p", QueryRetention: q,
			VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": restoreSchemaTestMeta()},
			Segments:      map[int64]*streamingpb.SegmentAssignmentMeta{100: seg},
		})
		require.NoError(t, err)
		t.Cleanup(writer.Close)
		require.Nil(t, writer.Module("v1").localQueries)
		require.Nil(t, writer.queryDispatcher)
		_, owned := writer.GetQueryRuntime(key)
		require.False(t, owned)
		_, owned = writer.GetQueryReadRuntime(key)
		require.False(t, owned)
		detach := q.BindWriter(writer)
		q.SetWriter(true)
		return writer, detach
	}
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 40}
	writer, detach := openWriter()
	require.Empty(t, writer.ConsumeCleanupSnapshots(cleanup), "the Preparing view already pins its version")
	close(canCapture)
	expectReplicaAdmission(t, ready, true)
	runtime, ok := resources.QueryRuntime(key)
	require.True(t, ok)
	require.False(t, writer.Module("v1").vchannelView.SegmentDataVersionSummary().GT(key.QueryViewVersion.DataVersion))
	detach()
	writer.Close()
	q.SetWriter(false)
	current, ok := resources.QueryRuntime(key)
	require.True(t, ok)
	require.Same(t, runtime, current)

	writer, detach = openWriter()
	defer detach()
	dropped := make(chan struct{})
	q.Release(snview.ReleaseResource{Key: key, OnDropped: func() { close(dropped) }})
	select {
	case <-dropped:
	case <-time.After(5 * time.Second):
		t.Fatal("old view did not retire")
	}
	require.False(t, q.active.Contain("v1"))
	addTestQueryChannel(q, nil) // A fresh manager has no runtime watermark of its own.
	require.Empty(t, writer.ConsumeCleanupSnapshots(cleanup))
	module := writer.Module("v1")
	require.EqualValues(t, 11, module.vchannelView.SegmentDataVersionSummary().StreamingVersion)
	_, late := acquireReplicaView(q, 10)
	expectReplicaAdmission(t, late, false)
	_, ok = q.OldestDataVersion("v1")
	require.False(t, ok)
	for _, snapshot := range writer.ConsumeDirtySnapshots() {
		snapshot.MarkPersisted()
	}
	deletes := writer.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, deletes, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, deletes[0].Op())
	deletes[0].MarkPersisted()
	require.Empty(t, module.segments)
}

func TestReplicaQueryPinsDroppedChannel(t *testing.T) {
	q, _ := newTestReplicaQueries(t, nil)
	key, ready := acquireReplicaView(q, 10)
	expectReplicaAdmission(t, ready, true)
	meta := restoreSchemaTestMeta()
	meta.CollectionInfo.Schemas = meta.CollectionInfo.Schemas[1:]
	meta.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
	meta.TransformMaterializedTimeTick = meta.CheckpointTimeTick
	writer, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel: "p", QueryRetention: q, VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": meta},
	})
	require.NoError(t, err)
	t.Cleanup(writer.Close)
	detach := q.BindWriter(writer)
	defer detach()
	q.SetWriter(true)
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 40, SummaryRetired: func(string, uint64) bool { return true }}
	require.Empty(t, writer.ConsumeCleanupSnapshots(cleanup))
	lateKey, late := acquireReplicaView(q, 11)
	expectReplicaAdmission(t, late, false)
	q.Release(snview.ReleaseResource{Key: key})
	deletes := writer.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, deletes, 1)
	deletes[0].MarkPersisted()
	require.Nil(t, writer.Module("v1"))
	_, removed := acquireReplicaView(q, 12)
	expectReplicaAdmission(t, removed, false)
	q.Release(snview.ReleaseResource{Key: lateKey})
}

func TestReplicaQueryCaptureReleasesPinOnShutdown(t *testing.T) {
	catalog := sncatalog.NewCataLog(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	defer mockey.Mock(mockey.GetMethod(catalog, "ListVChannel")).Return([]*streamingpb.VChannelMeta{restoreSchemaTestMeta()}, nil).Build().UnPatch()
	defer mockey.Mock(mockey.GetMethod(catalog, "ListSegmentAssignment")).Return([]*streamingpb.SegmentAssignmentMeta{{Vchannel: "other"}}, nil).Build().UnPatch()
	readBaseline := make(chan struct{}, 1)
	defer mockey.Mock(walsummary.ReadSnapshot).To(func(context.Context, *walsummary.Store, int64) (*walsummary.Snapshot, error) {
		readBaseline <- struct{}{}
		return &walsummary.Snapshot{}, nil
	}).Build().UnPatch()
	for _, tc := range []string{"view retired", "replica closed"} {
		t.Run(tc, func(t *testing.T) {
			ctx, cancelReplica := context.WithCancel(t.Context())
			defer cancelReplica()
			captureCtx, cancelView := context.WithCancel(t.Context())
			defer cancelView()
			buffer := replica.NewBuffer(10)
			q := NewReplicaQueries(ctx, "p", buffer, 10)
			defer q.Close()
			key := qviews.QueryViewKey{ShardID: qviews.ShardID{VChannel: "v1"}}
			_, ok := q.GetQueryReadRuntime(key)
			require.False(t, ok)
			done := make(chan error, 1)
			meta := &viewpb.QueryViewMeta{Vchannel: "v1", CollectionId: 1, SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}}}
			go func() {
				done <- q.capture(captureCtx, meta, func(walview.VChannelWALView) { t.Error("installed a view beyond the observed prefix") })
			}()
			<-readBaseline
			buffer.Observe(nil, 30)
			buffer.Truncate(30)
			if tc == "view retired" {
				cancelView()
			} else {
				cancelReplica()
			}
			require.ErrorIs(t, <-done, context.Canceled)
			cursor, floor, err := buffer.Pin(false)
			require.NoError(t, err)
			require.EqualValues(t, 30, floor)
			cursor.Close()
			released := make(chan bool, 1)
			q.Release(snview.ReleaseResource{Key: key, OnDropped: func() { released <- true }})
			expectReplicaAdmission(t, released, true)
			buffer.Close()
			require.ErrorIs(t, q.capture(captureCtx, meta, nil), io.EOF)
		})
	}
}

func TestReplicaViewReplaysRestoreSnapshot(t *testing.T) {
	meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v1", SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{}}}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		101: {SegmentId: 101, PartitionId: 10},
		201: {SegmentId: 201, PartitionId: 20},
	}
	creation := &message.RestoreSnapshotMessageBody{
		CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{20}},
		Collection: &message.CreateCollectionRequest{
			CollectionID:     1,
			PartitionIDs:     []int64{20},
			CollectionSchema: &schemapb.CollectionSchema{Name: "restored", Version: 1},
		},
	}
	restore := func(vchannel string, body *message.RestoreSnapshotMessageBody) message.ImmutableMessage {
		return message.NewRestoreSnapshotMessageBuilderV2().WithVChannel(vchannel).
			WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 1}).WithBody(body).MustBuildMutable().
			WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	}
	insert := message.NewInsertMessageBuilderV1().WithVChannel(meta.Vchannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1, Partitions: []*message.PartitionSegmentAssignment{
			{PartitionId: 20, Rows: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 201}},
		}}).WithBody(&message.InsertRequest{CollectionID: 1, PartitionID: 20, NumRows: 1}).
		MustBuildMutable().WithTimeTick(11).IntoImmutableMessage(walimplstest.NewTestMessageID(3))
	tail := []message.ImmutableMessage{restore(meta.Vchannel, creation), insert}
	for _, tc := range []struct {
		name string
		base *VChannelView
	}{
		{"creation only in WAL", nil},
		{"creation after a catalog tombstone", NewVChannelViewFromMeta(&streamingpb.VChannelMeta{
			Vchannel: meta.Vchannel, State: streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, CheckpointTimeTick: 5,
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, err := buildReplicaView(meta, tc.base, segments, tail, 30)
			require.NoError(t, err)
			require.Equal(t, "restored", view.Schema.GetName())
			require.Equal(t, "restored", view.WriteSchema.GetName())
			require.Len(t, view.SegmentSnapshot.Segments, 1)
			growing := view.SegmentSnapshot.Segments[0]
			require.EqualValues(t, 201, growing.SegmentID)
			require.Equal(t, []message.ImmutableMessage{insert}, growing.Data.InsertMessages)
		})
	}
	t.Run("creation belongs to another shard", func(t *testing.T) {
		_, err := buildReplicaView(meta, nil, segments, []message.ImmutableMessage{restore("v2", creation)}, 30)
		require.ErrorIs(t, err, merr.ErrServiceNotReady)
	})
	for _, tc := range []struct {
		name string
		body *message.RestoreSnapshotMessageBody
	}{
		{"creation already covered by catalog", creation},
		{"restore into an existing collection", &message.RestoreSnapshotMessageBody{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The newer catalog baseline has a different schema and partition membership.
			baseline := restoreSchemaTestMeta()
			view, err := buildReplicaView(meta, NewVChannelViewFromMeta(baseline), segments, []message.ImmutableMessage{restore(meta.Vchannel, tc.body)}, 30)
			require.NoError(t, err)
			require.Equal(t, baseline.CollectionInfo.Schemas[1].Schema.Fields, view.Schema.Fields)
			require.Len(t, view.SegmentSnapshot.Segments, 1)
			require.EqualValues(t, 101, view.SegmentSnapshot.Segments[0].SegmentID)
		})
	}
}

func TestReplicaViewReplaysPartitionLifecycle(t *testing.T) {
	tail := []message.ImmutableMessage{
		message.NewCreatePartitionMessageBuilderV1().WithVChannel("v1").
			WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 1, PartitionId: 20}).WithBody(&message.CreatePartitionRequest{}).
			MustBuildMutable().WithTimeTick(31).IntoImmutableMessage(walimplstest.NewTestMessageID(31)),
		message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: 20, SegmentId: 201, SchemaVersion: 1}).WithBody(&message.CreateSegmentMessageBody{}).
			MustBuildMutable().WithTimeTick(32).IntoImmutableMessage(walimplstest.NewTestMessageID(32)),
		message.NewSchemaChangeMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.SchemaChangeMessageHeader{CollectionId: 1}).WithBody(&message.SchemaChangeMessageBody{Schema: &schemapb.CollectionSchema{Version: 2}}).
			MustBuildMutable().WithTimeTick(33).IntoImmutableMessage(walimplstest.NewTestMessageID(33)),
		message.NewDropPartitionMessageBuilderV1().WithVChannel("v1").
			WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: 10}).WithBody(&message.DropPartitionRequest{}).
			MustBuildMutable().WithTimeTick(34).IntoImmutableMessage(walimplstest.NewTestMessageID(34)),
	}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		101: {PartitionId: 10, SegmentId: 101, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING},
		102: {PartitionId: 10, SegmentId: 102, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 1}},
	}
	meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v1", SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 2}}}
	got, err := buildReplicaView(meta, NewVChannelViewFromMeta(restoreSchemaTestMeta()), segments, tail, 34)
	require.NoError(t, err)
	require.EqualValues(t, 2, got.WriteSchema.GetVersion())
	require.Empty(t, got.SegmentSnapshot.FlushedSegments)
	require.Len(t, got.SegmentSnapshot.Segments, 1)
	growing := got.SegmentSnapshot.Segments[0]
	require.EqualValues(t, 201, growing.SegmentID)
	require.EqualValues(t, 1, growing.Schema.GetVersion())
}

func TestReplicaQueryReleaseWithFullQueue(t *testing.T) {
	q, _ := newTestReplicaQueries(t, nil)
	started, resume := make(chan struct{}), make(chan struct{})
	patch := mockey.Mock((*queryresource.QueryRuntime).Initialize).To(func(_ *queryresource.QueryRuntime, ctx context.Context, _ walview.VChannelWALView) error {
		close(started)
		<-resume
		return ctx.Err()
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	defer close(resume)
	key, _ := acquireReplicaView(q, 1)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("initialization did not start")
	}
	// The scanner must resume on detach even while a preparing module is busy.
	for tick := uint64(11); tick < 11+1024; tick++ {
		q.Observe(nil, tick)
	}
	done := make(chan struct{})
	go func() { q.Observe(nil, 1100); close(done) }()
	select {
	case <-done:
		t.Fatal("queue did not apply backpressure")
	case <-time.After(20 * time.Millisecond):
	}
	go q.Release(snview.ReleaseResource{Key: key})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dropping the blocked runtime did not unblock the scanner")
	}
	require.EqualValues(t, 1100, q.through)
}

func TestWriterAssignmentFenceCoversOutOfOrderCommits(t *testing.T) {
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	defer q.Close()
	m, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": restoreSchemaTestMeta()}})
	require.NoError(t, err)
	defer m.Close()
	detach := q.BindWriter(m)
	defer detach()
	a, b := new(segment.SegmentView), new(segment.SegmentView)
	mod := m.Module("v1")
	mod.segments = map[int64]*segment.SegmentView{8: a, 10: b}
	early, later := make(chan struct{}), make(chan struct{})
	seen := make(chan struct{}, 8)
	defer mockey.Mock((*segment.SegmentView).PublicationWait).To(func(s *segment.SegmentView, _ int64) <-chan struct{} {
		ch := early
		if s == b {
			ch = later
		}
		select {
		case <-ch:
			return nil
		default:
			seen <- struct{}{}
			return ch
		}
	}).Build().UnPatch()
	done := make(chan error, 1)
	go func() { done <- q.WaitForAssignments(t.Context(), "v1", 10) }()
	<-seen
	<-seen
	close(later)
	select {
	case <-done:
		t.Fatal("published 10 while 8 was pending")
	default:
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, q.WaitForAssignments(canceled, "v1", 10), context.Canceled)
	close(early)
	require.NoError(t, <-done)
	require.EqualValues(t, 10, mod.confirmedQueryStreamingVersion)
	require.NoError(t, q.WaitForAssignments(t.Context(), "v1", 8))
}

func TestReplicaAssignmentCatchupWaitsForLocalWAL(t *testing.T) {
	catalog := sncatalog.NewCataLog(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	defer q.Close()
	var covered atomic.Int64
	read := make(chan struct{}, 4)
	defer mockey.Mock(mockey.GetMethod(catalog, "ListSegmentAssignment")).To(func(_ context.Context, _ string) ([]*streamingpb.SegmentAssignmentMeta, error) {
		read <- struct{}{}
		return []*streamingpb.SegmentAssignmentMeta{
			{Vchannel: "v1", SegmentId: 1, CheckpointTimeTick: 40, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 8}},
			{Vchannel: "v1", SegmentId: 2, CheckpointTimeTick: 45, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 10}},
			{Vchannel: "v2", SegmentId: 3, CheckpointTimeTick: 90, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 9}},
		}, nil
	}).Build().UnPatch()
	done := make(chan error, 1)
	go func() {
		done <- q.syncAssignments(t.Context(), "v1", &covered, 10, func(walview.VChannelResourceEvent) bool { return false })
	}()
	<-read
	require.Zero(t, covered.Load())
	select {
	case <-done:
		t.Fatal("seal applied ahead of WAL")
	default:
	}
	q.Observe(nil, 45)
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, covered.Load())
	var ids []int64
	require.NoError(t, q.syncAssignments(t.Context(), "v1", &covered, 10, func(e walview.VChannelResourceEvent) bool { ids = append(ids, e.SegmentSealed.SegmentID); return true }))
	require.ElementsMatch(t, []int64{1, 2}, ids)
	require.EqualValues(t, 10, covered.Load())
	require.NoError(t, q.syncAssignments(t.Context(), "v1", &covered, 8, nil))
}

func TestDemotionCancelsAssignmentFence(t *testing.T) {
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	defer q.Close()
	m, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p", VChannelMetas: map[string]*streamingpb.VChannelMeta{"v1": restoreSchemaTestMeta()}})
	require.NoError(t, err)
	defer m.Close()
	detach := q.BindWriter(m)
	mod := m.Module("v1")
	mod.segments = map[int64]*segment.SegmentView{1: new(segment.SegmentView)}
	entered := make(chan struct{}, 2)
	blocked := make(chan struct{})
	defer mockey.Mock((*segment.SegmentView).PublicationWait).To(func(_ *segment.SegmentView, _ int64) <-chan struct{} {
		entered <- struct{}{}
		return blocked
	}).Build().UnPatch()
	done := make(chan error, 2)
	go func() {
		done <- q.capture(t.Context(), &viewpb.QueryViewMeta{Vchannel: "v1", RequiredStreamingVersion: 8}, nil)
	}()
	go func() { done <- q.syncAssignments(t.Context(), "v1", new(atomic.Int64), 8, nil) }()
	<-entered
	<-entered
	detach()
	require.ErrorIs(t, <-done, context.Canceled)
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, mod.confirmedQueryStreamingVersion)
}

func TestReplicaSnapshotKeepsUnpublishedFlush(t *testing.T) {
	meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v1", SchemaVersion: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 10}}}
	segments := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: {PartitionId: 10, SegmentId: 1, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED},
		2: {PartitionId: 10, SegmentId: 2, State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED},
		3: {PartitionId: 10, SegmentId: 3, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 8}},
		4: {PartitionId: 10, SegmentId: 4, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 11}},
		5: {PartitionId: 99, SegmentId: 5},
	}
	vc := NewVChannelViewFromMeta(restoreSchemaTestMeta())
	view, err := buildReplicaView(meta, vc, segments, nil, 40)
	require.NoError(t, err)
	var growing []int64
	for _, s := range view.SegmentSnapshot.Segments {
		growing = append(growing, s.SegmentID)
	}
	require.ElementsMatch(t, []int64{1, 4}, growing)
	require.Len(t, view.SegmentSnapshot.FlushedSegments, 1)
	require.EqualValues(t, 3, view.SegmentSnapshot.FlushedSegments[0].SegmentID)
	meta.SchemaVersion = 999
	_, err = buildReplicaView(meta, vc, segments, nil, 40)
	require.Error(t, err)
}

func TestFailedCatalogReadLeavesAssignmentPrefixUnchanged(t *testing.T) {
	catalog := sncatalog.NewCataLog(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	defer q.Close()
	meta := &viewpb.QueryViewMeta{CollectionId: 1, Vchannel: "v1", RequiredStreamingVersion: 8}
	fail := mockey.Mock(mockey.GetMethod(catalog, "ListVChannel")).Return(nil, io.ErrUnexpectedEOF).Build()
	require.ErrorIs(t, q.capture(t.Context(), meta, nil), io.ErrUnexpectedEOF)
	fail.UnPatch()
	defer mockey.Mock(mockey.GetMethod(catalog, "ListVChannel")).Return(nil, nil).Build().UnPatch()
	defer mockey.Mock(mockey.GetMethod(catalog, "ListSegmentAssignment")).Return(nil, io.ErrUnexpectedEOF).Build().UnPatch()
	require.ErrorIs(t, q.capture(t.Context(), meta, nil), io.ErrUnexpectedEOF)
	var covered atomic.Int64
	require.ErrorIs(t, q.syncAssignments(t.Context(), "v1", &covered, 8, nil), io.ErrUnexpectedEOF)
	require.Zero(t, covered.Load())
}
