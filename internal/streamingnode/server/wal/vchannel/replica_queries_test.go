package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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

func TestReplicaQueryAdmissionDuringForwardRecovery(t *testing.T) {
	scheduler := nodescheduler.New(1)
	defer scheduler.Close()
	q := NewReplicaQueries(t.Context(), "p", replica.NewBuffer(10), 10)
	defer q.Close()
	ch := &queryChannel{
		refs: make(map[qviews.QueryViewKey]*snview.AcquireResource),
		resources: queryresource.NewManager(queryresource.Config{
			Scheduler: scheduler, Dispatcher: q.dispatch,
			Builders: []queryresource.QueryRuntimeModuleBuilder{nil},
			CaptureView: func(_ context.Context, _ *viewpb.QueryViewMeta, install func(walview.VChannelWALView)) error {
				install(walview.VChannelWALView{})
				return nil
			},
		}),
	}
	q.active["v1"] = ch
	acquire := func(version int64) (qviews.QueryViewKey, <-chan bool) {
		meta := &viewpb.QueryViewMeta{
			Vchannel: "v1", ReplicaId: 1, Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: version}},
		}
		key := qviews.QueryViewKey{ShardID: qviews.NewShardIDFromQVMeta(meta), QueryViewVersion: qviews.FromProtoQueryViewVersion(meta.Version)}
		outcome := make(chan bool, 1)
		q.Acquire(snview.AcquireResource{Key: key, Meta: meta, OnReady: func() { outcome <- true }, OnUnrecoverable: func() { outcome <- false }})
		return key, outcome
	}
	expect := func(result <-chan bool, ready bool) {
		t.Helper()
		select {
		case got := <-result:
			require.Equal(t, ready, got)
		case <-time.After(5 * time.Second):
			t.Fatal("query resource preparation did not report")
		}
	}

	high, first := acquire(8)
	expect(first, true)
	expired, obsolete := acquire(2)
	_, third := acquire(3)
	_, fourth := acquire(4)
	q.Release(snview.ReleaseResource{Key: expired})
	q.Release(snview.ReleaseResource{Key: high})
	expect(third, true)
	expect(fourth, true)
	select {
	case <-obsolete:
		t.Fatal("a dropped pending acquisition was revived")
	default:
	}
	_, waiting := acquire(1)
	q.SetWriter(true)
	expect(waiting, false)
}
