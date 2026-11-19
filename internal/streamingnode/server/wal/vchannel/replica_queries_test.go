package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	q := NewReplicaQueries(ctx, "p", replica.NewBuffer(10), 10)
	defer q.Close()
	q.Observe(nil, 15)
	q.Observe(nil, 12)
	through, err := q.Prefix(ctx, 14)
	require.NoError(t, err)
	require.EqualValues(t, 15, through)
	stopped, stop := context.WithCancel(ctx)
	stop()
	_, err = q.Prefix(stopped, 16)
	require.ErrorIs(t, err, context.Canceled)
	cancel()
	_, err = q.Prefix(t.Context(), 16)
	require.ErrorIs(t, err, context.Canceled)
}
