package growingruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestLoadedPartitionScopeAppliesToSnapshotLiveAndHandles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		loaded   []int64
		expected []int64
	}{
		{"restricted", []int64{10}, []int64{10}},
		{"empty", []int64{}, []int64{}},
		{"legacy", nil, []int64{10, 20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRuntime()
			defer r.Close()
			ctx := context.Background()
			require.NoError(t, r.Prepare(ctx, walview.VChannelWALView{PartitionIDs: tc.loaded, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 1, PartitionID: 10}, {SegmentID: 2, PartitionID: 20}}}}))
			require.Len(t, r.SegmentIDs(), len(tc.expected))
			for _, s := range r.segments {
				s.segment = fakeCSegment{id: s.segmentID}
			}
			v := qviews.DataVersion{StreamingVersion: 10}
			handles, err := r.AcquireGrowingSegmentHandles(ctx, v, nil)
			require.NoError(t, err)
			actual := make([]int64, 0, len(handles))
			for _, h := range handles {
				actual = append(actual, h.PartitionID())
				h.Release()
			}
			require.ElementsMatch(t, tc.expected, actual)
			require.Equal(t, len(tc.expected) > 0, r.MayHaveVisibleGrowingSegments(v, 0, 0, nil))
			for _, p := range []int64{10, 20} {
				msg := message.NewCreateSegmentMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 1, PartitionId: p, SegmentId: p}).WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(uint64(p)).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(p))
				r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: msg})
				if !r.partitionLoaded(p) {
					require.NotContains(t, r.SegmentIDs(), p)
					hs, err := r.AcquireGrowingSegmentHandles(ctx, v, []int64{p})
					require.ErrorIs(t, err, merr.ErrPartitionNotLoaded)
					require.Empty(t, hs)
					require.True(t, r.MayHaveVisibleGrowingSegments(v, 0, 0, []int64{p}), "must not skip the error as an empty result")
				} else {
					require.Contains(t, r.SegmentIDs(), p)
				}
			}
			// Existing fixture creates an insert in partition 10. It must not allocate
			// an implicit growing segment if the resolved scope is empty.
			r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{Message: newTestAssignedInsertMessage(t, "v1", 99, 30)})
			require.Equal(t, r.partitionLoaded(10), r.segments[99] != nil)
		})
	}
}

func TestLoadedPartitionReceivesSealWithoutPartitionID(t *testing.T) {
	r := newRuntime()
	defer r.Close()
	ctx := context.Background()
	require.NoError(t, r.Prepare(ctx, walview.VChannelWALView{PartitionIDs: []int64{10}, SegmentSnapshot: walview.VisibleSegmentSnapshot{Segments: []walview.VisibleSegment{{SegmentID: 1, PartitionID: 10}}}}))
	r.segments[1].segment = fakeCSegment{id: 1}
	version := qviews.DataVersion{StreamingVersion: 10}
	r.ApplyLiveEvent(ctx, walview.VChannelResourceEvent{SegmentSealed: &walview.SegmentSealedEvent{SegmentID: 1, SealedAtDataVersion: version}})
	handles, err := r.AcquireGrowingSegmentHandles(ctx, version, nil)
	require.NoError(t, err)
	defer func() {
		for _, h := range handles {
			h.Release()
		}
	}()
	require.Empty(t, handles)
	require.False(t, r.MayHaveVisibleGrowingSegments(version, 0, 0, nil))
}
