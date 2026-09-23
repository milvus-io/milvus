package growingruntime

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestRetainedSegmentIsOnlyQueriedBeforeItsSealedDataVersion(t *testing.T) {
	runtime := newRuntime()
	defer runtime.Close()
	sealedAt := qviews.DataVersion{StreamingVersion: 10, CompactVersion: 2}
	retained := newGrowingSegment(nil, 10, 100)
	retained.segment = fakeCSegment{id: 10}
	retained.markFlushed(50)
	retained.markSealed(sealedAt)
	require.True(t, runtime.addSegment(retained))
	growing := newGrowingSegment(nil, 20, 200)
	growing.segment = fakeCSegment{id: 20}
	require.True(t, runtime.addSegment(growing))
	// A replay marker has a sealed version but no queryable physical segment.
	marker := newGrowingSegment(nil, 30, 100)
	marker.markSealed(sealedAt)
	require.True(t, runtime.addSegment(marker))
	runtime.markGrowingTimeTick(100)
	runtime.markTransformTimeTick(100)
	runtime.Advance(qviews.DataVersion{StreamingVersion: 9})

	for _, tc := range []struct {
		name    string
		version qviews.DataVersion
		visible bool
	}{
		{"older streaming version", qviews.DataVersion{StreamingVersion: 9, CompactVersion: 100}, true},
		{"older compact version", qviews.DataVersion{StreamingVersion: 10, CompactVersion: 1}, true},
		{"first sealed version", sealedAt, false},
		{"newer compact version", qviews.DataVersion{StreamingVersion: 10, CompactVersion: 3}, false},
		{"newer streaming version", qviews.DataVersion{StreamingVersion: 11}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.visible, runtime.MayHaveVisibleGrowingSegments(tc.version, 100, 100, []int64{100}))
			handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), tc.version, []int64{100})
			require.NoError(t, err)
			if tc.visible {
				require.Len(t, handles, 1)
				require.Equal(t, int64(10), handles[0].ID())
			} else {
				require.Empty(t, handles)
			}
			for _, handle := range handles {
				handle.Release()
			}
			// Unsealed growing data remains visible regardless of the view version.
			require.True(t, runtime.MayHaveVisibleGrowingSegments(tc.version, 100, 100, []int64{200}))
			handles, err = runtime.AcquireGrowingSegmentHandles(context.Background(), tc.version, []int64{200})
			require.NoError(t, err)
			require.Len(t, handles, 1)
			require.Equal(t, int64(20), handles[0].ID())
			handles[0].Release()
		})
	}
	require.Equal(t, 3, len(runtime.SegmentIDs()), "query filtering must not reclaim resources retained for an older view")
	// Phase 1 cannot prune before both requested MVCC frontiers are visible.
	require.True(t, runtime.MayHaveVisibleGrowingSegments(sealedAt, 101, 100, []int64{100}))
	require.True(t, runtime.MayHaveVisibleGrowingSegments(sealedAt, 100, 101, []int64{100}))
}

func TestSealedCutoverPreservesAlreadyAcquiredHandles(t *testing.T) {
	runtime := newRuntime()
	defer runtime.Close()
	released := atomic.Int32{}
	segment := newGrowingSegment(nil, 10, 100)
	segment.segment = fakeCSegment{id: 10, releaseCount: &released}
	require.True(t, runtime.addSegment(segment))
	old := qviews.DataVersion{StreamingVersion: 1}
	current := qviews.DataVersion{StreamingVersion: 2}
	runtime.markGrowingTimeTick(50)
	runtime.markTransformTimeTick(50)
	held, err := runtime.AcquireGrowingSegmentHandles(context.Background(), old, nil)
	require.NoError(t, err)
	require.Len(t, held, 1)
	segment.markFlushed(50)
	runtime.markSegmentSealed(10, current)
	require.False(t, runtime.MayHaveVisibleGrowingSegments(current, 50, 50, nil))
	handles, err := runtime.AcquireGrowingSegmentHandles(context.Background(), current, nil)
	require.NoError(t, err)
	require.Empty(t, handles)
	handles, err = runtime.AcquireGrowingSegmentHandles(context.Background(), old, nil)
	require.NoError(t, err)
	require.Len(t, handles, 1)
	handles[0].Release()
	// Dropping the old view can advance retention, but replay and active handles
	// still independently protect physical resources.
	runtime.Advance(current)
	require.Len(t, runtime.SegmentIDs(), 1)
	runtime.markGrowingTimeTick(51)
	require.Empty(t, runtime.SegmentIDs())
	require.Zero(t, released.Load())
	require.Equal(t, int64(10), held[0].Segment().ID())
	held[0].Release()
	held[0].Release()
	require.Equal(t, int32(1), released.Load())
}
