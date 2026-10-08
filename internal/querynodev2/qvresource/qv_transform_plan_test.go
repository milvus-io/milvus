package qvresource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestPlannedLoadHasOneTransformBaselineAcrossReopen(t *testing.T) {
	segment := &fakeQVSegment{id: 10}
	patches := []*mockey.Mocker{
		mockey.Mock((*fakeQVLoader).NewSegment).Return(segment, nil).Build(),
		mockey.Mock((*fakeQVLoader).LoadSegment).Return(nil).Build(),
		mockey.Mock((*fakeQVLoader).LoadDeltaLogs).Return(nil).Build(),
		mockey.Mock((*fakeQVLoader).LoadPKCandidate).Return(nil).Build(),
		mockey.Mock((*fakeQVLoader).ReopenSegment).Return(nil).Build(),
	}
	defer func() {
		for _, patch := range patches {
			patch.UnPatch()
		}
	}()
	for _, frontier := range []uint64{0, 99} {
		loader := newQueryViewPhysicalSegmentLoader(&fakeQVLoader{})
		info := &querypb.SegmentLoadInfo{SegmentID: 10, DeltaPosition: &msgpb.MsgPosition{Timestamp: 50}}
		loaded, err := loader.LoadWithPlan(context.Background(), qnview.SegmentLoadPlan{Collection: fakeQVCollectionRuntime{}, LoadInfo: info, TransformStartAfterTimeTick: frontier})
		require.NoError(t, err)
		require.IsType(t, &queryViewTransformSegment{}, loaded)
		require.Equal(t, frontier, loaded.TransformStartAfterTimeTick())
		require.Equal(t, frontier, loaded.AppliedTransformTimeTick())
		require.NoError(t, loaded.WaitTransformApplied(context.Background(), frontier))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, loaded.WaitTransformApplied(ctx, frontier+1), context.Canceled)
		require.NoError(t, loaded.ApplyTransform(context.Background(), &streamingpb.TransformLogEntry{TimeTick: 120}))
		require.NoError(t, loader.Update(context.Background(), loaded, fakeQVCollectionRuntime{}, qnview.SegmentLoadInfoSnapshot{LoadInfo: info}, qnview.SegmentUpdateReopen))
		require.EqualValues(t, 120, loaded.AppliedTransformTimeTick(), "Reopen must not reset applied progress")
		require.Equal(t, frontier, loaded.TransformStartAfterTimeTick())
	}
}
