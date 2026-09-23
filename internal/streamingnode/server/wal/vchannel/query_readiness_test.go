package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestQueryReadinessPublishesCompletedCommitBeforeOwnerNotification(t *testing.T) {
	sealed := segment.NewSegmentViewFromMetaWithConfig(&streamingpb.SegmentAssignmentMeta{
		SegmentId: 7, Vchannel: "v1",
		State:               streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 2},
	}, nil, segment.ViewConfig{})
	open := segment.NewSegmentViewFromMetaWithConfig(&streamingpb.SegmentAssignmentMeta{
		SegmentId: 8, Vchannel: "v1",
		State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
	}, nil, segment.ViewConfig{})
	module := &VChannelRecoveryModule{
		segments:       map[int64]*segment.SegmentView{7: sealed, 8: open},
		querySealed:    make(map[int64]bool),
		queryResources: queryresource.NewManager(queryresource.Config{}),
	}
	defer module.queryResources.Close()
	var events []walview.SegmentSealedEvent
	observed := mockey.Mock((*queryresource.Manager).ObserveEvent).To(func(_ *queryresource.Manager, _ context.Context, event walview.VChannelResourceEvent) {
		// Copy before return: the notification can be allocated on the caller's stack.
		events = append(events, *event.SegmentSealed)
	}).Build()
	defer observed.UnPatch()
	pending := mockey.Mock((*segment.SegmentView).EnsureFinalCommit).Return(false).Build()
	require.False(t, module.prepareQueryViewLocked())
	require.Empty(t, events)
	pending.UnPatch()
	// Durable metadata is installed, but the owner callback has not run yet.
	// The readiness check must publish it itself, without flushing the open segment.
	require.True(t, module.prepareQueryViewLocked())
	require.Equal(t, []walview.SegmentSealedEvent{{SegmentID: 7, SealedAtDataVersion: qviews.DataVersion{StreamingVersion: 2}}}, events)
	require.True(t, module.prepareQueryViewLocked())
	module.publishSegmentSealedLocked(7)
	require.Len(t, events, 1, "later owner callbacks must not duplicate publication")
}
