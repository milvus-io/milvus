package handler

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type syncViewService struct {
	lazygrpc.Service[viewpb.ViewSyncServiceClient]
}

func (*syncViewService) GetService(context.Context) (viewpb.ViewSyncServiceClient, error) {
	panic("mock with mockey")
}

type syncViewRPC struct{ viewpb.ViewSyncServiceClient }

func (*syncViewRPC) SyncQueryView(context.Context, ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
	panic("mock with mockey")
}

type syncViewStream struct {
	viewpb.ViewSyncService_SyncQueryViewClient
}

func TestQueryViewSyncClientAssignmentMigration(t *testing.T) {
	mockey.PatchConvey("reopen on the current SN assignment", t, func() {
		term := int64(1)
		mockey.Mock((*analyzerWatcher).Get).To(func(_ *analyzerWatcher, _ context.Context, channel string) *types.PChannelInfoAssigned {
			require.Equal(t, "p", channel)
			return &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: channel, Term: term, AccessMode: types.AccessModeRW}, Node: types.StreamingNodeInfo{ServerID: term + 10}}
		}).Build()
		mockey.Mock((*analyzerWatcher).Watch).To(func(_ *analyzerWatcher, _ context.Context, _ string, previous *types.PChannelInfoAssigned) error {
			require.EqualValues(t, 1, previous.Channel.Term)
			term++
			return nil
		}).Build()
		reports := mockey.Mock((*analyzerTrigger).ReportAssignmentError).Return(nil).Build()
		mockey.Mock((*syncViewService).GetService).Return(&syncViewRPC{}, nil).Build()
		stream := &syncViewStream{}
		rpc := mockey.Mock((*syncViewRPC).SyncQueryView).To(func(_ *syncViewRPC, ctx context.Context, _ ...grpc.CallOption) (viewpb.ViewSyncService_SyncQueryViewClient, error) {
			node, ok := contextutil.GetPickServerID(ctx)
			require.True(t, ok)
			require.Equal(t, term+10, node)
			md, ok := metadata.FromOutgoingContext(ctx)
			require.True(t, ok)
			channel, err := worknodehandler.DecodeQueryViewPChannelFromIncomingContext(metadata.NewIncomingContext(ctx, md))
			require.NoError(t, err)
			require.Equal(t, types.PChannelInfo{Name: "p", Term: term, AccessMode: types.AccessModeRW}, channel)
			if term == 1 {
				return nil, status.NewGRPCStatusFromStreamingError(status.NewChannelNotExist("moved")).Err()
			}
			return stream, nil
		}).Build()
		owner := &handlerClientImpl{watcher: &analyzerWatcher{}, rebalanceTrigger: &analyzerTrigger{}}
		owner.queryViewSyncClient = newQueryViewSyncClient(owner, nil)
		client := owner.QueryViewSyncClient().(*queryViewSyncClient)
		client.viewSyncService = &syncViewService{}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		got, err := client.SyncQueryView(ctx, "p")
		require.NoError(t, err)
		require.Same(t, stream, got)
		require.Equal(t, 2, rpc.Times())
		require.Equal(t, 1, reports.Times())
	})
}

func TestQueryViewSyncClientServiceFailureAndCancellation(t *testing.T) {
	mockey.PatchConvey("service acquisition failure honors cancellation", t, func() {
		mockey.Mock((*analyzerWatcher).Get).Return(&types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p", Term: 1, AccessMode: types.AccessModeRW}}).Build()
		mockey.Mock((*analyzerWatcher).Watch).Return(context.Canceled).Build()
		service := mockey.Mock((*syncViewService).GetService).Return(nil, context.Canceled).Build()
		client := &queryViewSyncClient{owner: &handlerClientImpl{watcher: &analyzerWatcher{}}, viewSyncService: &syncViewService{}}
		stream, err := client.SyncQueryView(t.Context(), "p")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, stream)
		require.Equal(t, 1, service.Times())
	})
}
