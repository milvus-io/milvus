package queryclient

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompositeViewQueryServiceClient(t *testing.T) {
	mockey.PatchConvey("dispatches every Legacy operation by work-node type", t, func() {
		var searchPChannel types.PChannelInfo
		var queryPChannel types.PChannelInfo
		var searchNodeID int64
		var queryNodeID int64
		searchResponse := &viewpb.SearchOnViewResponse{}
		queryResponse := &viewpb.QueryOnViewResponse{}

		mockey.Mock((*mockStreamingNodeClient).SearchOnView).To(
			func(_ *mockStreamingNodeClient, _ context.Context, channel types.PChannelInfo, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				searchPChannel = channel
				return searchResponse, nil
			}).Build()
		mockey.Mock((*mockStreamingNodeClient).QueryOnView).To(
			func(_ *mockStreamingNodeClient, _ context.Context, channel types.PChannelInfo, _ *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
				queryPChannel = channel
				return queryResponse, nil
			}).Build()
		mockey.Mock((*mockQueryNodeClient).SearchOnView).To(
			func(_ *mockQueryNodeClient, _ context.Context, nodeID int64, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				searchNodeID = nodeID
				return searchResponse, nil
			}).Build()
		mockey.Mock((*mockQueryNodeClient).QueryOnView).To(
			func(_ *mockQueryNodeClient, _ context.Context, nodeID int64, _ *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
				queryNodeID = nodeID
				return queryResponse, nil
			}).Build()

		client := NewCompositeViewQueryServiceClient(&mockStreamingNodeClient{}, &mockQueryNodeClient{})
		response, err := client.SearchOnView(context.Background(), qviews.StreamingNode{PChannel: "p0"}, &viewpb.SearchOnViewRequest{})
		require.NoError(t, err)
		require.Same(t, searchResponse, response)
		response, err = client.SearchOnView(context.Background(), qviews.NewQueryNode(10), &viewpb.SearchOnViewRequest{})
		require.NoError(t, err)
		require.Same(t, searchResponse, response)
		queryResult, err := client.QueryOnView(context.Background(), qviews.StreamingNode{PChannel: "p1"}, &viewpb.QueryOnViewRequest{})
		require.NoError(t, err)
		require.Same(t, queryResponse, queryResult)
		queryResult, err = client.QueryOnView(context.Background(), qviews.NewQueryNode(11), &viewpb.QueryOnViewRequest{})
		require.NoError(t, err)
		require.Same(t, queryResponse, queryResult)

		require.Equal(t, types.PChannelInfo{Name: "p0"}, searchPChannel)
		require.Equal(t, types.PChannelInfo{Name: "p1"}, queryPChannel)
		require.Equal(t, int64(10), searchNodeID)
		require.Equal(t, int64(11), queryNodeID)
	})
}

func TestCompositeViewQueryServiceClientRejectsMissingTargets(t *testing.T) {
	client := NewCompositeViewQueryServiceClient(nil, nil)

	_, err := client.SearchOnView(context.Background(), qviews.StreamingNode{PChannel: "p0"}, &viewpb.SearchOnViewRequest{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = client.QueryOnView(context.Background(), qviews.StreamingNode{PChannel: "p0"}, &viewpb.QueryOnViewRequest{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = client.SearchOnView(context.Background(), qviews.NewQueryNode(1), &viewpb.SearchOnViewRequest{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = client.QueryOnView(context.Background(), qviews.NewQueryNode(1), &viewpb.QueryOnViewRequest{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	_, err = client.QueryOnView(context.Background(), nil, &viewpb.QueryOnViewRequest{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
