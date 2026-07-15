package queryclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestCompositeViewQueryServiceClientDispatchesByWorkNode(t *testing.T) {
	snClient := &fakeStreamingNodeViewQueryServiceClient{searchResp: &viewpb.SearchOnViewResponse{}}
	qnClient := &fakeQueryNodeViewQueryServiceClient{queryResp: &viewpb.QueryOnViewResponse{}}
	client := NewCompositeViewQueryServiceClient(snClient, qnClient)

	searchResp, err := client.SearchOnView(context.Background(), qviews.StreamingNode{PChannel: "p0"}, &viewpb.SearchOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, snClient.searchResp, searchResp)
	require.Equal(t, types.PChannelInfo{Name: "p0"}, snClient.pchannel)

	queryResp, err := client.QueryOnView(context.Background(), qviews.NewQueryNode(11), &viewpb.QueryOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, qnClient.queryResp, queryResp)
	require.Equal(t, int64(11), qnClient.nodeID)
}

type fakeStreamingNodeViewQueryServiceClient struct {
	pchannel   types.PChannelInfo
	searchResp *viewpb.SearchOnViewResponse
}

func (f *fakeStreamingNodeViewQueryServiceClient) SearchOnView(_ context.Context, pchannel types.PChannelInfo, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	f.pchannel = pchannel
	return f.searchResp, nil
}

func (f *fakeStreamingNodeViewQueryServiceClient) QueryOnView(context.Context, types.PChannelInfo, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return &viewpb.QueryOnViewResponse{}, nil
}

func (f *fakeStreamingNodeViewQueryServiceClient) RequeryOnView(context.Context, types.PChannelInfo, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return &viewpb.RequeryOnViewResponse{}, nil
}

type fakeQueryNodeViewQueryServiceClient struct {
	nodeID    int64
	queryResp *viewpb.QueryOnViewResponse
}

func (f *fakeQueryNodeViewQueryServiceClient) SearchOnView(context.Context, int64, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return &viewpb.SearchOnViewResponse{}, nil
}

func (f *fakeQueryNodeViewQueryServiceClient) QueryOnView(_ context.Context, nodeID int64, _ *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	f.nodeID = nodeID
	return f.queryResp, nil
}

func (f *fakeQueryNodeViewQueryServiceClient) RequeryOnView(context.Context, int64, *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	return &viewpb.RequeryOnViewResponse{}, nil
}
