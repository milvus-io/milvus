package queryclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	queryreducestream "github.com/milvus-io/milvus/internal/util/queryutil/reduce_stream"
	searchreducestream "github.com/milvus-io/milvus/internal/util/searchutil/reduce_stream"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestCompositeViewQueryServiceClientDispatchesByWorkNode(t *testing.T) {
	snStream := &fakeSearchStream{}
	qnStream := &fakeSearchStream{}
	snQueryStream := &fakeQueryStream{}
	qnQueryStream := &fakeQueryStream{}
	snClient := &fakeStreamingNodeViewQueryServiceClient{
		searchResp:   &viewpb.SearchOnViewResponse{},
		searchStream: snStream,
		queryStream:  snQueryStream,
	}
	qnClient := &fakeQueryNodeViewQueryServiceClient{
		queryResp:    &viewpb.QueryOnViewResponse{},
		searchStream: qnStream,
		queryStream:  qnQueryStream,
	}
	client := NewCompositeViewQueryServiceClient(snClient, qnClient)

	searchResp, err := client.SearchOnView(context.Background(), qviews.StreamingNode{PChannel: "p0"}, &viewpb.SearchOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, snClient.searchResp, searchResp)
	require.Equal(t, types.PChannelInfo{Name: "p0"}, snClient.pchannel)

	queryResp, err := client.QueryOnView(context.Background(), qviews.NewQueryNode(11), &viewpb.QueryOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, qnClient.queryResp, queryResp)
	require.Equal(t, int64(11), qnClient.nodeID)

	stream, err := client.SearchOnViewStream(context.Background(), qviews.StreamingNode{PChannel: "p1"}, &viewpb.SearchOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, snStream, stream)
	require.Equal(t, types.PChannelInfo{Name: "p1"}, snClient.pchannel)

	stream, err = client.SearchOnViewStream(context.Background(), qviews.NewQueryNode(12), &viewpb.SearchOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, qnStream, stream)
	require.Equal(t, int64(12), qnClient.nodeID)

	queryStream, err := client.QueryOnViewStream(context.Background(), qviews.StreamingNode{PChannel: "p2"}, &viewpb.QueryOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, snQueryStream, queryStream)
	require.Equal(t, types.PChannelInfo{Name: "p2"}, snClient.pchannel)

	queryStream, err = client.QueryOnViewStream(context.Background(), qviews.NewQueryNode(13), &viewpb.QueryOnViewRequest{})
	require.NoError(t, err)
	require.Same(t, qnQueryStream, queryStream)
	require.Equal(t, int64(13), qnClient.nodeID)
}

type fakeStreamingNodeViewQueryServiceClient struct {
	StreamingNodeViewQueryServiceClient
	pchannel     types.PChannelInfo
	searchResp   *viewpb.SearchOnViewResponse
	searchStream searchreducestream.ReduceStream
	queryStream  queryreducestream.ReduceStream
}

func (f *fakeStreamingNodeViewQueryServiceClient) SearchOnView(_ context.Context, pchannel types.PChannelInfo, _ *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	f.pchannel = pchannel
	return f.searchResp, nil
}

func (f *fakeStreamingNodeViewQueryServiceClient) SearchOnViewStream(_ context.Context, pchannel types.PChannelInfo, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
	f.pchannel = pchannel
	return f.searchStream, nil
}

func (f *fakeStreamingNodeViewQueryServiceClient) QueryOnView(context.Context, types.PChannelInfo, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	return &viewpb.QueryOnViewResponse{}, nil
}

func (f *fakeStreamingNodeViewQueryServiceClient) QueryOnViewStream(_ context.Context, pchannel types.PChannelInfo, _ *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
	f.pchannel = pchannel
	return f.queryStream, nil
}

type fakeQueryNodeViewQueryServiceClient struct {
	QueryNodeViewQueryServiceClient
	nodeID       int64
	queryResp    *viewpb.QueryOnViewResponse
	searchStream searchreducestream.ReduceStream
	queryStream  queryreducestream.ReduceStream
}

func (f *fakeQueryNodeViewQueryServiceClient) SearchOnView(context.Context, int64, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	return &viewpb.SearchOnViewResponse{}, nil
}

func (f *fakeQueryNodeViewQueryServiceClient) SearchOnViewStream(_ context.Context, nodeID int64, _ *viewpb.SearchOnViewRequest) (searchreducestream.ReduceStream, error) {
	f.nodeID = nodeID
	return f.searchStream, nil
}

func (f *fakeQueryNodeViewQueryServiceClient) QueryOnView(_ context.Context, nodeID int64, _ *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	f.nodeID = nodeID
	return f.queryResp, nil
}

type prewarmStreamingTransport struct {
	StreamingNodeViewQueryServiceClient
	owner types.PChannelInfoAssigned
	req   *viewpb.PrewarmOnViewRequest
}

func (c *prewarmStreamingTransport) PrewarmOnView(_ context.Context, owner types.PChannelInfoAssigned, req *viewpb.PrewarmOnViewRequest) (*viewpb.PrewarmOnViewResponse, error) {
	c.owner, c.req = owner, req
	return &viewpb.PrewarmOnViewResponse{}, nil
}

type prewarmQueryTransport struct {
	QueryNodeViewQueryServiceClient
	nodeID int64
	req    *viewpb.PrewarmOnViewRequest
}

func (c *prewarmQueryTransport) PrewarmOnView(_ context.Context, nodeID int64, req *viewpb.PrewarmOnViewRequest) (*viewpb.PrewarmOnViewResponse, error) {
	c.nodeID, c.req = nodeID, req
	return &viewpb.PrewarmOnViewResponse{}, nil
}

func TestCompositePrewarmUsesCapturedWorkersAndOwner(t *testing.T) {
	sn, qn := &prewarmStreamingTransport{}, &prewarmQueryTransport{}
	c := NewCompositeViewQueryServiceClient(sn, qn)
	plan := &qviews.PrewarmPlan{
		Plan:          &viewpb.QueryPlan{ShardId: &viewpb.ShardID{ReplicaId: 2, Vchannel: "p0_100v0"}, Version: &viewpb.QueryViewVersion{QueryVersion: 7}},
		StreamingNode: types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: "p0", Term: 8}, Node: types.StreamingNodeInfo{ServerID: 9}},
	}
	require.NoError(t, c.PrewarmOnView(t.Context(), qviews.StreamingNode{PChannel: "p0"}, plan))
	require.Equal(t, plan.StreamingNode, sn.owner)
	require.Equal(t, plan.Plan.Version, sn.req.Version)
	require.NoError(t, c.PrewarmOnView(t.Context(), qviews.NewQueryNode(11), plan))
	require.EqualValues(t, 11, qn.nodeID)
	require.Equal(t, sn.req, qn.req)
	require.Error(t, c.PrewarmOnView(t.Context(), qviews.StreamingNode{PChannel: "wrong"}, plan))
}

func (f *fakeQueryNodeViewQueryServiceClient) QueryOnViewStream(_ context.Context, nodeID int64, _ *viewpb.QueryOnViewRequest) (queryreducestream.ReduceStream, error) {
	f.nodeID = nodeID
	return f.queryStream, nil
}
