package grpcmixcoordclient

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGetDataViewRetention(t *testing.T) {
	paramtable.Init()
	dc := &mocks.MockDataCoordClient{}
	transport := &mocks.MockGrpcClient[MixCoordClient]{}
	patchTransport := mockey.Mock((*mocks.MockGrpcClient[MixCoordClient]).ReCall).To(func(_ *mocks.MockGrpcClient[MixCoordClient], ctx context.Context, f func(MixCoordClient) (interface{}, error)) (interface{}, error) {
		return f(MixCoordClient{DataCoordClient: dc})
	}).Build()
	defer patchTransport.UnPatch()
	expected := &datapb.GetDataViewRetentionResponse{Status: merr.Success(), CollectionIds: []int64{1}, VchannelIndices: []uint32{0}, TransformStartAfterTimeticks: []uint64{30}}
	req := &datapb.GetDataViewRetentionRequest{Pchannel: "p"}
	option := grpc.WaitForReady(true)
	patchRPC := mockey.Mock((*mocks.MockDataCoordClient).GetDataViewRetention).To(func(_ *mocks.MockDataCoordClient, _ context.Context, actual *datapb.GetDataViewRetentionRequest, opts ...grpc.CallOption) (*datapb.GetDataViewRetentionResponse, error) {
		require.NotSame(t, req, actual)
		require.Equal(t, "p", actual.Pchannel)
		require.EqualValues(t, 42, actual.GetBase().GetTargetID())
		require.Equal(t, []grpc.CallOption{option}, opts)
		return expected, nil
	}).Build()
	defer patchRPC.UnPatch()
	c := &Client{grpcClient: transport, sess: &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 42}}}
	resp, err := c.GetDataViewRetention(t.Context(), req, option)
	require.NoError(t, err)
	require.Same(t, expected, resp)
	require.Nil(t, req.Base)
}
