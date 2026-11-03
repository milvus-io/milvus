package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	walbalancer "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	walbalance "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestWALTransferChannel(t *testing.T) {
	ctx := t.Context()
	server := &Server{ctx: ctx}
	b := &mock_balancer.MockBalancer{}
	get := mockey.Mock(walbalance.GetWithContext).Return(b, nil).Build()
	defer get.UnPatch()
	var received walbalancer.TransferChannelsRequest
	calls := 0
	transfer := mockey.Mock((*mock_balancer.MockBalancer).TransferChannels).To(func(_ *mock_balancer.MockBalancer, _ context.Context, req walbalancer.TransferChannelsRequest) error {
		received = req
		calls++
		return nil
	}).Build()
	defer transfer.UnPatch()

	server.UpdateStateCode(commonpb.StateCode_Healthy)
	req := &querypb.TransferChannelRequest{SourceNodeID: 11, TargetNodeID: 12, ChannelName: "p1_100v0"}
	resp, err := server.TransferChannel(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, walbalancer.TransferChannelsRequest{SourceNodeID: 11, TargetNodeID: 12, ChannelName: "p1_100v0"}, received)

	req.TransferAll, req.ToAllNodes = true, true
	resp, err = server.TransferChannel(ctx, req)
	require.NoError(t, merr.CheckRPCCall(resp, err))
	require.Equal(t, walbalancer.TransferChannelsRequest{SourceNodeID: 11}, received)
	require.Equal(t, 2, calls)

	req.CopyMode = true
	resp, err = server.TransferChannel(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceUnimplemented)
	req.CopyMode = false
	req.SourceNodeID = 0
	resp, err = server.TransferChannel(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	req.SourceNodeID = 11
	req.ToAllNodes = false
	req.TargetNodeID = 11
	resp, err = server.TransferChannel(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	req.TargetNodeID = 12
	req.TransferAll = false
	req.ChannelName = ""
	resp, err = server.TransferChannel(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)
	require.Equal(t, 2, calls)

	req.TransferAll = true
	transfer.UnPatch()
	transfer = mockey.Mock((*mock_balancer.MockBalancer).TransferChannels).Return(merr.ErrServiceUnavailable).Build()
	defer transfer.UnPatch()
	resp, err = server.TransferChannel(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceUnavailable)

	server.UpdateStateCode(commonpb.StateCode_Abnormal)
	resp, err = server.TransferChannel(ctx, req)
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrServiceNotReady)
}
