package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var _ ManagerService = (*managerServiceImpl)(nil)

// NewManagerService create a streamingnode manager service.
func NewManagerService(m walmanager.Manager) ManagerService {
	return &managerServiceImpl{
		m,
	}
}

type ManagerService interface {
	streamingpb.StreamingNodeManagerServiceServer
}

// managerServiceImpl implements ManagerService.
// managerServiceImpl is just a rpc level to handle incoming grpc.
// all manager logic should be done in wal.Manager.
type managerServiceImpl struct {
	walManager walmanager.Manager
}

// Assign assigns a wal instance for the channel on this Manager.
// After assign returns, the wal instance is ready to use.
func (ms *managerServiceImpl) Assign(ctx context.Context, req *streamingpb.StreamingNodeManagerAssignRequest) (*streamingpb.StreamingNodeManagerAssignResponse, error) {
	pchannelInfo := types.NewPChannelInfoFromProto(req.GetPchannel())
	if err := ms.walManager.Open(ctx, pchannelInfo); err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerAssignResponse{}, nil
}

// Remove removes the wal instance for the channel.
// After remove returns, the wal instance is removed and all underlying read write operation should be rejected.
func (ms *managerServiceImpl) Remove(ctx context.Context, req *streamingpb.StreamingNodeManagerRemoveRequest) (*streamingpb.StreamingNodeManagerRemoveResponse, error) {
	pchannelInfo := types.NewPChannelInfoFromProto(req.GetPchannel())
	if err := ms.walManager.Remove(ctx, pchannelInfo); err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerRemoveResponse{}, nil
}

// CollectStatus collects the status of all wal instances in these streamingnode.
func (ms *managerServiceImpl) CollectStatus(ctx context.Context, req *streamingpb.StreamingNodeManagerCollectStatusRequest) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
	// TODO: collect traffic metric for load balance.
	return &streamingpb.StreamingNodeManagerCollectStatusResponse{
		BalanceAttributes: &streamingpb.StreamingNodeBalanceAttributes{},
	}, nil
}
