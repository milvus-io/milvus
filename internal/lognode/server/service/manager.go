package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	"github.com/milvus-io/milvus/internal/proto/logpb"
)

var _ ManagerService = (*managerServiceImpl)(nil)

// NewManagerService create a lognode manager service.
func NewManagerService(m walmanager.Manager) ManagerService {
	return &managerServiceImpl{
		m,
	}
}

type ManagerService interface {
	logpb.LogNodeManagerServiceServer
}

// managerServiceImpl implements ManagerService.
// managerServiceImpl is just a rpc level to handle incoming grpc.
// all manager logic should be done in wal.Manager.
type managerServiceImpl struct {
	walManager walmanager.Manager
}

// Assign assigns a wal instance for the channel on this Manager.
// After assign returns, the wal instance is ready to use.
func (ms *managerServiceImpl) Assign(ctx context.Context, req *logpb.LogNodeManagerAssignRequest) (*logpb.LogNodeManagerAssignResponse, error) {
	if err := ms.walManager.Open(ctx, req.Channel); err != nil {
		return nil, err
	}
	return &logpb.LogNodeManagerAssignResponse{}, nil
}

// Remove removes the wal instance for the channel.
// After remove returns, the wal instance is removed and all underlying read write operation should be rejected.
func (ms *managerServiceImpl) Remove(ctx context.Context, req *logpb.LogNodeManagerRemoveRequest) (*logpb.LogNodeManagerRemoveResponse, error) {
	if err := ms.walManager.Remove(ctx, req.ChannelName, req.Term); err != nil {
		return nil, err
	}
	return &logpb.LogNodeManagerRemoveResponse{}, nil
}

// CollectStatus collects the status of all wal instances in these lognode.
func (ms *managerServiceImpl) CollectStatus(ctx context.Context, req *logpb.LogNodeManagerCollectStatusRequest) (*logpb.LogNodeManagerCollectStatusResponse, error) {
	channels, err := ms.walManager.GetAllAvailableChannels()
	if err != nil {
		return nil, err
	}
	return &logpb.LogNodeManagerCollectStatusResponse{
		BalancerAttributes: &logpb.LogNodeBalancerAttributes{},
		Channels:           channels,
	}, nil
}
