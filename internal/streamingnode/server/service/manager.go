package service

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-storage/go/common/log"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/typeconverter"
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
	pchannelInfo, err := typeconverter.NewPChannelInfoFromProto(req.GetPchannel())
	if err != nil {
		return nil, status.NewInvaildArgument("invalid pchannel info %s", err.Error())
	}

	if err := ms.walManager.Open(ctx, pchannelInfo); err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerAssignResponse{}, nil
}

// Remove removes the wal instance for the channel.
// After remove returns, the wal instance is removed and all underlying read write operation should be rejected.
func (ms *managerServiceImpl) Remove(ctx context.Context, req *streamingpb.StreamingNodeManagerRemoveRequest) (*streamingpb.StreamingNodeManagerRemoveResponse, error) {
	if req.GetPchannel() == "" {
		return nil, status.NewInvaildArgument("empty pchannel name")
	}
	if req.GetTerm() <= 0 {
		return nil, status.NewInvaildArgument("invalid term %d", req.GetTerm())
	}

	if err := ms.walManager.Remove(ctx, req.GetPchannel(), req.GetTerm()); err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerRemoveResponse{}, nil
}

// CollectStatus collects the status of all wal instances in these streamingnode.
func (ms *managerServiceImpl) CollectStatus(ctx context.Context, req *streamingpb.StreamingNodeManagerCollectStatusRequest) (*streamingpb.StreamingNodeManagerCollectStatusResponse, error) {
	channels, err := ms.walManager.GetAllAvailableChannels()
	if err != nil {
		return nil, err
	}
	pchannelInfos := make([]*streamingpb.PChannelInfo, 0, len(channels))
	for _, channel := range channels {
		pchannelInfo, err := typeconverter.NewProtoFromPChannelInfo(channel)
		if err != nil {
			log.Warn("invalid pchannel info when collecting status, it's critical issue on streaming node", zap.Any("channel", channel), zap.Error(err))
			return nil, status.NewUnknownError("invalid pchannel info %s when collecting status, it's a critical ", err.Error())
		}
		pchannelInfos = append(pchannelInfos, pchannelInfo)
	}
	return &streamingpb.StreamingNodeManagerCollectStatusResponse{
		Pchannels: pchannelInfos,
	}, nil
}
