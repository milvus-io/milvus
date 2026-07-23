package service

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	metrics, err := ms.walManager.Metrics()
	if err != nil {
		return nil, err
	}
	return &streamingpb.StreamingNodeManagerCollectStatusResponse{
		Metrics: types.NewProtoFromStreamingNodeMetrics(*metrics),
	}, nil
}

// ValidateRuntime validates runtime-dependent artifacts on this streaming node.
func (ms *managerServiceImpl) ValidateRuntime(ctx context.Context, req *streamingpb.StreamingNodeManagerValidateRuntimeRequest) (*streamingpb.StreamingNodeManagerValidateRuntimeResponse, error) {
	switch validation := req.GetValidation().(type) {
	case *streamingpb.StreamingNodeManagerValidateRuntimeRequest_Analyzer:
		return validateRuntimeAnalyzer(validation.Analyzer), nil
	default:
		return &streamingpb.StreamingNodeManagerValidateRuntimeResponse{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("unsupported runtime validation")),
		}, nil
	}
}

func validateRuntimeAnalyzer(validation *streamingpb.StreamingNodeRuntimeAnalyzerValidation) *streamingpb.StreamingNodeManagerValidateRuntimeResponse {
	resourceSet := typeutil.NewSet[int64]()
	for _, info := range validation.GetAnalyzerInfos() {
		ids, err := analyzer.ValidateAnalyzer(info.GetParams(), "")
		if err != nil {
			if info.GetName() != "" {
				return &streamingpb.StreamingNodeManagerValidateRuntimeResponse{
					Status: merr.Status(merr.WrapErrParameterInvalidMsg("validate analyzer failed for field: %s, name: %s, error: %v", info.GetField(), info.GetName(), err)),
				}
			}
			return &streamingpb.StreamingNodeManagerValidateRuntimeResponse{
				Status: merr.Status(merr.WrapErrParameterInvalidMsg("validate analyzer failed for field: %s, error: %v", info.GetField(), err)),
			}
		}
		resourceSet.Insert(ids...)
	}
	return &streamingpb.StreamingNodeManagerValidateRuntimeResponse{
		Status:      merr.Success(),
		ResourceIds: resourceSet.Collect(),
	}
}
