package querynode

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.uber.org/zap"
)

func (node *QueryNode) TransferLoad(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	if len(req.GetInfos()) == 0 {
		return &commonpb.Status{}, nil
	}

	shard := req.GetInfos()[0].GetInsertChannel()
	shardCluster, ok := node.ShardClusterService.getShardCluster(shard)
	if !ok {
		log.Warn("TransferLoad failed to find shard cluster", zap.String("shard", shard))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "shard cluster not found, the leader may have changed",
		}, nil
	}

	req.NeedTransfer = false
	err := shardCluster.LoadSegments(ctx, req)
	if err != nil {
		log.Warn("shard cluster failed to load segments", zap.String("shard", shard), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (node *QueryNode) TransferRelease(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	shardCluster, ok := node.ShardClusterService.getShardCluster(req.GetShard())
	if !ok {
		log.Warn("TransferLoad failed to find shard cluster", zap.String("shard", req.GetShard()))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "shard cluster not found, the leader may have changed",
		}, nil
	}

	req.NeedTransfer = false
	err := shardCluster.ReleaseSegments(ctx, req, false)
	if err != nil {
		log.Warn("shard cluster failed to release segments", zap.String("shard", req.GetShard()), zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
