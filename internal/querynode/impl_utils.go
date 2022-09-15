package querynode

import (
	"context"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func (node *QueryNode) TransferLoad(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	if len(req.GetInfos()) == 0 {
		return &commonpb.Status{}, nil
	}

	shard := req.GetInfos()[0].GetInsertChannel()
	shardCluster, ok := node.ShardClusterService.getShardCluster(shard)
	if !ok {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "shard cluster not found, the leader may have changed",
		}, nil
	}

	req.NeedTransfer = false
	err := shardCluster.loadSegments(ctx, req)
	if err != nil {
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
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "shard cluster not found, the leader may have changed",
		}, nil
	}

	req.NeedTransfer = false
	err := shardCluster.releaseSegments(ctx, req)
	if err != nil {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
