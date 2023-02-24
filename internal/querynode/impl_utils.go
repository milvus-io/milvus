package querynode

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// TransferLoad transfers load segments with shard cluster.
func (node *QueryNode) TransferLoad(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error) {
	if len(req.GetInfos()) == 0 {
		return &commonpb.Status{}, nil
	}

	shard := req.GetInfos()[0].GetInsertChannel()
	segmentIDs := lo.Map(req.GetInfos(), func(info *querypb.SegmentLoadInfo, _ int) int64 {
		return info.GetSegmentID()
	})
	log := log.Ctx(ctx).With(
		zap.String("shard", shard),
		zap.Int64s("segmentIDs", segmentIDs),
	)

	log.Info("LoadSegment start to transfer load with shard cluster")
	shardCluster, ok := node.ShardClusterService.getShardCluster(shard)
	if !ok {
		log.Warn("TransferLoad failed to find shard cluster")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "shard cluster not found, the leader may have changed",
		}, nil
	}

	req.NeedTransfer = false
	err := shardCluster.LoadSegments(ctx, req)
	if err != nil {
		if errors.Is(err, ErrInsufficientMemory) {
			log.Warn("insufficient memory when shard cluster load segments", zap.Error(err))
			return &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_InsufficientMemoryToLoad,
				Reason:    fmt.Sprintf("insufficient memory when shard cluster load segments, err:%s", err.Error()),
			}, nil
		}
		log.Warn("shard cluster failed to load segments", zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Info("LoadSegment transfer load done")

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

// TransferRelease transfers release segments with shard cluster.
func (node *QueryNode) TransferRelease(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	log := log.Ctx(ctx).With(
		zap.String("shard", req.GetShard()),
		zap.Int64s("segmentIDs", req.GetSegmentIDs()),
		zap.String("scope", req.GetScope().String()),
	)

	log.Info("ReleaseSegments start to transfer release with shard cluster")

	shardCluster, ok := node.ShardClusterService.getShardCluster(req.GetShard())
	if !ok {
		log.Warn("TransferLoad failed to find shard cluster")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_NotShardLeader,
			Reason:    "shard cluster not found, the leader may have changed",
		}, nil
	}

	req.NeedTransfer = false
	err := shardCluster.ReleaseSegments(ctx, req, false)
	if err != nil {
		log.Warn("shard cluster failed to release segments", zap.Error(err))
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    err.Error(),
		}, nil
	}

	log.Info("ReleaseSegments transfer release done")
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}
