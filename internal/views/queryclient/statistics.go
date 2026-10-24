package queryclient

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Statistics makes one SN call per attempt, followed only by the selected QNs.
// Counts are current physical statistics, not MVCC/TTL-filtered query results.
func (c *legacyClient) Statistics(ctx context.Context, collectionID int64, req *viewpb.StatisticsRequest) (int64, error) {
	vchannels, err := c.shardResolver.ResolveVChannels(ctx, collectionID)
	if err != nil {
		return 0, err
	}
	counts := make([]int64, len(vchannels))
	group, groupCtx := errgroup.WithContext(ctx)
	for i, channel := range vchannels {
		group.Go(func() error {
			rows, err := c.shardClient.statistics(groupCtx, channel, collectionID, req)
			counts[i] = rows
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return 0, err
	}
	var rows int64
	for _, count := range counts {
		rows += count
	}
	return rows, nil
}

func (s *shardViewQueryClient) statistics(ctx context.Context, channel string, collectionID int64, req *viewpb.StatisticsRequest) (int64, error) {
	var rows atomic.Int64
	_, err := s.executeShard(ctx, channel, &shardExecParams{
		// The legacy statistics guarantee is a freshness lower bound. Do not
		// turn it into a fixed BarrierMVCC or wait for unrelated future writes.
		consistencyLevel: commonpb.ConsistencyLevel_Bounded,
		buildPlanReq: func(shard qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: collectionID, ShardId: shard.IntoProto(),
				Request: &viewpb.GetQueryPlanRequest_StatisticsRequest{StatisticsRequest: req},
			}
		},
		consumePlan: func(plan *viewpb.QueryPlan) error {
			if plan.GrowingRowCount == nil || plan.GetStatisticsRequest() == nil {
				return merr.WrapErrServiceInternalMsg("statistics plan misses growing result or partition scope")
			}
			for _, node := range plan.GetWorkNodes() {
				if node.GetQueryNode() == nil {
					return merr.WrapErrServiceInternalMsg("statistics plan contains a non-QueryNode worker")
				}
			}
			rows.Store(plan.GetGrowingRowCount())
			return nil
		},
		dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shard qviews.ShardID) error {
			result, err := s.queryServiceClient.StatisticsOnView(ctx, node, &viewpb.StatisticsOnViewRequest{
				ShardId: shard.IntoProto(), Version: plan.GetVersion(), Mvcc: plan.GetMvcc(),
				PartitionIds: plan.GetStatisticsRequest().GetPartitionIds(),
			})
			if err != nil {
				return err
			}
			rows.Add(result.GetRowCount())
			return nil
		},
		// executeShard joins all workers before resetting and selecting a new View.
		resetShard: func(qviews.ShardID) { rows.Store(0) },
	})
	if err != nil {
		return 0, err
	}
	return rows.Load(), nil
}
