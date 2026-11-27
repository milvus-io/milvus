package queryclient

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Statistics makes one SN call per attempt, followed only by the selected QNs.
// Counts are current physical statistics, not MVCC/TTL-filtered query results.
// barrierMVCC reuses Phase 1 barrier coverage; zero uses ordinary Strong sampling.
func (c *legacyClient) Statistics(ctx context.Context, collectionID int64, req *viewpb.StatisticsRequest, barrierMVCC uint64) (count int64, err error) {
	defer func() { err = ToMilvusError(err) }()
	shards, err := resolver.ResolveShards(ctx, c.shardResolver, collectionID)
	if err != nil {
		return 0, err
	}
	counts := make([]int64, len(shards))
	// Zero barrierMVCC samples at Strong, the zero consistency level.
	err = c.shardClient.fanOutShards(ctx, shards, requestReadPoint{barrierMVCC: barrierMVCC}, func(ctx context.Context, i int, read shardReadPoint) error {
		rows, err := c.shardClient.statisticsAt(ctx, collectionID, req, read)
		counts[i] = rows
		return err
	})
	if err != nil {
		return 0, err
	}
	var rows int64
	for _, count := range counts {
		rows += count
	}
	return rows, nil
}

func (s *shardViewQueryClient) statistics(ctx context.Context, channel string, collectionID int64, req *viewpb.StatisticsRequest, barrierMVCC uint64) (int64, error) {
	return s.statisticsAt(ctx, collectionID, req, shardReadPoint{vchannel: channel, barrierMVCC: barrierMVCC})
}

// statisticsAt counts one shard at the read point its serving list assigns it.
func (s *shardViewQueryClient) statisticsAt(ctx context.Context, collectionID int64, req *viewpb.StatisticsRequest, read shardReadPoint) (int64, error) {
	var rows atomic.Int64
	_, err := s.executeShard(ctx, read.vchannel, &shardExecParams{
		collectionID: collectionID,
		mvcc:         read.mvcc,
		barrierMVCC:  read.barrierMVCC,
		observePlan:  read.onPlan,
		awaitPlans:   read.awaitPlans,
		buildPlanReq: func(shard qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: collectionID, ShardId: shard.IntoProto(),
				Request: &viewpb.GetQueryPlanRequest_StatisticsRequest{StatisticsRequest: req},
			}
		},
		consumeResponse: func(response *viewpb.GetQueryPlanResponse) error {
			plan := response.GetPlan()
			statistics := response.GetGrowingInlineResponse().GetStatisticsResponse()
			if statistics == nil || plan.GetStatisticsRequest() == nil {
				return merr.WrapErrServiceInternalMsg("statistics response misses growing result or partition scope")
			}
			for _, node := range plan.GetWorkNodes() {
				if node.GetQueryNode() == nil {
					return merr.WrapErrServiceInternalMsg("statistics plan contains a non-QueryNode worker")
				}
			}
			rows.Store(statistics.GetRowCount())
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
