package queryclient

import (
	"context"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/reducer"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// shardViewQueryClient executes two-phase queries at the shard granularity.
// It owns replica resolution, replica selection, consistency routing,
// Phase 1 (GetQueryPlan), Phase 2 (SearchOnView/QueryOnView) dispatch,
// and shard-level retry.
//
// Both Search and Query share the same executeShard framework, differing only
// in what request goes into the GetQueryPlanRequest and which Phase 2 RPC is called.
type shardViewQueryClient struct {
	maxRetries         int
	queryPlanClient    QueryPlanClient
	queryServiceClient ViewQueryServiceClient
	shardResolver      resolver.ShardResolver
	replicaPicker      ReplicaPicker
}

func newShardViewQueryClient(
	maxRetries int,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver resolver.ShardResolver,
	replicaPicker ReplicaPicker,
) *shardViewQueryClient {
	return &shardViewQueryClient{
		maxRetries:         maxRetries,
		queryPlanClient:    queryPlanClient,
		queryServiceClient: queryServiceClient,
		shardResolver:      shardResolver,
		replicaPicker:      replicaPicker,
	}
}

// ShardSearchRequest contains the parameters for a shard-level search execution.
type ShardSearchRequest struct {
	VChannel string
	Req      *internalpb.SearchRequest
	Reducer  reducer.SearchResultReducer
}

// ShardQueryRequest contains the parameters for a shard-level query (retrieve) execution.
type ShardQueryRequest struct {
	VChannel string
	Req      *internalpb.RetrieveRequest
	Reducer  reducer.RetrieveResultReducer
}

// Search executes Phase 1 + Phase 2 for a single shard's search.
// Replica resolution is handled internally. Results are fed into the provided reducer.
// Returns the ShardPlan for potential requery.
func (s *shardViewQueryClient) Search(ctx context.Context, req *ShardSearchRequest) (*ShardPlan, error) {
	return s.executeShard(ctx, req.Req.CollectionID, req.VChannel, &shardExecParams{
		consistencyLevel: req.Req.ConsistencyLevel,
		buildPlanReq: func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: req.Req.CollectionID,
				ShardId:      targetShardID.IntoProto(),
				PartitionIds: req.Req.PartitionIDs,
				Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{
					LegacySearchRequest: req.Req,
				},
			}
		},
		dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			resp, err := s.queryServiceClient.SearchOnView(ctx, node, &viewpb.SearchOnViewRequest{
				LegacyReq: legacySearchRequestForNode(plan, node),
				ShardId:   shardID.IntoProto(),
				Version:   plan.Version,
				Mvcc:      plan.GetMvcc(),
			})
			if err != nil {
				return err
			}
			return req.Reducer.Add(shardID, resp)
		},
		resetShard: req.Reducer.ResetShard,
	})
}

// Query executes Phase 1 + Phase 2 for a single shard's query (retrieve).
// Replica resolution is handled internally. Results are fed into the provided reducer.
// Returns the ShardPlan for potential requery.
func (s *shardViewQueryClient) Query(ctx context.Context, req *ShardQueryRequest) (*ShardPlan, error) {
	return s.executeShard(ctx, req.Req.CollectionID, req.VChannel, &shardExecParams{
		consistencyLevel: req.Req.ConsistencyLevel,
		buildPlanReq: func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: req.Req.CollectionID,
				ShardId:      targetShardID.IntoProto(),
				PartitionIds: req.Req.PartitionIDs,
				Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{
					LegacyRetrieveRequest: req.Req,
				},
			}
		},
		dispatchNode: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			resp, err := s.queryServiceClient.QueryOnView(ctx, node, &viewpb.QueryOnViewRequest{
				LegacyReq: legacyRetrieveRequestForNode(plan, node),
				ShardId:   shardID.IntoProto(),
				Version:   plan.Version,
				Mvcc:      plan.GetMvcc(),
			})
			if err != nil {
				return err
			}
			return req.Reducer.Add(shardID, resp)
		},
		resetShard: req.Reducer.ResetShard,
	})
}

// ============================================================================
// Shared shard execution framework
// ============================================================================

// shardExecParams parameterizes the per-shard Phase 1 + Phase 2 execution.
// Both Search and Query use the same executeShard loop, differing only in these callbacks.
type shardExecParams struct {
	consistencyLevel commonpb.ConsistencyLevel
	// buildPlanReq creates the GetQueryPlanRequest for a target shard.
	buildPlanReq func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest
	// dispatchNode executes a Phase 2 RPC on a single work node.
	// Called concurrently for each work node in the plan, with per-node retry
	// for non-ViewError transient failures.
	dispatchNode func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error
	// resetShard resets the reducer state for the given shard on retry.
	resetShard func(shardID qviews.ShardID)
}

// executeShard runs Phase 1 + Phase 2 for a single shard with retry.
// Replica resolution is performed at the beginning of each attempt (including the first),
// so stale primary mappings are automatically refreshed on retry.
//
// Shard-level retry handles ViewErrors (view invalidated, not found, etc.).
// Per-node retry within fanOutToWorkNodes handles transient non-view errors.
func (s *shardViewQueryClient) executeShard(
	ctx context.Context,
	collectionID int64,
	vchannel string,
	params *shardExecParams,
) (*ShardPlan, error) {
	var lastErr error

	for attempt := 0; attempt < s.maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Resolve shard replicas (every attempt, including first).
		// ShardResolver uses a local cache, so this is a zero-overhead lookup.
		shardReplicas, err := s.shardResolver.ResolveShard(ctx, collectionID, vchannel)
		if err != nil {
			return nil, err
		}

		// Select target replica via picker.
		pickResult, err := s.replicaPicker.Pick(ctx, ReplicaPickInfo{ShardReplicas: shardReplicas})
		if err != nil {
			return nil, err
		}
		targetShardID := pickResult.ShardID

		// Phase 1: GetQueryPlan with consistency routing.
		planReq := params.buildPlanReq(targetShardID)
		plan, err := s.executeGetQueryPlan(ctx, targetShardID, shardReplicas, planReq, params)
		if err != nil {
			if pickResult.Done != nil {
				pickResult.Done(ReplicaDoneInfo{Err: err})
			}
			if ve := viewerror.AsViewError(err); ve != nil && ve.IsRetryable() {
				lastErr = err
				continue
			}
			return nil, err
		}

		shardID := qviews.FromProtoShardID(plan.ShardId)
		workNodes := workNodesFromPlan(plan)

		// Phase 2: Fan out to all work nodes concurrently.
		err = s.fanOutToWorkNodes(ctx, workNodes, plan, shardID, params.dispatchNode)
		if pickResult.Done != nil {
			pickResult.Done(ReplicaDoneInfo{Err: err})
		}
		if err != nil {
			if ve := viewerror.AsViewError(err); ve != nil && ve.IsRetryable() {
				lastErr = err
				params.resetShard(shardID)
				continue
			}
			return nil, err
		}

		return &ShardPlan{
			ShardID:   shardID,
			Version:   plan.Version,
			Mvcc:      plan.GetMvcc(),
			WorkNodes: workNodes,
		}, nil
	}
	return nil, lastErr
}

// executeGetQueryPlan handles consistency-level routing and dispatches Phase 1.
//
// Routing logic per consistency level:
//   - Strong on primary: GetQueryPlan(consistency_level=Strong)
//   - Strong cross-replica: GetMVCCTimestamp from primary → GetQueryPlan(query_plan_mvcc=mvcc)
//   - Session: same routing as Strong; SN sees consistency_level=Strong for primary planning
//   - Bounded/Eventually: GetQueryPlan(consistency_level=...) — SN generates MVCC from WAL
func (s *shardViewQueryClient) executeGetQueryPlan(
	ctx context.Context,
	targetShardID qviews.ShardID,
	shardReplicas *resolver.ShardReplicas,
	planReq *viewpb.GetQueryPlanRequest,
	params *shardExecParams,
) (*viewpb.QueryPlan, error) {
	switch params.consistencyLevel {
	case commonpb.ConsistencyLevel_Strong, commonpb.ConsistencyLevel_Session:
		if targetShardID != shardReplicas.PrimaryShardID {
			mvccResp, err := s.queryPlanClient.GetMVCCTimestamp(ctx, shardReplicas.PrimaryShardID,
				&viewpb.GetMVCCTimestampRequest{
					Vchannel: targetShardID.VChannel,
				})
			if err != nil {
				return nil, err
			}
			planReq.Mvcc = &viewpb.GetQueryPlanRequest_QueryPlanMvcc{
				QueryPlanMvcc: mvccResp.GetMvcc(),
			}
		} else {
			planReq.Mvcc = &viewpb.GetQueryPlanRequest_ConsistencyLevel{
				ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			}
		}
	default:
		planReq.Mvcc = &viewpb.GetQueryPlanRequest_ConsistencyLevel{
			ConsistencyLevel: params.consistencyLevel,
		}
	}

	resp, err := s.queryPlanClient.GetQueryPlan(ctx, targetShardID, planReq)
	if err != nil {
		return nil, err
	}
	return resp.Plan, nil
}

// fanOutToWorkNodes dispatches Phase 2 to all work nodes concurrently.
// Uses errgroup.WithContext for fast-fail: if any node fails, gCtx is canceled
// and in-flight RPCs on other nodes are aborted. The caller (executeShard) then
// retries the entire shard.
//
// Per-node transient retry (network timeout, etc.) is handled by the
// ViewQueryServiceClient implementation, not here.
func (s *shardViewQueryClient) fanOutToWorkNodes(
	ctx context.Context,
	workNodes []qviews.WorkNode,
	plan *viewpb.QueryPlan,
	shardID qviews.ShardID,
	dispatchNode func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error,
) error {
	g, gCtx := errgroup.WithContext(ctx)
	for _, node := range workNodes {
		node := node
		g.Go(func() error {
			return dispatchNode(gCtx, node, plan, shardID)
		})
	}
	return g.Wait()
}

func legacySearchRequestForNode(plan *viewpb.QueryPlan, node qviews.WorkNode) *internalpb.SearchRequest {
	req := proto.Clone(plan.GetLegacySearchRequest()).(*internalpb.SearchRequest)
	req.MvccTimestamp = legacyMVCCForNode(plan.GetMvcc(), node)
	return req
}

func legacyRetrieveRequestForNode(plan *viewpb.QueryPlan, node qviews.WorkNode) *internalpb.RetrieveRequest {
	req := proto.Clone(plan.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)
	req.MvccTimestamp = legacyMVCCForNode(plan.GetMvcc(), node)
	return req
}

func legacyMVCCForNode(mvcc *viewpb.QueryPlanMVCC, node qviews.WorkNode) uint64 {
	if mvcc == nil {
		return 0
	}
	if node.NodeType() == qviews.NodeTypeStreamingNode {
		return mvcc.GetGrowingTimetick()
	}
	return mvcc.GetTransformingTimetick()
}

// workNodesFromPlan converts proto QueryPlanWorkNode list to domain WorkNode types.
func workNodesFromPlan(plan *viewpb.QueryPlan) []qviews.WorkNode {
	nodes := make([]qviews.WorkNode, 0, len(plan.WorkNodes))
	for _, n := range plan.WorkNodes {
		switch v := n.Node.(type) {
		case *viewpb.QueryPlanWorkNode_QueryNode:
			nodes = append(nodes, qviews.NewQueryNode(v.QueryNode.NodeId))
		case *viewpb.QueryPlanWorkNode_StreamingNode:
			nodes = append(nodes, qviews.StreamingNode{PChannel: v.StreamingNode.Pchannel})
		}
	}
	return nodes
}
