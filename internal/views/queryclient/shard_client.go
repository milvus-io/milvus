package queryclient

import (
	"context"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/queryclient/reducer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// shardViewQueryClient executes two-phase queries at the shard granularity.
// It owns replica resolution, consistency routing, Phase 1 (GetQueryPlan),
// Phase 2 (SearchOnView/QueryOnView) dispatch, and shard-level retry.
//
// Both Search and Query share the same executeShard framework, differing only
// in what request goes into the GetQueryPlanRequest and which Phase 2 RPC is called.
type shardViewQueryClient struct {
	maxRetries         int
	replicas           *replicaRouter
	queryPlanClient    QueryPlanClient
	queryServiceClient ViewQueryServiceClient
}

func newShardViewQueryClient(
	maxRetries int,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
) *shardViewQueryClient {
	return &shardViewQueryClient{
		maxRetries:         maxRetries,
		replicas:           newReplicaRouter(queryPlanClient),
		queryPlanClient:    queryPlanClient,
		queryServiceClient: queryServiceClient,
	}
}

// ShardSearchRequest contains the parameters for a shard-level search execution.
type ShardSearchRequest struct {
	VChannel    string
	Req         *internalpb.SearchRequest
	MVCC        *viewpb.QueryPlanMVCC
	BarrierMVCC uint64
	Reducer     reducer.SearchResultReducer
}

// ShardQueryRequest contains the parameters for a shard-level query (retrieve) execution.
type ShardQueryRequest struct {
	VChannel    string
	Req         *internalpb.RetrieveRequest
	MVCC        *viewpb.QueryPlanMVCC
	BarrierMVCC uint64
	Reducer     reducer.RetrieveResultReducer
}

// Search executes Phase 1 + Phase 2 for a single shard's search.
// Replica resolution is handled internally. Results are fed into the provided reducer.
// Returns the ShardPlan for potential requery.
func (s *shardViewQueryClient) Search(ctx context.Context, req *ShardSearchRequest) (*ShardPlan, error) {
	return s.executeShard(ctx, req.VChannel, &shardExecParams{
		consistencyLevel: req.Req.ConsistencyLevel,
		mvcc:             req.MVCC,
		barrierMVCC:      req.BarrierMVCC,
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
				LegacyReq:        legacySearchRequestForNode(plan, node),
				SegmentSelection: qviews.WorkNodeSelection(plan, node).GetSegmentSelection(),
				SubSelections:    qviews.WorkNodeSelection(plan, node).GetSubSelections(),
				ShardId:          shardID.IntoProto(),
				Version:          plan.Version,
				Mvcc:             plan.GetMvcc(),
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
	return s.executeShard(ctx, req.VChannel, &shardExecParams{
		consistencyLevel: req.Req.ConsistencyLevel,
		mvcc:             req.MVCC,
		barrierMVCC:      req.BarrierMVCC,
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
				LegacyReq:        legacyRetrieveRequestForNode(plan, node),
				SegmentSelection: qviews.WorkNodeSelection(plan, node).GetSegmentSelection(),
				ShardId:          shardID.IntoProto(),
				Version:          plan.Version,
				Mvcc:             plan.GetMvcc(),
			})
			if err != nil {
				return err
			}
			if isEmptySuccessfulQueryResponse(resp) {
				return nil
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
	mvcc             *viewpb.QueryPlanMVCC
	barrierMVCC      uint64
	// buildPlanReq creates the GetQueryPlanRequest for a target shard.
	buildPlanReq func(targetShardID qviews.ShardID) *viewpb.GetQueryPlanRequest
	// consumeResponse accepts inline SN results before worker dispatch.
	consumeResponse func(*viewpb.GetQueryPlanResponse) error
	// dispatchNode executes a Phase 2 RPC on a single work node.
	// Called concurrently for each work node in the plan.
	dispatchNode func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error
	// resetShard resets the reducer state for the given shard on retry.
	resetShard func(shardID qviews.ShardID)
}

// executeShard runs Phase 1 + Phase 2 for a single shard with retry.
// Candidates come from the WAL owner directory. GetQueryPlan selects the
// exact-schema View within the explicitly selected replica.
//
// Shard-level retry handles ViewErrors (view invalidated, not found, etc.).
// Transport unavailability also restarts the shard on another candidate.
func (s *shardViewQueryClient) executeShard(
	ctx context.Context,
	vchannel string,
	params *shardExecParams,
) (*ShardPlan, error) {
	var lastErr error
	route, err := s.replicas.route(vchannel)
	if err != nil {
		return nil, err
	}
	tried := make(map[qviews.Replica]bool)
	deadNodes := make(map[int64]struct{})
	var sessionChanged <-chan struct{}
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if s.maxRetries > 0 && attempt >= s.maxRetries && !route.hasUntried(tried) {
			return nil, lastErr
		}

		candidate, sessionsUpdated, err := route.nextReplica(ctx, tried, sessionChanged)
		if sessionsUpdated {
			clear(deadNodes)
			clear(tried)
			sessionChanged = nil
			attempt--
			continue
		}
		if err != nil {
			return nil, err
		}
		tried[candidate] = false
		targetShardID := qviews.ShardID{ReplicaID: candidate.ReplicaID, VChannel: vchannel}

		// Phase 1: GetQueryPlan with consistency routing.
		planReq := params.buildPlanReq(targetShardID)
		response, err := s.executeGetQueryPlan(ctx, targetShardID, planReq, params)
		if err != nil {
			if retryableQueryError(err) {
				lastErr = err
				continue
			}
			return nil, err
		}

		plan := response.GetPlan()
		if err := qviews.RestoreQueryPlanPayloads(plan, planReq); err != nil {
			return nil, err
		}
		// A newer advertised version does not certify removal of a dead QN.
		// Recheck the actual exact-schema plan before accepting inline results.
		blocked := false
		for _, worker := range plan.GetWorkNodes() {
			if qn := worker.GetQueryNode(); qn != nil {
				if _, dead := deadNodes[qn.GetNodeId()]; dead {
					blocked = true
					break
				}
			}
		}
		if blocked {
			tried[candidate] = true
			continue
		}
		if params.consumeResponse != nil {
			if err := params.consumeResponse(response); err != nil {
				return nil, err
			}
		}
		shardID := qviews.FromProtoShardID(plan.ShardId)
		workNodes := workNodesFromPlan(plan)

		// Phase 2: Fan out to all work nodes concurrently.
		err = s.fanOutToWorkNodes(ctx, workNodes, plan, shardID, params.dispatchNode)
		if err != nil {
			if retryableQueryError(err) {
				lastErr = err
				var failed *workNodeFailure
				var sessionError *viewerror.NodeSessionUnavailable
				missingSession := errors.As(err, &sessionError)
				if errors.As(err, &failed) && (missingSession || picker.IsErrSubConnNoExist(failed.err)) {
					if node, ok := failed.node.(qviews.QueryNode); ok {
						deadNodes[node.ID] = struct{}{}
						if missingSession && sessionChanged == nil {
							sessionChanged = sessionError.Changed
						}
						tried[candidate] = true
					}
				}
				params.resetShard(shardID)
				continue
			}
			return nil, err
		}

		return &ShardPlan{
			ShardID:          shardID,
			Version:          plan.Version,
			Mvcc:             plan.GetMvcc(),
			WorkNodes:        workNodes,
			Selections:       plan.GetWorkNodes(),
			ExecutionSummary: response.GetExecutionSummary(),
		}, nil
	}
}

// executeGetQueryPlan handles consistency-level routing and dispatches Phase 1.
//
// All replicas are planned on the primary WAL owner, so strong consistency
// is satisfied directly by that WAL:
//   - Strong/Session: GetQueryPlan(consistency_level=Strong)
//   - Bounded/Eventually: GetQueryPlan(consistency_level=...) — SN generates MVCC from WAL
func (s *shardViewQueryClient) executeGetQueryPlan(
	ctx context.Context,
	targetShardID qviews.ShardID,
	planReq *viewpb.GetQueryPlanRequest,
	params *shardExecParams,
) (*viewpb.GetQueryPlanResponse, error) {
	if params.mvcc != nil && params.barrierMVCC != 0 {
		return nil, merr.WrapErrServiceInternalMsg("query specifies both executor MVCC and BarrierMVCC")
	}
	if params.barrierMVCC != 0 {
		planReq.Mvcc = &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: params.barrierMVCC}
	} else if params.mvcc != nil {
		planReq.Mvcc = &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: proto.Clone(params.mvcc).(*viewpb.QueryPlanMVCC)}
	} else {
		switch params.consistencyLevel {
		case commonpb.ConsistencyLevel_Strong, commonpb.ConsistencyLevel_Session:
			planReq.Mvcc = &viewpb.GetQueryPlanRequest_ConsistencyLevel{
				ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			}
		default:
			planReq.Mvcc = &viewpb.GetQueryPlanRequest_ConsistencyLevel{
				ConsistencyLevel: params.consistencyLevel,
			}
		}
	}
	return s.queryPlanClient.GetQueryPlan(ctx, targetShardID, planReq)
}

// fanOutToWorkNodes dispatches Phase 2 to all work nodes concurrently.
// Uses errgroup.WithContext for fast-fail: if any node fails, gCtx is canceled
// and in-flight RPCs on other nodes are aborted. The caller (executeShard) then
// retries the entire shard.
//
// QN connection failures return promptly for replica reselection. SN assignment
// readiness remains owned by its WAL handler.
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
			if err := dispatchNode(gCtx, node, plan, shardID); err != nil {
				return &workNodeFailure{node: node, err: err}
			}
			return nil
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

func isEmptySuccessfulQueryResponse(resp *viewpb.QueryOnViewResponse) bool {
	result := resp.GetLegacyResults()
	if result == nil || !merr.Ok(result.GetStatus()) {
		return false
	}
	return result.GetAllRetrieveCount() == 0 &&
		typeutil.GetSizeOfIDs(result.GetIds()) == 0 &&
		len(result.GetFieldsData()) == 0
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

// workNodeFailure retains the worker identity without masking its error code.
type workNodeFailure struct {
	node qviews.WorkNode
	err  error
}

func (e *workNodeFailure) Error() string { return e.err.Error() }
func (e *workNodeFailure) Unwrap() error { return e.err }
func retryableQueryError(err error) bool {
	if ve := viewerror.AsViewError(err); ve != nil && ve.IsRetryable() {
		return true
	}
	return status.Code(err) == codes.Unavailable
}
