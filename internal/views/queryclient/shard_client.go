package queryclient

import (
	"context"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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

func (c *shardViewQueryClient) Search(
	ctx context.Context,
	vchannel string,
	request *internalpb.SearchRequest,
	collector searchResultCollector,
) (*ShardPlan, error) {
	return c.execute(ctx, request.GetCollectionID(), vchannel, &shardExecution{
		buildPlanRequest: func(shardID qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: request.GetCollectionID(),
				ShardId:      shardID.IntoProto(),
				Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{
					ConsistencyLevel: primaryPlanConsistencyLevel(request.GetConsistencyLevel()),
				},
				PartitionIds: request.GetPartitionIDs(),
				Request: &viewpb.GetQueryPlanRequest_LegacySearchRequest{
					LegacySearchRequest: request,
				},
			}
		},
		dispatch: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			nodeRequest, err := legacySearchRequestForNode(plan, node)
			if err != nil {
				return err
			}
			response, err := c.queryServiceClient.SearchOnView(ctx, node, &viewpb.SearchOnViewRequest{
				LegacyReq: nodeRequest,
				ShardId:   shardID.IntoProto(),
				Version:   plan.GetVersion(),
				Mvcc:      plan.GetMvcc(),
			})
			if err != nil {
				return err
			}
			return collector.Add(shardID, response)
		},
		reset: collector.ResetShard,
	})
}

func (c *shardViewQueryClient) Query(
	ctx context.Context,
	vchannel string,
	request *internalpb.RetrieveRequest,
	collector queryResultCollector,
) (*ShardPlan, error) {
	return c.execute(ctx, request.GetCollectionID(), vchannel, &shardExecution{
		buildPlanRequest: func(shardID qviews.ShardID) *viewpb.GetQueryPlanRequest {
			return &viewpb.GetQueryPlanRequest{
				CollectionId: request.GetCollectionID(),
				ShardId:      shardID.IntoProto(),
				Mvcc: &viewpb.GetQueryPlanRequest_ConsistencyLevel{
					ConsistencyLevel: primaryPlanConsistencyLevel(request.GetConsistencyLevel()),
				},
				PartitionIds: request.GetPartitionIDs(),
				Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{
					LegacyRetrieveRequest: request,
				},
			}
		},
		dispatch: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			nodeRequest, err := legacyRetrieveRequestForNode(plan, node)
			if err != nil {
				return err
			}
			response, err := c.queryServiceClient.QueryOnView(ctx, node, &viewpb.QueryOnViewRequest{
				LegacyReq: nodeRequest,
				ShardId:   shardID.IntoProto(),
				Version:   plan.GetVersion(),
				Mvcc:      plan.GetMvcc(),
			})
			if err != nil {
				return err
			}
			if isEmptySuccessfulQueryResponse(response) {
				return nil
			}
			return collector.Add(shardID, response)
		},
		reset: collector.ResetShard,
	})
}

type shardExecution struct {
	buildPlanRequest func(qviews.ShardID) *viewpb.GetQueryPlanRequest
	dispatch         func(context.Context, qviews.WorkNode, *viewpb.QueryPlan, qviews.ShardID) error
	reset            func(qviews.ShardID)
}

func (c *shardViewQueryClient) execute(
	ctx context.Context,
	collectionID int64,
	vchannel string,
	execution *shardExecution,
) (*ShardPlan, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		replicas, err := c.shardResolver.ResolveShard(ctx, collectionID, vchannel)
		if err != nil {
			return nil, err
		}
		shardID, err := c.replicaPicker.Pick(ctx, replicas)
		if err != nil {
			return nil, err
		}
		if shardID != replicas.PrimaryShardID {
			return nil, merr.WrapErrServiceInternalMsg(
				"primary-only query client rejected non-primary shard: primary=%s, picked=%s",
				replicas.PrimaryShardID, shardID)
		}
		response, err := c.queryPlanClient.GetQueryPlan(ctx, shardID, execution.buildPlanRequest(shardID))
		if err != nil {
			if isRetryableViewError(err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		plan, nodes, err := validateQueryPlan(response, shardID)
		if err != nil {
			return nil, err
		}
		if err := fanOut(ctx, nodes, plan, shardID, execution.dispatch); err != nil {
			if isRetryableViewError(err) {
				lastErr = err
				execution.reset(shardID)
				continue
			}
			return nil, err
		}
		return &ShardPlan{
			ShardID:   shardID,
			Version:   plan.GetVersion(),
			Mvcc:      plan.GetMvcc(),
			WorkNodes: nodes,
		}, nil
	}
	return nil, merr.WrapErrServiceUnavailableErr(lastErr,
		"query view shard %q remained unavailable after %d attempts", vchannel, c.maxRetries)
}

func validateQueryPlan(response *viewpb.GetQueryPlanResponse, expected qviews.ShardID) (*viewpb.QueryPlan, []qviews.WorkNode, error) {
	if response == nil || response.GetPlan() == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("query plan response is empty for shard %s", expected)
	}
	plan := response.GetPlan()
	if plan.GetShardId() == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("query plan has no shard ID for shard %s", expected)
	}
	actual := qviews.FromProtoShardID(plan.GetShardId())
	if actual != expected {
		return nil, nil, merr.WrapErrServiceInternalMsg(
			"query plan shard mismatch: expected=%s, actual=%s", expected, actual)
	}
	if plan.GetVersion() == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("query plan has no version for shard %s", expected)
	}
	nodes := make([]qviews.WorkNode, 0, len(plan.GetWorkNodes()))
	seen := make(map[qviews.WorkNodeKey]struct{}, len(plan.GetWorkNodes()))
	for _, wireNode := range plan.GetWorkNodes() {
		node, err := workNodeFromProto(wireNode)
		if err != nil {
			return nil, nil, err
		}
		if _, ok := seen[node.Key()]; ok {
			return nil, nil, merr.WrapErrServiceInternalMsg(
				"query plan contains duplicate work node %s for shard %s", node, expected)
		}
		seen[node.Key()] = struct{}{}
		nodes = append(nodes, node)
	}
	return plan, nodes, nil
}

func workNodeFromProto(wireNode *viewpb.QueryPlanWorkNode) (qviews.WorkNode, error) {
	if wireNode == nil {
		return nil, merr.WrapErrServiceInternalMsg("query plan contains nil work node")
	}
	switch node := wireNode.GetNode().(type) {
	case *viewpb.QueryPlanWorkNode_QueryNode:
		if node.QueryNode == nil || node.QueryNode.GetNodeId() == 0 {
			return nil, merr.WrapErrServiceInternalMsg("query plan contains invalid query node")
		}
		return qviews.NewQueryNode(node.QueryNode.GetNodeId()), nil
	case *viewpb.QueryPlanWorkNode_StreamingNode:
		if node.StreamingNode == nil || node.StreamingNode.GetPchannel() == "" {
			return nil, merr.WrapErrServiceInternalMsg("query plan contains invalid streaming node")
		}
		return qviews.StreamingNode{PChannel: node.StreamingNode.GetPchannel()}, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("query plan contains unknown work node")
	}
}

func fanOut(
	ctx context.Context,
	nodes []qviews.WorkNode,
	plan *viewpb.QueryPlan,
	shardID qviews.ShardID,
	dispatch func(context.Context, qviews.WorkNode, *viewpb.QueryPlan, qviews.ShardID) error,
) error {
	group, groupCtx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		node := node
		group.Go(func() error {
			return dispatch(groupCtx, node, plan, shardID)
		})
	}
	return group.Wait()
}

func legacySearchRequestForNode(plan *viewpb.QueryPlan, node qviews.WorkNode) (*internalpb.SearchRequest, error) {
	request := plan.GetLegacySearchRequest()
	if request == nil {
		return nil, merr.WrapErrServiceInternalMsg("query plan is missing legacy search request")
	}
	cloned := proto.Clone(request).(*internalpb.SearchRequest)
	cloned.MvccTimestamp = legacyMVCCForNode(plan.GetMvcc(), node)
	return cloned, nil
}

func legacyRetrieveRequestForNode(plan *viewpb.QueryPlan, node qviews.WorkNode) (*internalpb.RetrieveRequest, error) {
	request := plan.GetLegacyRetrieveRequest()
	if request == nil {
		return nil, merr.WrapErrServiceInternalMsg("query plan is missing legacy retrieve request")
	}
	cloned := proto.Clone(request).(*internalpb.RetrieveRequest)
	cloned.MvccTimestamp = legacyMVCCForNode(plan.GetMvcc(), node)
	return cloned, nil
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

func primaryPlanConsistencyLevel(level commonpb.ConsistencyLevel) commonpb.ConsistencyLevel {
	if level == commonpb.ConsistencyLevel_Session {
		return commonpb.ConsistencyLevel_Strong
	}
	return level
}

func isEmptySuccessfulQueryResponse(response *viewpb.QueryOnViewResponse) bool {
	result := response.GetLegacyResults()
	if result == nil || !merr.Ok(result.GetStatus()) {
		return false
	}
	return result.GetAllRetrieveCount() == 0 &&
		typeutil.GetSizeOfIDs(result.GetIds()) == 0 &&
		len(result.GetFieldsData()) == 0
}

func isRetryableViewError(err error) bool {
	viewErr := viewerror.TryAsViewError(err)
	return viewErr != nil && viewErr.IsRetryable()
}

func normalizeBoundaryError(err error, operation string) error {
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) || merr.IsMilvusError(err) {
		return err
	}
	if viewErr := viewerror.TryAsViewError(err); viewErr != nil {
		return merr.WrapErrServiceUnavailableErr(err, "%s", operation)
	}
	return merr.WrapErrServiceInternalErr(err, "%s", operation)
}
