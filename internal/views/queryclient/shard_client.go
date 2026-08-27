package queryclient

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type shardViewQueryClient struct {
	maxAttempts         int
	retryInitialBackoff time.Duration
	retryMaxBackoff     time.Duration
	queryPlanClient     QueryPlanClient
	queryServiceClient  ViewQueryServiceClient
	shardResolver       resolver.ShardResolver
	replicaPicker       ReplicaPicker
}

func newShardViewQueryClient(
	maxAttempts int,
	retryInitialBackoff time.Duration,
	retryMaxBackoff time.Duration,
	queryPlanClient QueryPlanClient,
	queryServiceClient ViewQueryServiceClient,
	shardResolver resolver.ShardResolver,
	replicaPicker ReplicaPicker,
) *shardViewQueryClient {
	return &shardViewQueryClient{
		maxAttempts:         maxAttempts,
		retryInitialBackoff: retryInitialBackoff,
		retryMaxBackoff:     retryMaxBackoff,
		queryPlanClient:     queryPlanClient,
		queryServiceClient:  queryServiceClient,
		shardResolver:       shardResolver,
		replicaPicker:       replicaPicker,
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
		validatePlan: validateLegacySearchPlan,
		dispatch: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			nodeRequest, err := legacySearchRequestForNode(request, plan, node)
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
		validatePlan: validateLegacyRetrievePlan,
		dispatch: func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
			nodeRequest, err := legacyRetrieveRequestForNode(request, plan, node)
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
			return collector.Add(shardID, response)
		},
		reset: collector.ResetShard,
	})
}

type shardExecution struct {
	buildPlanRequest func(qviews.ShardID) *viewpb.GetQueryPlanRequest
	validatePlan     func(*viewpb.QueryPlan) error
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
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
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
				if err := c.waitBeforeRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		plan, nodes, err := validateQueryPlan(response, shardID)
		if err != nil {
			return nil, err
		}
		if err := execution.validatePlan(plan); err != nil {
			return nil, err
		}
		if plan.GetSkipExecution() {
			return newShardPlan(shardID, plan, nodes), nil
		}
		if err := fanOut(ctx, nodes, plan, shardID, execution.dispatch); err != nil {
			if isRetryableViewError(err) {
				lastErr = err
				execution.reset(shardID)
				if err := c.waitBeforeRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		return newShardPlan(shardID, plan, nodes), nil
	}
	return nil, merr.WrapErrServiceUnavailableErr(lastErr,
		"query view shard %q remained unavailable after %d attempts", vchannel, c.maxAttempts)
}

func newShardPlan(shardID qviews.ShardID, plan *viewpb.QueryPlan, nodes []qviews.WorkNode) *ShardPlan {
	return &ShardPlan{
		ShardID:       shardID,
		Version:       plan.GetVersion(),
		Mvcc:          plan.GetMvcc(),
		WorkNodes:     nodes,
		SkipExecution: plan.GetSkipExecution(),
	}
}

func (c *shardViewQueryClient) waitBeforeRetry(ctx context.Context, attempt int) error {
	if attempt+1 >= c.maxAttempts {
		return nil
	}
	delay := c.retryInitialBackoff
	for current := 0; current < attempt && delay < c.retryMaxBackoff; current++ {
		if delay > c.retryMaxBackoff/2 {
			delay = c.retryMaxBackoff
			break
		}
		delay *= 2
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
	if plan.GetMvcc() == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("query plan has no MVCC for shard %s", expected)
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
	if len(nodes) == 0 && !plan.GetSkipExecution() {
		return nil, nil, merr.WrapErrServiceInternalMsg(
			"query plan has no work nodes and did not explicitly skip shard %s", expected)
	}
	if len(nodes) > 0 && plan.GetSkipExecution() {
		return nil, nil, merr.WrapErrServiceInternalMsg(
			"query plan both skips execution and contains work nodes for shard %s", expected)
	}
	for _, node := range nodes {
		switch node.NodeType() {
		case qviews.NodeTypeStreamingNode:
			if plan.GetMvcc().GetGrowingTimetick() == 0 {
				return nil, nil, merr.WrapErrServiceInternalMsg(
					"query plan has no growing MVCC for streaming node %s on shard %s", node, expected)
			}
		case qviews.NodeTypeQueryNode:
			if plan.GetMvcc().GetTransformingTimetick() == 0 {
				return nil, nil, merr.WrapErrServiceInternalMsg(
					"query plan has no transforming MVCC for query node %s on shard %s", node, expected)
			}
		default:
			return nil, nil, merr.WrapErrServiceInternalMsg(
				"query plan contains work node %s with unknown type for shard %s", node, expected)
		}
	}
	return plan, nodes, nil
}

func validateLegacySearchPlan(plan *viewpb.QueryPlan) error {
	hasDelta := plan.GetLegacySearchPlan() != nil
	hasDeprecatedRequest := plan.GetLegacySearchRequest() != nil
	if hasDelta == hasDeprecatedRequest {
		return merr.WrapErrServiceInternalMsg(
			"query plan must contain exactly one legacy search plan delta or deprecated request")
	}
	if plan.GetLegacyRetrievePlan() != nil || plan.GetLegacyRetrieveRequest() != nil {
		return merr.WrapErrServiceInternalMsg("legacy search query plan contains retrieve payload")
	}
	return nil
}

func validateLegacyRetrievePlan(plan *viewpb.QueryPlan) error {
	hasDelta := plan.GetLegacyRetrievePlan() != nil
	hasDeprecatedRequest := plan.GetLegacyRetrieveRequest() != nil
	if hasDelta == hasDeprecatedRequest {
		return merr.WrapErrServiceInternalMsg(
			"query plan must contain exactly one legacy retrieve plan delta or deprecated request")
	}
	if plan.GetLegacySearchPlan() != nil || plan.GetLegacySearchRequest() != nil {
		return merr.WrapErrServiceInternalMsg("legacy retrieve query plan contains search payload")
	}
	return nil
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
	errs := make([]error, len(nodes))
	for index, node := range nodes {
		index, node := index, node
		group.Go(func() error {
			err := dispatch(groupCtx, node, plan, shardID)
			errs[index] = err
			return err
		})
	}
	_ = group.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}
	return selectFanOutError(errs)
}

func selectFanOutError(errs []error) error {
	var firstCanceled error
	var firstRetryable error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) {
			if firstCanceled == nil {
				firstCanceled = err
			}
			continue
		}
		if !isRetryableViewError(err) {
			return err
		}
		if firstRetryable == nil {
			firstRetryable = err
		}
	}
	if firstRetryable != nil {
		return firstRetryable
	}
	return firstCanceled
}

func legacySearchRequestForNode(
	original *internalpb.SearchRequest,
	plan *viewpb.QueryPlan,
	node qviews.WorkNode,
) (*internalpb.SearchRequest, error) {
	var cloned *internalpb.SearchRequest
	switch {
	case plan.GetLegacySearchPlan() != nil:
		cloned = proto.Clone(original).(*internalpb.SearchRequest)
		delta := plan.GetLegacySearchPlan()
		if delta.SerializedExprPlan != nil {
			cloned.SerializedExprPlan = append([]byte(nil), delta.GetSerializedExprPlan()...)
		}
		if delta.PlaceholderGroup != nil {
			cloned.PlaceholderGroup = append([]byte(nil), delta.GetPlaceholderGroup()...)
		}
	case plan.GetLegacySearchRequest() != nil:
		cloned = proto.Clone(plan.GetLegacySearchRequest()).(*internalpb.SearchRequest)
	default:
		return nil, merr.WrapErrServiceInternalMsg("query plan is missing legacy search plan")
	}
	cloned.MvccTimestamp = legacyMVCCForNode(plan.GetMvcc(), node)
	return cloned, nil
}

func legacyRetrieveRequestForNode(
	original *internalpb.RetrieveRequest,
	plan *viewpb.QueryPlan,
	node qviews.WorkNode,
) (*internalpb.RetrieveRequest, error) {
	var cloned *internalpb.RetrieveRequest
	switch {
	case plan.GetLegacyRetrievePlan() != nil:
		cloned = proto.Clone(original).(*internalpb.RetrieveRequest)
	case plan.GetLegacyRetrieveRequest() != nil:
		cloned = proto.Clone(plan.GetLegacyRetrieveRequest()).(*internalpb.RetrieveRequest)
	default:
		return nil, merr.WrapErrServiceInternalMsg("query plan is missing legacy retrieve plan")
	}
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

func isRetryableViewError(err error) bool {
	viewErr := viewerror.TryAsViewError(err)
	return viewErr != nil && viewErr.IsRetryable()
}

func normalizeBoundaryError(err error, operation string) error {
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded) || merr.IsMilvusError(err) {
		return err
	}
	if viewErr := viewerror.TryAsViewError(err); viewErr != nil {
		if viewErr.IsRetryable() {
			return merr.WrapErrServiceUnavailableErr(err, "%s", operation)
		}
		return merr.WrapErrServiceInternalErr(err, "%s", operation)
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return merr.WrapErrServiceUnavailableErr(err, "%s", operation)
	default:
		return merr.WrapErrServiceInternalErr(err, "%s", operation)
	}
}
