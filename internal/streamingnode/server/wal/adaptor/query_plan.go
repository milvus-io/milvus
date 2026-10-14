package adaptor

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAdaptorImpl) GetLatestQueryPlanMVCC(ctx context.Context, vchannel string) (*viewpb.QueryPlanMVCC, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()
	currentMVCC := w.param.MVCCManager.GetQueryMVCCOfVChannel(vchannel)
	if currentMVCC.GrowingTimetick == 0 && currentMVCC.TransformingTimetick == 0 {
		return nil, viewerror.NewViewNotFound("query mvcc for vchannel %s is unavailable", vchannel)
	}
	if !currentMVCC.Confirmed {
		// if the mvcc is not confirmed, trigger a sync operation to make it confirmed as soon as possible.
		resource.Resource().TimeTickInspector().TriggerSync(w.rwWALImpls.Channel(), false)
	}
	mlog.Debug(ctx, "query view latest mvcc resolved",
		mlog.FieldVChannel(vchannel),
		mlog.Uint64("growingTimeTick", currentMVCC.GrowingTimetick),
		mlog.Uint64("transformingTimeTick", currentMVCC.TransformingTimetick),
		mlog.Bool("confirmed", currentMVCC.Confirmed),
	)
	return &viewpb.QueryPlanMVCC{
		GrowingTimetick:      currentMVCC.GrowingTimetick,
		TransformingTimetick: currentMVCC.TransformingTimetick,
	}, nil
}

func (w *walAdaptorImpl) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlan, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	if req == nil || req.GetShardId() == nil {
		return nil, viewerror.NewUnknownError("query plan request misses shard id")
	}
	if w.queryViewHandler == nil {
		return nil, viewerror.NewViewNotFound("query view handler is unavailable")
	}

	shardID := qviews.FromProtoShardID(req.GetShardId())
	var schemaVersion int32
	switch request := req.GetRequest().(type) {
	case *viewpb.GetQueryPlanRequest_LegacySearchRequest:
		schemaVersion = request.LegacySearchRequest.GetSchemaVersion()
	case *viewpb.GetQueryPlanRequest_LegacyRetrieveRequest:
		schemaVersion = request.LegacyRetrieveRequest.GetSchemaVersion()
	}
	lease, err := w.queryViewHandler.AcquireUpViewAtSchema(ctx, shardID, schemaVersion)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if schemaVersion != lease.Meta.GetSchemaVersion() {
		return nil, viewerror.NewViewNotFound("query view schema is not ready")
	}
	if req.GetCollectionId() != 0 && lease.Meta.GetCollectionId() != req.GetCollectionId() {
		return nil, viewerror.NewViewNotFound("query view collection mismatch, expected %d, got %d", req.GetCollectionId(), lease.Meta.GetCollectionId())
	}

	// The request may carry an unknown replica ID (resolved by vchannel only);
	// the plan always echoes the actual view's shard ID so Phase 2 targets the
	// real replica.
	viewShardID := qviews.NewShardIDFromQVMeta(lease.Meta)

	mvcc, err := w.resolveQueryPlanMVCC(ctx, req, shardID.VChannel)
	if err != nil {
		return nil, err
	}

	var readRuntime *queryresource.ReadRuntime
	if w.viewResourceManager != nil {
		readRuntime, _ = w.viewResourceManager.GetQueryReadRuntime(qviews.QueryViewKey{
			ShardID:          viewShardID,
			QueryViewVersion: lease.Version,
		})
	}
	if readRuntime == nil {
		return nil, viewerror.NewViewNotFound("query view read resources are unavailable")
	}
	runtime := readRuntime.QueryRuntime
	// Fix one MVCC and wait for its WAL prefix before validating schema
	// removal boundaries. Returning a retry here would repeatedly sample a
	// newer write frontier and could keep outrunning the recovery observer.
	if err := runtime.WaitMVCCVisible(ctx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	optimizer := readRuntime.GlobalOptimizer()
	plan := &viewpb.QueryPlan{
		Version: lease.Version.IntoProto(),
		ShardId: viewShardID.IntoProto(),
		Mvcc:    mvcc,
	}
	switch request := req.GetRequest().(type) {
	case *viewpb.GetQueryPlanRequest_LegacySearchRequest:
		if request.LegacySearchRequest == nil {
			return nil, viewerror.NewUnknownError("query plan request misses legacy search request")
		}
		searchReq := proto.Clone(request.LegacySearchRequest).(*internalpb.SearchRequest)
		if len(searchReq.GetPartitionIDs()) == 0 && len(req.GetPartitionIds()) > 0 {
			searchReq.PartitionIDs = append([]int64(nil), req.GetPartitionIds()...)
		}
		searchReq.PartitionIDs, err = runtime.ResolvePartitions(lease.Meta.GetLoadInfoVersion(), searchReq.GetPartitionIDs())
		if err != nil {
			return nil, err
		}
		for _, sub := range searchReq.GetSubReqs() {
			if sub == nil {
				continue
			}
			if len(sub.GetPartitionIDs()) == 0 {
				sub.PartitionIDs = append([]int64(nil), searchReq.PartitionIDs...)
			}
			sub.PartitionIDs, err = runtime.ResolvePartitions(lease.Meta.GetLoadInfoVersion(), sub.GetPartitionIDs())
			if err != nil {
				return nil, err
			}
		}
		optimization, err := optimizer.OptimizeSearch(ctx, searchReq)
		if err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: searchReq}
		if !optimization.Skip {
			plan.WorkNodes = buildQueryPlanWorkNodes(lease.View, searchQueryPlanWorkNodeOptions(searchReq, runtime, mvcc))
		}
	case *viewpb.GetQueryPlanRequest_LegacyRetrieveRequest:
		if request.LegacyRetrieveRequest == nil {
			return nil, viewerror.NewUnknownError("query plan request misses legacy retrieve request")
		}
		retrieveReq := proto.Clone(request.LegacyRetrieveRequest).(*internalpb.RetrieveRequest)
		if len(retrieveReq.GetPartitionIDs()) == 0 && len(req.GetPartitionIds()) > 0 {
			retrieveReq.PartitionIDs = append([]int64(nil), req.GetPartitionIds()...)
		}
		retrieveReq.PartitionIDs, err = runtime.ResolvePartitions(lease.Meta.GetLoadInfoVersion(), retrieveReq.GetPartitionIDs())
		if err != nil {
			return nil, err
		}
		if err := optimizer.OptimizeRetrieve(ctx, retrieveReq); err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: retrieveReq}
		plan.WorkNodes = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
			ignoreGrowing: retrieveReq.GetIgnoreGrowing(),
			partitionIDs:  retrieveReq.GetPartitionIDs(),
			runtime:       runtime,
			mvcc:          mvcc,
		})
	default:
		return nil, viewerror.NewUnknownError("query plan request misses legacy request")
	}
	var plannedRequest proto.Message
	switch request := plan.GetRequest().(type) {
	case *viewpb.QueryPlan_LegacySearchRequest:
		plannedRequest = request.LegacySearchRequest
	case *viewpb.QueryPlan_LegacyRetrieveRequest:
		plannedRequest = request.LegacyRetrieveRequest
	}
	fields, err := viewquery.ReferencedSchemaFields(plannedRequest, readRuntime.Schema())
	if err != nil {
		return nil, err
	}
	if err := w.validateQuerySchemaFields(lease.Meta, mvcc, fields); err != nil {
		return nil, err
	}
	// Never mutate a caller-supplied MVCC when attaching a new plan's proof.
	plan.Mvcc = proto.Clone(mvcc).(*viewpb.QueryPlanMVCC)
	plan.Mvcc.SchemaBinding = &viewpb.QuerySchemaBinding{
		SchemaVersion: schemaVersion, GrowingTimetick: mvcc.GetGrowingTimetick(),
		TransformingTimetick: mvcc.GetTransformingTimetick(), FieldIds: fields,
	}
	mlog.Debug(ctx, "query view plan created",
		mlog.FieldCollectionID(lease.Meta.GetCollectionId()),
		mlog.FieldVChannel(shardID.VChannel),
		mlog.Int64("replicaID", shardID.ReplicaID),
		mlog.Uint64("growingTimeTick", mvcc.GetGrowingTimetick()),
		mlog.Uint64("transformingTimeTick", mvcc.GetTransformingTimetick()),
		mlog.Int("workNodeCount", len(plan.WorkNodes)),
	)
	// Preserve a full inter-phase serving window after planning completes.
	lease.Renew()
	return plan, nil
}

func (w *walAdaptorImpl) GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error) {
	if req == nil || req.GetVchannel() == "" {
		return nil, viewerror.NewUnknownError("mvcc request misses vchannel")
	}
	if w.Channel().AccessMode != types.AccessModeRW {
		return nil, viewerror.NewNotPrimaryError("wal %s is not primary", w.Channel().String())
	}
	mvcc, err := w.GetLatestQueryPlanMVCC(ctx, req.GetVchannel())
	if err != nil {
		return nil, err
	}
	return &viewpb.GetMVCCTimestampResponse{Mvcc: mvcc}, nil
}

func (w *walAdaptorImpl) resolveQueryPlanMVCC(ctx context.Context, req *viewpb.GetQueryPlanRequest, vchannel string) (*viewpb.QueryPlanMVCC, error) {
	switch mvcc := req.GetMvcc().(type) {
	case *viewpb.GetQueryPlanRequest_QueryPlanMvcc:
		return mvcc.QueryPlanMvcc, nil
	case *viewpb.GetQueryPlanRequest_ConsistencyLevel:
		if w.Channel().AccessMode != types.AccessModeRW {
			return nil, viewerror.NewNotPrimaryError("wal %s is not primary", w.Channel().String())
		}
		return w.GetLatestQueryPlanMVCC(ctx, vchannel)
	default:
		return nil, viewerror.NewUnknownError("query plan request misses mvcc source")
	}
}

type queryPlanGrowingRuntime interface {
	MayHaveVisibleGrowingSegments(dataVersion qviews.DataVersion, growingTimetick uint64, transformingTimetick uint64, partitionIDs []int64) bool
}

type queryPlanWorkNodeOptions struct {
	ignoreGrowing bool
	partitionIDs  []int64
	runtime       queryPlanGrowingRuntime
	mvcc          *viewpb.QueryPlanMVCC
}

func searchQueryPlanWorkNodeOptions(req *internalpb.SearchRequest, runtime queryPlanGrowingRuntime, mvcc *viewpb.QueryPlanMVCC) queryPlanWorkNodeOptions {
	if !req.GetIsAdvanced() {
		return queryPlanWorkNodeOptions{
			ignoreGrowing: req.GetIgnoreGrowing(),
			partitionIDs:  req.GetPartitionIDs(),
			runtime:       runtime,
			mvcc:          mvcc,
		}
	}

	ignoreGrowing := true
	allPartitions := false
	partitionSet := make(map[int64]struct{})
	for _, subReq := range req.GetSubReqs() {
		if subReq.GetSkip() {
			continue
		}
		if !subReq.GetIgnoreGrowing() {
			ignoreGrowing = false
		}
		if len(subReq.GetPartitionIDs()) == 0 {
			allPartitions = true
			continue
		}
		for _, partitionID := range subReq.GetPartitionIDs() {
			partitionSet[partitionID] = struct{}{}
		}
	}

	var partitionIDs []int64
	if !allPartitions {
		partitionIDs = make([]int64, 0, len(partitionSet))
		for partitionID := range partitionSet {
			partitionIDs = append(partitionIDs, partitionID)
		}
	}
	return queryPlanWorkNodeOptions{
		ignoreGrowing: ignoreGrowing,
		partitionIDs:  partitionIDs,
		runtime:       runtime,
		mvcc:          mvcc,
	}
}

func buildQueryPlanWorkNodes(view *viewpb.QueryViewOfShard, options queryPlanWorkNodeOptions) []*viewpb.QueryPlanWorkNode {
	nodes := make([]*viewpb.QueryPlanWorkNode, 0, 1+len(view.GetQueryNode()))
	if queryPlanIncludesStreamingNode(view, options) {
		nodes = append(nodes, &viewpb.QueryPlanWorkNode{
			Node: &viewpb.QueryPlanWorkNode_StreamingNode{
				StreamingNode: &viewpb.StreamingWorkNode{
					Pchannel: qviews.NewStreamingNodeFromVChannel(view.GetMeta().GetVchannel()).PChannel,
				},
			},
		})
	}
	for _, qn := range view.GetQueryNode() {
		if !queryNodeHasSelectedSegments(qn, options.partitionIDs) {
			continue
		}
		nodes = append(nodes, &viewpb.QueryPlanWorkNode{
			Node: &viewpb.QueryPlanWorkNode_QueryNode{
				QueryNode: &viewpb.QueryWorkNode{NodeId: qn.GetNodeId()},
			},
		})
	}
	return nodes
}

func queryPlanIncludesStreamingNode(view *viewpb.QueryViewOfShard, options queryPlanWorkNodeOptions) bool {
	if view.GetStreamingNode() == nil || options.ignoreGrowing {
		return false
	}
	if options.runtime == nil || options.mvcc == nil {
		return true
	}
	return options.runtime.MayHaveVisibleGrowingSegments(
		qviews.FromProtoDataVersion(view.GetMeta().GetVersion().GetDataVersion()),
		options.mvcc.GetGrowingTimetick(),
		options.mvcc.GetTransformingTimetick(),
		options.partitionIDs,
	)
}

func queryNodeHasSelectedSegments(qn *viewpb.QueryViewOfQueryNode, partitionIDs []int64) bool {
	if len(partitionIDs) == 0 {
		for _, partition := range qn.GetPartitions() {
			if len(partition.GetSegmentIds()) > 0 {
				return true
			}
		}
		return false
	}
	selectedPartitions := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		selectedPartitions[partitionID] = struct{}{}
	}
	for _, partition := range qn.GetPartitions() {
		if _, ok := selectedPartitions[partition.GetPartitionId()]; ok && len(partition.GetSegmentIds()) > 0 {
			return true
		}
	}
	return false
}

func (w *walAdaptorImpl) validateQuerySchemaFields(meta *viewpb.QueryViewMeta, mvcc *viewpb.QueryPlanMVCC, fields []int64) error {
	module := w.viewResourceManager.Module(meta.GetVchannel())
	if module == nil {
		return viewerror.NewViewNotFound("query schema history is unavailable")
	}
	return module.ValidateSchemaFields(meta.GetCollectionId(), meta.GetSchemaVersion(), mvcc.GetTransformingTimetick(), fields)
}
