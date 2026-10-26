package adaptor

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (w *walAdaptorImpl) GetLatestQueryPlanMVCC(ctx context.Context, vchannel string) (*viewpb.QueryPlanMVCC, error) {
	return w.getQueryPlanMVCC(ctx, vchannel, true, 0)
}

func (w *walAdaptorImpl) getQueryPlanMVCC(ctx context.Context, vchannel string, strong bool, minimum uint64) (*viewpb.QueryPlanMVCC, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()
	callerCtx := ctx
	ctx, cancel := contextutil.MergeContext(ctx, w.availableCtx)
	defer cancel()
	manager := w.param.MVCCManager.QueryMVCC()
	minimum = max(minimum, manager.Minimum(vchannel, strong))
	currentMVCC, ready := manager.Snapshot(vchannel, minimum)
	if !ready {
		resource.Resource().TimeTickInspector().TriggerSync(w.Channel(), false)
		var err error
		currentMVCC, err = manager.WaitSnapshot(ctx, vchannel, minimum)
		if err != nil {
			if errors.Is(err, context.Canceled) && callerCtx.Err() == nil && w.availableCtx.Err() != nil {
				return nil, viewerror.NewOnShutdownError("query mvcc WAL is unavailable")
			}
			if errors.Is(err, merr.ErrChannelNotFound) {
				return nil, viewerror.NewViewNotFound("query mvcc for vchannel %s is unavailable", vchannel)
			}
			return nil, err
		}
	}
	if currentMVCC.GrowingTimetick == 0 && currentMVCC.TransformingTimetick == 0 {
		return nil, viewerror.NewViewNotFound("query mvcc for vchannel %s is unavailable", vchannel)
	}
	mlog.Debug(ctx, "query view latest mvcc resolved",
		mlog.FieldVChannel(vchannel),
		mlog.Uint64("growingTimeTick", currentMVCC.GrowingTimetick),
		mlog.Uint64("transformingTimeTick", currentMVCC.TransformingTimetick),
		mlog.Uint64("minimumPrefix", minimum),
	)
	return &viewpb.QueryPlanMVCC{
		GrowingTimetick:      currentMVCC.GrowingTimetick,
		TransformingTimetick: currentMVCC.TransformingTimetick,
	}, nil
}

func (w *walAdaptorImpl) GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (response *viewpb.GetQueryPlanResponse, err error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	// A pending barrier must not hold WAL shutdown waiting for progress from
	// the owner that is being fenced or replaced.
	callerCtx := ctx
	ctx, cancel := contextutil.MergeContext(ctx, w.availableCtx)
	defer cancel()
	defer func() {
		// Retiring a WAL cancels its pending waits, not the caller's request.
		// Preserve caller cancellation; otherwise let the client replan on
		// the replacement owner through the existing shutdown retry path.
		if errors.Is(err, context.Canceled) && callerCtx.Err() == nil && w.availableCtx.Err() != nil {
			response = nil
			err = viewerror.NewOnShutdownError("query plan WAL is unavailable")
		}
	}()

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
	var lease *snview.QueryViewLease
	if req.GetStatisticsRequest() != nil {
		lease, err = w.queryViewHandler.AcquireLatestUpView(ctx, shardID)
	} else {
		lease, err = w.queryViewHandler.AcquireUpViewAtSchema(ctx, shardID, schemaVersion)
	}
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if req.GetStatisticsRequest() == nil && schemaVersion != lease.Meta.GetSchemaVersion() {
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
	// Fix one MVCC and wait for the selected runtime's data visibility.
	// Query admission and schema identity come from the acquired View.
	if err := runtime.WaitMVCCVisible(ctx, mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	plan := &viewpb.QueryPlan{
		Version: lease.Version.IntoProto(),
		ShardId: viewShardID.IntoProto(),
		Mvcc:    mvcc,
	}
	response = &viewpb.GetQueryPlanResponse{Plan: plan}
	var partitionFiltered bool
	switch request := req.GetRequest().(type) {
	case *viewpb.GetQueryPlanRequest_StatisticsRequest:
		statistics := proto.Clone(request.StatisticsRequest).(*viewpb.StatisticsRequest)
		statistics.PartitionIds, err = runtime.ResolvePartitions(lease.Meta.GetLoadInfoVersion(), statistics.GetPartitionIds())
		if err != nil {
			return nil, err
		}
		// Count growing data while the chosen View is still leased. The returned
		// routing contains QNs only, so no second SN request is necessary.
		rows, countErr := w.queryViewHandler.Statistics(ctx, &viewpb.StatisticsOnViewRequest{
			ShardId: viewShardID.IntoProto(), Version: lease.Version.IntoProto(),
			Mvcc: mvcc, PartitionIds: statistics.PartitionIds,
		})
		if countErr != nil {
			return nil, countErr
		}
		response.GrowingInlineResponse = &viewpb.GrowingInlineResponse{
			Response: &viewpb.GrowingInlineResponse_StatisticsResponse{
				StatisticsResponse: &viewpb.StatisticsOnViewResponse{RowCount: rows},
			},
		}
		plan.Request = &viewpb.QueryPlan_StatisticsRequest{StatisticsRequest: statistics}
		plan.WorkNodes, _ = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
			ignoreGrowing: true, partitionIDs: statistics.PartitionIds,
		})

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
		optimizer := readRuntime.GlobalOptimizer()
		optimization, err := optimizer.OptimizeSearch(ctx, searchReq)
		if err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: searchReq}
		if !optimization.Skip {
			plan.WorkNodes, partitionFiltered = buildQueryPlanWorkNodes(lease.View, searchQueryPlanWorkNodeOptions(searchReq, runtime, mvcc))
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
		optimizer := readRuntime.GlobalOptimizer()
		if err := optimizer.OptimizeRetrieve(ctx, retrieveReq); err != nil {
			return nil, err
		}
		plan.Request = &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: retrieveReq}
		plan.WorkNodes, partitionFiltered = buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{
			ignoreGrowing: retrieveReq.GetIgnoreGrowing(),
			partitionIDs:  retrieveReq.GetPartitionIDs(),
			runtime:       runtime,
			mvcc:          mvcc,
		})
	default:
		return nil, viewerror.NewUnknownError("query plan request misses legacy request")
	}
	if req.GetStatisticsRequest() == nil {
		summary, err := readRuntime.GlobalOptimizer().OptimizePlan(ctx, plan, lease.View)
		if err != nil {
			return nil, err
		}
		if partitionFiltered {
			if summary == nil {
				summary = &viewpb.GlobalOptimizerExecutionSummary{}
			}
			summary.Flags |= 1 << viewpb.GlobalOptimizerExecutionSummary_PrunedByPartitionFiltering
		}
		if summary != nil {
			response.ExecutionSummary = &viewpb.ExecutionSummary{GlobalOptimizer: summary}
		}
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
	qviews.OmitUnchangedQueryPayloads(plan, req)
	lease.Renew()
	return response, nil
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
	case *viewpb.GetQueryPlanRequest_BarrierMvcc:
		return w.resolveBarrierMVCC(ctx, vchannel, mvcc.BarrierMvcc)
	case *viewpb.GetQueryPlanRequest_QueryPlanMvcc:
		return mvcc.QueryPlanMvcc, nil
	case *viewpb.GetQueryPlanRequest_ConsistencyLevel:
		if w.Channel().AccessMode != types.AccessModeRW {
			return nil, viewerror.NewNotPrimaryError("wal %s is not primary", w.Channel().String())
		}
		strong := mvcc.ConsistencyLevel == commonpb.ConsistencyLevel_Strong || mvcc.ConsistencyLevel == commonpb.ConsistencyLevel_Session
		var minimum uint64
		if mvcc.ConsistencyLevel == commonpb.ConsistencyLevel_Bounded {
			// Proxy supplies the bounded-staleness lower bound in the legacy request.
			minimum = max(req.GetLegacySearchRequest().GetGuaranteeTimestamp(), req.GetLegacyRetrieveRequest().GetGuaranteeTimestamp())
		}
		if mvcc.ConsistencyLevel == commonpb.ConsistencyLevel_Customized {
			// A customized guarantee is a lower bound, not an exact historical
			// snapshot. Preserve newer visible writes when advancing to it.
			current, err := w.getQueryPlanMVCC(ctx, vchannel, false, 0)
			if err != nil {
				return nil, err
			}
			minimum = max(req.GetLegacySearchRequest().GetGuaranteeTimestamp(), req.GetLegacyRetrieveRequest().GetGuaranteeTimestamp())
			if current.GrowingTimetick >= minimum && current.TransformingTimetick >= minimum {
				return current, nil
			}
			return w.resolveBarrierMVCC(ctx, vchannel, max(minimum, current.GrowingTimetick, current.TransformingTimetick))
		}
		return w.getQueryPlanMVCC(ctx, vchannel, strong, minimum)
	default:
		return nil, viewerror.NewUnknownError("query plan request misses mvcc source")
	}
}

func (w *walAdaptorImpl) resolveBarrierMVCC(ctx context.Context, vchannel string, barrier uint64) (*viewpb.QueryPlanMVCC, error) {
	if w.Channel().AccessMode != types.AccessModeRW {
		return nil, viewerror.NewNotPrimaryError("query barrier requires the primary WAL")
	}
	if barrier == 0 {
		return nil, viewerror.NewUnknownError("query barrier timestamp is zero")
	}
	current, ready := w.param.MVCCManager.QueryMVCC().Snapshot(vchannel, 0)
	if !ready || current.GrowingTimetick < barrier || current.TransformingTimetick < barrier {
		// Wait for the allocator before registering shared sync work. An
		// unallocated future timestamp must only hold this request, not the
		// PChannel's ordinary or persisted TimeTick sync. GetQueryPlan merges
		// caller and WAL-owner cancellation into ctx; keep the requested B intact.
		if err := resource.Resource().TSOAllocator().BarrierUntil(ctx, barrier); err != nil {
			return nil, err
		}
		if err := resource.Resource().TimeTickInspector().EnsureBarrier(ctx, w.Channel(), vchannel, barrier); err != nil {
			if se := status.AsStreamingError(err); se.IsOnShutdown() || se.IsFenced() {
				return nil, viewerror.NewOnShutdownError("query barrier WAL is unavailable")
			}
			return nil, err
		}
	}
	return &viewpb.QueryPlanMVCC{GrowingTimetick: barrier, TransformingTimetick: barrier}, nil
}

type queryPlanGrowingRuntime interface {
	MayHaveVisibleGrowingSegments(dataVersion qviews.DataVersion, growingTimetick uint64, transformingTimetick uint64, partitionIDs []int64) bool
}

type queryPlanWorkNodeOptions struct {
	ignoreGrowing bool
	partitionIDs  []int64
	// Hybrid dispatch uses the union above; commonPartitions is the intersection
	// of non-skipped scopes. Nil means every subquery accepts all partitions.
	advanced         bool
	commonPartitions map[int64]struct{}
	runtime          queryPlanGrowingRuntime
	mvcc             *viewpb.QueryPlanMVCC
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
	var commonPartitions map[int64]struct{}
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
		subPartitions := make(map[int64]struct{}, len(subReq.GetPartitionIDs()))
		for _, partitionID := range subReq.GetPartitionIDs() {
			partitionSet[partitionID] = struct{}{}
			subPartitions[partitionID] = struct{}{}
		}
		if commonPartitions == nil {
			commonPartitions = subPartitions
		} else {
			for partitionID := range commonPartitions {
				if _, ok := subPartitions[partitionID]; !ok {
					delete(commonPartitions, partitionID)
				}
			}
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
		ignoreGrowing:    ignoreGrowing,
		partitionIDs:     partitionIDs,
		advanced:         true,
		commonPartitions: commonPartitions,
		runtime:          runtime,
		mvcc:             mvcc,
	}
}

func buildQueryPlanWorkNodes(view *viewpb.QueryViewOfShard, options queryPlanWorkNodeOptions) ([]*viewpb.QueryPlanWorkNode, bool) {
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
	// Build request membership once, shared by all QNs. Collect summary facts in
	// the same partition pass used to select workers, without visiting segment IDs.
	var selectedPartitions map[int64]struct{}
	if len(options.partitionIDs) > 0 {
		selectedPartitions = make(map[int64]struct{}, len(options.partitionIDs))
		for _, id := range options.partitionIDs {
			selectedPartitions[id] = struct{}{}
		}
	}
	commonPartitions := selectedPartitions
	if options.advanced {
		commonPartitions = options.commonPartitions
	}
	filtered := false
	for _, qn := range view.GetQueryNode() {
		selected := false
		for _, partition := range qn.GetPartitions() {
			if len(partition.GetSegmentIds()) == 0 {
				continue
			}
			_, included := selectedPartitions[partition.GetPartitionId()]
			selected = selected || selectedPartitions == nil || included
			if !filtered && commonPartitions != nil {
				_, common := commonPartitions[partition.GetPartitionId()]
				filtered = !common
			}
			if selected && (filtered || commonPartitions == nil) {
				break
			}
		}
		if !selected {
			continue
		}
		nodes = append(nodes, &viewpb.QueryPlanWorkNode{
			Node: &viewpb.QueryPlanWorkNode_QueryNode{
				QueryNode: &viewpb.QueryWorkNode{NodeId: qn.GetNodeId()},
			},
		})
	}
	return nodes, filtered
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
