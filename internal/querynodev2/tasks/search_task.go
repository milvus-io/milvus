package tasks

import "C"

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	_ scheduler.Task      = &SearchTask{}
	_ scheduler.MergeTask = &SearchTask{}
)

type SearchTask struct {
	ctx              context.Context
	collection       *segments.Collection
	segmentManager   *segments.Manager
	req              *querypb.SearchRequest
	result           *internalpb.SearchResults
	merged           bool
	groupSize        int64
	topk             int64
	nq               int64
	placeholderGroup []byte
	originTopks      []int64
	originNqs        []int64
	others           []*SearchTask
	notifier         chan error
	serverID         int64

	tr           *timerecord.TimeRecorder
	scheduleSpan trace.Span
}

func NewSearchTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.SearchRequest,
	serverID int64,
) *SearchTask {
	ctx, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "schedule")
	return &SearchTask{
		ctx:              ctx,
		collection:       collection,
		segmentManager:   manager,
		req:              req,
		merged:           false,
		groupSize:        1,
		topk:             req.GetReq().GetTopk(),
		nq:               req.GetReq().GetNq(),
		placeholderGroup: req.GetReq().GetPlaceholderGroup(),
		originTopks:      []int64{req.GetReq().GetTopk()},
		originNqs:        []int64{req.GetReq().GetNq()},
		notifier:         make(chan error, 1),
		tr:               timerecord.NewTimeRecorderWithTrace(ctx, "searchTask"),
		scheduleSpan:     span,
		serverID:         serverID,
	}
}

// Return the username which task is belong to.
// Return "" if the task do not contain any user info.
func (t *SearchTask) Username() string {
	return t.req.Req.GetUsername()
}

func (t *SearchTask) GetNodeID() int64 {
	return t.serverID
}

// subTaskAt returns the i-th sub-task: the receiver itself for i==0,
// otherwise t.others[i-1]. Sub-tasks index in lock-step with originNqs.
func (t *SearchTask) subTaskAt(i int) *SearchTask {
	if i == 0 {
		return t
	}
	return t.others[i-1]
}

func (t *SearchTask) IsGpuIndex() bool {
	return t.collection.IsGpuIndex()
}

func (t *SearchTask) Context() context.Context {
	return t.ctx
}

func (t *SearchTask) PreExecute() error {
	// Update task wait time metric before execute
	nodeID := strconv.FormatInt(t.GetNodeID(), 10)
	inQueueDuration := t.tr.ElapseSpan()
	inQueueDurationMS := inQueueDuration.Seconds() * 1000

	// Update in queue metric for prometheus.
	metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.SearchLabel,
		t.collection.GetDBName(),
		t.collection.GetResourceGroup(),
		// TODO: resource group and db name may be removed at runtime,
		// should be refactor into metricsutil.observer in the future.
	).Observe(inQueueDurationMS)

	username := t.Username()
	metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.SearchLabel,
		username).
		Observe(inQueueDurationMS)

	// Execute merged task's PreExecute.
	for _, subTask := range t.others {
		err := subTask.PreExecute()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *SearchTask) Execute() error {
	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "SearchTask")

	// A filter-only pass (two-stage search stage 1) needs no branch handling:
	// every branch of a group carries the same predicate, so branch 0's single
	// pass already produces the group's per-segment valid counts. Running it
	// once for the whole group instead of once per sub-request is exactly the
	// saving this feature exists for, applied to stage 1.
	if t.req.GetFilterOnly() {
		return t.executeSingle(tr)
	}
	// Shared-filter hybrid search: sub-requests that agree on their filter
	// predicate arrive as one task and evaluate that filter once per segment.
	if len(t.req.GetExtraFilterSharingReqs()) > 0 {
		return t.executeSharedFilter(tr)
	}
	return t.executeSingle(tr)
}

func (t *SearchTask) executeSingle(tr *timerecord.TimeRecorder) error {
	req := t.req
	err := t.combinePlaceHolderGroups()
	if err != nil {
		return err
	}
	searchReq, err := t.collection.NewSearchRequest(req, t.placeholderGroup)
	if err != nil {
		return err
	}
	defer searchReq.Delete()

	var (
		results          []*segments.SearchResult
		searchedSegments []segments.Segment
	)
	if req.GetScope() == querypb.DataScope_Historical {
		results, searchedSegments, err = segments.SearchHistorical(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			req.GetReq().GetPartitionIDs(),
			req.GetSegmentIDs(),
		)
	} else if req.GetScope() == querypb.DataScope_Streaming {
		results, searchedSegments, err = segments.SearchStreaming(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			req.GetReq().GetPartitionIDs(),
			req.GetSegmentIDs(),
		)
	}
	defer t.segmentManager.Segment.Unpin(searchedSegments)
	defer segments.DeleteSearchResults(results)
	if err != nil {
		return err
	}

	// In filter-only mode, extract filter statistics and return early.
	// This supports two-stage search: stage-1 collects per-segment valid
	// counts so the delegator can optimize search params for stage-2.
	if searchReq.FilterOnly() {
		if len(results) != len(searchedSegments) {
			return merr.WrapErrServiceInternalMsg("filter-only search: result count %d != segment count %d", len(results), len(searchedSegments))
		}
		segmentIDs := make([]int64, 0, len(searchedSegments))
		validCounts := make([]int64, 0, len(searchedSegments))
		for i, result := range results {
			segmentIDs = append(segmentIDs, searchedSegments[i].ID())
			validCounts = append(validCounts, result.ValidCount())
		}
		relatedDataSize := lo.Reduce(searchedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
			return acc + segments.GetSegmentRelatedDataSize(seg)
		}, 0)
		for i := range t.originNqs {
			task := t.subTaskAt(i)
			task.result = &internalpb.SearchResults{
				Status:                   merr.Success(),
				SealedSegmentIDsSearched: segmentIDs,
				FilterValidCounts:        validCounts,
				CostAggregation: &internalpb.CostAggregation{
					ServiceTime:          tr.ElapseSpan().Milliseconds(),
					TotalRelatedDataSize: relatedDataSize,
				},
			}
		}
		mlog.Debug(t.ctx, "filter-only search completed", mlog.Int("segments", len(segmentIDs)))
		return nil
	}

	return t.reduceSegmentResults(searchReq, results, searchedSegments, tr)
}

// reduceSegmentResults turns one branch's per-segment results into this task's
// internalpb.SearchResults. It is the second half of executeSingle, factored
// out so the shared-filter path can run it once per branch unchanged.
func (t *SearchTask) reduceSegmentResults(
	searchReq *segcore.SearchRequest,
	results []*segcore.SearchResult,
	searchedSegments []segments.Segment,
	tr *timerecord.TimeRecorder,
) error {
	req := t.req
	// plan.MetricType is accurate, though req.MetricType may be empty
	metricType := searchReq.Plan().GetMetricType()

	if len(results) == 0 {
		for i := range t.originNqs {
			task := t.subTaskAt(i)

			searchResults, err := segments.EncodeSearchResultData(
				t.ctx,
				emptySearchResultData(t.originNqs[i], t.originTopks[i]),
				t.originNqs[i],
				t.originTopks[i],
				metricType,
			)
			if err != nil {
				return err
			}
			searchResults.Base = &commonpb.MsgBase{
				SourceID: t.GetNodeID(),
			}
			searchResults.SlicedOffset = 1
			searchResults.SlicedNumCount = 1
			searchResults.CostAggregation = &internalpb.CostAggregation{
				ServiceTime: tr.ElapseSpan().Milliseconds(),
			}
			task.result = searchResults
		}
		return nil
	}

	relatedDataSize := lo.Reduce(searchedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
		return acc + segments.GetSegmentRelatedDataSize(seg)
	}, 0)

	tr.RecordSpan() // consume search latency so reduce metric is pure reduce time

	// Use a dedicated TimeRecorder for the reduce metric. The result-building
	// path calls tr.ElapseSpan() for CostAggregation.ServiceTime — that has a
	// side effect of resetting tr.last and would steal part of the span if we
	// measured the reduce metric off tr.
	reduceTR := timerecord.NewTimeRecorder("reduce")

	// Mutates results in place; must run before Arrow export.
	allSearchCount, err := segcore.PrepareSearchResultsForExport(
		t.ctx,
		searchReq.Plan(),
		searchReq.PlaceholderGroup(),
		results,
		t.originNqs,
		t.originTopks,
	)
	if err != nil {
		mlog.Warn(t.ctx, "failed to prepare search results for export", mlog.Err(err))
		return err
	}

	preparedChains, err := prepareQueryNodeFunctionChains(req.GetReq().GetSerializedExprPlan(), t.collection.Schema())
	if err != nil {
		return err
	}

	// Export per-segment results as Arrow DataFrames
	var l0InputFieldIDs []int64
	if preparedChains.l0 != nil {
		l0InputFieldIDs = preparedChains.l0.inputFieldIDs
	}
	segDFs, err := t.exportSearchResultsAsArrow(results, searchReq.Plan(), l0InputFieldIDs)
	if err != nil {
		return err
	}
	defer func() {
		for _, df := range segDFs {
			if df != nil {
				df.Release()
			}
		}
	}()

	if err := t.applyL0Rerank(segDFs, preparedChains.l0, searchedSegments, searchReq); err != nil {
		return err
	}

	groupByOpts := resolveGroupByOptions(segDFs, results)
	layout, err := t.buildReduceLayout(groupByOpts, preparedChains.l1 != nil)
	if err != nil {
		return err
	}
	if layout.PerRequestReduce {
		for i, reduceRange := range layout.Ranges {
			reduced, err := t.executeGoReduce(
				segDFs,
				reduceRange.ReduceTopK,
				groupByOpts,
				reduceRange.NQOffset,
				reduceRange.NQCount,
			)
			if err != nil {
				return err
			}
			if err := t.processReducedSlice(
				i,
				reduced,
				results,
				searchReq.Plan(),
				preparedChains.l1,
				metricType,
				tr,
				relatedDataSize,
				allSearchCount,
			); err != nil {
				return err
			}
		}
	} else {
		reduced, err := t.executeGoReduce(segDFs, t.topk, groupByOpts, 0, layout.NQ)
		if err != nil {
			return err
		}
		defer func() {
			if reduced != nil && reduced.DF != nil {
				reduced.DF.Release()
			}
		}()

		reduced, err = t.applyL1RerankResult(reduced, results, searchReq.Plan(), preparedChains.l1)
		if err != nil {
			return err
		}

		for i, reduceRange := range layout.Ranges {
			rowLimit := reduceRange.OutputTopK * layout.GroupSize
			sliced, err := extractSlice(reduced, reduceRange.NQOffset, reduceRange.NQCount, rowLimit)
			if err != nil {
				return err
			}
			if err := func() error {
				if sliced != reduced && sliced.DF != nil {
					defer sliced.DF.Release()
				}
				return t.materializeAndAssignResult(
					i,
					sliced,
					results,
					searchReq.Plan(),
					metricType,
					tr,
					relatedDataSize,
					allSearchCount,
				)
			}(); err != nil {
				return err
			}
		}
	}
	t.attributeStorageCost(results)

	// Reduce metric covers the full Go-reduce pipeline (Arrow export +
	// heap merge + Late Materialization + proto marshal), aligned with the
	// legacy C++ reduce-and-fill boundary so A/B comparisons are meaningful.
	metrics.QueryNodeReduceLatency.WithLabelValues(
		fmt.Sprint(t.GetNodeID()),
		metrics.SearchLabel,
		metrics.ReduceSegments,
		metrics.BatchReduce).
		Observe(float64(reduceTR.RecordSpan().Microseconds()) / 1000.0)
	return nil
}

// executeSharedFilter runs a group of sub-requests that share one filter
// predicate. The filter is evaluated once per segment and every branch's
// vector search runs against that single bitset.
//
// Branch 0 is `t.req` itself, exactly as an ordinary search would carry it;
// branches 1..N-1 ride in ExtraFilterSharingReqs. ReqIndex on the emitted
// sub-results is the branch's position within the group -- the delegator owns
// the mapping back to the caller's original sub-request order.
func (t *SearchTask) executeSharedFilter(tr *timerecord.TimeRecorder) error {
	branchReqs := buildSharedFilterBranches(t.req)

	searchReqs := make([]*segcore.SearchRequest, 0, len(branchReqs))
	defer func() {
		for _, searchReq := range searchReqs {
			searchReq.Delete()
		}
	}()
	for _, branchReq := range branchReqs {
		searchReq, err := t.collection.NewSearchRequest(branchReq, branchReq.GetReq().GetPlaceholderGroup())
		if err != nil {
			return err
		}
		searchReqs = append(searchReqs, searchReq)
	}

	var (
		grouped          [][]*segments.SearchResult
		searchedSegments []segments.Segment
		err              error
	)
	switch t.req.GetScope() {
	case querypb.DataScope_Historical:
		grouped, searchedSegments, err = segments.SearchHistoricalGrouped(
			t.ctx,
			t.segmentManager,
			searchReqs,
			t.req.GetReq().GetCollectionID(),
			t.req.GetReq().GetPartitionIDs(),
			t.req.GetSegmentIDs(),
		)
	case querypb.DataScope_Streaming:
		grouped, searchedSegments, err = segments.SearchStreamingGrouped(
			t.ctx,
			t.segmentManager,
			searchReqs,
			t.req.GetReq().GetCollectionID(),
			t.req.GetReq().GetPartitionIDs(),
			t.req.GetSegmentIDs(),
		)
	default:
		return merr.WrapErrServiceInternalMsg("unexpected data scope %s for shared-filter search", t.req.GetScope())
	}
	defer t.segmentManager.Segment.Unpin(searchedSegments)
	defer func() {
		for _, perBranch := range grouped {
			segments.DeleteSearchResults(perBranch)
		}
	}()
	if err != nil {
		return err
	}

	subResults := make([]*internalpb.SubSearchResults, len(branchReqs))
	var costAggregation *internalpb.CostAggregation
	channelsMvcc := make(map[string]uint64)
	for i, branchReq := range branchReqs {
		branch := t.branchTask(branchReq)
		if err := branch.reduceSegmentResults(searchReqs[i], grouped[i], searchedSegments, tr); err != nil {
			return err
		}
		branchResult := branch.result
		subResults[i] = &internalpb.SubSearchResults{
			MetricType:     branchResult.GetMetricType(),
			NumQueries:     branchResult.GetNumQueries(),
			TopK:           branchResult.GetTopK(),
			SlicedBlob:     branchResult.GetSlicedBlob(),
			ResultData:     branchResult.GetResultData(),
			SlicedNumCount: branchResult.GetSlicedNumCount(),
			SlicedOffset:   branchResult.GetSlicedOffset(),
			ReqIndex:       int64(i),
		}
		// Costs are per-branch but the response is per-group; the branches
		// searched the same segments, so summing service time and keeping one
		// copy of the data size matches what N separate responses reported.
		if costAggregation == nil {
			costAggregation = branchResult.GetCostAggregation()
		} else if branchCost := branchResult.GetCostAggregation(); branchCost != nil {
			costAggregation.ServiceTime += branchCost.GetServiceTime()
		}
		for ch, ts := range branchResult.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
	}

	if costAggregation == nil {
		costAggregation = &internalpb.CostAggregation{}
	}
	t.result = &internalpb.SearchResults{
		Status:          merr.Success(),
		Base:            &commonpb.MsgBase{SourceID: t.GetNodeID()},
		IsAdvanced:      true,
		SubResults:      subResults,
		ChannelsMvcc:    channelsMvcc,
		CostAggregation: costAggregation,
	}
	return nil
}

// buildSharedFilterBranches expands a grouped request back into one flat
// request per branch. Branch 0 is the request itself; the rest project a
// SubSearchRequest onto the shared envelope.
//
// The field set here must mirror buildSharedFilterSearchRequest in the
// delegator exactly, or branch 0 and its siblings would be built differently
// from the same sub-request. That is why Offset, ConsistencyLevel and
// IsRecallEvaluation are absent: the delegator's flattening does not carry
// them either, so copying them here would make the extras diverge from
// branch 0.
func buildSharedFilterBranches(req *querypb.SearchRequest) []*querypb.SearchRequest {
	extras := req.GetExtraFilterSharingReqs()
	branches := make([]*querypb.SearchRequest, 0, len(extras)+1)
	branches = append(branches, req)

	for _, sub := range extras {
		base := req.GetReq()
		branchReq := &internalpb.SearchRequest{
			Base:                    base.GetBase(),
			ReqID:                   base.GetReqID(),
			DbID:                    base.GetDbID(),
			CollectionID:            base.GetCollectionID(),
			PartitionIDs:            sub.GetPartitionIDs(),
			Dsl:                     sub.GetDsl(),
			PlaceholderGroup:        sub.GetPlaceholderGroup(),
			DslType:                 sub.GetDslType(),
			SerializedExprPlan:      sub.GetSerializedExprPlan(),
			OutputFieldsId:          base.GetOutputFieldsId(),
			MvccTimestamp:           base.GetMvccTimestamp(),
			GuaranteeTimestamp:      base.GetGuaranteeTimestamp(),
			TimeoutTimestamp:        base.GetTimeoutTimestamp(),
			Nq:                      sub.GetNq(),
			Topk:                    sub.GetTopk(),
			MetricType:              sub.GetMetricType(),
			IgnoreGrowing:           sub.GetIgnoreGrowing(),
			Username:                base.GetUsername(),
			IsAdvanced:              false,
			GroupByFieldId:          sub.GetGroupByFieldId(),
			GroupSize:               sub.GetGroupSize(),
			FieldId:                 sub.GetFieldId(),
			GroupByFieldIds:         base.GetGroupByFieldIds(),
			IsTopkReduce:            base.GetIsTopkReduce(),
			IsIterator:              base.GetIsIterator(),
			AnalyzerName:            sub.GetAnalyzerName(),
			CollectionTtlTimestamps: base.GetCollectionTtlTimestamps(),
			EntityTtlPhysicalTime:   base.GetEntityTtlPhysicalTime(),
			PkFilter:                common.PkFilterNoPkFilter,
			SearchType:              sub.GetSearchType(),
		}
		branches = append(branches, &querypb.SearchRequest{
			Req:             branchReq,
			DmlChannels:     req.GetDmlChannels(),
			SegmentIDs:      req.GetSegmentIDs(),
			FromShardLeader: req.GetFromShardLeader(),
			Scope:           req.GetScope(),
			TotalChannelNum: req.GetTotalChannelNum(),
		})
	}
	return branches
}

// branchTask is a view of this task scoped to a single branch: same segments,
// same lifetime, but that branch's plan, nq and topk. It exists so the
// per-branch reduce can run the ordinary single-branch code unchanged.
func (t *SearchTask) branchTask(branchReq *querypb.SearchRequest) *SearchTask {
	return &SearchTask{
		ctx:              t.ctx,
		collection:       t.collection,
		segmentManager:   t.segmentManager,
		req:              branchReq,
		groupSize:        branchReq.GetReq().GetGroupSize(),
		topk:             branchReq.GetReq().GetTopk(),
		nq:               branchReq.GetReq().GetNq(),
		placeholderGroup: branchReq.GetReq().GetPlaceholderGroup(),
		originTopks:      []int64{branchReq.GetReq().GetTopk()},
		originNqs:        []int64{branchReq.GetReq().GetNq()},
		notifier:         make(chan error, 1),
		tr:               t.tr,
		serverID:         t.serverID,
	}
}

func emptySearchResultData(nq, topK int64) *schemapb.SearchResultData {
	return &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topK,
		Ids:        &schemapb.IDs{},
		Scores:     []float32{},
		Topks:      make([]int64, int(nq)),
		FieldsData: []*schemapb.FieldData{},
	}
}

func (t *SearchTask) Merge(other *SearchTask) bool {
	var (
		nq        = t.nq
		topk      = t.topk
		otherNq   = other.nq
		otherTopk = other.topk
	)

	diffTopk := topk != otherTopk
	pre := funcutil.Min(nq*topk, otherNq*otherTopk)
	maxTopk := funcutil.Max(topk, otherTopk)
	after := (nq + otherNq) * maxTopk
	ratio := float64(after) / float64(pre)

	// A shared-filter group is a merge along a different axis (same rows,
	// different vector fields) than this one (same plan, concatenated
	// placeholder groups). The two must not compose.
	if len(t.req.GetExtraFilterSharingReqs()) > 0 || len(other.req.GetExtraFilterSharingReqs()) > 0 {
		return false
	}

	// Check mergeable
	if t.req.GetFilterOnly() != other.req.GetFilterOnly() ||
		t.req.GetEnableExprCache() != other.req.GetEnableExprCache() ||
		t.req.GetReq().GetDbID() != other.req.GetReq().GetDbID() ||
		t.req.GetReq().GetCollectionID() != other.req.GetReq().GetCollectionID() ||
		t.req.GetReq().GetMvccTimestamp() != other.req.GetReq().GetMvccTimestamp() ||
		t.req.GetReq().GetDslType() != other.req.GetReq().GetDslType() ||
		t.req.GetDmlChannels()[0] != other.req.GetDmlChannels()[0] ||
		(diffTopk && ratio > paramtable.Get().QueryNodeCfg.TopKMergeRatio.GetAsFloat()) ||
		!funcutil.SliceSetEqual(t.req.GetReq().GetPartitionIDs(), other.req.GetReq().GetPartitionIDs()) ||
		!funcutil.SliceSetEqual(t.req.GetSegmentIDs(), other.req.GetSegmentIDs()) ||
		!bytes.Equal(t.req.GetReq().GetSerializedExprPlan(), other.req.GetReq().GetSerializedExprPlan()) {
		return false
	}

	// Merge
	t.groupSize += other.groupSize
	t.topk = maxTopk
	t.nq += otherNq
	t.originTopks = append(t.originTopks, other.originTopks...)
	t.originNqs = append(t.originNqs, other.originNqs...)
	t.others = append(t.others, other)
	other.merged = true

	return true
}

func (t *SearchTask) Done(err error) {
	if !t.merged {
		metrics.QueryNodeSearchGroupSize.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.groupSize))
		metrics.QueryNodeSearchGroupNQ.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.nq))
		metrics.QueryNodeSearchGroupTopK.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.topk))
	}
	t.notifier <- err
	for _, other := range t.others {
		other.Done(err)
	}
}

func (t *SearchTask) Wait() error {
	return <-t.notifier
}

func (t *SearchTask) SearchResult() *internalpb.SearchResults {
	if t.result != nil {
		channelsMvcc := make(map[string]uint64)
		for _, ch := range t.req.GetDmlChannels() {
			channelsMvcc[ch] = t.req.GetReq().GetMvccTimestamp()
		}
		t.result.ChannelsMvcc = channelsMvcc
	}
	return t.result
}

func (t *SearchTask) NQ() int64 {
	// A shared-filter group processes every branch's queries in one task; the
	// scheduler counter feeds the proxy's load estimate, so report the sum.
	nq := t.nq
	for _, sub := range t.req.GetExtraFilterSharingReqs() {
		nq += sub.GetNq()
	}
	return nq
}

func (t *SearchTask) MinNQ() int64 {
	minNQ := t.nq
	if len(t.originNqs) > 0 {
		minNQ = t.originNqs[0]
		for _, nq := range t.originNqs[1:] {
			if nq < minNQ {
				minNQ = nq
			}
		}
	}
	for _, sub := range t.req.GetExtraFilterSharingReqs() {
		if sub.GetNq() < minNQ {
			minNQ = sub.GetNq()
		}
	}
	return minNQ
}

func (t *SearchTask) MergeWith(other scheduler.Task) bool {
	switch other := other.(type) {
	case *SearchTask:
		return t.Merge(other)
	}
	return false
}

// combinePlaceHolderGroups combine all the placeholder groups.
func (t *SearchTask) combinePlaceHolderGroups() error {
	if len(t.others) == 0 {
		return nil
	}

	ret := &commonpb.PlaceholderGroup{}
	if err := proto.Unmarshal(t.placeholderGroup, ret); err != nil {
		return merr.WrapErrParameterInvalidMsg("invalid search vector placeholder: %v", err)
	}

	if len(ret.GetPlaceholders()) == 0 {
		return merr.WrapErrParameterInvalidMsg("empty search vector is not allowed")
	}
	for _, t := range t.others {
		x := &commonpb.PlaceholderGroup{}
		if err := proto.Unmarshal(t.placeholderGroup, x); err != nil {
			return merr.WrapErrParameterInvalidMsg("invalid search vector placeholder: %v", err)
		}
		if len(x.GetPlaceholders()) == 0 {
			return merr.WrapErrParameterInvalidMsg("empty search vector is not allowed")
		}
		ret.Placeholders[0].Values = append(ret.Placeholders[0].Values, x.Placeholders[0].Values...)
	}
	t.placeholderGroup, _ = proto.Marshal(ret)
	return nil
}
