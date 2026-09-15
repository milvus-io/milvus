package tasks

// TODO: rename this file into search_task.go

import "C"

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	_ scheduler.Task      = &SearchTask{}
	_ scheduler.MergeTask = &SearchTask{}
)

type SearchTask struct {
	ctx            context.Context
	collection     *segments.Collection
	segmentManager *segments.Manager
	req            *querypb.SearchRequest
	result         *internalpb.SearchResults
	// resultBlobPinned reports that result owns C-backed SlicedBlob memory
	// registered in MsgPins. Grouped execution transfers that ownership from
	// its temporary branch result to the returned envelope.
	resultBlobPinned bool
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
	searchReq, err := segcore.NewSearchRequest(t.collection.GetCCollection(), req, t.placeholderGroup)
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
	if err != nil {
		return err
	}
	defer segments.DeleteSearchResults(results)

	return t.reduceSegmentResults(searchReq, results, relatedDataSizeOf(searchedSegments), tr)
}

// relatedDataSizeOf is the size of the data the searched segments hold. A
// sealed segment walks every binlog, statslog and deltalog entry for it, so it
// is computed once per request and handed to the reduce rather than recomputed
// inside it -- a shared-filter group reduces once per branch over the same
// segments, and the group reports the figure once anyway.
func relatedDataSizeOf(searchedSegments []segments.Segment) int64 {
	return lo.Reduce(searchedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
		return acc + segments.GetSegmentRelatedDataSize(seg)
	}, 0)
}

// reduceSegmentResults turns one branch's per-segment results into this task's
// internalpb.SearchResults. It is the second half of executeSingle, factored
// out so the shared-filter path can run it once per branch unchanged.
func (t *SearchTask) reduceSegmentResults(
	searchReq *segcore.SearchRequest,
	results []*segcore.SearchResult,
	relatedDataSize int64,
	tr *timerecord.TimeRecorder,
) error {
	log := log.Ctx(t.ctx).With(
		zap.Int64("collectionID", t.collection.ID()),
		zap.String("shard", t.req.GetDmlChannels()[0]),
	)
	// plan.MetricType is accurate, though req.MetricType may be empty
	metricType := searchReq.Plan().GetMetricType()

	if len(results) == 0 {
		for i := range t.originNqs {
			var task *SearchTask
			if i == 0 {
				task = t
			} else {
				task = t.others[i-1]
			}

			task.result = &internalpb.SearchResults{
				Base: &commonpb.MsgBase{
					SourceID: t.GetNodeID(),
				},
				Status:         merr.Success(),
				MetricType:     metricType,
				NumQueries:     t.originNqs[i],
				TopK:           t.originTopks[i],
				SlicedOffset:   1,
				SlicedNumCount: 1,
				CostAggregation: &internalpb.CostAggregation{
					ServiceTime: tr.ElapseSpan().Milliseconds(),
				},
			}
			task.resultBlobPinned = false
		}
		return nil
	}

	tr.RecordSpan()
	blobs, err := segcore.ReduceSearchResultsAndFillData(
		t.ctx,
		searchReq.Plan(),
		results,
		int64(len(results)),
		t.originNqs,
		t.originTopks,
	)
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return err
	}
	allTasks := append([]*SearchTask{t}, t.others...)
	refs, err := resource.NewSharedPinnedRefs(
		blobs,
		len(allTasks),
		segcore.DeleteSearchResultDataBlobs,
		"SearchResultDataBlobs",
	)
	if err != nil {
		segcore.DeleteSearchResultDataBlobs(blobs)
		return err
	}

	metrics.QueryNodeReduceLatency.WithLabelValues(
		fmt.Sprint(t.GetNodeID()),
		metrics.SearchLabel,
		metrics.ReduceSegments,
		metrics.BatchReduce).
		Observe(float64(tr.RecordSpan().Microseconds()) / 1000.0)

	// Zero-copy hands the response a slice that points straight into the C
	// result blobs and pins their release to that response object.
	zeroCopy := paramtable.Get().QueryNodeCfg.EnableResultZeroCopy.GetAsBool()

	// Phase 1: build all results.
	var phaseErr error
	for i := range t.originNqs {
		blob, cost, err := segcore.GetSearchResultDataBlob(t.ctx, blobs, i)
		if err != nil {
			phaseErr = err
			break
		}
		// When zero-copy is enabled, blob references C memory directly and is
		// freed after gRPC marshal via MsgPins. Otherwise copy to Go heap so C
		// memory can be released immediately in Phase 2.
		slicedBlob := blob
		if !zeroCopy && len(blob) > 0 {
			slicedBlob = make([]byte, len(blob))
			copy(slicedBlob, blob)
		}
		allTasks[i].result = &internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				SourceID: t.GetNodeID(),
			},
			Status:         merr.Success(),
			MetricType:     metricType,
			NumQueries:     t.originNqs[i],
			TopK:           t.originTopks[i],
			SlicedBlob:     slicedBlob,
			SlicedOffset:   1,
			SlicedNumCount: 1,
			CostAggregation: &internalpb.CostAggregation{
				ServiceTime:          tr.ElapseSpan().Milliseconds(),
				TotalRelatedDataSize: relatedDataSize,
			},
			ScannedRemoteBytes: cost.ScannedRemoteBytes,
			ScannedTotalBytes:  cost.ScannedTotalBytes,
		}
		allTasks[i].resultBlobPinned = false
	}

	// Phase 2: on error, nil out all results and release all refs.
	// On success, pin or release refs based on zero-copy mode.
	if phaseErr != nil {
		for _, task := range allTasks {
			task.result = nil
			task.resultBlobPinned = false
		}
		for _, ref := range refs {
			ref.Release()
		}
		return phaseErr
	}
	if zeroCopy {
		for i, task := range allTasks {
			if len(task.result.GetSlicedBlob()) > 0 {
				resource.MsgPins.Pin(task.result, refs[i].Release)
				task.resultBlobPinned = true
			} else {
				refs[i].Release()
			}
		}
	} else {
		for _, ref := range refs {
			ref.Release()
		}
	}

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
	for branchIdx, branchReq := range branchReqs {
		searchReq, err := segcore.NewSearchRequest(t.collection.GetCCollection(), branchReq, branchReq.GetReq().GetPlaceholderGroup())
		if err != nil {
			return err
		}
		searchReqs = append(searchReqs, searchReq)
		actualNQ := searchReq.GetNumOfQuery()
		declaredNQ := branchReq.GetReq().GetNq()
		if actualNQ != declaredNQ {
			// The proxy and managed-function pipeline produced this internal
			// request. A mismatch here violates that component contract rather
			// than validating user input. Reject it before allocating the
			// branch-by-segment result matrix or running any segment search.
			return merr.WrapErrServiceInternalMsg(
				"shared-filter branch %d parsed NQ %d does not match declared NQ %d",
				branchIdx, actualNQ, declaredNQ)
		}
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

	// Reduce the branches concurrently: they are independent, and before
	// grouping each sub-request was its own task reducing on its own
	// goroutine. Each branch gets its own TimeRecorder because the reduce
	// path records spans on it; the envelope's ServiceTime is taken from the
	// task's recorder once everything is done.
	branchResults := make([]*internalpb.SearchResults, len(branchReqs))
	branchResultPinned := make([]bool, len(branchReqs))
	releasePinsOnReturn := true
	defer func() {
		if releasePinsOnReturn {
			releaseSharedFilterResultPins(branchResults, branchResultPinned)
		}
	}()
	relatedDataSize := relatedDataSizeOf(searchedSegments)
	// The branches run on the group's context, not the task's, so the first
	// failure reaches the siblings instead of leaving Wait to sit through up
	// to a thousand more reductions. The fan-out is bounded, so most of a
	// large group is still queued when a branch fails; the check below is what
	// turns the cancellation into work not done, because the reduce itself is
	// a blocking cgo call that takes ctx only to carry the trace.
	group, gctx := errgroup.WithContext(t.ctx)
	group.SetLimit(min(len(branchReqs), hardware.GetCPUNum()))
	for i, branchReq := range branchReqs {
		i, branchReq := i, branchReq
		group.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			branch := t.branchTask(gctx, branchReq)
			branchTR := timerecord.NewTimeRecorderWithTrace(gctx, "SearchTaskBranch")
			if err := branch.reduceSegmentResults(searchReqs[i], grouped[i], relatedDataSize, branchTR); err != nil {
				return err
			}
			branchResults[i] = branch.result
			branchResultPinned[i] = branch.resultBlobPinned
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	envelope := assembleSharedFilterEnvelope(t.GetNodeID(), branchResults, tr.ElapseSpan().Milliseconds())
	adoptSharedFilterResultPins(envelope, branchResults, branchResultPinned)
	t.result = envelope
	// Success publishes the envelope after it has adopted any branch pins.
	// With no pins, this simply disables the error-path no-op cleanup.
	releasePinsOnReturn = false
	return nil
}

func releaseSharedFilterResultPins(branchResults []*internalpb.SearchResults, pinned []bool) {
	for i, isPinned := range pinned {
		if isPinned {
			resource.MsgPins.Release(branchResults[i])
			pinned[i] = false
		}
	}
}

func adoptSharedFilterResultPins(
	envelope *internalpb.SearchResults,
	branchResults []*internalpb.SearchResults,
	pinned []bool,
) {
	for _, isPinned := range pinned {
		if isPinned {
			// The cleanup captures only the temporary branch results and pin
			// flags. It keeps their C-backed blobs alive without retaining the
			// envelope or task in a finalizer cycle.
			resource.MsgPins.Pin(envelope, func() {
				releaseSharedFilterResultPins(branchResults, pinned)
			})
			return
		}
	}
}

// assembleSharedFilterEnvelope folds N per-branch results into the one
// response a grouped request returns. SubSearchResults carries no cost or
// storage fields, so everything per-branch that the caller still needs has to
// be attributed onto the envelope here, and the rule for each field is the
// contract the delegator's demux and the proxy rely on:
//
//   - ServiceTime is the whole task's, measured once by the caller after every
//     branch has been reduced. The branches reduce concurrently on their own
//     recorders, so their individual readings are neither additive nor a
//     measure of the task; summing them would report roughly N times the real
//     duration and hand the load-aside balancer a negative execute speed.
//   - Scanned{Remote,Total}Bytes are summed: each branch's vector search
//     scanned its own data, and the proxy's storage-cost metrics expect the
//     total for the request.
//   - TotalRelatedDataSize is taken once, from branch 0. It is the size of the
//     segments the request touched, not work done, and every branch touched the
//     same segments. N separate responses would have reported it N times; a
//     group reports it once.
//   - ResponseTime / TotalNQ are filled in by the RPC handler, and
//     IsTopkReduce / IsRecallEvaluation are echoed from the request there too.
//
// CostAggregation is never nil: the RPC handler assigns through it
// unconditionally, and a nil there is a process panic, not a request error.
func assembleSharedFilterEnvelope(nodeID int64, branchResults []*internalpb.SearchResults, serviceTimeMs int64) *internalpb.SearchResults {
	subResults := make([]*internalpb.SubSearchResults, len(branchResults))
	channelsMvcc := make(map[string]uint64)
	var costAggregation *internalpb.CostAggregation
	var scannedRemote, scannedTotal int64
	for i, branchResult := range branchResults {
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
		if costAggregation == nil {
			costAggregation = branchResult.GetCostAggregation()
		}
		scannedRemote += branchResult.GetScannedRemoteBytes()
		scannedTotal += branchResult.GetScannedTotalBytes()
		for ch, ts := range branchResult.GetChannelsMvcc() {
			channelsMvcc[ch] = ts
		}
	}
	if costAggregation == nil {
		costAggregation = &internalpb.CostAggregation{}
	}
	costAggregation.ServiceTime = serviceTimeMs
	return &internalpb.SearchResults{
		Status:             merr.Success(),
		Base:               &commonpb.MsgBase{SourceID: nodeID},
		IsAdvanced:         true,
		SubResults:         subResults,
		ChannelsMvcc:       channelsMvcc,
		CostAggregation:    costAggregation,
		ScannedRemoteBytes: scannedRemote,
		ScannedTotalBytes:  scannedTotal,
	}
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
			IsTopkReduce:            base.GetIsTopkReduce(),
			IsIterator:              base.GetIsIterator(),
			AnalyzerName:            sub.GetAnalyzerName(),
			CollectionTtlTimestamps: base.GetCollectionTtlTimestamps(),
			PkFilter:                common.PkFilterNoPkFilter,
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

// branchTask contains only the state consumed while reducing one branch. It
// uses the fan-out's context so the branch stops when a sibling fails.
func (t *SearchTask) branchTask(ctx context.Context, branchReq *querypb.SearchRequest) *SearchTask {
	return &SearchTask{
		ctx:         ctx,
		collection:  t.collection,
		req:         branchReq,
		originTopks: []int64{branchReq.GetReq().GetTopk()},
		originNqs:   []int64{branchReq.GetReq().GetNq()},
		serverID:    t.serverID,
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
	if t.req.GetReq().GetDbID() != other.req.GetReq().GetDbID() ||
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

// maxTopK is the widest top-K this task builds. Merge already keeps t.topk as
// the maximum over the tasks it merged along the NQ axis; a shared-filter group
// extends the same rule over its branches, so that neither kind of grouping
// reports the first sub-request's top-K as if it were the task's.
func (t *SearchTask) maxTopK() int64 {
	topk := t.topk
	for _, sub := range t.req.GetExtraFilterSharingReqs() {
		if sub.GetTopk() > topk {
			topk = sub.GetTopk()
		}
	}
	return topk
}

func (t *SearchTask) Done(err error) {
	if !t.merged {
		// One task may contain several shared-filter branches, so its operational
		// metrics describe all of them: NQ() sums the branches and maxTopK()
		// spans them. These are workload observations, not scheduler admission
		// weights. Both collapse to t.nq and t.topk for an ungrouped task.
		metrics.QueryNodeSearchGroupSize.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.groupSize))
		metrics.QueryNodeSearchGroupNQ.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.NQ()))
		metrics.QueryNodeSearchGroupTopK.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.maxTopK()))
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

type StreamingSearchTask struct {
	SearchTask
	others        []*StreamingSearchTask
	resultBlobs   segcore.SearchResultDataBlobs
	streamReducer segcore.StreamSearchReducer
}

func NewStreamingSearchTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.SearchRequest,
	serverID int64,
) *StreamingSearchTask {
	ctx, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "schedule")
	return &StreamingSearchTask{
		SearchTask: SearchTask{
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
		},
	}
}

func (t *StreamingSearchTask) MergeWith(other scheduler.Task) bool {
	return false
}

func (t *StreamingSearchTask) Execute() error {
	log := log.Ctx(t.ctx).With(
		zap.Int64("collectionID", t.collection.ID()),
		zap.String("shard", t.req.GetDmlChannels()[0]),
	)
	// 0. prepare search req
	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "SearchTask")
	req := t.req
	t.combinePlaceHolderGroups()
	searchReq, err := segcore.NewSearchRequest(t.collection.GetCCollection(), req, t.placeholderGroup)
	if err != nil {
		return err
	}
	defer searchReq.Delete()

	// 1. search&&reduce or streaming-search&&streaming-reduce
	metricType := searchReq.Plan().GetMetricType()
	var relatedDataSize int64
	if req.GetScope() == querypb.DataScope_Historical {
		streamReduceFunc := func(result *segments.SearchResult) error {
			reduceErr := t.streamReduce(t.ctx, searchReq.Plan(), result, t.originNqs, t.originTopks)
			return reduceErr
		}
		pinnedSegments, err := segments.SearchHistoricalStreamly(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
			streamReduceFunc)
		defer segcore.DeleteStreamReduceHelper(t.streamReducer)
		defer t.segmentManager.Segment.Unpin(pinnedSegments)
		if err != nil {
			log.Error("Failed to search sealed segments streamly", zap.Error(err))
			return err
		}
		t.resultBlobs, err = segcore.GetStreamReduceResult(t.ctx, t.streamReducer)
		defer segcore.DeleteSearchResultDataBlobs(t.resultBlobs)
		if err != nil {
			log.Error("Failed to get stream-reduced search result")
			return err
		}
		relatedDataSize = relatedDataSizeOf(pinnedSegments)
	} else if req.GetScope() == querypb.DataScope_Streaming {
		results, pinnedSegments, err := segments.SearchStreaming(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			req.GetReq().GetPartitionIDs(),
			req.GetSegmentIDs(),
		)
		defer segments.DeleteSearchResults(results)
		defer t.segmentManager.Segment.Unpin(pinnedSegments)
		if err != nil {
			return err
		}
		if t.maybeReturnForEmptyResults(results, metricType, tr) {
			return nil
		}
		tr.RecordSpan()
		t.resultBlobs, err = segcore.ReduceSearchResultsAndFillData(
			t.ctx,
			searchReq.Plan(),
			results,
			int64(len(results)),
			t.originNqs,
			t.originTopks,
		)
		if err != nil {
			log.Warn("failed to reduce search results", zap.Error(err))
			return err
		}
		defer segcore.DeleteSearchResultDataBlobs(t.resultBlobs)
		metrics.QueryNodeReduceLatency.WithLabelValues(
			fmt.Sprint(t.GetNodeID()),
			metrics.SearchLabel,
			metrics.ReduceSegments,
			metrics.BatchReduce).
			Observe(float64(tr.RecordSpan().Microseconds()) / 1000.0)
		relatedDataSize = relatedDataSizeOf(pinnedSegments)
	}

	// 2. reorganize blobs to original search request
	for i := range t.originNqs {
		blob, cost, err := segcore.GetSearchResultDataBlob(t.ctx, t.resultBlobs, i)
		if err != nil {
			return err
		}

		var task *StreamingSearchTask
		if i == 0 {
			task = t
		} else {
			task = t.others[i-1]
		}

		// Note: blob is unsafe because get from C
		bs := make([]byte, len(blob))
		copy(bs, blob)

		task.result = &internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				SourceID: t.GetNodeID(),
			},
			Status:         merr.Success(),
			MetricType:     metricType,
			NumQueries:     t.originNqs[i],
			TopK:           t.originTopks[i],
			SlicedBlob:     bs,
			SlicedOffset:   1,
			SlicedNumCount: 1,
			CostAggregation: &internalpb.CostAggregation{
				ServiceTime:          tr.ElapseSpan().Milliseconds(),
				TotalRelatedDataSize: relatedDataSize,
			},
			ScannedRemoteBytes: cost.ScannedRemoteBytes,
			ScannedTotalBytes:  cost.ScannedTotalBytes,
		}
	}

	return nil
}

func (t *StreamingSearchTask) maybeReturnForEmptyResults(results []*segments.SearchResult,
	metricType string, tr *timerecord.TimeRecorder,
) bool {
	if len(results) == 0 {
		for i := range t.originNqs {
			var task *StreamingSearchTask
			if i == 0 {
				task = t
			} else {
				task = t.others[i-1]
			}

			task.result = &internalpb.SearchResults{
				Base: &commonpb.MsgBase{
					SourceID: t.GetNodeID(),
				},
				Status:         merr.Success(),
				MetricType:     metricType,
				NumQueries:     t.originNqs[i],
				TopK:           t.originTopks[i],
				SlicedOffset:   1,
				SlicedNumCount: 1,
				CostAggregation: &internalpb.CostAggregation{
					ServiceTime: tr.ElapseSpan().Milliseconds(),
				},
				ScannedRemoteBytes: 0,
				ScannedTotalBytes:  0,
			}
		}
		return true
	}
	return false
}

func (t *StreamingSearchTask) streamReduce(ctx context.Context,
	plan *segcore.SearchPlan,
	newResult *segments.SearchResult,
	sliceNQs []int64,
	sliceTopKs []int64,
) error {
	if t.streamReducer == nil {
		var err error
		t.streamReducer, err = segcore.NewStreamReducer(ctx, plan, sliceNQs, sliceTopKs)
		if err != nil {
			log.Error("Fail to init stream reducer, return")
			return err
		}
	}

	return segcore.StreamReduceSearchResult(ctx, newResult, t.streamReducer)
}
