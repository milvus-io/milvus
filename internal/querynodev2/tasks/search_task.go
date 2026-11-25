package tasks

// TODO: rename this file into search_task.go

import "C"

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/resource"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	_ scheduler.Task      = &SearchTask{}
	_ scheduler.MergeTask = &SearchTask{}
	// The scheduler asks for this interface at run time. Without the
	// assertion a signature drift would silently fall back to the
	// single-task path and stop isolating cancellation inside a group.
	_ scheduler.PrunableTask = &SearchTask{}
	// Likewise for the queue's expiry sweep, which would otherwise decide on
	// the owner's context alone and end every request merged behind it.
	_ scheduler.ExpirableGroup = &SearchTask{}
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
	queueBudget      scheduler.QueueBudget
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
	log := log.Ctx(t.ctx).With(
		zap.Int64("collectionID", t.collection.ID()),
		zap.String("shard", t.req.GetDmlChannels()[0]),
	)

	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	if len(t.others) > 0 {
		restore := t.useGroupContext()
		defer restore()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "SearchTask")

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
		}
		return nil
	}

	relatedDataSize := lo.Reduce(searchedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
		return acc + segments.GetSegmentRelatedDataSize(seg)
	}, 0)

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
	}

	// Phase 2: on error, nil out all results and release all refs.
	// On success, pin or release refs based on zero-copy mode.
	if phaseErr != nil {
		for _, task := range allTasks {
			task.result = nil
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

// cancellationOf reports why a task's request was canceled, or nil if it was
// not. A task built by NewSearchTask always carries a context, but the struct
// is also built directly, notably in tests that exercise the merge rules
// alone; a task with no context has nothing that could cancel it, and reading
// the field directly would panic on one.
func cancellationOf(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// Merge folds other into this task's group when the two are compatible. A
// task whose context is already canceled is never merged, in either
// direction: it would only be dropped again at dequeue.
func (t *SearchTask) Merge(other *SearchTask) bool {
	if cancellationOf(t.ctx) != nil || cancellationOf(other.ctx) != nil {
		return false
	}
	if !t.canMerge(other) {
		return false
	}
	t.absorb(other)
	return true
}

func (t *SearchTask) canMerge(other *SearchTask) bool {
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

	return true
}

// absorb adds other, which must be a standalone task, to this group without
// re-checking compatibility.
func (t *SearchTask) absorb(other *SearchTask) {
	t.groupSize += other.groupSize
	t.topk = funcutil.Max(t.topk, other.topk)
	t.nq += other.nq
	t.originTopks = append(t.originTopks, other.originTopks...)
	t.originNqs = append(t.originNqs, other.originNqs...)
	t.others = append(t.others, other)
	other.merged = true
}

// members returns the group owner followed by the merged members.
func (t *SearchTask) members() []*SearchTask {
	return append([]*SearchTask{t}, t.others...)
}

// resetGroup turns a task back into a standalone task carrying only its own
// request, so that it can be regrouped.
func (t *SearchTask) resetGroup() {
	t.others = nil
	t.merged = false
	t.groupSize = 1
	t.originTopks = []int64{t.originTopks[0]}
	t.originNqs = []int64{t.originNqs[0]}
	t.topk = t.originTopks[0]
	t.nq = t.originNqs[0]
}

func (t *SearchTask) SetQueueBudget(budget scheduler.QueueBudget) { t.queueBudget = budget }

func (t *SearchTask) QueueBudget() scheduler.QueueBudget { return t.queueBudget }

// PruneQueued preserves each member's own expiry and completion error.
func (t *SearchTask) PruneQueued(now time.Time) (scheduler.Task, int, error) {
	return t.pruneMembers(func(m *SearchTask) error { return m.queueBudget.ExpiryError(m.ctx, now) })
}

// PruneCanceled implements scheduler.PrunableTask. It must run before
// Execute, while every member still holds its own placeholder group.
//
// Members whose context is canceled are completed right away with their own
// context error and leave the group. The survivors are regrouped, with the
// first of them as the new owner, so that canceling the owner does not end
// the requests merged behind it.
func (t *SearchTask) PruneCanceled() (scheduler.Task, int, error) {
	return t.pruneMembers(func(m *SearchTask) error { return cancellationOf(m.ctx) })
}

func (t *SearchTask) pruneMembers(expiryError func(*SearchTask) error) (scheduler.Task, int, error) {
	members := t.members()
	alive := make([]*SearchTask, 0, len(members))
	var cause error
	for _, m := range members {
		if err := expiryError(m); err != nil {
			m.finishPruned(err)
			if cause == nil {
				cause = err
			}
			continue
		}
		alive = append(alive, m)
	}
	dropped := len(members) - len(alive)
	if dropped == 0 {
		return t, 0, nil
	}
	if len(alive) == 0 || alive[0] != t {
		// The old owner is finished; drop its references to the survivors.
		t.resetGroup()
	}
	if len(alive) == 0 {
		return nil, dropped, cause
	}
	owner := alive[0]
	owner.resetGroup()
	for _, m := range alive[1:] {
		m.resetGroup()
		owner.absorb(m)
	}
	return owner, dropped, cause
}

// ExpiryReady implements scheduler.ExpirableGroup. The group may be taken out
// of the queue only when every request in it is canceled or due by
// cleanupTime: members can carry deadlines up to the merge gap apart, and one
// member's reaching its deadline says nothing about the others'.
func (t *SearchTask) ExpiryReady(cleanupTime time.Time) bool {
	if !t.dueBy(cleanupTime) {
		return false
	}
	for _, m := range t.others {
		if !m.dueBy(cleanupTime) {
			return false
		}
	}
	return true
}

// dueBy reports whether this request is canceled or has a deadline no later
// than cleanupTime.
func (t *SearchTask) dueBy(cleanupTime time.Time) bool {
	if cancellationOf(t.ctx) != nil {
		return true
	}
	if t.ctx == nil {
		return false
	}
	deadline, ok := t.ctx.Deadline()
	return ok && !cleanupTime.Before(deadline)
}

// FinishExpired implements scheduler.ExpirableGroup. Every request is told its
// own reason: its context error if it has one, otherwise DeadlineExceeded for
// a request taken out a little ahead of its deadline.
func (t *SearchTask) FinishExpired() {
	t.FinishUnrun(context.DeadlineExceeded)
}

// FinishUnrun completes each member without execution. Every request is told its own
// context error if it has one, otherwise err, and none of them is counted in
// the group histograms, which describe the groups that reach the executor.
func (t *SearchTask) FinishUnrun(err error) {
	for _, m := range t.members() {
		reason := cancellationOf(m.ctx)
		if reason == nil {
			reason = err
		}
		m.finishPruned(reason)
	}
}

// finishPruned completes a request that leaves without running, pruned or
// expired. It records none of the group histograms that Done records: those
// describe the groups that reach the executor, and this request is not in one.
func (t *SearchTask) finishPruned(err error) {
	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	t.notify(err)
}

func (t *SearchTask) notify(err error) {
	t.notifier <- err
}

// resultErr is what a group member is told when the group finishes with
// groupErr: its own context error if its request was canceled meanwhile,
// otherwise the group's outcome.
func (t *SearchTask) resultErr(groupErr error) error {
	if err := cancellationOf(t.ctx); err != nil {
		return err
	}
	return groupErr
}

// useGroupContext makes the group execute under a context that is canceled
// only once every member's context is canceled, so that one member's
// cancellation cannot fail the segcore call the others are waiting on. Trace
// and other values of the owner's context are kept. The returned function
// restores the owner's own context and releases the listeners.
func (t *SearchTask) useGroupContext() func() {
	members := t.members()
	ownerCtx := t.ctx
	groupCtx, cancel := context.WithCancel(context.WithoutCancel(ownerCtx))
	var remaining atomic.Int64
	remaining.Store(int64(len(members)))
	stops := make([]func() bool, 0, len(members))
	for _, m := range members {
		stops = append(stops, context.AfterFunc(m.ctx, func() {
			if remaining.Add(-1) == 0 {
				cancel()
			}
		}))
	}
	t.ctx = groupCtx
	return func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
		t.ctx = ownerCtx
	}
}

func (t *SearchTask) Done(err error) {
	// The group histograms describe the groups that reach the executor, once
	// each, at the size they reach it with. A request pruned or expired before
	// then leaves through finishPruned and is not counted, and a group every
	// one of whose requests was canceled never reaches the executor and is not
	// counted at all.
	if !t.merged {
		metrics.QueryNodeSearchGroupSize.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.groupSize))
		metrics.QueryNodeSearchGroupNQ.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.nq))
		metrics.QueryNodeSearchGroupTopK.WithLabelValues(fmt.Sprint(t.GetNodeID())).Observe(float64(t.topk))
	}
	if len(t.others) == 0 {
		t.notify(err)
		return
	}
	// A merged group: each member learns its own outcome. A member canceled
	// while the group ran gets its own context error; the others get the
	// group's result.
	t.notify(t.resultErr(err))
	for _, other := range t.others {
		other.notify(other.resultErr(err))
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
	return t.nq
}

func (t *SearchTask) MinNQ() int64 {
	if len(t.originNqs) == 0 {
		return t.nq
	}
	minNQ := t.originNqs[0]
	for _, nq := range t.originNqs[1:] {
		if nq < minNQ {
			minNQ = nq
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

var _ scheduler.PrunableTask = (*StreamingSearchTask)(nil)

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

// PruneCanceled retains this task's streaming executor. Streaming tasks never
// merge, and the embedded SearchTask's implementation would return that inner
// task instead of this receiver.
func (t *StreamingSearchTask) PruneCanceled() (scheduler.Task, int, error) {
	if err := cancellationOf(t.ctx); err != nil {
		t.finishPruned(err)
		return nil, 1, err
	}
	return t, 0, nil
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
		relatedDataSize = lo.Reduce(pinnedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
			return acc + segments.GetSegmentRelatedDataSize(seg)
		}, 0)
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
		relatedDataSize = lo.Reduce(pinnedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
			return acc + segments.GetSegmentRelatedDataSize(seg)
		}, 0)
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
