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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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

func (t *SearchTask) IsGpuIndex() bool {
	return t.collection.IsGpuIndex()
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
	defer segcore.DeleteSearchResultDataBlobs(blobs)
	metrics.QueryNodeReduceLatency.WithLabelValues(
		fmt.Sprint(t.GetNodeID()),
		metrics.SearchLabel,
		metrics.ReduceSegments,
		metrics.BatchReduce).
		Observe(float64(tr.RecordSpan().Milliseconds()))
	for i := range t.originNqs {
		blob, err := segcore.GetSearchResultDataBlob(t.ctx, blobs, i)
		if err != nil {
			return err
		}

		var task *SearchTask
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
		}
	}

	return nil
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

	// Check mergeable
	if t.req.GetReq().GetDbID() != other.req.GetReq().GetDbID() ||
		t.req.GetReq().GetCollectionID() != other.req.GetReq().GetCollectionID() ||
		t.req.GetReq().GetMvccTimestamp() != other.req.GetReq().GetMvccTimestamp() ||
		t.req.GetReq().GetDslType() != other.req.GetReq().GetDslType() ||
		t.req.GetDmlChannels()[0] != other.req.GetDmlChannels()[0] ||
		nq+otherNq > paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt64() ||
		diffTopk && ratio > paramtable.Get().QueryNodeCfg.TopKMergeRatio.GetAsFloat() ||
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

func (t *SearchTask) Canceled() error {
	return t.ctx.Err()
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
			Observe(float64(tr.RecordSpan().Milliseconds()))
		relatedDataSize = lo.Reduce(pinnedSegments, func(acc int64, seg segments.Segment, _ int) int64 {
			return acc + segments.GetSegmentRelatedDataSize(seg)
		}, 0)
	}

	// 2. reorganize blobs to original search request
	for i := range t.originNqs {
		blob, err := segcore.GetSearchResultDataBlob(t.ctx, t.resultBlobs, i)
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
