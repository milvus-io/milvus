package tasks

// TODO: rename this file into search_task.go

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var (
	_ Task      = &SearchTask{}
	_ MergeTask = &SearchTask{}
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

	tr *timerecord.TimeRecorder
}

func NewSearchTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.SearchRequest,
) *SearchTask {
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
	}
}

// Return the username which task is belong to.
// Return "" if the task do not contain any user info.
func (t *SearchTask) Username() string {
	return t.req.Req.GetUsername()
}

func (t *SearchTask) PreExecute() error {
	// Update task wait time metric before execute
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	inQueueDuration := t.tr.ElapseSpan()

	// Update in queue metric for prometheus.
	metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.SearchLabel).
		Observe(float64(inQueueDuration.Milliseconds()))

	username := t.Username()
	metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.SearchLabel,
		username).
		Observe(float64(inQueueDuration.Milliseconds()))

	// Update collector for query node quota.
	collector.Average.Add(metricsinfo.SearchQueueMetric, float64(inQueueDuration.Microseconds()))

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

	executeRecord := timerecord.NewTimeRecorderWithTrace(t.ctx, "searchTaskExecute")

	req := t.req
	t.combinePlaceHolderGroups()
	searchReq, err := segments.NewSearchRequest(t.collection, req, t.placeholderGroup)
	if err != nil {
		return err
	}
	defer searchReq.Delete()

	var results []*segments.SearchResult
	if req.GetScope() == querypb.DataScope_Historical {
		results, _, _, err = segments.SearchHistorical(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
		)
	} else if req.GetScope() == querypb.DataScope_Streaming {
		results, _, _, err = segments.SearchStreaming(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
		)
	}
	if err != nil {
		return err
	}
	defer segments.DeleteSearchResults(results)

	if len(results) == 0 {
		for i := range t.originNqs {
			var task *SearchTask
			if i == 0 {
				task = t
			} else {
				task = t.others[i-1]
			}

			task.result = &internalpb.SearchResults{
				Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				MetricType:     req.GetReq().GetMetricType(),
				NumQueries:     t.originNqs[i],
				TopK:           t.originTopks[i],
				SlicedOffset:   1,
				SlicedNumCount: 1,
				CostAggregation: &internalpb.CostAggregation{
					ServiceTime: executeRecord.ElapseSpan().Milliseconds(),
				},
			}
		}
		return nil
	}

	reduceRecord := timerecord.NewTimeRecorderWithTrace(t.ctx, "searchTaskReduce")
	blobs, err := segments.ReduceSearchResultsAndFillData(
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
	defer segments.DeleteSearchResultDataBlobs(blobs)

	for i := range t.originNqs {
		blob, err := segments.GetSearchResultDataBlob(blobs, i)
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

		metrics.QueryNodeReduceLatency.WithLabelValues(
			fmt.Sprint(paramtable.GetNodeID()),
			metrics.SearchLabel).
			Observe(float64(reduceRecord.ElapseSpan().Milliseconds()))

		task.result = &internalpb.SearchResults{
			Status:         util.WrapStatus(commonpb.ErrorCode_Success, ""),
			MetricType:     req.GetReq().GetMetricType(),
			NumQueries:     t.originNqs[i],
			TopK:           t.originTopks[i],
			SlicedBlob:     bs,
			SlicedOffset:   1,
			SlicedNumCount: 1,
			CostAggregation: &internalpb.CostAggregation{
				ServiceTime: executeRecord.ElapseSpan().Milliseconds(),
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
		t.req.GetReq().GetTravelTimestamp() != other.req.GetReq().GetTravelTimestamp() ||
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
		metrics.QueryNodeSearchGroupSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(t.groupSize))
		metrics.QueryNodeSearchGroupNQ.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(t.nq))
		metrics.QueryNodeSearchGroupTopK.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(t.topk))
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

func (t *SearchTask) Result() *internalpb.SearchResults {
	return t.result
}

func (t *SearchTask) NQ() int64 {
	return t.nq
}

func (t *SearchTask) MergeWith(other Task) bool {
	switch other := other.(type) {
	case *SearchTask:
		return t.Merge(other)
	}
	return false
}

// combinePlaceHolderGroups combine all the placeholder groups.
func (t *SearchTask) combinePlaceHolderGroups() {
	if len(t.others) > 0 {
		ret := &commonpb.PlaceholderGroup{}
		_ = proto.Unmarshal(t.placeholderGroup, ret)
		for _, t := range t.others {
			x := &commonpb.PlaceholderGroup{}
			_ = proto.Unmarshal(t.placeholderGroup, x)
			ret.Placeholders[0].Values = append(ret.Placeholders[0].Values, x.Placeholders[0].Values...)
		}
		t.placeholderGroup, _ = proto.Marshal(ret)
	}
}
