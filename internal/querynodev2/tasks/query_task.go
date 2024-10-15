package tasks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ scheduler.Task = &QueryTask{}

func NewQueryTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.QueryRequest,
) *QueryTask {
	ctx, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "schedule")
	return &QueryTask{
		ctx:            ctx,
		collection:     collection,
		segmentManager: manager,
		req:            req,
		notifier:       make(chan error, 1),
		tr:             timerecord.NewTimeRecorderWithTrace(ctx, "queryTask"),
		scheduleSpan:   span,
	}
}

type QueryTask struct {
	ctx            context.Context
	collection     *segments.Collection
	segmentManager *segments.Manager
	req            *querypb.QueryRequest
	result         *internalpb.RetrieveResults
	notifier       chan error
	tr             *timerecord.TimeRecorder
	scheduleSpan   trace.Span
}

// Return the username which task is belong to.
// Return "" if the task do not contain any user info.
func (t *QueryTask) Username() string {
	return t.req.Req.GetUsername()
}

func (t *QueryTask) IsGpuIndex() bool {
	return false
}

// PreExecute the task, only call once.
func (t *QueryTask) PreExecute() error {
	// Update task wait time metric before execute
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	inQueueDuration := t.tr.ElapseSpan()
	inQueueDurationMS := inQueueDuration.Seconds() * 1000

	// Update in queue metric for prometheus.
	metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.QueryLabel,
		t.collection.GetDBName(),
		t.collection.GetResourceGroup(), // TODO: resource group and db name may be removed at runtime.
		// should be refactor into metricsutil.observer in the future.
	).Observe(inQueueDurationMS)

	username := t.Username()
	metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.QueryLabel,
		username).
		Observe(inQueueDurationMS)

	return nil
}

func (t *QueryTask) SearchResult() *internalpb.SearchResults {
	return nil
}

// Execute the task, only call once.
func (t *QueryTask) Execute() error {
	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "QueryTask")

	retrievePlan, err := segcore.NewRetrievePlan(
		t.collection.GetCCollection(),
		t.req.Req.GetSerializedExprPlan(),
		t.req.Req.GetMvccTimestamp(),
		t.req.Req.Base.GetMsgID(),
		t.req.Req.GetConsistencyLevel(),
	)
	if err != nil {
		return err
	}
	defer retrievePlan.Delete()
	results, pinnedSegments, err := segments.Retrieve(t.ctx, t.segmentManager, retrievePlan, t.req)
	defer t.segmentManager.Segment.Unpin(pinnedSegments)
	if err != nil {
		return err
	}

	reducer := segments.CreateSegCoreReducer(
		t.req,
		t.collection.Schema(),
		t.segmentManager,
	)
	beforeReduce := time.Now()

	reduceResults := make([]*segcorepb.RetrieveResults, 0, len(results))
	querySegments := make([]segments.Segment, 0, len(results))
	for _, result := range results {
		reduceResults = append(reduceResults, result.Result)
		querySegments = append(querySegments, result.Segment)
	}
	reducedResult, err := reducer.Reduce(t.ctx, reduceResults, querySegments, retrievePlan)

	metrics.QueryNodeReduceLatency.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel,
		metrics.ReduceSegments,
		metrics.BatchReduce).Observe(float64(time.Since(beforeReduce).Milliseconds()))
	if err != nil {
		return err
	}

	relatedDataSize := lo.Reduce(querySegments, func(acc int64, seg segments.Segment, _ int) int64 {
		return acc + segments.GetSegmentRelatedDataSize(seg)
	}, 0)

	t.result = &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{
			SourceID: paramtable.GetNodeID(),
		},
		Status:     merr.Success(),
		Ids:        reducedResult.Ids,
		FieldsData: reducedResult.FieldsData,
		CostAggregation: &internalpb.CostAggregation{
			ServiceTime:          tr.ElapseSpan().Milliseconds(),
			TotalRelatedDataSize: relatedDataSize,
		},
		AllRetrieveCount: reducedResult.GetAllRetrieveCount(),
		HasMoreResult:    reducedResult.HasMoreResult,
	}
	return nil
}

func (t *QueryTask) Done(err error) {
	t.notifier <- err
}

func (t *QueryTask) Canceled() error {
	return t.ctx.Err()
}

func (t *QueryTask) Wait() error {
	return <-t.notifier
}

func (t *QueryTask) Result() *internalpb.RetrieveResults {
	return t.result
}

func (t *QueryTask) NQ() int64 {
	return 1
}
