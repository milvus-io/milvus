package tasks

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/collector"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

var _ Task = &QueryTask{}

func NewQueryTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.QueryRequest,
) *QueryTask {
	return &QueryTask{
		ctx:            ctx,
		collection:     collection,
		segmentManager: manager,
		req:            req,
		notifier:       make(chan error, 1),
		tr:             timerecord.NewTimeRecorderWithTrace(ctx, "queryTask"),
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
}

// Return the username which task is belong to.
// Return "" if the task do not contain any user info.
func (t *QueryTask) Username() string {
	return t.req.Req.GetUsername()
}

// PreExecute the task, only call once.
func (t *QueryTask) PreExecute() error {
	// Update task wait time metric before execute
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	inQueueDuration := t.tr.ElapseSpan()

	// Update in queue metric for prometheus.
	metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.QueryLabel).
		Observe(float64(inQueueDuration.Milliseconds()))

	username := t.Username()
	metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
		nodeID,
		metrics.QueryLabel,
		username).
		Observe(float64(inQueueDuration.Milliseconds()))

	// Update collector for query node quota.
	collector.Average.Add(metricsinfo.QueryQueueMetric, float64(inQueueDuration.Microseconds()))
	return nil
}

// Execute the task, only call once.
func (t *QueryTask) Execute() error {
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "QueryTask")

	retrievePlan, err := segments.NewRetrievePlan(
		t.collection,
		t.req.Req.GetSerializedExprPlan(),
		t.req.Req.GetMvccTimestamp(),
		t.req.Req.Base.GetMsgID(),
	)
	if err != nil {
		return err
	}
	defer retrievePlan.Delete()
	results, querySegments, err := segments.Retrieve(t.ctx, t.segmentManager, retrievePlan, t.req)
	defer t.segmentManager.Segment.Unpin(querySegments)
	if err != nil {
		return err
	}

	reducer := segments.CreateSegCoreReducer(
		t.req,
		t.collection.Schema(),
	)
	beforeReduce := time.Now()
	reducedResult, err := reducer.Reduce(t.ctx, results)

	metrics.QueryNodeReduceLatency.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel,
		metrics.ReduceSegments).Observe(float64(time.Since(beforeReduce).Milliseconds()))
	if err != nil {
		return err
	}

	t.result = &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{
			SourceID: paramtable.GetNodeID(),
		},
		Status:     merr.Status(nil),
		Ids:        reducedResult.Ids,
		FieldsData: reducedResult.FieldsData,
		CostAggregation: &internalpb.CostAggregation{
			ServiceTime: tr.ElapseSpan().Milliseconds(),
		},
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
