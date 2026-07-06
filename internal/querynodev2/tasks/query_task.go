package tasks

import (
	"context"
	"strconv"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
		plan:           &planpb.PlanNode{},
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
	plan           *planpb.PlanNode // used by RunQNQueryPipeline for reduce
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

func (t *QueryTask) Context() context.Context {
	return t.ctx
}

// PreExecute the task, only call once.
func (t *QueryTask) PreExecute() error {
	// Update task wait time metric before execute
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	inQueueDuration := t.tr.ElapseSpan()
	inQueueDurationMS := inQueueDuration.Seconds() * 1000

	// Update in queue metric for prometheus.
	queryLabel := contextutil.GetQueryLabel(t.ctx)
	metrics.QueryNodeSQLatencyInQueue.WithLabelValues(
		nodeID,
		queryLabel,
		t.collection.GetDBName(),
		t.collection.GetResourceGroup(), // TODO: resource group and db name may be removed at runtime.
		// should be refactor into metricsutil.observer in the future.
	).Observe(inQueueDurationMS)

	username := t.Username()
	metrics.QueryNodeSQPerUserLatencyInQueue.WithLabelValues(
		nodeID,
		queryLabel,
		username).
		Observe(inQueueDurationMS)

	logger := mlog.With(
		mlog.Int64("collectionID", t.collection.ID()),
		mlog.String("scope", t.req.GetScope().String()),
		mlog.String("queryLabel", queryLabel),
		mlog.Int("segmentNum", len(t.req.GetSegmentIDs())),
		mlog.Int64s("segmentIDs", t.req.GetSegmentIDs()))
	logger.Info(t.ctx, "[sss][www] qn scheduler task start",
		mlog.Duration("queueDuration", inQueueDuration))
	logger.Info(t.ctx, "[sss] query task pre execute",
		mlog.Duration("queueDuration", inQueueDuration))

	// Unmarshal the origin plan
	stageStart := time.Now()
	if err := proto.Unmarshal(t.req.Req.GetSerializedExprPlan(), t.plan); err != nil {
		logger.Warn(t.ctx, "[sss] query task pre execute failed",
			mlog.Duration("duration", time.Since(stageStart)),
			mlog.Err(err))
		return err
	}
	logger.Info(t.ctx, "[sss] query task pre execute done",
		mlog.Duration("duration", time.Since(stageStart)),
		mlog.Duration("queueDuration", inQueueDuration))

	return nil
}

func (t *QueryTask) SearchResult() *internalpb.SearchResults {
	return nil
}

// Execute the task, only call once.
func (t *QueryTask) Execute() error {
	logger := mlog.With(
		mlog.Int64("collectionID", t.collection.ID()),
		mlog.String("scope", t.req.GetScope().String()),
		mlog.String("queryLabel", contextutil.GetQueryLabel(t.ctx)),
		mlog.Int("segmentNum", len(t.req.GetSegmentIDs())),
		mlog.Int64s("segmentIDs", t.req.GetSegmentIDs()))
	executeStart := time.Now()
	logger.Info(t.ctx, "[sss] query task execute start")

	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "QueryTask")

	stageStart := time.Now()
	retrievePlan, err := segcore.NewRetrievePlan(
		t.collection.GetCCollection(),
		t.req.Req.GetSerializedExprPlan(),
		t.req.Req.GetMvccTimestamp(),
		t.req.Req.Base.GetMsgID(),
		t.req.Req.GetConsistencyLevel(),
		t.req.Req.GetCollectionTtlTimestamps(),
		t.req.Req.GetEntityTtlPhysicalTime(),
	)
	if err != nil {
		logger.Warn(t.ctx, "[sss] query task build retrieve plan failed",
			mlog.Duration("duration", time.Since(stageStart)),
			mlog.Err(err))
		return err
	}
	logger.Info(t.ctx, "[sss] query task build retrieve plan done",
		mlog.Duration("duration", time.Since(stageStart)),
		mlog.Bool("planShouldIgnoreNonPk", retrievePlan.ShouldIgnoreNonPk()))
	defer retrievePlan.Delete()

	stageStart = time.Now()
	logger.Info(t.ctx, "[sss] query task retrieve start")
	results, pinnedSegments, err := segments.Retrieve(t.ctx, t.segmentManager, retrievePlan, t.req)
	defer t.segmentManager.Segment.Unpin(pinnedSegments)
	if err != nil {
		logger.Warn(t.ctx, "[sss] query task retrieve failed",
			mlog.Duration("duration", time.Since(stageStart)),
			mlog.Int("pinnedSegmentNum", len(pinnedSegments)),
			mlog.Err(err))
		return err
	}
	logger.Info(t.ctx, "[sss] query task retrieve done",
		mlog.Duration("duration", time.Since(stageStart)),
		mlog.Int("resultNum", len(results)),
		mlog.Int("pinnedSegmentNum", len(pinnedSegments)))

	beforeReduce := time.Now()
	logger.Info(t.ctx, "[sss] query task reduce start",
		mlog.Int("resultNum", len(results)))

	reduceResults := make([]*segcorepb.RetrieveResults, 0, len(results))
	querySegments := make([]segments.Segment, 0, len(results))
	for _, result := range results {
		reduceResults = append(reduceResults, result.Result)
		querySegments = append(querySegments, result.Segment)
	}
	reducedResult, err := segments.RunQNQueryPipeline(
		t.ctx, t.req, t.collection.Schema(), t.plan,
		reduceResults, querySegments, t.segmentManager, retrievePlan,
	)

	metrics.QueryNodeReduceLatency.WithLabelValues(
		paramtable.GetStringNodeID(),
		contextutil.GetQueryLabel(t.ctx),
		metrics.ReduceSegments,
		metrics.BatchReduce).Observe(float64(time.Since(beforeReduce).Microseconds()) / 1000.0)
	if err != nil {
		logger.Warn(t.ctx, "[sss] query task reduce failed",
			mlog.Duration("duration", time.Since(beforeReduce)),
			mlog.Err(err))
		return err
	}
	logger.Info(t.ctx, "[sss] query task reduce done",
		mlog.Duration("duration", time.Since(beforeReduce)))

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
		AllRetrieveCount:   reducedResult.GetAllRetrieveCount(),
		HasMoreResult:      reducedResult.HasMoreResult,
		ScannedRemoteBytes: reducedResult.GetScannedRemoteBytes(),
		ScannedTotalBytes:  reducedResult.GetScannedTotalBytes(),
		ElementLevel:       reducedResult.GetElementLevel(),
		ElementIndices:     convertSegcoreElementIndicesToInternal(reducedResult.GetElementIndices()),
	}
	logger.Info(t.ctx, "[sss] query task execute done",
		mlog.Duration("duration", time.Since(executeStart)),
		mlog.Int64("allRetrieveCount", reducedResult.GetAllRetrieveCount()))
	return nil
}

func (t *QueryTask) Done(err error) {
	t.notifier <- err
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

// convertSegcoreElementIndicesToInternal converts segcorepb.ElementIndices to internalpb.ElementIndices
func convertSegcoreElementIndicesToInternal(src []*segcorepb.ElementIndices) []*internalpb.ElementIndices {
	if src == nil {
		return nil
	}
	dst := make([]*internalpb.ElementIndices, len(src))
	for i, s := range src {
		if s != nil {
			dst[i] = &internalpb.ElementIndices{Indices: s.GetIndices()}
		}
	}
	return dst
}
