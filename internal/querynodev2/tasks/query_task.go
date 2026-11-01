package tasks

import (
	"context"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
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

// NewMaterializeTask is NewQueryTask for MaterializeSegments: the retrieve
// takes the Arrow path and the task replies with one Arrow batch
// (MaterializeResult) instead of Result (design doc 20261019 §4).
func NewMaterializeTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.QueryRequest,
) *QueryTask {
	t := NewQueryTask(ctx, collection, manager, req)
	t.materialize = true
	return t
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

	// materialize (NewMaterializeTask) sets materializeResult instead of
	// result.
	materialize       bool
	materializeResult *internalpb.MaterializeResults
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
		username,
	).
		Observe(inQueueDurationMS)

	// Unmarshal the origin plan
	if err := proto.Unmarshal(t.req.Req.GetSerializedExprPlan(), t.plan); err != nil {
		return err
	}

	return nil
}

func (t *QueryTask) SearchResult() *internalpb.SearchResults {
	return nil
}

// Execute the task, only call once.
func (t *QueryTask) Execute() error {
	return t.execute(nil)
}

// TODO(#40451): move selected-segment retrieval and reduction into the shared
// QueryView execution layer so SN no longer depends on the legacy QueryNode task.
// Keep segment selection and handle ownership with the QueryView caller.
func (t *QueryTask) ExecuteOnSegments(selected []segments.Segment) error {
	if selected == nil {
		selected = []segments.Segment{}
	}
	return t.execute(selected)
}

func (t *QueryTask) execute(selected []segments.Segment) error {
	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "QueryTask")

	retrievePlan, err := t.collection.NewRetrievePlan(t.req)
	if err != nil {
		return err
	}
	defer retrievePlan.Delete()
	resultCount := retrieveTakeForOutputResultCount(t.req.GetReq(), t.plan)
	takeAllowed := requestAllowsTakeForOutput(resultCount)
	retrievePlan.SetTakeForOutputAllowed(takeAllowed)

	var results []segments.RetrieveSegmentResult
	var pinnedSegments []segments.Segment
	// A materialize task's segments take the Arrow path, its reply's format.
	var opts []segments.RetrieveOption
	if t.materialize {
		opts = append(opts, segments.WithArrowTransport())
	}
	if selected != nil {
		results, err = segments.RetrieveSelectedSegments(t.ctx, retrievePlan, t.req, selected, opts...)
	} else {
		results, pinnedSegments, err = segments.Retrieve(t.ctx, t.segmentManager, retrievePlan, t.req, opts...)
		defer t.segmentManager.Segment.Unpin(pinnedSegments)
	}
	// The Arrow transport hands back records that MaterializeArrowSelection
	// reads below -- the reduce only records which rows won. They are
	// owned here, not by the pipeline, and must be released on every path --
	// including the error returns below, hence a defer rather than an explicit
	// release after the reduce.
	defer segments.ReleaseRecords(results)

	if err != nil {
		return err
	}

	beforeReduce := time.Now()

	reduceResults := make([]*segcorepb.RetrieveResults, 0, len(results))
	querySegments := make([]segments.Segment, 0, len(results))
	// arrowRecords stays positionally aligned with reduceResults; the reduce
	// addresses rows by index into it.
	arrowRecords := make([]arrow.Record, 0, len(results))
	hasArrow := false
	for _, result := range results {
		reduceResults = append(reduceResults, result.Result)
		querySegments = append(querySegments, result.Segment)
		arrowRecords = append(arrowRecords, result.Record)
		if result.Record != nil {
			hasArrow = true
		}
	}
	if !hasArrow {
		// All-nil would make the reduce think Arrow is in play and produce an
		// empty gather; nil means "protobuf path" unambiguously.
		arrowRecords = nil
	}
	reducedResult, arrowSelection, err := segments.RunQNQueryPipeline(
		t.ctx, t.req, t.collection.Schema(), t.plan,
		reduceResults, arrowRecords, querySegments, t.segmentManager, retrievePlan,
	)

	metrics.QueryNodeReduceLatency.WithLabelValues(
		paramtable.GetStringNodeID(),
		contextutil.GetQueryLabel(t.ctx),
		metrics.ReduceSegments,
		metrics.BatchReduce,
	).Observe(float64(time.Since(beforeReduce).Microseconds()) / 1000.0)
	if err != nil {
		return err
	}

	relatedDataSize := lo.Reduce(querySegments, func(acc int64, seg segments.Segment, _ int) int64 {
		return acc + segments.GetSegmentRelatedDataSize(seg)
	}, 0)
	cost := &internalpb.CostAggregation{TotalRelatedDataSize: relatedDataSize}

	if t.materialize {
		// ServiceTime covers building the batch, mirroring the Query path
		// which covers MaterializeArrowSelection.
		if err := t.buildMaterializeResult(reducedResult, arrowSelection, cost); err != nil {
			return err
		}
		t.materializeResult.Header.FeatureBits = retrievePlan.FeatureBits() | segments.ColdReadFeatureBit(reducedResult.GetScannedRemoteBytes())
		cost.ServiceTime = tr.ElapseSpan().Milliseconds()
		return nil
	}

	// Materialize the Arrow path's chosen rows into FieldsData. This is the
	// response's own format, so it ends the Arrow representation here, at the
	// RPC boundary, exactly where the records are still alive -- the deferred
	// ReleaseRecords above has not run yet. No-op on the protobuf path.
	if err := queryutil.MaterializeArrowSelection(reducedResult, arrowSelection, t.collection.Schema()); err != nil {
		return err
	}
	cost.ServiceTime = tr.ElapseSpan().Milliseconds()

	t.result = &internalpb.RetrieveResults{
		Base: &commonpb.MsgBase{
			SourceID: paramtable.GetNodeID(),
		},
		Status:             merr.Success(),
		Ids:                reducedResult.Ids,
		FieldsData:         reducedResult.FieldsData,
		CostAggregation:    cost,
		AllRetrieveCount:   reducedResult.GetAllRetrieveCount(),
		HasMoreResult:      reducedResult.HasMoreResult,
		ScannedRemoteBytes: reducedResult.GetScannedRemoteBytes(),
		ScannedTotalBytes:  reducedResult.GetScannedTotalBytes(),
		ElementLevel:       reducedResult.GetElementLevel(),
		ElementIndices:     convertSegcoreElementIndicesToInternal(reducedResult.GetElementIndices()),
		FeatureBits:        retrievePlan.FeatureBits() | segments.ColdReadFeatureBit(reducedResult.GetScannedRemoteBytes()),
	}
	return nil
}

// buildMaterializeResult writes the reduce output as one Arrow IPC batch. It
// runs while the selection's records are alive (before the deferred
// ReleaseRecords in execute).
func (t *QueryTask) buildMaterializeResult(reduced *segcorepb.RetrieveResults, sel *queryutil.ArrowSelection, cost *internalpb.CostAggregation) error {
	header := &internalpb.MaterializeHeader{
		Status:             merr.Success(),
		SourceId:           paramtable.GetNodeID(),
		AllRetrieveCount:   reduced.GetAllRetrieveCount(),
		ScannedRemoteBytes: reduced.GetScannedRemoteBytes(),
		ScannedTotalBytes:  reduced.GetScannedTotalBytes(),
		CostAggregation:    cost,
	}
	t.materializeResult = &internalpb.MaterializeResults{Header: header}
	enc, err := materialize.Encode(reduced, sel)
	if err != nil || enc.Entry == nil {
		return err
	}
	// The caller names a schema it already holds; send the bytes only when
	// this batch's schema is a different one. Zero means "none named".
	header.SchemaFingerprint = materialize.SchemaFingerprint(enc.SchemaMsg)
	if want := t.req.GetMaterializeSchemaFingerprint(); want == 0 || want != header.SchemaFingerprint {
		header.BodySchema = enc.SchemaMsg
	}
	header.BodyRawSize = enc.RawSize
	header.ResultSize = enc.ResultSize
	t.materializeResult.Body = [][]byte{enc.Entry}
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

func (t *QueryTask) MaterializeResult() *internalpb.MaterializeResults {
	return t.materializeResult
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
