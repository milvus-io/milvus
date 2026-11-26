package adaptor

import (
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/queryresource"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/querymeter"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The planner supplies column sizes for the effective segment selections. A
// column missing on any selected segment is left absent for the Proxy to detect.
func planQueryUsage(ctx context.Context, response *viewpb.GetQueryPlanResponse, view *viewpb.QueryViewOfShard, runtime *queryresource.ReadRuntime) (*viewpb.PlannedQueryUsage, error) {
	plan := response.GetPlan()
	search, retrieve := plan.GetLegacySearchRequest(), plan.GetLegacyRetrieveRequest()
	if !search.GetCollectSearchReadUsage() && !retrieve.GetCollectQueryReadUsage() {
		return nil, nil
	}
	var growing []snview.GrowingSegmentHandle
	for _, worker := range plan.GetWorkNodes() {
		if worker.GetStreamingNode() != nil {
			partitions, err := runtime.ResolvePartitions(view.GetMeta().GetLoadInfoVersion(), nil)
			if err != nil {
				return nil, err
			}
			growing, err = runtime.AcquireGrowingSegmentHandles(ctx, qviews.FromProtoDataVersion(plan.GetVersion().GetDataVersion()), partitions)
			if err != nil {
				return nil, err
			}
			for _, handle := range growing {
				defer handle.Release()
			}
			break
		}
	}
	usage := &viewpb.PlannedQueryUsage{}
	if retrieve != nil {
		fields, err := retrieveMeterFields(retrieve, runtime.Schema())
		if err != nil {
			return nil, err
		}
		read := newPlannedColumnRead(fields, 1)
		if err := collectPlanColumns(read, plan, view, growing, retrieve.GetPartitionIDs(), -1); err != nil {
			return nil, err
		}
		usage.Reads = []*viewpb.PlannedColumnRead{read}
		return usage, nil
	}

	requests := []*internalpb.SearchRequest{search}
	if search.GetIsAdvanced() {
		var err error
		requests, err = viewquery.BuildSubSearchRequests(search)
		if err != nil {
			return nil, err
		}
	}
	for i, request := range requests {
		sub := -1
		if search.GetIsAdvanced() {
			sub = i
			if search.GetSubReqs()[i].GetSkip() {
				continue
			}
		}
		var expr planpb.PlanNode
		if err := proto.Unmarshal(request.GetSerializedExprPlan(), &expr); err != nil {
			return nil, merr.Wrap(err, "decode search plan for metering")
		}
		// Aggregation and ordering columns belong to the outer request. Each
		// HybridSearch child supplies its own expression plan and NQ.
		fields, err := searchMeterFields(&expr, search.GetSearchReadFieldIds(), runtime.Schema())
		if err != nil {
			return nil, err
		}
		read := newPlannedColumnRead(fields, request.GetNq())
		selectedGrowing := growing
		if request.GetIgnoreGrowing() {
			selectedGrowing = nil
		}
		if err := collectPlanColumns(read, plan, view, selectedGrowing, request.GetPartitionIDs(), sub); err != nil {
			return nil, err
		}
		usage.Reads = append(usage.Reads, read)
		if response.GetClientOptimizeInfo().GetSearchOptimization() != nil {
			usage.FilterOnly = newPlannedColumnRead(querymeter.SearchFieldIDs(&expr, true), request.GetNq())
			if err := collectPlanColumns(usage.FilterOnly, plan, view, nil, request.GetPartitionIDs(), -1); err != nil {
				return nil, err
			}
		}
	}

	pk, err := typeutil.GetPrimaryFieldSchema(runtime.Schema())
	if err != nil {
		return nil, err
	}
	usage.RequeryPartitions, err = collectRequeryColumns(pk.GetFieldID(), plan, view, growing)
	if err != nil {
		return nil, err
	}
	return usage, nil
}

func retrieveMeterFields(req *internalpb.RetrieveRequest, schema *schemapb.CollectionSchema) ([]int64, error) {
	var plan planpb.PlanNode
	if err := proto.Unmarshal(req.GetSerializedExprPlan(), &plan); err != nil {
		return nil, merr.Wrap(err, "decode retrieve plan for metering")
	}
	fields := append(querymeter.QueryFieldIDs(&plan), req.GetQueryReadFieldIds()...)
	queryChain, err := chain.PrepareQueryPlanChainExecution(&plan, schema, req.GetIsIterator())
	if err != nil {
		return nil, err
	}
	if queryChain != nil {
		fields = append(fields, queryChain.InputPlan.Inputs[0].SourceFieldID)
	}
	return fields, nil
}

func searchMeterFields(plan *planpb.PlanNode, extraFields []int64, schema *schemapb.CollectionSchema) ([]int64, error) {
	fields := append(querymeter.SearchFieldIDs(plan, false), extraFields...)
	for _, chainPB := range plan.GetQuerynodeFunctionChains() {
		repr, err := chain.ProtoChainToRepr(chainPB)
		if err != nil {
			return nil, err
		}
		input, err := chain.CompileDataFrameInputPlan(repr, schema)
		if err != nil {
			return nil, err
		}
		fields = append(fields, input.PhysicalFieldIDs()...)
	}
	return fields, nil
}

func collectPlanColumns(read *viewpb.PlannedColumnRead, plan *viewpb.QueryPlan, view *viewpb.QueryViewOfShard, growing []snview.GrowingSegmentHandle, partitions []int64, sub int) error {
	if len(read.GetFieldIds()) == 0 {
		return nil
	}
	if err := collectSealedPlanColumns(read, plan, view, partitions, sub); err != nil {
		return err
	}
	for _, handle := range growing {
		if len(partitions) == 0 || slices.Contains(partitions, handle.PartitionID()) {
			if err := addGrowingColumns(read, handle); err != nil {
				return err
			}
		}
	}
	return nil
}

func collectRequeryColumns(pk int64, plan *viewpb.QueryPlan, view *viewpb.QueryViewOfShard, growing []snview.GrowingSegmentHandle) (map[int64]*viewpb.PlannedColumnRead, error) {
	reads := make(map[int64]*viewpb.PlannedColumnRead)
	readFor := func(partitionID int64) *viewpb.PlannedColumnRead {
		if reads[partitionID] == nil {
			reads[partitionID] = newPlannedColumnRead([]int64{pk}, 1)
		}
		return reads[partitionID]
	}
	if err := visitSealedPlanSegments(plan, view, nil, -1, func(partitionID, segmentID int64) error {
		return addPlannedColumns(readFor(partitionID), view.GetStreamingNode().GetSealedFieldBytes()[segmentID])
	}); err != nil {
		return nil, err
	}
	for _, handle := range growing {
		if err := addGrowingColumns(readFor(handle.PartitionID()), handle); err != nil {
			return nil, err
		}
	}
	return reads, nil
}

func addGrowingColumns(read *viewpb.PlannedColumnRead, handle snview.GrowingSegmentHandle) error {
	fields := make([]int64, 0, len(read.GetColumns().GetPayloadBytes()))
	for id := range read.GetColumns().GetPayloadBytes() {
		fields = append(fields, id)
	}
	if len(fields) == 0 {
		return nil
	}
	values := segcore.GrowingFieldMemoryBytes(handle.Segment(), fields)
	memory := &viewpb.FieldLogicalBytes{PayloadBytes: make(map[int64]uint64, len(values))}
	for i, value := range values {
		memory.PayloadBytes[fields[i]] = value
	}
	return addPlannedColumns(read, memory)
}
