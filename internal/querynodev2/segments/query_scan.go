// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

import (
	"context"
	"runtime"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type queryScanResult struct {
	identities *segcorepb.RetrieveResults
	details    arrow.Array
	rows       map[int64]int
}

// RunQueryScanPipeline executes independent segment chains, merges matching
// identities, then materializes only final output rows. Caller owns segment pins.
func RunQueryScanPipeline(ctx context.Context, req *querypb.QueryRequest, schema *schemapb.CollectionSchema,
	prepared *chain.QueryChainPlan, results []RetrieveSegmentResult, manager *Manager, plan *segcore.RetrievePlan,
) (*segcorepb.RetrieveResults, *commonpb.ResultData, error) {
	if prepared == nil || prepared.Chain == nil || prepared.InputPlan == nil ||
		len(prepared.InputPlan.Inputs) != 1 || plan == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("QueryScan requires a prepared chain with one resolved input and a retrieve plan")
	}
	if paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64() <= 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("QueryScan requires a positive result byte budget")
	}
	repr, err := chain.ProtoChainToRepr(prepared.Chain)
	if err != nil {
		return nil, nil, err
	}
	fieldID := prepared.InputPlan.Inputs[0].SourceFieldID
	scanned := make([]*queryScanResult, len(results))
	defer func() {
		for _, r := range scanned {
			if r != nil && r.details != nil {
				r.details.Release()
			}
		}
	}()
	group, runCtx := errgroup.WithContext(ctx)
	group.SetLimit(runtime.GOMAXPROCS(0))
	for i := range results {
		i := i
		group.Go(func() error {
			var err error
			scanned[i], err = runQueryScanSegment(runCtx, fieldID, repr, results[i], plan)
			return err
		})
	}
	if err = group.Wait(); err != nil {
		return nil, nil, err
	}
	identities := make([]*segcorepb.RetrieveResults, 0, len(results))
	segs := make([]Segment, 0, len(results))
	valid := make([]*queryScanResult, 0, len(results))
	for i, r := range scanned {
		if typeutil.GetSizeOfIDs(r.identities.GetIds()) == 0 {
			continue
		}
		identities = append(identities, r.identities)
		segs = append(segs, results[i].Segment)
		valid = append(valid, r)
	}
	var merged *segcorepb.RetrieveResults
	var extra *commonpb.ResultData
	if len(identities) == 0 {
		merged, err = emptySegcoreResult(req, schema)
		extra = emptyQueryScanExtra(repr)
	} else {
		mergeOp := NewMergeByPKWithOffsetsOperator(req.GetReq().GetLimit(), reduce.ToReduceType(req.GetReq().GetReduceType()))
		output, mergeErr := mergeOp.Run(ctx, nil, identities)
		if mergeErr != nil {
			return nil, nil, mergeErr
		}
		selected := output[0].(*MergedResultWithOffsets)
		fields := make(map[int64]*schemapb.FieldSchema)
		for _, f := range typeutil.GetAllFieldSchemas(schema) {
			fields[f.GetFieldID()] = f
		}
		fetch := NewFetchFieldsDataOperator(segs, manager, plan, fields)
		output, err = fetch.Run(ctx, nil, selected)
		if err != nil {
			return nil, nil, err
		}
		merged = output[0].(*segcorepb.RetrieveResults)
		extra = emptyQueryScanExtra(repr)
		if extra != nil {
			rows := extra.Fields[repr.Name].GetObjectValue().Fields[repr.OutputColumns[0]].GetListValue()
			for _, selection := range selected.Selections {
				if err := ctx.Err(); err != nil {
					return nil, nil, err
				}
				source := valid[selection.SegmentIndex]
				row, ok := source.rows[selection.Offset]
				if !ok {
					return nil, nil, merr.WrapErrServiceInternalMsg("selected Grep offset has no matching details")
				}
				rows.Values = append(rows.Values, queryScanDetailRow(source.details.(*array.List), row))
			}
		}
	}
	if err != nil {
		return nil, nil, err
	}
	for _, input := range results {
		merged.AllRetrieveCount += input.Result.GetAllRetrieveCount()
		merged.HasMoreResult = merged.HasMoreResult || input.Result.GetHasMoreResult()
		merged.ScannedRemoteBytes += input.Result.GetScannedRemoteBytes()
		merged.ScannedTotalBytes += input.Result.GetScannedTotalBytes()
	}
	maxBytes := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	resultBytes := int64(proto.Size(merged)) + int64(proto.Size(extra))
	if resultBytes > maxBytes {
		return nil, nil, merr.Wrapf(merr.WrapErrParameterTooLarge("query results including Grep details exceed maxOutputSize"),
			"query results including Grep details too large: actual=%d bytes, limit=%d bytes (quotaAndLimits.limits.maxOutputSize)", resultBytes, maxBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return merged, extra, nil
}

func runQueryScanSegment(ctx context.Context, fieldID int64, repr *chain.ChainRepr,
	input RetrieveSegmentResult, plan *segcore.RetrievePlan) (*queryScanResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	local, ok := input.Segment.(*LocalSegment)
	if !ok || local.csegment == nil {
		return nil, merr.WrapErrServiceInternalMsg("QueryScan requires a local segcore segment")
	}
	binding := types.FunctionBuildContext{QueryScan: &types.QueryScanBuildContext{
		FieldID: fieldID, Segment: segcore.NewGrepSegment(local.csegment),
	}}
	fc, err := chain.FuncChainFromReprWithContext(repr, memory.DefaultAllocator, binding)
	if err != nil {
		return nil, err
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues(input.Result.GetOffset(), nil)
	offsets := builder.NewInt64Array()
	builder.Release()
	defer offsets.Release()
	frameBuilder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{int64(offsets.Len())})
	defer frameBuilder.Release()
	if err = frameBuilder.AddColumnFromChunks(types.SegOffsetFieldName, []arrow.Array{offsets}); err != nil {
		return nil, err
	}
	frame := frameBuilder.Build()
	defer frame.Release()
	output, err := fc.ExecuteWithContext(ctx, frame)
	if err != nil {
		return nil, err
	}
	defer output.Release()
	selected := output.Column(types.SegOffsetFieldName).Chunk(0).(*array.Int64).Int64Values()
	identity := &segcorepb.RetrieveResults{}
	if len(selected) > 0 {
		// Identity reads precede the Query limit and may include many more rows.
		// Keep their take policy local; the shared plan governs final outputs.
		takeLimit := paramtable.Get().QueryNodeCfg.TakeForOutputResultCountLimit.GetAsInt64()
		takeAllowed := takeLimit == 0 || int64(len(selected)) <= takeLimit
		nativeResult, readErr := retrySegmentReadGate(ctx, local.segmentType, func() (*segcore.RetrieveResult, error) {
			return segcore.RetrieveIdentitiesByOffsets(ctx, local.csegment, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: selected}, takeAllowed)
		}, waitSegmentReadGateRetry)
		if readErr != nil {
			return nil, readErr
		}
		defer nativeResult.Release()
		identity, err = nativeResult.GetResult()
		if err != nil {
			return nil, err
		}
		if typeutil.GetSizeOfIDs(identity.GetIds()) != len(selected) || len(identity.GetOffset()) != len(selected) {
			return nil, merr.WrapErrServiceInternalMsg("Grep identity rows do not match selected offsets")
		}
	}
	rows := make(map[int64]int, len(selected))
	for i, offset := range selected {
		rows[offset] = i
	}
	// Candidate offsets are not required to be PK ordered. Only identities are
	// sorted; the immutable offset-to-detail map preserves the row association.
	sort.Stable(&byPK{r: identity})
	details := output.Column(repr.Operators[0].Outputs[0]).Chunk(0)
	details.Retain()
	return &queryScanResult{identities: identity, details: details, rows: rows}, nil
}

func resultList(values ...*commonpb.ResultValue) *commonpb.ResultValue {
	return &commonpb.ResultValue{Value: &commonpb.ResultValue_ListValue{ListValue: &commonpb.ResultList{Values: values}}}
}
func resultObject(fields map[string]*commonpb.ResultValue) *commonpb.ResultValue {
	return &commonpb.ResultValue{Value: &commonpb.ResultValue_ObjectValue{ObjectValue: &commonpb.ResultData{Fields: fields}}}
}
func emptyQueryScanExtra(repr *chain.ChainRepr) *commonpb.ResultData {
	if len(repr.OutputColumns) == 0 {
		return nil
	}
	return &commonpb.ResultData{Fields: map[string]*commonpb.ResultValue{repr.Name: resultObject(map[string]*commonpb.ResultValue{repr.OutputColumns[0]: resultList()})}}
}
func queryScanDetailRow(rows *array.List, row int) *commonpb.ResultValue {
	lines := rows.ListValues().(*array.Struct)
	text := lines.Field(0).(*array.String)
	spans := lines.Field(1).(*array.List)
	bounds := spans.ListValues().(*array.Struct)
	starts, ends := bounds.Field(0).(*array.Uint64), bounds.Field(1).(*array.Uint64)
	begin, end := rows.ValueOffsets(row)
	values := make([]*commonpb.ResultValue, 0, end-begin)
	for i := begin; i < end; i++ {
		start, stop := spans.ValueOffsets(int(i))
		matches := make([]*commonpb.ResultValue, 0, stop-start)
		for j := start; j < stop; j++ {
			matches = append(matches, resultObject(map[string]*commonpb.ResultValue{
				"start": {Value: &commonpb.ResultValue_Uint64Value{Uint64Value: starts.Value(int(j))}},
				"end":   {Value: &commonpb.ResultValue_Uint64Value{Uint64Value: ends.Value(int(j))}},
			}))
		}
		values = append(values, resultObject(map[string]*commonpb.ResultValue{"content": {Value: &commonpb.ResultValue_StringValue{StringValue: strings.Clone(text.Value(int(i)))}}, "spans": resultList(matches...)}))
	}
	return resultList(values...)
}
