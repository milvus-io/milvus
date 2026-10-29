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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RunQueryScanPipeline executes independent segment chains, merges matching
// identities, then materializes only final output rows. Caller owns segment pins.
func RunQueryScanPipeline(ctx context.Context, req *querypb.QueryRequest, schema *schemapb.CollectionSchema,
	prepared *chain.QueryChainPlan, results []RetrieveSegmentResult, plan *segcore.RetrievePlan,
) (*segcorepb.RetrieveResults, error) {
	if prepared == nil || prepared.Chain == nil || prepared.InputPlan == nil ||
		len(prepared.InputPlan.Inputs) != 1 || prepared.OutputSchema == nil || plan == nil {
		return nil, merr.WrapErrServiceInternalMsg("QueryScan requires a prepared chain with one resolved input and a retrieve plan")
	}
	repr, err := chain.ProtoChainToRepr(prepared.Chain)
	if err != nil {
		return nil, err
	}
	outputSchema := prepared.OutputSchema
	fieldID := prepared.InputPlan.Inputs[0].SourceFieldID
	scanned := make([]*chain.DataFrame, len(results))
	defer func() {
		for _, r := range scanned {
			if r != nil {
				r.Release()
			}
		}
	}()
	group, runCtx := errgroup.WithContext(ctx)
	group.SetLimit(runtime.GOMAXPROCS(0))
	for i := range results {
		i := i
		group.Go(func() error {
			var err error
			scanned[i], err = runQueryScanSegment(runCtx, fieldID, req.GetReq().GetLimit(), repr, results[i], plan)
			return err
		})
	}
	if err = group.Wait(); err != nil {
		return nil, err
	}
	segs := make([]Segment, 0, len(results))
	valid := make([]*chain.DataFrame, 0, len(results))
	for i, r := range scanned {
		if r.NumRows() == 0 {
			continue
		}
		segs = append(segs, results[i].Segment)
		valid = append(valid, r)
	}
	fields := make(map[int64]*schemapb.FieldSchema)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fields[field.FieldID] = field
	}
	var outputFields []*schemapb.FieldSchema
	for _, id := range req.GetReq().GetOutputFieldsId() {
		if id == common.TimeStampField {
			continue // Exported from the retained $timestamp identity column.
		}
		field, ok := fields[id]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("missing retrieve output field %d", id)
		}
		outputFields = append(outputFields, field)
	}
	var finalFrame *chain.DataFrame
	if len(valid) == 0 {
		finalFrame, err = emptyQueryFrame(schema, outputFields, outputSchema)
	} else {
		selected, mergeErr := mergeQueryScanResults(valid, req.GetReq().GetLimit(), reduce.ToReduceType(req.GetReq().GetReduceType()))
		if mergeErr != nil {
			return nil, mergeErr
		}
		defer selected.Frame.Release()
		if selected.Frame.NumRows() == 0 {
			finalFrame, err = emptyQueryFrame(schema, outputFields, outputSchema)
		} else {
			finalFrame, err = materializeQueryFrame(ctx, segs, plan, selected, outputFields)
		}
	}
	if err != nil {
		return nil, err
	}
	defer finalFrame.Release()
	merged, err := chain.ToRetrieveResults(finalFrame)
	if err != nil {
		return nil, err
	}
	// Ordinary and computed columns share the same output budget.
	var size int64
	for _, field := range merged.FieldsData {
		size += int64(proto.Size(field))
	}
	for _, field := range merged.ComputedResults {
		size += int64(proto.Size(field))
	}
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	if size > maxOutputSize {
		return nil, merr.WrapErrParameterInvalidMsg("query results exceed the maxOutputSize Limit %d", maxOutputSize)
	}
	for _, input := range results {
		merged.AllRetrieveCount += input.Result.GetAllRetrieveCount()
		merged.HasMoreResult = merged.HasMoreResult || input.Result.GetHasMoreResult()
		merged.ScannedRemoteBytes += input.Result.GetScannedRemoteBytes()
		merged.ScannedTotalBytes += input.Result.GetScannedTotalBytes()
	}
	return merged, nil
}

// emptyQueryFrame preserves the complete export schema even when no segment
// produced rows. It uses the same column names and physical types as a populated DF.
func emptyQueryFrame(schema *schemapb.CollectionSchema, outputs []*schemapb.FieldSchema, computed *arrow.Schema) (*chain.DataFrame, error) {
	var pk *schemapb.FieldSchema
	for _, field := range schema.GetFields() {
		if field.IsPrimaryKey {
			pk = field
			break
		}
	}
	if pk == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing QueryScan primary field schema")
	}
	builder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{0})
	defer builder.Release()
	add := func(name string, field *schemapb.FieldSchema) error {
		dtype, err := arrowconv.NativeColumnType(field)
		if err != nil {
			return err
		}
		builder.SetFieldSchema(name, field)
		return builder.AddColumns([]string{name}, []*arrow.Chunked{arrow.NewChunked(dtype, nil)})
	}
	if err := add(types.IDFieldName, pk); err != nil {
		return nil, err
	}
	if err := add(types.TimestampFieldName, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}); err != nil {
		return nil, err
	}
	for _, field := range computed.Fields() {
		if err := builder.SetComputedField(field); err != nil {
			return nil, err
		}
		if err := builder.AddColumns([]string{field.Name}, []*arrow.Chunked{arrow.NewChunked(field.Type, nil)}); err != nil {
			return nil, err
		}
	}
	for _, field := range outputs {
		if err := add(field.Name, field); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}

// queryScanGrepSegment runs each native attempt on the shared SQ pool.
// Read-gate backoff stays outside the pool and does not rerun the chain.
// The query task keeps the underlying segment pinned across attempts.
type queryScanGrepSegment struct {
	native.Segment
	segmentType SegmentType
}

func (s *queryScanGrepSegment) ScanGrep(ctx context.Context, offsets *array.Int64, req native.ScanRequest) (*native.ScanResult, error) {
	return retrySegmentReadGate(ctx, s.segmentType, func() (*native.ScanResult, error) {
		var result *native.ScanResult
		_, err := GetSQPool().Submit(func() (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var err error
			result, err = s.Segment.ScanGrep(ctx, offsets, req)
			return nil, err
		}).Await()
		if err != nil {
			// ScanResult owns its buffers even on failure. Release them before
			// retrying, including when cancellation interrupts the backoff.
			result.Release()
			return nil, err
		}
		return result, nil
	}, waitSegmentReadGateRetry)
}

func runQueryScanSegment(ctx context.Context, fieldID, queryLimit int64, repr *chain.ChainRepr,
	input RetrieveSegmentResult, plan *segcore.RetrievePlan,
) (*chain.DataFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	local, ok := input.Segment.(*LocalSegment)
	if !ok || local.csegment == nil {
		return nil, merr.WrapErrServiceInternalMsg("QueryScan requires a local segcore segment")
	}
	binding := types.FunctionBuildContext{QueryScan: &types.QueryScanBuildContext{
		FieldID: fieldID, QueryLimit: queryLimit, Segment: &queryScanGrepSegment{
			Segment: segcore.NewGrepSegment(local.csegment), segmentType: local.segmentType,
		},
	}}
	fc, err := chain.FuncChainFromReprWithContext(repr, memory.DefaultAllocator, binding)
	if err != nil {
		return nil, err
	}
	candidateOffsets, err := queryScanColumn(input.Record, types.SegOffsetFieldName)
	if err != nil {
		return nil, err
	}
	frameBuilder := chain.NewDataFrameBuilder().SetChunkSizes([]int64{int64(candidateOffsets.Len())})
	defer frameBuilder.Release()
	candidateOffsets.Retain() // The builder consumes its reference; input.Record still owns one.
	if err := frameBuilder.AddColumnFromChunks(types.SegOffsetFieldName, []arrow.Array{candidateOffsets}); err != nil {
		return nil, err
	}
	frame := frameBuilder.Build()
	defer frame.Release()
	output, err := fc.ExecuteWithOptions(ctx, chain.ExecuteOptions{
		EnableColumnPruning: true,
		Downstream:          chain.DownstreamSpec{RequiredColumns: repr.OutputColumns},
		SystemColumnPolicy:  chain.SystemColumnPolicy{KeepAllSystemColumns: true},
	}, frame)
	if err != nil {
		return nil, err
	}
	defer func() {
		if output != nil {
			output.Release()
		}
	}()
	offsets, err := queryFrameInt64Column(output, types.SegOffsetFieldName)
	if err != nil {
		return nil, err
	}
	if offsets.Len() == 0 {
		result := output
		output = nil
		return result, nil
	}
	selected := offsets.Int64Values()
	takeLimit := paramtable.Get().QueryNodeCfg.TakeForOutputResultCountLimit.GetAsInt64()
	takeAllowed := takeLimit == 0 || int64(len(selected)) <= takeLimit
	nativeResult, err := retrySegmentReadGate(ctx, local.segmentType, func() (*segcore.RetrieveArrowResult, error) {
		return segcore.RetrieveIdentitiesByOffsets(ctx, local.csegment, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: selected}, takeAllowed)
	}, waitSegmentReadGateRetry)
	if err != nil {
		return nil, err
	}
	defer nativeResult.Release()
	_, identities, err := nativeResult.GetResult()
	if err != nil {
		return nil, err
	}
	defer identities.Release()
	return withQueryIdentities(output, identities)
}

// withQueryIdentities borrows both inputs. Only retained column references are
// added to the returned frame; no payload buffers are copied.
func withQueryIdentities(output *chain.DataFrame, identities arrow.Record) (*chain.DataFrame, error) {
	selected, err := queryFrameInt64Column(output, types.SegOffsetFieldName)
	if err != nil {
		return nil, err
	}
	col, err := queryScanColumn(identities, types.SegOffsetFieldName)
	if err != nil {
		return nil, err
	}
	offsets, ok := col.(*array.Int64)
	if !ok || offsets.Len() != selected.Len() {
		return nil, merr.WrapErrServiceInternalMsg("query identity offsets do not match selected rows")
	}
	for i, offset := range selected.Int64Values() {
		if offsets.Value(i) != offset {
			return nil, merr.WrapErrServiceInternalMsg("query identity offsets changed row order")
		}
	}
	builder := chain.NewDataFrameBuilder().SetChunkSizes(output.ChunkSizes())
	defer builder.Release()
	for _, name := range output.ColumnNames() {
		if err := builder.AddColumnFrom(output, name); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{types.IDFieldName, types.TimestampFieldName} {
		column, err := queryScanColumn(identities, name)
		if err != nil {
			return nil, err
		}
		column.Retain()
		if err := builder.AddColumnFromChunks(name, []arrow.Array{column}); err != nil {
			return nil, err
		}
	}
	builder.CopyAllMetadata(output)
	return builder.Build(), nil
}

// materializeQueryFrame attaches the late-read ordinary fields without copying
// their Arrow buffers. Source segment indices and offsets are reducer metadata.
// The caller handles empty results before materialization.
func materializeQueryFrame(ctx context.Context, segments []Segment, plan *segcore.RetrievePlan, selected *queryScanMergeResult, outputFields []*schemapb.FieldSchema) (*chain.DataFrame, error) {
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	if maxOutputSize < 0 {
		return nil, merr.WrapErrServiceInternalMsg("negative QueryScan output budget")
	}
	var computedSize int64
	for _, name := range selected.Frame.ColumnNames() {
		if chain.IsFunctionChainSystemName(name) || !selected.Frame.IsComputedColumn(name) {
			continue
		}
		for _, chunk := range selected.Frame.Column(name).Chunks() {
			computedSize += int64(chunk.Data().SizeInBytes())
		}
	}
	if computedSize > maxOutputSize {
		return nil, merr.WrapErrParameterInvalidMsg("computed query results exceed the maxOutputSize Limit %d", maxOutputSize)
	}
	builder := chain.NewDataFrameBuilder().SetChunkSizes(selected.Frame.ChunkSizes())
	defer builder.Release()
	for _, name := range selected.Frame.ColumnNames() {
		if err := builder.AddColumnFrom(selected.Frame, name); err != nil {
			return nil, err
		}
	}
	builder.CopyAllMetadata(selected.Frame)
	record, err := fetchFieldsAsRecord(ctx, segments, plan, &MergedResultWithOffsets{Selections: selected.Selections}, maxOutputSize-computedSize)
	if err != nil {
		return nil, err
	}
	defer record.Release()
	if record.NumRows() != selected.Frame.NumRows() {
		return nil, merr.WrapErrServiceInternalMsg("materialized query fields do not align with selected rows")
	}
	definitions := make(map[string]*schemapb.FieldSchema, len(outputFields))
	for _, field := range outputFields {
		definitions[field.Name] = field
	}
	for i, field := range record.Schema().Fields() {
		// Identities already carry the winning row's timestamp under its chain
		// system name. Do not attach a second column using the native name.
		if field.Name == common.TimeStampFieldName {
			continue
		}
		definition := definitions[field.Name]
		if definition == nil {
			return nil, merr.WrapErrServiceInternalMsg("missing materialized field definition %s", field.Name)
		}
		builder.SetFieldSchema(field.Name, definition)
		col := record.Column(i)
		col.Retain()
		if err := builder.AddColumnFromChunks(field.Name, []arrow.Array{col}); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}
