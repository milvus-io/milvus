// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const l1SourceIndexColumn = "$l1_source_index"

var fillL1FieldsOrdered = segcore.FillFieldsOrderedAsArrowRecordBatch

func validateL1FunctionChain(repr *chain.ChainRepr) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Operators {
		switch op.Type {
		case chaintypes.OpTypeMap:
			fn, err := chain.FunctionFromReprWithContext(op.Function, chaintypes.FunctionBuildContext{})
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("op[%d]: %v", opIdx, err)
			}
			if len(op.Inputs) == 0 {
				return merr.WrapErrParameterInvalidMsg("op[%d]: map operator requires inputs", opIdx)
			}
			if len(op.Outputs) == 0 {
				return merr.WrapErrParameterInvalidMsg("op[%d]: map operator requires outputs", opIdx)
			}
			outputTypes := fn.OutputDataTypes()
			if outputTypes != nil && len(op.Outputs) != len(outputTypes) {
				return merr.WrapErrParameterInvalidMsg("op[%d]: map output columns count %d does not match function output count %d", opIdx, len(op.Outputs), len(outputTypes))
			}
			if !fn.IsRunnable(chaintypes.StageL1Rerank) {
				return merr.WrapErrParameterInvalidMsg("op[%d] function %q does not support stage %q", opIdx, fn.Name(), chaintypes.StageL1Rerank)
			}
		case chaintypes.OpTypeSort:
			if op.Function != nil || len(op.Outputs) > 0 {
				return merr.WrapErrParameterInvalidMsg("op[%d] sort does not accept expression or outputs", opIdx)
			}
			if _, err := chain.NewSortOpFromRepr(&op); err != nil {
				return merr.WrapErrParameterInvalidMsg("op[%d]: %v", opIdx, err)
			}
		case chaintypes.OpTypeLimit:
			if op.Function != nil || len(op.Inputs) > 0 || len(op.Outputs) > 0 {
				return merr.WrapErrParameterInvalidMsg("op[%d] limit does not accept expression, inputs, or outputs", opIdx)
			}
			if _, err := chain.NewLimitOpFromRepr(&op); err != nil {
				return merr.WrapErrParameterInvalidMsg("op[%d]: %v", opIdx, err)
			}
		default:
			return merr.WrapErrParameterInvalidMsg("op[%d] type %q is not supported by L1 rerank function chain", opIdx, op.Type)
		}
	}
	return validateQueryNodeFunctionChainSystemOutputs(repr, "L1")
}

func (t *SearchTask) applyL1Rerank(reduced *mergeResult, results []*segments.SearchResult, plan *segcore.SearchPlan, prepared *preparedL1FunctionChain) (result *mergeResult, retErr error) {
	if prepared == nil || prepared.chain == nil {
		return nil, merr.WrapErrServiceInternalMsg("l1_rerank: prepared L1 function chain is nil")
	}

	start := time.Now()
	defer func() {
		status := metrics.SuccessLabel
		if retErr != nil {
			status = metrics.FailLabel
		}
		metrics.QueryNodeFunctionChainLatency.WithLabelValues(
			fmt.Sprint(t.GetNodeID()),
			metrics.FunctionChainLevelL1,
			status,
		).Observe(float64(time.Since(start).Microseconds()) / 1000.0)
	}()

	if err := validateL1MergeResult(reduced); err != nil {
		return nil, err
	}

	input, err := buildL1InputDataFrame(t.ctx, defaultAllocator, reduced, results, plan, prepared.inputFieldIDs)
	if err != nil {
		return nil, err
	}
	defer input.Release()

	userChain, err := chain.FuncChainFromReprWithContext(prepared.chain, defaultAllocator, chaintypes.FunctionBuildContext{})
	if err != nil {
		return nil, merr.Wrap(err, "l1_rerank: build function chain")
	}

	userResult, err := userChain.ExecuteWithOptions(t.ctx, chain.ExecuteOptions{
		EnableColumnPruning: true,
		SystemColumnPolicy:  chain.SystemColumnPolicy{KeepAllSystemColumns: true},
	}, input)
	if err != nil {
		return nil, merr.Wrap(err, "l1_rerank: execute function chain")
	}
	if userResult != input {
		defer userResult.Release()
	}

	// Restore the reduce ordering contract after the user chain: score DESC,
	// with PK ASC as the deterministic tie-breaker.
	normalize := chain.NewFuncChainWithAllocator(defaultAllocator).
		SetStage(chaintypes.StageL1Rerank).
		Sort(chaintypes.ScoreFieldName, true, chaintypes.IDFieldName)
	reranked, err := normalize.ExecuteWithContext(t.ctx, userResult)
	if err != nil {
		return nil, merr.Wrap(err, "l1_rerank: normalize reduce order")
	}
	if reranked != userResult {
		defer reranked.Release()
	}

	sources, err := rebuildL1Sources(reduced.Sources, reranked)
	if err != nil {
		return nil, err
	}
	finalDF, err := stripL1InternalColumns(reranked)
	if err != nil {
		return nil, err
	}
	return &mergeResult{DF: finalDF, Sources: sources}, nil
}

func validateL1MergeResult(reduced *mergeResult) error {
	if reduced == nil {
		return merr.WrapErrServiceInternal("l1_rerank: merge result is nil")
	}
	if reduced.DF == nil {
		return merr.WrapErrServiceInternal("l1_rerank: merged DataFrame is nil")
	}
	if reduced.DF.NumChunks() != len(reduced.Sources) {
		return merr.WrapErrServiceInternalMsg("l1_rerank: DataFrame chunks %d does not match source chunks %d", reduced.DF.NumChunks(), len(reduced.Sources))
	}
	chunkSizes := reduced.DF.ChunkSizes()
	for i, size := range chunkSizes {
		if int64(len(reduced.Sources[i])) != size {
			return merr.WrapErrServiceInternalMsg("l1_rerank: chunk %d has %d rows but %d sources", i, size, len(reduced.Sources[i]))
		}
	}
	if reduced.DF.HasColumn(l1SourceIndexColumn) {
		return merr.WrapErrServiceInternalMsg("l1_rerank: reserved column %q already exists", l1SourceIndexColumn)
	}
	return nil
}

func buildL1InputDataFrame(
	ctx context.Context,
	pool memory.Allocator,
	reduced *mergeResult,
	results []*segments.SearchResult,
	plan *segcore.SearchPlan,
	inputFieldIDs []int64,
) (*chain.DataFrame, error) {
	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(reduced.DF.ChunkSizes())
	builder.CopyAllMetadata(reduced.DF)

	for _, name := range reduced.DF.ColumnNames() {
		if err := builder.AddColumnFrom(reduced.DF, name); err != nil {
			return nil, err
		}
	}

	if len(inputFieldIDs) > 0 {
		segIndices, segOffsets := flattenL1Sources(reduced.Sources)
		record, err := fillL1FieldsOrdered(
			ctx,
			results,
			plan,
			inputFieldIDs,
			segIndices,
			segOffsets,
		)
		if err != nil {
			return nil, merr.Wrap(err, "l1_rerank: materialize input fields")
		}
		defer record.Release()

		fields, err := dataFrameFromArrowRecordBatch(record, reduced.DF.ChunkSizes())
		if err != nil {
			return nil, merr.Wrap(err, "l1_rerank: build input fields dataframe")
		}
		defer fields.Release()
		if len(fields.ColumnNames()) != len(inputFieldIDs) {
			return nil, merr.WrapErrServiceInternalMsg("l1_rerank: materialized %d input fields, expected %d", len(fields.ColumnNames()), len(inputFieldIDs))
		}
		materializedFieldIDs := make(map[int64]struct{}, len(inputFieldIDs))
		for _, name := range fields.ColumnNames() {
			if reduced.DF.HasColumn(name) {
				return nil, merr.WrapErrServiceInternalMsg("l1_rerank: materialized field %q conflicts with reduced dataframe column", name)
			}
			fieldID, ok := fields.FieldID(name)
			if !ok {
				return nil, merr.WrapErrServiceInternalMsg("l1_rerank: materialized field %q is missing field id metadata", name)
			}
			materializedFieldIDs[fieldID] = struct{}{}
			if err := builder.AddColumnFrom(fields, name); err != nil {
				return nil, err
			}
		}
		for _, fieldID := range inputFieldIDs {
			if _, ok := materializedFieldIDs[fieldID]; !ok {
				return nil, merr.WrapErrServiceInternalMsg("l1_rerank: materialized input is missing field id %d", fieldID)
			}
		}
	}

	tokenChunks := make([]arrow.Array, len(reduced.Sources))
	for chunkIdx, sources := range reduced.Sources {
		b := array.NewInt64Builder(pool)
		for rowIdx := range sources {
			b.Append(int64(rowIdx))
		}
		tokenChunks[chunkIdx] = b.NewArray()
		b.Release()
	}
	if err := builder.AddColumnFromChunks(l1SourceIndexColumn, tokenChunks); err != nil {
		return nil, err
	}
	return builder.Build(), nil
}

func flattenL1Sources(sources [][]segmentSource) ([]int32, []int64) {
	totalRows := 0
	for _, chunk := range sources {
		totalRows += len(chunk)
	}
	segIndices := make([]int32, 0, totalRows)
	segOffsets := make([]int64, 0, totalRows)
	for _, chunk := range sources {
		for _, source := range chunk {
			segIndices = append(segIndices, int32(source.InputIdx))
			segOffsets = append(segOffsets, source.SegOffset)
		}
	}
	return segIndices, segOffsets
}

func rebuildL1Sources(original [][]segmentSource, reranked *chain.DataFrame) ([][]segmentSource, error) {
	tokens := reranked.Column(l1SourceIndexColumn)
	if tokens == nil {
		return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance column %q is missing", l1SourceIndexColumn)
	}
	if tokens.DataType().ID() != arrow.INT64 {
		return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance column has type %s, expected int64", tokens.DataType())
	}
	if len(tokens.Chunks()) != len(original) || reranked.NumChunks() != len(original) {
		return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance chunks do not match source chunks")
	}

	result := make([][]segmentSource, len(original))
	chunkSizes := reranked.ChunkSizes()
	for chunkIdx, sourceChunk := range original {
		arr, ok := tokens.Chunk(chunkIdx).(*array.Int64)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance chunk %d is not int64", chunkIdx)
		}
		if int64(arr.Len()) != chunkSizes[chunkIdx] {
			return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance chunk %d has %d rows, expected %d", chunkIdx, arr.Len(), chunkSizes[chunkIdx])
		}
		result[chunkIdx] = make([]segmentSource, arr.Len())
		for row := 0; row < arr.Len(); row++ {
			if arr.IsNull(row) {
				return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance token is null at chunk %d row %d", chunkIdx, row)
			}
			idx := arr.Value(row)
			if idx < 0 || idx >= int64(len(sourceChunk)) {
				return nil, merr.WrapErrServiceInternalMsg("l1_rerank: provenance token %d out of range at chunk %d row %d", idx, chunkIdx, row)
			}
			result[chunkIdx][row] = sourceChunk[int(idx)]
		}
	}
	return result, nil
}

func stripL1InternalColumns(df *chain.DataFrame) (*chain.DataFrame, error) {
	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(df.ChunkSizes())
	builder.CopyAllMetadata(df)
	for _, name := range df.ColumnNames() {
		if name == l1SourceIndexColumn || !isL1DownstreamColumn(name) {
			continue
		}
		if err := builder.AddColumnFrom(df, name); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}

func isL1DownstreamColumn(name string) bool {
	return name == chaintypes.IDFieldName ||
		name == chaintypes.ScoreFieldName ||
		name == chaintypes.SegOffsetFieldName ||
		isReduceOutputColumn(name)
}

func isReduceOutputColumn(name string) bool {
	return name == elementIndicesCol || isGroupByColumnName(name)
}
