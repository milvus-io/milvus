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

package transform

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func testTable(t *testing.T, chunks ...[]int64) arrow.Table {
	t.Helper()
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { pool.AssertSize(t, 0) })
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := make([]arrow.Record, len(chunks))
	for i, values := range chunks {
		builder := array.NewInt64Builder(pool)
		builder.AppendValues(values, nil)
		column := builder.NewArray()
		builder.Release()
		records[i] = array.NewRecord(schema, []arrow.Array{column}, int64(len(values)))
		column.Release()
	}
	table := array.NewTableFromRecords(schema, records)
	for _, record := range records {
		record.Release()
	}
	t.Cleanup(table.Release)
	return table
}

func source(group int, row int64) RowLineage {
	return RowLineage{{Group: group, Row: row}}
}

func TestPostTablePermitsCrossQuerySourcesAndEmptyAggregate(t *testing.T) {
	plan, err := PostTable("summary", &schemapb.FunctionChainOp{Op: "concat"})
	require.NoError(t, err)
	input := Batch{Groups: []arrow.Table{testTable(t, []int64{1, 2}), testTable(t, []int64{3})}}
	result := Result{Data: Batch{Groups: []arrow.Table{testTable(t, []int64{6})}}, Lineage: [][]RowLineage{{{{Group: 0, Row: 0}, {Group: 0, Row: 1}, {Group: 1, Row: 0}}}}}
	require.NoError(t, ValidateResultForPlan(plan, input, result))
	require.ErrorIs(t, ValidateResult(plan.Stage, input, result), merr.ErrFunctionFailed)
	result.Lineage[0][0][2].Row = 1
	require.ErrorIs(t, ValidateResultForPlan(plan, input, result), merr.ErrFunctionFailed)
	input = Batch{Groups: []arrow.Table{testTable(t, []int64{})}}
	result.Lineage = [][]RowLineage{{nil}}
	require.NoError(t, ValidateResultForPlan(plan, input, result))
}

func TestExpansionAndLogicalGroups(t *testing.T) {
	// Arrow chunking does not create a logical query/request boundary.
	input := Batch{Groups: []arrow.Table{testTable(t, []int64{10}, []int64{20}), testTable(t, []int64{30})}}
	result := Result{
		Data: Batch{Groups: []arrow.Table{testTable(t, []int64{11, 12, 21}), testTable(t, []int64{})}},
		Lineage: [][]RowLineage{
			{source(0, 0), source(0, 0), source(0, 1)},
			{},
		},
	}
	require.NoError(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, input, result))
	for _, stage := range []schemapb.FunctionChainStage{
		schemapb.FunctionChainStage_FunctionChainStagePreProcess,
		schemapb.FunctionChainStage_FunctionChainStagePostProcess,
	} {
		err := ValidateResult(stage, input, result)
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	}
	result.Data.Groups = result.Data.Groups[:1]
	result.Lineage = result.Lineage[:1]
	require.ErrorIs(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, input, result), merr.ErrFunctionFailed)
}

func TestPreservedIdentityAndOrder(t *testing.T) {
	input := Batch{Groups: []arrow.Table{testTable(t, []int64{1, 2}), testTable(t, []int64{3})}}
	result := Result{
		Data:    input,
		Lineage: [][]RowLineage{{source(0, 0), source(0, 1)}, {source(1, 0)}},
	}
	for _, stage := range []schemapb.FunctionChainStage{
		schemapb.FunctionChainStage_FunctionChainStagePreProcess,
		schemapb.FunctionChainStage_FunctionChainStagePostProcess,
	} {
		require.NoError(t, ValidateResult(stage, input, result))
	}
	result.Lineage[0] = []RowLineage{source(0, 1), source(0, 0)}
	require.ErrorIs(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStagePreProcess, input, result), merr.ErrFunctionFailed)
	require.ErrorIs(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, input, result), merr.ErrFunctionFailed, "ingestion preserves parent row order")
	result.Lineage[0][0] = source(1, 0)
	require.NoError(t, ValidateLineage(input, result), "source exists, but crosses a logical group boundary")
	require.ErrorIs(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, input, result), merr.ErrFunctionFailed)
}

func TestManySourceRepresentationIsReserved(t *testing.T) {
	input := Batch{Groups: []arrow.Table{testTable(t, []int64{1, 2, 3})}}
	result := Result{
		Data:    Batch{Groups: []arrow.Table{testTable(t, []int64{6})}},
		Lineage: [][]RowLineage{{{{Group: 0, Row: 0}, {Group: 0, Row: 1}, {Group: 0, Row: 2}}}},
	}
	require.NoError(t, ValidateLineage(input, result), "future aggregate can describe all contributors")
	require.ErrorIs(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, input, result), merr.ErrFunctionFailed, "v1 does not execute aggregation")
}

func TestInvalidLineage(t *testing.T) {
	input := Batch{Groups: []arrow.Table{testTable(t, []int64{1, 2})}}
	output := Batch{Groups: []arrow.Table{testTable(t, []int64{10})}}
	tests := []struct {
		name    string
		lineage [][]RowLineage
	}{
		{"missing group", nil},
		{"extra group", [][]RowLineage{{source(0, 0)}, {}}},
		{"missing row", [][]RowLineage{{}}},
		{"extra row", [][]RowLineage{{source(0, 0), source(0, 1)}}},
		{"no source", [][]RowLineage{{nil}}},
		{"negative group", [][]RowLineage{{source(-1, 0)}}},
		{"group out of bounds", [][]RowLineage{{source(1, 0)}}},
		{"negative row", [][]RowLineage{{source(0, -1)}}},
		{"row out of bounds", [][]RowLineage{{source(0, 2)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateLineage(input, Result{Data: output, Lineage: test.lineage})
			require.ErrorIs(t, err, merr.ErrFunctionFailed)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
	require.ErrorIs(t, ValidateLineage(Batch{Groups: []arrow.Table{nil}}, Result{}), merr.ErrServiceInternal)
	var typedNil *invalidShapeTable
	require.ErrorIs(t, ValidateLineage(Batch{Groups: []arrow.Table{typedNil}}, Result{}), merr.ErrServiceInternal)
	require.ErrorIs(t, ValidateLineage(Batch{Groups: []arrow.Table{invalidShapeTable{Table: input.Groups[0], rows: 999}}}, Result{}), merr.ErrServiceInternal)
	require.ErrorIs(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageL2Rerank, input, Result{}), merr.ErrParameterInvalid)
}

type invalidShapeTable struct {
	arrow.Table
	rows int64
}

func (table invalidShapeTable) NumRows() int64 { return table.rows }

func TestEmptyGroups(t *testing.T) {
	input := Batch{Groups: []arrow.Table{testTable(t, []int64{})}}
	result := Result{Data: Batch{Groups: append([]arrow.Table(nil), input.Groups...)}, Lineage: [][]RowLineage{{}}}
	require.NoError(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStagePostProcess, input, result))
	require.NoError(t, ValidateResult(schemapb.FunctionChainStage_FunctionChainStageIngestion, Batch{}, Result{}))
	result.Data.Groups[0] = testTable(t, []int64{1})
	result.Lineage[0] = []RowLineage{source(0, 0)}
	require.ErrorIs(t, ValidateLineage(input, result), merr.ErrFunctionFailed, "no source row exists in an empty input")
}
