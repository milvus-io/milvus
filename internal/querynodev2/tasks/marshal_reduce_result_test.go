/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
)

func TestMarshalReduceResult_Basic(t *testing.T) {
	pool := memory.NewGoAllocator()

	// Build a simple mergeResult with 2 NQs
	// NQ0: 3 results, NQ1: 2 results
	df := buildTestDF(pool,
		[][]int64{{1, 2, 3}, {4, 5}},
		[][]float32{{0.9, 0.8, 0.7}, {0.6, 0.5}},
	)
	defer df.Release()

	result := &mergeResult{
		DF: df,
		Sources: [][]segmentSource{
			{{InputIdx: 0, SegOffset: 10}, {InputIdx: 0, SegOffset: 20}, {InputIdx: 1, SegOffset: 30}},
			{{InputIdx: 0, SegOffset: 40}, {InputIdx: 1, SegOffset: 50}},
		},
	}

	data, err := marshalReduceResult(result)
	require.NoError(t, err)

	// Verify structure
	assert.Equal(t, int64(2), data.NumQueries)
	assert.Equal(t, int64(3), data.TopK) // max of 3, 2
	assert.Equal(t, []int64{3, 2}, data.Topks)

	// Verify IDs
	require.NotNil(t, data.Ids.GetIntId())
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, data.Ids.GetIntId().Data)

	// Verify Scores
	assert.Len(t, data.Scores, 5)
	assert.InDelta(t, float32(0.9), data.Scores[0], 0.001)
	assert.InDelta(t, float32(0.5), data.Scores[4], 0.001)
}

func TestMarshalReduceResult_Empty(t *testing.T) {
	pool := memory.NewGoAllocator()

	df := buildTestDF(pool, [][]int64{{}}, [][]float32{{}})
	defer df.Release()

	result := &mergeResult{
		DF:      df,
		Sources: [][]segmentSource{{}},
	}

	data, err := marshalReduceResult(result)
	require.NoError(t, err)
	assert.Equal(t, int64(1), data.NumQueries)
	assert.Equal(t, int64(0), data.TopK)
	assert.Len(t, data.Scores, 0)
}

func TestMarshalReduceResult_Nil(t *testing.T) {
	_, err := marshalReduceResult(nil)
	assert.Error(t, err)
}

// TestMarshalReduceResult_GroupBy verifies the $group_by column from the merged
// DataFrame is exported to SearchResultData.GroupByFieldValues. The proxy reduces
// across shards by reading this field; dropping it (the bug we just fixed)
// silently breaks group-by queries through the Go-reduce path.
// TestMarshalReduceResult_ElementIndices verifies that the $element_indices
// column is exported as SearchResultData.ElementIndices (LongArray).
func TestMarshalReduceResult_ElementIndices(t *testing.T) {
	pool := memory.NewGoAllocator()

	df := buildTestDFWithElementIndices(pool,
		[][]int64{{1, 2, 3}, {4, 5}},
		[][]float32{{0.9, 0.8, 0.7}, {0.6, 0.5}},
		[][]int32{{10, 20, 30}, {40, 50}})
	defer df.Release()

	result := &mergeResult{
		DF: df,
		Sources: [][]segmentSource{
			{{InputIdx: 0}, {InputIdx: 0}, {InputIdx: 0}},
			{{InputIdx: 0}, {InputIdx: 0}},
		},
	}

	data, err := marshalReduceResult(result)
	require.NoError(t, err)

	// ElementIndices should be populated as a LongArray
	require.NotNil(t, data.ElementIndices, "ElementIndices must be set")
	assert.Equal(t, []int64{10, 20, 30, 40, 50}, data.ElementIndices.Data)

	// $element_indices must NOT be in FieldsData
	for _, fd := range data.FieldsData {
		assert.NotEqual(t, elementIndicesCol, fd.FieldName,
			"$element_indices leaked into FieldsData")
	}
}

// TestMarshalReduceResult_NoElementIndices verifies that when the DataFrame
// has no $element_indices column, ElementIndices is nil in the output proto.
func TestMarshalReduceResult_NoElementIndices(t *testing.T) {
	pool := memory.NewGoAllocator()

	df := buildTestDF(pool, [][]int64{{1, 2}}, [][]float32{{0.9, 0.8}})
	defer df.Release()

	result := &mergeResult{
		DF:      df,
		Sources: [][]segmentSource{{{InputIdx: 0}, {InputIdx: 0}}},
	}

	data, err := marshalReduceResult(result)
	require.NoError(t, err)
	assert.Nil(t, data.ElementIndices,
		"ElementIndices should be nil when input has no $element_indices column")
}

func TestMarshalReduceResult_GroupBy(t *testing.T) {
	pool := memory.NewGoAllocator()

	// 2 NQs, each with two group-by buckets
	df := buildTestDFWithGroupBy(pool,
		[][]int64{{1, 2, 3}, {4, 5}},
		[][]float32{{0.9, 0.8, 0.7}, {0.6, 0.5}},
		[][]int64{{10, 10, 20}, {30, 30}},
		groupByCol)
	defer df.Release()

	result := &mergeResult{
		DF: df,
		Sources: [][]segmentSource{
			{{InputIdx: 0}, {InputIdx: 0}, {InputIdx: 0}},
			{{InputIdx: 0}, {InputIdx: 0}},
		},
	}

	data, err := marshalReduceResult(result)
	require.NoError(t, err)

	require.Len(t, data.GetGroupByFieldValues(), 1, "GroupByFieldValues must be set when group-by exists")
	gbv := data.GetGroupByFieldValues()[0]
	gbInts := gbv.GetScalars().GetLongData().GetData()
	assert.Equal(t, []int64{10, 10, 20, 30, 30}, gbInts)
	// FieldName must be empty so proxy.fillFieldNames can resolve the real
	// schema field name from FieldId. Leaking the internal "$group_by_<fieldID>"
	// column name to the user is wrong.
	assert.Empty(t, gbv.FieldName,
		"FieldName must be empty so proxy auto-fills the real name from FieldId")
	// Sanity: group-by columns must NOT be exported as regular fields.
	for _, fd := range data.FieldsData {
		assert.NotEqual(t, groupByCol, fd.FieldName, "group-by column leaked into FieldsData")
	}
}

func TestMarshalReduceResult_GroupByNullableValidData(t *testing.T) {
	pool := memory.NewGoAllocator()

	gbBuilder := array.NewInt64Builder(pool)
	gbBuilder.AppendValues([]int64{0, 0}, []bool{false, true})
	gbArr := gbBuilder.NewArray()
	gbBuilder.Release()
	defer gbArr.Release()

	df := buildDFWithGroupByArrowArray(t, pool, gbArr, schemapb.DataType_Int64, 105, true)
	defer df.Release()

	data, err := marshalReduceResult(&mergeResult{
		DF:      df,
		Sources: [][]segmentSource{{{InputIdx: 0}, {InputIdx: 0}}},
	})
	require.NoError(t, err)

	require.Len(t, data.GetGroupByFieldValues(), 1)
	gbv := data.GetGroupByFieldValues()[0]
	assert.Equal(t, []int64{0, 0}, gbv.GetScalars().GetLongData().GetData())
	assert.Equal(t, []bool{false, true}, gbv.GetValidData())
	assert.Equal(t, int64(105), gbv.GetFieldId())
}

func TestMarshalReduceResult_GroupByTimestamptz(t *testing.T) {
	pool := memory.NewGoAllocator()

	gbBuilder := array.NewInt64Builder(pool)
	gbBuilder.AppendValues([]int64{1000, 0}, []bool{true, false})
	gbArr := gbBuilder.NewArray()
	gbBuilder.Release()
	defer gbArr.Release()

	df := buildDFWithGroupByArrowArray(t, pool, gbArr, schemapb.DataType_Timestamptz, 107, true)
	defer df.Release()

	data, err := marshalReduceResult(&mergeResult{
		DF:      df,
		Sources: [][]segmentSource{{{InputIdx: 0}, {InputIdx: 0}}},
	})
	require.NoError(t, err)

	require.Len(t, data.GetGroupByFieldValues(), 1)
	gbv := data.GetGroupByFieldValues()[0]
	assert.Equal(t, schemapb.DataType_Timestamptz, gbv.GetType())
	assert.Equal(t, int64(107), gbv.GetFieldId())
	assert.Equal(t, []int64{1000, 0}, gbv.GetScalars().GetTimestamptzData().GetData())
	assert.Equal(t, []bool{true, false}, gbv.GetValidData())
	assert.Empty(t, gbv.GetFieldName())
}

func TestMarshalReduceResult_GroupByGeometry(t *testing.T) {
	pool := memory.NewGoAllocator()

	gbBuilder := array.NewStringBuilder(pool)
	gbBuilder.AppendValues([]string{"wkb\x01", ""}, []bool{true, false})
	gbArr := gbBuilder.NewArray()
	gbBuilder.Release()
	defer gbArr.Release()

	df := buildDFWithGroupByArrowArray(t, pool, gbArr, schemapb.DataType_Geometry, 106, true)
	defer df.Release()

	data, err := marshalReduceResult(&mergeResult{
		DF:      df,
		Sources: [][]segmentSource{{{InputIdx: 0}, {InputIdx: 0}}},
	})
	require.NoError(t, err)

	require.Len(t, data.GetGroupByFieldValues(), 1)
	gbv := data.GetGroupByFieldValues()[0]
	assert.Equal(t, schemapb.DataType_Geometry, gbv.GetType())
	assert.Equal(t, int64(106), gbv.GetFieldId())
	expectedGeometryData := [][]byte{
		[]byte("wkb\x01"),
		{},
	}
	assert.Equal(t, expectedGeometryData, gbv.GetScalars().GetGeometryData().GetData())
	assert.Equal(t, []bool{true, false}, gbv.GetValidData())
	assert.Empty(t, gbv.GetFieldName())
}

func buildDFWithGroupByArrowArray(
	t *testing.T,
	pool memory.Allocator,
	groupByArr arrow.Array,
	groupByType schemapb.DataType,
	groupByFieldID int64,
	groupByNullable bool,
) *chain.DataFrame {
	t.Helper()

	idBuilder := array.NewInt64Builder(pool)
	idBuilder.AppendValues([]int64{1, 2}, nil)
	idArr := idBuilder.NewArray()
	idBuilder.Release()

	scoreBuilder := array.NewFloat32Builder(pool)
	scoreBuilder.AppendValues([]float32{0.9, 0.8}, nil)
	scoreArr := scoreBuilder.NewArray()
	scoreBuilder.Release()

	groupByArr.Retain()

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{2})
	builder.SetFieldType(idFieldName, schemapb.DataType_Int64)
	builder.SetFieldNullable(idFieldName, false)
	require.NoError(t, builder.AddColumnFromChunks(idFieldName, []arrow.Array{idArr}))
	builder.SetFieldType(scoreFieldName, schemapb.DataType_Float)
	builder.SetFieldNullable(scoreFieldName, false)
	require.NoError(t, builder.AddColumnFromChunks(scoreFieldName, []arrow.Array{scoreArr}))
	groupByColName := groupByColumnName(groupByFieldID)
	builder.SetFieldType(groupByColName, groupByType)
	builder.SetFieldID(groupByColName, groupByFieldID)
	builder.SetFieldNullable(groupByColName, groupByNullable)
	require.NoError(t, builder.AddColumnFromChunks(groupByColName, []arrow.Array{groupByArr}))
	return builder.Build()
}
