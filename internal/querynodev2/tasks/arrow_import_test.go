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
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestDataFrameFromArrowRecordBatch_PreservesFieldMetadataAndNullability(t *testing.T) {
	pool := memory.NewGoAllocator()

	gbBuilder := array.NewInt64Builder(pool)
	gbBuilder.AppendValues([]int64{0, 42}, []bool{false, true})
	gbArr := gbBuilder.NewArray()
	gbBuilder.Release()
	defer gbArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     groupByCol,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata(
				[]string{arrowMetadataFieldIDKey, arrowMetadataDataTypeKey},
				[]string{"105", strconv.Itoa(int(schemapb.DataType_Timestamptz))},
			),
		},
	}, nil)

	rec := array.NewRecord(schema, []arrow.Array{gbArr}, int64(gbArr.Len()))
	defer rec.Release()
	df, err := dataFrameFromArrowRecordBatch(rec, []int64{int64(gbArr.Len())})
	require.NoError(t, err)
	defer df.Release()

	fieldID, ok := df.FieldID(groupByCol)
	require.True(t, ok)
	assert.Equal(t, int64(105), fieldID)

	fieldType, ok := df.FieldType(groupByCol)
	require.True(t, ok)
	assert.Equal(t, schemapb.DataType_Timestamptz, fieldType)

	fields := df.Schema().Fields()
	require.Len(t, fields, 1)
	assert.True(t, fields[0].Nullable, "array nulls must mark the DataFrame field nullable")
}

func TestDataFrameFromArrowRecordBatch_SplitsLogicalChunks(t *testing.T) {
	pool := memory.NewGoAllocator()

	builder := array.NewInt64Builder(pool)
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, []bool{true, true, false, true, true})
	arr := builder.NewArray()
	builder.Release()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     groupByCol,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata(
				[]string{arrowMetadataFieldIDKey, arrowMetadataDataTypeKey},
				[]string{"105", strconv.Itoa(int(schemapb.DataType_Timestamptz))},
			),
		},
	}, nil)

	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()
	df, err := dataFrameFromArrowRecordBatch(rec, []int64{2, 0, 3})
	require.NoError(t, err)
	defer df.Release()
	assert.Equal(t, []int64{2, 0, 3}, df.ChunkSizes())

	col := df.Column(groupByCol)
	require.NotNil(t, col)
	require.Equal(t, 5, col.Len())
	assert.Equal(t, 2, col.Chunk(0).Len())
	assert.Equal(t, 0, col.Chunk(1).Len())
	assert.Equal(t, 3, col.Chunk(2).Len())

	fields := df.Schema().Fields()
	require.Len(t, fields, 1)
	assert.True(t, fields[0].Nullable, "array nulls must mark the DataFrame field nullable")
}

func TestDataFrameFromArrowRecordBatch_ReleasesRetainedChunksOnMetadataError(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	builder := array.NewInt64Builder(pool)
	builder.AppendValues([]int64{1, 2}, nil)
	arr := builder.NewArray()
	builder.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name: groupByCol,
			Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.NewMetadata(
				[]string{arrowMetadataFieldIDKey},
				[]string{"not-an-int64"},
			),
		},
	}, nil)

	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	arr.Release()

	_, err := dataFrameFromArrowRecordBatch(rec, []int64{2})
	require.Error(t, err)

	rec.Release()
}
